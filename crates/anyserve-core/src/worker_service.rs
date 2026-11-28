//! gRPC adapter for workers: one control `Session` per worker and one data
//! `Attempt` stream per lease. State transitions belong to the runtime.

// Protocol errors are `tonic::Status` values handed straight to the stream.
#![allow(clippy::result_large_err)]

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyserve_proto::v2::{self as pb, worker_server::Worker};
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::auth::{
    AuthPolicy, DEFAULT_NAMESPACE, NAMESPACE_HEADER, Principal, Role, validate_namespace,
};
use crate::error::ErrorKind;
use crate::runtime::Runtime;
use crate::runtime::api::{ErrorInfo, Limits, Metadata, Object, codes};
use crate::runtime::events::{ByteBudget, Permit, budgeted_bytes};
use crate::runtime::input::InputStatus;
use crate::runtime::state::FinishStatus;
use crate::runtime::worker::{AttemptLink, AttemptOutcome, InputPlan, WorkerOpenError};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

pub struct WorkerGrpcService {
    runtime: Runtime,
    default_auth: AuthPolicy,
}

impl WorkerGrpcService {
    pub fn new(runtime: Runtime) -> Self {
        Self {
            runtime,
            default_auth: AuthPolicy::default(),
        }
    }

    /// The tonic server with message limits matching the runtime's budgets.
    pub fn server(runtime: Runtime) -> pb::worker_server::WorkerServer<Self> {
        pb::worker_server::WorkerServer::new(Self::new(runtime))
            .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
    }
}

fn require_worker<T>(request: &Request<T>) -> Result<(), Status> {
    match request.extensions().get::<Role>() {
        Some(Role::Client) => Err(Status::permission_denied(
            "client credentials cannot open worker streams",
        )),
        _ => Ok(()),
    }
}

pub(crate) fn down(msg: pb::attempt_down::Msg) -> pb::AttemptDown {
    pb::AttemptDown { msg: Some(msg) }
}

pub(crate) fn finished(
    status: pb::attempt_finished::Status,
    reason: impl Into<String>,
) -> pb::AttemptDown {
    down(pb::attempt_down::Msg::Finished(pb::AttemptFinished {
        status: status as i32,
        reason: reason.into(),
    }))
}

pub(crate) fn finish_to_proto(status: FinishStatus) -> pb::AttemptDown {
    match status {
        FinishStatus::Committed => finished(pb::attempt_finished::Status::Committed, ""),
        FinishStatus::Fenced(reason) => finished(pb::attempt_finished::Status::Fenced, reason),
        FinishStatus::Rejected(reason) => finished(pb::attempt_finished::Status::Rejected, reason),
    }
}

fn worker_principal<T>(request: &Request<T>) -> Result<Principal, Status> {
    require_worker(request)?;
    if let Some(principal) = request.extensions().get::<Principal>() {
        return Ok(principal.clone());
    }
    let namespace = match request
        .metadata()
        .get(NAMESPACE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(ns) => validate_namespace(ns)?.to_string(),
        None => DEFAULT_NAMESPACE.to_string(),
    };
    Ok(Principal {
        namespace,
        role: Role::Worker,
        subject: "anonymous".to_string(),
        forced_pool: None,
        is_operator: false,
        jti: None,
        exp: None,
    })
}

#[tonic::async_trait]
impl Worker for WorkerGrpcService {
    type SessionStream = ResponseStream<pb::ServerMessage>;
    type AttemptStream = ResponseStream<pb::AttemptDown>;

    async fn session(
        &self,
        request: Request<Streaming<pb::WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        self.runtime.ensure_active_leader()?;
        let principal = worker_principal(&request)?;
        let auth = request.extensions().get::<Arc<AuthPolicy>>().cloned();
        let mut inbound = request.into_inner();
        let timeout = self.runtime.config().heartbeat_timeout;
        let hello = match tokio::time::timeout(timeout, inbound.message()).await {
            Ok(Ok(Some(pb::WorkerMessage {
                msg: Some(pb::worker_message::Msg::Hello(hello)),
            }))) => hello,
            Ok(Ok(_)) => return Err(Status::invalid_argument("a session must start with Hello")),
            Ok(Err(status)) => return Err(status),
            Err(_) => return Err(Status::deadline_exceeded("no Hello received")),
        };
        let handle = self.runtime.open_session_for(
            hello,
            principal,
            auth.as_deref().unwrap_or(&self.default_auth),
        )?;
        let runtime = self.runtime.clone();
        let (session_id, connection) = (handle.session_id.clone(), handle.connection);
        let (closed_tx, mut closed_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let hb_sleep = tokio::time::sleep(timeout);
            tokio::pin!(hb_sleep);
            loop {
                tokio::select! {
                    _ = &mut closed_rx => break,
                    _ = &mut hb_sleep => {
                        tracing::warn!(session_id, "worker session missed its heartbeat timeout");
                        break;
                    }
                    next = inbound.message() => match next {
                        Ok(Some(pb::WorkerMessage { msg: Some(msg) })) => {
                            hb_sleep
                                .as_mut()
                                .reset(tokio::time::Instant::now() + timeout);
                            if let Err(error) = runtime.session_message(&session_id, connection, msg)
                            {
                                if error.kind == ErrorKind::StaleLease {
                                    tracing::debug!(
                                        session_id,
                                        error = %error,
                                        "ignored worker message"
                                    );
                                } else {
                                    tracing::warn!(
                                        session_id,
                                        error = %error,
                                        "invalid worker message"
                                    );
                                }
                            }
                        }
                        Ok(Some(pb::WorkerMessage { msg: None })) => {}
                        Ok(None) => break,
                        Err(status) => {
                            tracing::debug!(
                                session_id,
                                status = %status,
                                "worker session stream failed"
                            );
                            break;
                        }
                    }
                }
            }
            runtime.session_closed(&session_id, connection);
        });
        let mut messages = handle.messages;
        let stream: Self::SessionStream = Box::pin(async_stream::stream! {
            let _closed = closed_tx;
            while let Some(msg) = messages.recv().await {
                yield Ok(msg);
            }
        });
        Ok(Response::new(stream))
    }

    async fn attempt(
        &self,
        request: Request<Streaming<pb::AttemptUp>>,
    ) -> Result<Response<Self::AttemptStream>, Status> {
        self.runtime.ensure_active_leader()?;
        let principal = worker_principal(&request)?;
        let auth = request.extensions().get::<Arc<AuthPolicy>>().cloned();
        let mut inbound = request.into_inner();
        let timeout = self.runtime.config().heartbeat_timeout;
        let open = match tokio::time::timeout(timeout, inbound.message()).await {
            Ok(Ok(Some(pb::AttemptUp {
                msg: Some(pb::attempt_up::Msg::Open(open)),
            }))) => open,
            Ok(Ok(_)) => {
                return Err(Status::invalid_argument(
                    "an attempt stream must start with Open",
                ));
            }
            Ok(Err(status)) => return Err(status),
            Err(_) => return Err(Status::deadline_exceeded("no Open received")),
        };
        let (tx, rx) = mpsc::channel::<Result<pb::AttemptDown, Status>>(64);
        let (link, resume_from) = match self
            .runtime
            .open_attempt_for(
                &principal,
                auth.as_deref().unwrap_or(&self.default_auth),
                &open,
            )
            .await
        {
            Ok(opened) => opened,
            Err(WorkerOpenError::Runtime(error)) if error.kind == ErrorKind::StaleLease => {
                let _ = tx
                    .send(Ok(finished(
                        pb::attempt_finished::Status::Fenced,
                        error.message(),
                    )))
                    .await;
                tokio::spawn(drain(inbound, timeout));
                return Ok(Response::new(Box::pin(ReceiverStream::new(rx))));
            }
            Err(error) => {
                tokio::spawn(drain(inbound, timeout));
                return Err(error.into());
            }
        };
        let _ = tx
            .send(Ok(down(pb::attempt_down::Msg::Opened(pb::AttemptOpened {
                resume_from_sequence: resume_from,
                terminal_replay: matches!(link, AttemptLink::Replay { .. }),
            }))))
            .await;
        let limits = self.runtime.config().limits;
        let link = match link {
            AttemptLink::Live {
                lease_id,
                resume_from,
                inputs,
                stream,
            } => {
                if let Some(plan) = inputs {
                    tokio::spawn(send_inputs(
                        plan,
                        tx.clone(),
                        limits.max_chunk_bytes as usize,
                    ));
                }
                AttemptLink::Live {
                    lease_id,
                    resume_from,
                    inputs: None,
                    stream,
                }
            }
            replay => replay,
        };
        tokio::spawn(receive_uploads(
            self.runtime.clone(),
            link,
            inbound,
            tx,
            limits,
            timeout,
            true,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Reads what the worker still sends on an answered attempt stream until it
/// closes its side. Dropping the stream earlier makes h2 reset it, and the
/// worker's messages still in flight then arrive on a stream the server has
/// forgotten: each counts against h2's limit of 1024 locally reset streams
/// per connection, past which the server sends GOAWAY and every attempt on
/// the worker's connection breaks. Bounded by `limit`.
async fn drain<S: Stream<Item = Result<pb::AttemptUp, Status>> + Unpin>(
    mut inbound: S,
    limit: Duration,
) {
    let _ = tokio::time::timeout(limit, async {
        while let Some(Ok(_)) = inbound.next().await {}
    })
    .await;
}

/// Streams the job's inputs: every object in chunks, then params, then each
/// streaming input chunk as its own object as it arrives, and always a closing
/// trailer. `object_end` marks the last piece of each object and of params.
#[allow(clippy::too_many_arguments)]
async fn send_object_pieces(
    tx: &mpsc::Sender<Result<pb::AttemptDown, Status>>,
    sequence: &mut u64,
    chunk: usize,
    data: &Bytes,
    index: Option<u32>,
    uri: &str,
    metadata: &Metadata,
    parameter: bool,
) -> bool {
    let mut offset = 0;
    loop {
        let end = (offset + chunk).min(data.len());
        *sequence += 1;
        let msg = down(pb::attempt_down::Msg::Input(pb::InputData {
            sequence: *sequence,
            data: data.slice(offset..end),
            last: false,
            metadata: if offset == 0 {
                crate::runtime::machine::to_proto_map(metadata)
            } else {
                Default::default()
            },
            object_index: index,
            uri: uri.to_string(),
            parameter_chunk: parameter,
            object_end: end >= data.len(),
        }));
        if tx.send(Ok(msg)).await.is_err() {
            return false;
        }
        offset = end;
        if offset >= data.len() {
            return true;
        }
    }
}

pub(crate) async fn send_inputs(
    plan: InputPlan,
    tx: mpsc::Sender<Result<pb::AttemptDown, Status>>,
    chunk: usize,
) {
    let chunk = chunk.max(1);
    let mut sequence = 0u64;
    for (index, object) in plan.objects.iter().enumerate() {
        let index = Some(index as u32);
        let ok = match object {
            Object::Uri { uri, metadata } => {
                send_object_pieces(
                    &tx,
                    &mut sequence,
                    chunk,
                    &Bytes::new(),
                    index,
                    uri,
                    metadata,
                    false,
                )
                .await
            }
            Object::Inline { data, metadata } => {
                send_object_pieces(&tx, &mut sequence, chunk, data, index, "", metadata, false)
                    .await
            }
        };
        if !ok {
            return;
        }
    }
    if !plan.params.is_empty()
        && !send_object_pieces(
            &tx,
            &mut sequence,
            chunk,
            &plan.params,
            None,
            "",
            &Metadata::new(),
            true,
        )
        .await
    {
        return;
    }
    if let Some(log) = plan.streaming {
        let mut next_index = plan.objects.len() as u32;
        let mut changes = log.subscribe();
        let mut cursor = 0;
        loop {
            changes.borrow_and_update();
            let (chunks, status) = log.read_from(cursor);
            cursor += chunks.len();
            for (data, metadata) in chunks {
                if !send_object_pieces(
                    &tx,
                    &mut sequence,
                    chunk,
                    &data,
                    Some(next_index),
                    "",
                    &metadata,
                    false,
                )
                .await
                {
                    return;
                }
                next_index += 1;
            }
            match status {
                InputStatus::Finished => break,
                InputStatus::Aborted => return,
                InputStatus::Open => {
                    if changes.changed().await.is_err() {
                        return;
                    }
                }
            }
        }
    }
    sequence += 1;
    let _ = tx
        .send(Ok(down(pb::attempt_down::Msg::Input(pb::InputData {
            sequence,
            last: true,
            ..Default::default()
        }))))
        .await;
}

/// Accounted per result object in addition to its URI, metadata and data, so
/// empty objects are not free.
const OBJECT_OVERHEAD: u64 = 64;

struct PendingObject {
    index: u32,
    buffer: BytesMut,
    metadata: Metadata,
    uri: Option<String>,
    permit: Permit,
}

/// Reassembles chunked result objects. Every byte held — data, URIs,
/// metadata, per-object overhead — counts against the attempt's inline
/// budget, and data also against the runtime-wide result budget for as long
/// as any copy of it is alive. Past a limit nothing more is kept; the attempt
/// then ends as `output_too_large`.
struct ResultAssembler {
    limits: Limits,
    budget: Arc<ByteBudget>,
    /// Frees result budget held by terminal jobs; tried once per refusal.
    reclaim: Runtime,
    objects: Vec<Object>,
    current: Option<PendingObject>,
    next_index: u32,
    total: u64,
    too_large: bool,
}

impl ResultAssembler {
    fn new(limits: Limits, budget: Arc<ByteBudget>, reclaim: Runtime) -> Self {
        Self {
            limits,
            budget,
            reclaim,
            objects: Vec::new(),
            current: None,
            next_index: 0,
            total: 0,
            too_large: false,
        }
    }

    fn overflow(&mut self) {
        self.too_large = true;
        self.objects.clear();
        self.current = None;
    }

    fn push(&mut self, chunk: pb::ResultChunk) -> Result<(), Status> {
        let expected = self
            .current
            .as_ref()
            .map(|current| current.index)
            .unwrap_or(self.next_index);
        if chunk.index != expected {
            return Err(Status::invalid_argument(format!(
                "result chunk for object {} while object {expected} is expected",
                chunk.index
            )));
        }
        if !chunk.uri.is_empty() && !chunk.data.is_empty() {
            return Err(Status::invalid_argument("a URI result carries no data"));
        }
        if self.current.is_none() {
            let metadata: Metadata = chunk.metadata.into_iter().collect();
            let metadata_bytes = metadata
                .iter()
                .map(|(k, v)| (k.len() + v.len()) as u64)
                .sum::<u64>();
            self.total = self
                .total
                .saturating_add(OBJECT_OVERHEAD)
                .saturating_add(metadata_bytes)
                .saturating_add(chunk.uri.len() as u64);
            if self.next_index >= self.limits.max_objects
                || metadata_bytes > self.limits.max_metadata_bytes as u64
            {
                self.overflow();
            }
            if chunk.last {
                // Single-chunk fast path: wrap `chunk.data` directly without allocating
                // a `BytesMut` or copying payload bytes.
                self.total = self.total.saturating_add(chunk.data.len() as u64);
                if self.total > self.limits.max_inline_total_bytes
                    || chunk.data.len() as u64 > self.limits.max_inline_object_bytes
                {
                    self.overflow();
                }
                if !self.too_large {
                    if !chunk.uri.is_empty() {
                        self.objects.push(Object::Uri {
                            uri: chunk.uri,
                            metadata,
                        });
                    } else {
                        let mut permit = Permit::new(Arc::clone(&self.budget));
                        if !permit.grow(chunk.data.len()) && {
                            self.reclaim.reclaim_results(chunk.data.len());
                            !permit.grow(chunk.data.len())
                        } {
                            return Err(Status::resource_exhausted(
                                "result buffers are at capacity; retry the completion later",
                            ));
                        }
                        // Small retained results copy into a tight buffer so
                        // 10k retained jobs never pin multi-KiB HTTP/2 frames.
                        let data = if chunk.data.len() >= 4096 {
                            chunk.data
                        } else {
                            Bytes::copy_from_slice(&chunk.data)
                        };
                        self.objects.push(Object::Inline {
                            data: budgeted_bytes(data, permit),
                            metadata,
                        });
                    }
                }
                self.next_index = chunk.index + 1;
                return Ok(());
            }
            if !self.too_large {
                self.current = Some(PendingObject {
                    index: chunk.index,
                    buffer: BytesMut::new(),
                    metadata,
                    uri: (!chunk.uri.is_empty()).then_some(chunk.uri),
                    permit: Permit::new(Arc::clone(&self.budget)),
                });
            }
        }
        self.total = self.total.saturating_add(chunk.data.len() as u64);
        if self.total > self.limits.max_inline_total_bytes {
            self.overflow();
        }
        if let Some(current) = self.current.as_mut() {
            if current.buffer.len() as u64 + chunk.data.len() as u64
                > self.limits.max_inline_object_bytes
            {
                self.overflow();
            } else if !current.permit.grow(chunk.data.len()) && {
                self.reclaim.reclaim_results(chunk.data.len());
                !current.permit.grow(chunk.data.len())
            } {
                return Err(Status::resource_exhausted(
                    "result buffers are at capacity; retry the completion later",
                ));
            } else {
                current.buffer.extend_from_slice(&chunk.data);
            }
        }
        if chunk.last {
            self.next_index = chunk.index + 1;
            if let Some(current) = self.current.take() {
                self.objects.push(match current.uri {
                    Some(uri) => Object::Uri {
                        uri,
                        metadata: current.metadata,
                    },
                    None => Object::Inline {
                        data: budgeted_bytes(current.buffer.freeze(), current.permit),
                        metadata: current.metadata,
                    },
                });
            }
        }
        Ok(())
    }

    fn finish(
        self,
        finish: pb::Finish,
    ) -> Result<(AttemptOutcome, Vec<Object>, Option<u64>), Status> {
        if self.current.is_some() {
            return Err(Status::invalid_argument(
                "Finish arrived before the last result chunk",
            ));
        }
        let handler_duration_ms = finish.handler_duration_ms;
        let outcome =
            match finish.outcome {
                Some(pb::finish::Outcome::Succeeded(_)) => AttemptOutcome::Succeeded,
                Some(pb::finish::Outcome::Failed(error)) => AttemptOutcome::Failed(
                    ErrorInfo::bounded(error.code, error.message, error.retryable),
                ),
                Some(pb::finish::Outcome::Cancelled(_)) => AttemptOutcome::Cancelled,
                None => return Err(Status::invalid_argument("Finish requires an outcome")),
            };
        if self.too_large && matches!(outcome, AttemptOutcome::Succeeded) {
            return Ok((
                AttemptOutcome::Failed(ErrorInfo::new(
                    codes::OUTPUT_TOO_LARGE,
                    format!(
                        "result exceeds the inline budget ({} objects, {} bytes per object, {} in total)",
                        self.limits.max_objects,
                        self.limits.max_inline_object_bytes,
                        self.limits.max_inline_total_bytes
                    ),
                    false,
                )),
                Vec::new(),
                handler_duration_ms,
            ));
        }
        Ok((outcome, self.objects, handler_duration_ms))
    }
}

pub(crate) async fn receive_uploads<S>(
    runtime: Runtime,
    link: AttemptLink,
    mut inbound: S,
    tx: mpsc::Sender<Result<pb::AttemptDown, Status>>,
    limits: Limits,
    idle: Duration,
    drain_on_exit: bool,
) where
    S: Stream<Item = Result<pb::AttemptUp, Status>> + Unpin,
{
    let budget = runtime.result_budget();
    let mut results = ResultAssembler::new(limits, Arc::clone(&budget), runtime.clone());
    let mut last_frame = match &link {
        AttemptLink::Live { resume_from, .. } => *resume_from,
        AttemptLink::Replay { .. } => 0,
    };
    let mut pending_finish = None;
    loop {
        let message = match inbound.next().await {
            Some(Ok(pb::AttemptUp { msg: Some(msg) })) => msg,
            Some(Ok(_)) => continue,
            None | Some(Err(_)) => return,
        };
        let reply = match message {
            pb::attempt_up::Msg::Open(_) => {
                Err(Status::invalid_argument("Open was already received"))
            }
            pb::attempt_up::Msg::Frame(frame) => match &link {
                AttemptLink::Replay { .. } => {
                    last_frame = last_frame.max(frame.sequence);
                    Ok(down(pb::attempt_down::Msg::Ack(pb::Ack {
                        up_to_sequence: last_frame,
                        result_bytes: results.total,
                    })))
                }
                AttemptLink::Live {
                    lease_id, stream, ..
                } => match runtime.push_frame_live(lease_id, stream, frame).await {
                    Ok(sequence) => {
                        last_frame = sequence;
                        Ok(down(pb::attempt_down::Msg::Ack(pb::Ack {
                            up_to_sequence: sequence,
                            result_bytes: results.total,
                        })))
                    }
                    Err(error) if error.kind == ErrorKind::StaleLease => {
                        let _ = tx
                            .send(Ok(finished(
                                pb::attempt_finished::Status::Fenced,
                                error.message(),
                            )))
                            .await;
                        break;
                    }
                    Err(error) => Err(error.into()),
                },
            },
            pb::attempt_up::Msg::Result(chunk) => results.push(chunk).map(|()| {
                down(pb::attempt_down::Msg::Ack(pb::Ack {
                    up_to_sequence: last_frame,
                    result_bytes: results.total,
                }))
            }),
            pb::attempt_up::Msg::Finish(finish) => {
                pending_finish = Some(finish);
                break;
            }
        };
        let failed = reply.is_err();
        if tx.send(reply).await.is_err() || failed {
            break;
        }
    }
    if let Some(finish) = pending_finish {
        let reply = match results.finish(finish) {
            Ok((outcome, outputs, handler_duration_ms)) => runtime
                .finish_attempt_with_duration(&link, outcome, outputs, handler_duration_ms)
                .await
                .map(finish_to_proto)
                .map_err(Status::from),
            Err(status) => Err(status),
        };
        let _ = tx.send(reply).await;
    } else {
        // The answer is final: free any partial result before draining inbound.
        drop(results);
    }
    drop(tx);
    if drain_on_exit {
        drain(inbound, idle).await;
    }
}
