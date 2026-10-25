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

use crate::auth::Role;
use crate::error::ErrorKind;
use crate::runtime::Runtime;
use crate::runtime::api::{ErrorInfo, Limits, Metadata, Object, codes};
use crate::runtime::events::{ByteBudget, Permit, budgeted_bytes};
use crate::runtime::input::InputStatus;
use crate::runtime::state::FinishStatus;
use crate::runtime::worker::{AttemptLink, AttemptOutcome, InputPlan};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

pub struct WorkerGrpcService {
    runtime: Runtime,
}

impl WorkerGrpcService {
    pub fn new(runtime: Runtime) -> Self {
        Self { runtime }
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

fn down(msg: pb::attempt_down::Msg) -> pb::AttemptDown {
    pb::AttemptDown { msg: Some(msg) }
}

fn finished(status: pb::attempt_finished::Status, reason: impl Into<String>) -> pb::AttemptDown {
    down(pb::attempt_down::Msg::Finished(pb::AttemptFinished {
        status: status as i32,
        reason: reason.into(),
    }))
}

fn finish_to_proto(status: FinishStatus) -> pb::AttemptDown {
    match status {
        FinishStatus::Committed => finished(pb::attempt_finished::Status::Committed, ""),
        FinishStatus::Fenced(reason) => finished(pb::attempt_finished::Status::Fenced, reason),
        FinishStatus::Rejected(reason) => finished(pb::attempt_finished::Status::Rejected, reason),
    }
}

#[tonic::async_trait]
impl Worker for WorkerGrpcService {
    type SessionStream = ResponseStream<pb::ServerMessage>;
    type AttemptStream = ResponseStream<pb::AttemptDown>;

    async fn session(
        &self,
        request: Request<Streaming<pb::WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        require_worker(&request)?;
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
        let handle = self.runtime.open_session(hello)?;
        let runtime = self.runtime.clone();
        let (session_id, connection) = (handle.session_id.clone(), handle.connection);
        tokio::spawn(async move {
            loop {
                match tokio::time::timeout(timeout, inbound.message()).await {
                    Ok(Ok(Some(pb::WorkerMessage { msg: Some(msg) }))) => {
                        if let Err(error) = runtime.session_message(&session_id, connection, msg) {
                            if error.kind == ErrorKind::StaleLease {
                                tracing::debug!(session_id, error = %error, "ignored worker message");
                            } else {
                                tracing::warn!(session_id, error = %error, "invalid worker message");
                            }
                        }
                    }
                    Ok(Ok(Some(_))) => {}
                    Ok(Ok(None)) => break,
                    Ok(Err(status)) => {
                        tracing::debug!(session_id, status = %status, "worker session stream failed");
                        break;
                    }
                    Err(_) => {
                        tracing::warn!(session_id, "worker session missed its heartbeat timeout");
                        break;
                    }
                }
            }
            runtime.session_closed(&session_id, connection);
        });
        Ok(Response::new(Box::pin(
            ReceiverStream::new(handle.messages).map(Ok),
        )))
    }

    async fn attempt(
        &self,
        request: Request<Streaming<pb::AttemptUp>>,
    ) -> Result<Response<Self::AttemptStream>, Status> {
        require_worker(&request)?;
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
        let (link, resume_from) = match self.runtime.open_attempt(&open).await {
            Ok(opened) => opened,
            Err(error) if error.kind == ErrorKind::StaleLease => {
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
        let limits = self.runtime.config().limits.clone();
        let link = match link {
            AttemptLink::Live {
                lease_id,
                resume_from,
                inputs,
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
async fn drain(mut inbound: Streaming<pb::AttemptUp>, limit: Duration) {
    let _ = tokio::time::timeout(limit, async {
        while let Ok(Some(_)) = inbound.message().await {}
    })
    .await;
}

/// Streams the job's inputs: every object in chunks, then params, then each
/// streaming input chunk as its own object as it arrives, and always a closing
/// trailer. `object_end` marks the last piece of each object and of params.
async fn send_inputs(
    plan: InputPlan,
    tx: mpsc::Sender<Result<pb::AttemptDown, Status>>,
    chunk: usize,
) {
    let chunk = chunk.max(1);
    let mut sequence = 0u64;
    let mut pieces =
        |data: &Bytes, index: Option<u32>, uri: &str, metadata: &Metadata, parameter: bool| {
            let mut messages = Vec::new();
            let mut offset = 0;
            loop {
                let end = (offset + chunk).min(data.len());
                sequence += 1;
                messages.push(down(pb::attempt_down::Msg::Input(pb::InputData {
                    sequence,
                    data: data.slice(offset..end),
                    last: false,
                    metadata: if offset == 0 {
                        metadata.clone().into_iter().collect()
                    } else {
                        Default::default()
                    },
                    object_index: index,
                    uri: uri.to_string(),
                    parameter_chunk: parameter,
                    object_end: end >= data.len(),
                })));
                offset = end;
                if offset >= data.len() {
                    return messages;
                }
            }
        };
    let mut messages = Vec::new();
    for (index, object) in plan.objects.iter().enumerate() {
        let index = Some(index as u32);
        messages.extend(match object {
            Object::Uri { uri, metadata } => pieces(&Bytes::new(), index, uri, metadata, false),
            Object::Inline { data, metadata } => pieces(data, index, "", metadata, false),
        });
    }
    if !plan.params.is_empty() {
        messages.extend(pieces(&plan.params, None, "", &Metadata::new(), true));
    }
    for message in messages {
        if tx.send(Ok(message)).await.is_err() {
            return;
        }
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
                for message in pieces(&data, Some(next_index), "", &metadata, false) {
                    if tx.send(Ok(message)).await.is_err() {
                        return;
                    }
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
            self.total += OBJECT_OVERHEAD + metadata_bytes + chunk.uri.len() as u64;
            if self.next_index >= self.limits.max_objects
                || metadata_bytes > self.limits.max_metadata_bytes as u64
            {
                self.overflow();
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
        self.total += chunk.data.len() as u64;
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

    fn finish(self, finish: pb::Finish) -> Result<(AttemptOutcome, Vec<Object>), Status> {
        if self.current.is_some() {
            return Err(Status::invalid_argument(
                "Finish arrived before the last result chunk",
            ));
        }
        let outcome = match finish.outcome {
            Some(pb::finish::Outcome::Succeeded(_)) => AttemptOutcome::Succeeded,
            Some(pb::finish::Outcome::Failed(error)) => AttemptOutcome::Failed(ErrorInfo {
                code: error.code,
                message: error.message,
                retryable: error.retryable,
            }),
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
            ));
        }
        Ok((outcome, self.objects))
    }
}

async fn receive_uploads(
    runtime: Runtime,
    link: AttemptLink,
    mut inbound: Streaming<pb::AttemptUp>,
    tx: mpsc::Sender<Result<pb::AttemptDown, Status>>,
    limits: Limits,
    idle: Duration,
) {
    let budget = runtime.result_budget();
    let mut results = ResultAssembler::new(limits.clone(), Arc::clone(&budget), runtime.clone());
    let mut last_frame = match &link {
        AttemptLink::Live { resume_from, .. } => *resume_from,
        AttemptLink::Replay { .. } => 0,
    };
    loop {
        let message = match inbound.message().await {
            Ok(Some(pb::AttemptUp { msg: Some(msg) })) => msg,
            Ok(Some(_)) => continue,
            Ok(None) | Err(_) => return,
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
                AttemptLink::Live { lease_id, .. } => {
                    match runtime.push_frame(lease_id, frame).await {
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
                    }
                }
            },
            pb::attempt_up::Msg::Result(chunk) => results.push(chunk).map(|()| {
                down(pb::attempt_down::Msg::Ack(pb::Ack {
                    up_to_sequence: last_frame,
                    result_bytes: results.total,
                }))
            }),
            pb::attempt_up::Msg::Finish(finish) => {
                let assembled = std::mem::replace(
                    &mut results,
                    ResultAssembler::new(limits.clone(), Arc::clone(&budget), runtime.clone()),
                );
                let reply = match assembled.finish(finish) {
                    Ok((outcome, outputs)) => runtime
                        .finish_attempt(&link, outcome, outputs)
                        .await
                        .map(finish_to_proto)
                        .map_err(Status::from),
                    Err(status) => Err(status),
                };
                let _ = tx.send(reply).await;
                break;
            }
        };
        let failed = reply.is_err();
        if tx.send(reply).await.is_err() || failed {
            break;
        }
    }
    // The answer is final: end the response and give back any partial
    // result, then let the worker close its side.
    drop((tx, results));
    drain(inbound, idle).await;
}
