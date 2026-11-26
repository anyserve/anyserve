#![allow(
    clippy::result_large_err,
    reason = "tonic::Status is the transport error type"
)]
//! Single-port HTTP + WebSocket Protobuf transport adapter for Tier 2 clients
//! and workers. Exposes the same runtime semantics as [`crate::client_service`]
//! and [`crate::worker_service`] over `/rpc/v2/*` and `/ws/v2/*`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use anyserve_proto::v2 as pb;
use axum::Router;
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, Stream, StreamExt};
use prost::Message as ProstMessage;
use tokio::sync::{Notify, mpsc};
use tonic::Status;

use crate::auth::{AuthPolicy, Principal, Role, wait_credential_invalidated};
use crate::client_service::{
    cancel_request, event_to_proto, get_job_request, handle_invoke_message, list_jobs_request,
    start_invoke, submit_request,
};
use crate::error::ErrorKind;
use crate::runtime::worker::WorkerOpenError;
use crate::runtime::{Runtime, api};
use crate::worker_service::{down, finished, receive_uploads, send_inputs};

pub const GRPC_CODE_HEADER: &str = "x-anyserve-grpc-code";
const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";
const WS_MAX_FRAME_SIZE: usize = 32 * 1024 * 1024;
const WS_WATCH_MAX_BYTES: usize = 16 * 1024;
const WS_ATTEMPT_INBOUND_SLOTS: usize = 16;
const WS_PING_INTERVAL: Duration = Duration::from_secs(30);

type WsTx = SplitSink<WebSocket, Message>;
type WsRx = SplitStream<WebSocket>;

#[derive(Clone)]
struct HttpWsState {
    runtime: Runtime,
    auth: Arc<AuthPolicy>,
    ping_interval: Duration,
}

/// Builds the Axum router serving the HTTP unary (`/rpc/v2/client/*`) and
/// WebSocket streaming (`/ws/v2/*`) Protobuf protocol.
pub fn router(runtime: Runtime, auth: Arc<AuthPolicy>) -> Router<()> {
    router_with_ping_interval(runtime, auth, WS_PING_INTERVAL)
}

fn router_with_ping_interval(
    runtime: Runtime,
    auth: Arc<AuthPolicy>,
    ping_interval: Duration,
) -> Router<()> {
    let state = HttpWsState {
        runtime,
        auth,
        ping_interval,
    };
    let unary = Router::new()
        .route("/rpc/v2/client/submit", post(rpc_submit))
        .route("/rpc/v2/client/execute", post(rpc_execute))
        .route("/rpc/v2/client/get_job", post(rpc_get_job))
        .route("/rpc/v2/client/cancel", post(rpc_cancel))
        .route("/rpc/v2/client/list_jobs", post(rpc_list_jobs))
        .layer(DefaultBodyLimit::max(anyserve_proto::MAX_MESSAGE_SIZE));

    let ws = Router::new()
        .route("/ws/v2/worker/session", get(ws_worker_session))
        .route("/ws/v2/worker/attempt", get(ws_worker_attempt))
        .route("/ws/v2/client/invoke", get(ws_client_invoke))
        .route("/ws/v2/client/watch", get(ws_client_watch));

    unary.merge(ws).with_state(state)
}

fn http_status_for_grpc_code(code: tonic::Code) -> StatusCode {
    match code {
        tonic::Code::InvalidArgument | tonic::Code::OutOfRange => StatusCode::BAD_REQUEST,
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::AlreadyExists | tonic::Code::Aborted => StatusCode::CONFLICT,
        tonic::Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn status_response(status: Status) -> Response {
    let http_status = http_status_for_grpc_code(status.code());
    let grpc_code = (status.code() as i32).to_string();
    let mut response = (http_status, status.message().to_owned()).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    if let Ok(value) = HeaderValue::from_str(&grpc_code) {
        headers.insert(HeaderName::from_static(GRPC_CODE_HEADER), value);
    }
    response
}

fn authorize_http(
    state: &HttpWsState,
    headers: &HeaderMap,
    role: Role,
) -> Result<Principal, Response> {
    let principal = match role {
        Role::Client => state.auth.verify_client_headers(headers),
        Role::Worker => state.auth.verify_worker_headers(headers),
    }
    .map_err(status_response)?;
    state
        .runtime
        .ensure_active_leader()
        .map_err(|err| status_response(Status::from(err)))?;
    Ok(principal)
}

async fn handle_unary_client<Req, Resp, F, Fut>(
    state: HttpWsState,
    headers: HeaderMap,
    body: Bytes,
    handler: F,
) -> Response
where
    Req: ProstMessage + Default,
    Resp: ProstMessage,
    F: FnOnce(Runtime, Arc<AuthPolicy>, Principal, Req) -> Fut,
    Fut: Future<Output = Result<Resp, Status>>,
{
    let principal = match authorize_http(&state, &headers, Role::Client) {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let req = match Req::decode(body) {
        Ok(req) => req,
        Err(err) => {
            return status_response(Status::invalid_argument(format!(
                "invalid protobuf request: {err}"
            )));
        }
    };
    match handler(state.runtime, state.auth, principal, req).await {
        Ok(resp) => {
            let mut out = Vec::with_capacity(resp.encoded_len());
            if let Err(err) = resp.encode(&mut out) {
                return status_response(Status::internal(format!(
                    "encode protobuf response: {err}"
                )));
            }
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, PROTOBUF_CONTENT_TYPE)],
                out,
            )
                .into_response()
        }
        Err(status) => status_response(status),
    }
}

fn extract_http_trace_headers(headers: &HeaderMap) -> Vec<(&'static str, String)> {
    crate::runtime::api::IDENTITY_EXCLUDED_METADATA_KEYS
        .iter()
        .filter_map(|&key| {
            let val = headers.get(key)?.to_str().ok()?.trim();
            (!val.is_empty() && val.len() <= crate::trace::MAX_TRACE_HEADER_BYTES)
                .then(|| (key, val.to_owned()))
        })
        .collect()
}

fn merge_trace_header_pairs(spec: Option<&mut pb::JobSpec>, pairs: &[(&'static str, String)]) {
    if let Some(spec) = spec
        && !pairs.is_empty()
    {
        crate::trace::merge_trace_headers_into_proto(&mut spec.metadata, |key| {
            pairs
                .iter()
                .find_map(|(k, v)| (*k == key).then_some(v.as_str()))
        });
    }
}

async fn rpc_submit(State(state): State<HttpWsState>, headers: HeaderMap, body: Bytes) -> Response {
    let trace_headers = extract_http_trace_headers(&headers);
    handle_unary_client(
        state,
        headers,
        body,
        |runtime, _auth, principal, mut req: pb::SubmitRequest| async move {
            merge_trace_header_pairs(req.spec.as_mut(), &trace_headers);
            submit_request(&runtime, principal.namespace, req).await
        },
    )
    .await
}

async fn rpc_execute(
    State(state): State<HttpWsState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let trace_headers = extract_http_trace_headers(&headers);
    handle_unary_client(
        state,
        headers,
        body,
        |runtime, auth, principal, mut start: pb::InvokeStart| async move {
            if start.streaming_input {
                return Err(Status::invalid_argument(
                    "/rpc/v2/client/execute does not accept streaming_input",
                ));
            }
            merge_trace_header_pairs(start.spec.as_mut(), &trace_headers);
            let mut execution = start_invoke(&runtime, principal.namespace.clone(), start).await?;
            let invalidated = wait_credential_invalidated(&auth, &principal);
            tokio::pin!(invalidated);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut invalidated => {
                        auth.record_rejection();
                        return Err(Status::unauthenticated(
                            "bearer token has been revoked or expired",
                        ));
                    }
                    next = execution.next() => {
                        let Some(event) = next else {
                            return Err(Status::unavailable(
                                "execution ended without a terminal event",
                            ));
                        };
                        let event = event?;
                        if matches!(event.kind, api::EventKind::Finished { .. }) {
                            return Ok(event_to_proto(event));
                        }
                    }
                }
            }
        },
    )
    .await
}

async fn rpc_get_job(
    State(state): State<HttpWsState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_unary_client(
        state,
        headers,
        body,
        |runtime, _auth, principal, req: pb::GetJobRequest| async move {
            get_job_request(&runtime, &principal.namespace, req).await
        },
    )
    .await
}

async fn rpc_cancel(State(state): State<HttpWsState>, headers: HeaderMap, body: Bytes) -> Response {
    handle_unary_client(
        state,
        headers,
        body,
        |runtime, _auth, principal, req: pb::CancelRequest| async move {
            cancel_request(&runtime, &principal.namespace, req).await
        },
    )
    .await
}

async fn rpc_list_jobs(
    State(state): State<HttpWsState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_unary_client(
        state,
        headers,
        body,
        |runtime, _auth, principal, req: pb::ListJobsRequest| async move {
            list_jobs_request(&runtime, principal.namespace, req).await
        },
    )
    .await
}

async fn ws_worker_session(
    State(state): State<HttpWsState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let principal = match authorize_http(&state, &headers, Role::Worker) {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let max_ctrl = state.runtime.config().limits.max_control_message_bytes as usize;
    ws.max_frame_size(max_ctrl)
        .max_message_size(max_ctrl)
        .on_upgrade(move |socket| {
            handle_worker_session(
                state.runtime,
                state.auth,
                principal,
                socket,
                state.ping_interval,
            )
        })
}

async fn ws_worker_attempt(
    State(state): State<HttpWsState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let principal = match authorize_http(&state, &headers, Role::Worker) {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    ws.max_frame_size(WS_MAX_FRAME_SIZE)
        .max_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| {
            handle_worker_attempt(
                state.runtime,
                state.auth,
                principal,
                socket,
                state.ping_interval,
            )
        })
}

async fn ws_client_invoke(
    State(state): State<HttpWsState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let principal = match authorize_http(&state, &headers, Role::Client) {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let trace_headers = extract_http_trace_headers(&headers);
    ws.max_frame_size(WS_MAX_FRAME_SIZE)
        .max_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| {
            handle_client_invoke(
                state.runtime,
                state.auth,
                principal,
                socket,
                state.ping_interval,
                trace_headers,
            )
        })
}

async fn ws_client_watch(
    State(state): State<HttpWsState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let principal = match authorize_http(&state, &headers, Role::Client) {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    ws.max_frame_size(WS_WATCH_MAX_BYTES)
        .max_message_size(WS_WATCH_MAX_BYTES)
        .on_upgrade(move |socket| {
            handle_client_watch(
                state.runtime,
                state.auth,
                principal,
                socket,
                state.ping_interval,
            )
        })
}

fn new_ping_interval(period: Duration) -> tokio::time::Interval {
    let period = period.max(Duration::from_millis(1));
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

async fn send_ok<M: ProstMessage>(ws_tx: &mut WsTx, msg: &M) -> bool {
    let mut buf = Vec::with_capacity(1 + msg.encoded_len());
    buf.push(0x00);
    if msg.encode(&mut buf).is_err() {
        return false;
    }
    ws_tx.send(Message::Binary(buf)).await.is_ok()
}

async fn send_error_and_close(ws_tx: &mut WsTx, status: &Status) {
    let message = status.message().as_bytes();
    let mut buf = Vec::with_capacity(2 + message.len());
    buf.push(0x01);
    buf.push((status.code() as i32) as u8);
    buf.extend_from_slice(message);
    let _ = ws_tx.send(Message::Binary(buf)).await;
    send_close(ws_tx).await;
}

async fn send_close(ws_tx: &mut WsTx) {
    let _ = ws_tx.send(Message::Close(None)).await;
    let _ = ws_tx.close().await;
}

async fn recv_initial_proto<M: ProstMessage + Default>(
    ws_rx: &mut WsRx,
    ws_tx: &mut WsTx,
    ping_interval: &mut tokio::time::Interval,
    timeout: Option<(Duration, &'static str)>,
) -> Result<M, Option<Status>> {
    let deadline = timeout.map(|(dur, msg)| (tokio::time::Instant::now() + dur, msg));
    loop {
        enum Next {
            Frame(Option<Result<Message, axum::Error>>),
            Ping,
            TimedOut(&'static str),
        }
        let next = match deadline {
            Some((at, msg)) => {
                tokio::select! {
                    frame = ws_rx.next() => Next::Frame(frame),
                    _ = ping_interval.tick() => Next::Ping,
                    _ = tokio::time::sleep_until(at) => Next::TimedOut(msg),
                }
            }
            None => {
                tokio::select! {
                    frame = ws_rx.next() => Next::Frame(frame),
                    _ = ping_interval.tick() => Next::Ping,
                }
            }
        };
        match next {
            Next::Ping => {
                if ws_tx.send(Message::Ping(Vec::new())).await.is_err() {
                    return Err(None);
                }
            }
            Next::TimedOut(msg) => return Err(Some(Status::deadline_exceeded(msg))),
            Next::Frame(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
            Next::Frame(Some(Ok(Message::Binary(bytes)))) => {
                return M::decode(bytes.as_slice()).map_err(|err| {
                    Some(Status::invalid_argument(format!(
                        "invalid protobuf frame: {err}"
                    )))
                });
            }
            Next::Frame(Some(Ok(Message::Text(_)))) => {
                return Err(Some(Status::invalid_argument(
                    "expected binary protobuf frame",
                )));
            }
            Next::Frame(Some(Ok(Message::Close(_))) | Some(Err(_)) | None) => return Err(None),
        }
    }
}

struct SessionGuard {
    runtime: Runtime,
    session_id: String,
    connection: u64,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.runtime
            .session_closed(&self.session_id, self.connection);
    }
}

struct MeteredInbound {
    rx: mpsc::Receiver<(Result<pb::AttemptUp, Status>, usize)>,
    queued_bytes: Arc<AtomicUsize>,
    drained: Arc<Notify>,
}

impl Stream for MeteredInbound {
    type Item = Result<pb::AttemptUp, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some((item, bytes))) => {
                self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                self.drained.notify_one();
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn handle_worker_session(
    runtime: Runtime,
    auth: Arc<AuthPolicy>,
    principal: Principal,
    socket: WebSocket,
    ping_period: Duration,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let mut ping_interval = new_ping_interval(ping_period);
    let timeout = runtime.config().heartbeat_timeout;

    let first = match recv_initial_proto::<pb::WorkerMessage>(
        &mut ws_rx,
        &mut ws_tx,
        &mut ping_interval,
        Some((timeout, "no Hello received")),
    )
    .await
    {
        Ok(msg) => msg,
        Err(Some(status)) => {
            send_error_and_close(&mut ws_tx, &status).await;
            return;
        }
        Err(None) => return,
    };
    let hello = match first.msg {
        Some(pb::worker_message::Msg::Hello(hello)) => hello,
        _ => {
            send_error_and_close(
                &mut ws_tx,
                &Status::invalid_argument("a session must start with Hello"),
            )
            .await;
            return;
        }
    };
    let mut handle = match runtime.open_session_for(hello, principal, &auth) {
        Ok(handle) => handle,
        Err(error) => {
            send_error_and_close(&mut ws_tx, &Status::from(error)).await;
            return;
        }
    };
    let _guard = SessionGuard {
        runtime: runtime.clone(),
        session_id: handle.session_id.clone(),
        connection: handle.connection,
    };

    let hb_sleep = tokio::time::sleep(timeout);
    tokio::pin!(hb_sleep);

    loop {
        tokio::select! {
            _ = &mut hb_sleep => {
                tracing::warn!(session_id = %handle.session_id, "worker session missed its heartbeat timeout");
                send_close(&mut ws_tx).await;
                break;
            }
            _ = ping_interval.tick() => {
                if ws_tx.send(Message::Ping(Vec::new())).await.is_err() {
                    break;
                }
            }
            outbound = handle.messages.recv() => {
                match outbound {
                    Some(msg) => {
                        if !send_ok(&mut ws_tx, &msg).await {
                            break;
                        }
                    }
                    None => {
                        send_close(&mut ws_tx).await;
                        break;
                    }
                }
            }
            inbound = ws_rx.next() => {
                match inbound {
                    Some(Ok(Message::Binary(bytes))) => {
                        match pb::WorkerMessage::decode(bytes.as_slice()) {
                            Ok(pb::WorkerMessage { msg: Some(msg) }) => {
                                hb_sleep.as_mut().reset(tokio::time::Instant::now() + timeout);
                                if let Err(error) = runtime.session_message(&handle.session_id, handle.connection, msg) {
                                    if error.kind == ErrorKind::StaleLease {
                                        tracing::debug!(session_id = %handle.session_id, error = %error, "ignored worker message");
                                    } else {
                                        tracing::warn!(session_id = %handle.session_id, error = %error, "invalid worker message");
                                    }
                                }
                            }
                            Ok(pb::WorkerMessage { msg: None }) => {}
                            Err(err) => {
                                send_error_and_close(
                                    &mut ws_tx,
                                    &Status::invalid_argument(format!("invalid protobuf WorkerMessage: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Text(_))) => {
                        send_error_and_close(
                            &mut ws_tx,
                            &Status::invalid_argument("expected binary protobuf frame"),
                        )
                        .await;
                        break;
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                }
            }
        }
    }
}

async fn handle_worker_attempt(
    runtime: Runtime,
    auth: Arc<AuthPolicy>,
    principal: Principal,
    socket: WebSocket,
    ping_period: Duration,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let mut ping_interval = new_ping_interval(ping_period);
    let timeout = runtime.config().heartbeat_timeout;

    let first = match recv_initial_proto::<pb::AttemptUp>(
        &mut ws_rx,
        &mut ws_tx,
        &mut ping_interval,
        Some((timeout, "no Open received")),
    )
    .await
    {
        Ok(msg) => msg,
        Err(Some(status)) => {
            send_error_and_close(&mut ws_tx, &status).await;
            return;
        }
        Err(None) => return,
    };
    let open = match first.msg {
        Some(pb::attempt_up::Msg::Open(open)) => open,
        _ => {
            send_error_and_close(
                &mut ws_tx,
                &Status::invalid_argument("an attempt stream must start with Open"),
            )
            .await;
            return;
        }
    };

    let (link, resume_from) = match runtime.open_attempt_for(&principal, &auth, &open).await {
        Ok(opened) => opened,
        Err(WorkerOpenError::Runtime(error)) if error.kind == ErrorKind::StaleLease => {
            let _ = send_ok(
                &mut ws_tx,
                &finished(pb::attempt_finished::Status::Fenced, error.message()),
            )
            .await;
            send_close(&mut ws_tx).await;
            return;
        }
        Err(error) => {
            send_error_and_close(&mut ws_tx, &Status::from(error)).await;
            return;
        }
    };

    let opened = down(pb::attempt_down::Msg::Opened(pb::AttemptOpened {
        resume_from_sequence: resume_from,
        terminal_replay: matches!(link, crate::runtime::worker::AttemptLink::Replay { .. }),
    }));
    if !send_ok(&mut ws_tx, &opened).await {
        return;
    }

    let (tx, mut rx) = mpsc::channel::<Result<pb::AttemptDown, Status>>(64);
    let limits = runtime.config().limits;
    let (link, mut inputs_abort) = match link {
        crate::runtime::worker::AttemptLink::Live {
            lease_id,
            resume_from,
            inputs,
        } => {
            let abort = inputs.map(|plan| {
                tokio::spawn(send_inputs(
                    plan,
                    tx.clone(),
                    limits.max_chunk_bytes as usize,
                ))
                .abort_handle()
            });
            (
                crate::runtime::worker::AttemptLink::Live {
                    lease_id,
                    resume_from,
                    inputs: None,
                },
                abort,
            )
        }
        replay => (replay, None),
    };

    let (inbound_tx, inbound_rx) =
        mpsc::channel::<(Result<pb::AttemptUp, Status>, usize)>(WS_ATTEMPT_INBOUND_SLOTS);
    let queued_bytes = Arc::new(AtomicUsize::new(0));
    let drained = Arc::new(Notify::new());
    let window_bytes = (limits.attempt_window_bytes as usize).max(1);
    let mut inbound_tx = Some(inbound_tx);
    let mut upload_task = tokio::spawn(receive_uploads(
        runtime,
        link,
        MeteredInbound {
            rx: inbound_rx,
            queued_bytes: Arc::clone(&queued_bytes),
            drained: Arc::clone(&drained),
        },
        tx,
        limits,
        timeout,
        false,
    ));
    let mut upload_done = false;
    let mut pending_up: Option<(Result<pb::AttemptUp, Status>, usize)> = None;

    loop {
        let mut close_inbound = false;
        let window_wait = drained.notified();
        tokio::pin!(window_wait);
        window_wait.as_mut().enable();
        let can_read = pending_up.is_none() && queued_bytes.load(Ordering::Acquire) < window_bytes;
        let should_exit = tokio::select! {
            _ = &mut upload_task, if !upload_done => {
                upload_done = true;
                if let Some(abort) = inputs_abort.take() {
                    abort.abort();
                }
                false
            }
            down_msg = rx.recv() => {
                match down_msg {
                    Some(Ok(msg)) => !send_ok(&mut ws_tx, &msg).await,
                    Some(Err(status)) => {
                        send_error_and_close(&mut ws_tx, &status).await;
                        true
                    }
                    None => {
                        send_close(&mut ws_tx).await;
                        true
                    }
                }
            }
            _ = ping_interval.tick() => {
                ws_tx.send(Message::Ping(Vec::new())).await.is_err()
            }
            _ = &mut window_wait, if pending_up.is_none() && !can_read && inbound_tx.is_some() => {
                false
            }
            frame = ws_rx.next(), if can_read => {
                match frame {
                    Some(Ok(Message::Binary(bytes))) => {
                        let frame_bytes = bytes.len().max(1);
                        match pb::AttemptUp::decode(bytes.as_slice()) {
                            Ok(up) => {
                                if let Some(tx_in) = inbound_tx.as_ref() {
                                    queued_bytes.fetch_add(frame_bytes, Ordering::AcqRel);
                                    match tx_in.try_send((Ok(up), frame_bytes)) {
                                        Ok(()) => {}
                                        Err(mpsc::error::TrySendError::Full(item)) => {
                                            queued_bytes.fetch_sub(frame_bytes, Ordering::AcqRel);
                                            pending_up = Some(item);
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            queued_bytes.fetch_sub(frame_bytes, Ordering::AcqRel);
                                            close_inbound = true;
                                        }
                                    }
                                }
                                false
                            }
                            Err(err) => {
                                send_error_and_close(
                                    &mut ws_tx,
                                    &Status::invalid_argument(format!("invalid protobuf AttemptUp: {err}")),
                                )
                                .await;
                                true
                            }
                        }
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => false,
                    Some(Ok(Message::Text(_))) => {
                        send_error_and_close(
                            &mut ws_tx,
                            &Status::invalid_argument("expected binary protobuf frame"),
                        )
                        .await;
                        true
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => true,
                }
            }
            permit = async {
                inbound_tx
                    .as_ref()
                    .expect("checked by select precondition")
                    .reserve()
                    .await
            }, if pending_up.is_some() && inbound_tx.is_some() => {
                match permit {
                    Ok(permit) => {
                        if let Some((item, frame_bytes)) = pending_up.take() {
                            queued_bytes.fetch_add(frame_bytes, Ordering::AcqRel);
                            permit.send((item, frame_bytes));
                        }
                    }
                    Err(_) => {
                        pending_up = None;
                        close_inbound = true;
                    }
                }
                false
            }
        };
        if close_inbound {
            inbound_tx = None;
        }
        if should_exit {
            break;
        }
    }

    if let Some(abort) = inputs_abort.take() {
        abort.abort();
    }
    drop(inbound_tx);
    if !upload_done {
        upload_task.abort();
    }
}

async fn handle_client_invoke(
    runtime: Runtime,
    auth: Arc<AuthPolicy>,
    principal: Principal,
    socket: WebSocket,
    ping_period: Duration,
    trace_headers: Vec<(&'static str, String)>,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let mut ping_interval = new_ping_interval(ping_period);
    let initial_timeout = runtime.config().heartbeat_timeout;

    let first = match recv_initial_proto::<pb::InvokeRequest>(
        &mut ws_rx,
        &mut ws_tx,
        &mut ping_interval,
        Some((initial_timeout, "no Start received")),
    )
    .await
    {
        Ok(msg) => msg,
        Err(Some(status)) => {
            send_error_and_close(&mut ws_tx, &status).await;
            return;
        }
        Err(None) => return,
    };
    let mut start = match first.msg {
        Some(pb::invoke_request::Msg::Start(start)) => start,
        _ => {
            send_error_and_close(
                &mut ws_tx,
                &Status::invalid_argument("first invocation message must be Start"),
            )
            .await;
            return;
        }
    };
    if auth.is_principal_invalidated(&principal, crate::now_ms() / 1000) {
        auth.record_rejection();
        send_error_and_close(
            &mut ws_tx,
            &Status::unauthenticated("bearer token has been revoked or expired"),
        )
        .await;
        return;
    }
    merge_trace_header_pairs(start.spec.as_mut(), &trace_headers);
    let mut execution = match start_invoke(&runtime, principal.namespace.clone(), start).await {
        Ok(execution) => execution,
        Err(status) => {
            send_error_and_close(&mut ws_tx, &status).await;
            return;
        }
    };
    let input = execution.take_input();
    let mut input_done = input.is_none();
    let invalidated = wait_credential_invalidated(&auth, &principal);
    tokio::pin!(invalidated);

    loop {
        tokio::select! {
            biased;
            _ = &mut invalidated => {
                auth.record_rejection();
                send_error_and_close(
                    &mut ws_tx,
                    &Status::unauthenticated("bearer token has been revoked or expired"),
                )
                .await;
                break;
            }
            _ = ping_interval.tick() => {
                if ws_tx.send(Message::Ping(Vec::new())).await.is_err() {
                    break;
                }
            }
            next_event = execution.next() => {
                match next_event {
                    None => {
                        send_error_and_close(
                            &mut ws_tx,
                            &Status::unavailable("execution ended without a terminal event"),
                        )
                        .await;
                        break;
                    }
                    Some(Err(err)) => {
                        send_error_and_close(&mut ws_tx, &Status::from(err)).await;
                        break;
                    }
                    Some(Ok(event)) => {
                        let terminal = matches!(event.kind, api::EventKind::Finished { .. });
                        let proto = event_to_proto(event);
                        if !send_ok(&mut ws_tx, &proto).await {
                            break;
                        }
                        if terminal {
                            send_close(&mut ws_tx).await;
                            break;
                        }
                    }
                }
            }
            frame = ws_rx.next() => {
                match frame {
                    Some(Ok(Message::Binary(bytes))) => {
                        let msg = match pb::InvokeRequest::decode(bytes.as_slice()) {
                            Ok(msg) => msg,
                            Err(err) => {
                                send_error_and_close(
                                    &mut ws_tx,
                                    &Status::invalid_argument(format!("invalid protobuf InvokeRequest: {err}")),
                                )
                                .await;
                                break;
                            }
                        };
                        if let Err(status) = handle_invoke_message(msg, &input, &mut input_done).await {
                            send_error_and_close(&mut ws_tx, &status).await;
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Text(_))) => {
                        send_error_and_close(
                            &mut ws_tx,
                            &Status::invalid_argument("expected binary protobuf frame"),
                        )
                        .await;
                        break;
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                }
            }
        }
    }
}

async fn handle_client_watch(
    runtime: Runtime,
    auth: Arc<AuthPolicy>,
    principal: Principal,
    socket: WebSocket,
    ping_period: Duration,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let mut ping_interval = new_ping_interval(ping_period);
    let initial_timeout = runtime.config().heartbeat_timeout;

    let req = match recv_initial_proto::<pb::WatchRequest>(
        &mut ws_rx,
        &mut ws_tx,
        &mut ping_interval,
        Some((initial_timeout, "no WatchRequest received")),
    )
    .await
    {
        Ok(req) => req,
        Err(Some(status)) => {
            send_error_and_close(&mut ws_tx, &status).await;
            return;
        }
        Err(None) => return,
    };
    if auth.is_principal_invalidated(&principal, crate::now_ms() / 1000) {
        auth.record_rejection();
        send_error_and_close(
            &mut ws_tx,
            &Status::unauthenticated("bearer token has been revoked or expired"),
        )
        .await;
        return;
    }
    let mut events = match runtime.watch_in(&principal.namespace, &req.job_id, req.after_sequence) {
        Ok(events) => events,
        Err(err) => {
            send_error_and_close(&mut ws_tx, &Status::from(err)).await;
            return;
        }
    };
    let invalidated = wait_credential_invalidated(&auth, &principal);
    tokio::pin!(invalidated);

    loop {
        tokio::select! {
            biased;
            _ = &mut invalidated => {
                auth.record_rejection();
                send_error_and_close(
                    &mut ws_tx,
                    &Status::unauthenticated("bearer token has been revoked or expired"),
                )
                .await;
                break;
            }
            _ = ping_interval.tick() => {
                if ws_tx.send(Message::Ping(Vec::new())).await.is_err() {
                    break;
                }
            }
            next_event = events.next() => {
                match next_event {
                    None => {
                        send_close(&mut ws_tx).await;
                        break;
                    }
                    Some(Err(err)) => {
                        send_error_and_close(&mut ws_tx, &Status::from(err)).await;
                        break;
                    }
                    Some(Ok(event)) => {
                        let terminal = matches!(event.kind, api::EventKind::Finished { .. });
                        let proto = event_to_proto(event);
                        if !send_ok(&mut ws_tx, &proto).await {
                            break;
                        }
                        if terminal {
                            send_close(&mut ws_tx).await;
                            break;
                        }
                    }
                }
            }
            frame = ws_rx.next() => {
                match frame {
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {
                        send_error_and_close(
                            &mut ws_tx,
                            &Status::invalid_argument("unexpected watch message"),
                        )
                        .await;
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{NAMESPACE_HEADER, ScopedToken};
    use crate::journal::{BoxFuture, CommitOutcome, JournalBackend, JournalOp, Leadership};
    use crate::runtime::api::{JobState, RuntimeConfig};
    use std::collections::HashMap;
    use tokio_tungstenite::tungstenite::{self, Message as TsMessage, client::IntoClientRequest};

    struct TestServer {
        runtime: Runtime,
        base_http: String,
        base_ws: String,
        http: reqwest::Client,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl TestServer {
        async fn start(config: RuntimeConfig, auth: AuthPolicy) -> Self {
            let runtime = Runtime::start(config).await.unwrap();
            Self::start_with_runtime(runtime, auth, WS_PING_INTERVAL).await
        }

        async fn start_with_runtime(
            runtime: Runtime,
            auth: AuthPolicy,
            ping_interval: Duration,
        ) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let app = router_with_ping_interval(runtime.clone(), Arc::new(auth), ping_interval);
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                runtime,
                base_http: format!("http://{addr}"),
                base_ws: format!("ws://{addr}"),
                http: reqwest::Client::new(),
                task,
            }
        }

        async fn unary<Req: ProstMessage, Resp: ProstMessage + Default>(
            &self,
            path: &str,
            token: Option<&str>,
            namespace: Option<&str>,
            req: &Req,
        ) -> Result<Resp, (StatusCode, i32, String)> {
            let mut builder = self
                .http
                .post(format!("{}{path}", self.base_http))
                .header(header::CONTENT_TYPE, PROTOBUF_CONTENT_TYPE)
                .body(req.encode_to_vec());
            if let Some(token) = token {
                builder = builder.bearer_auth(token);
            }
            if let Some(ns) = namespace {
                builder = builder.header(NAMESPACE_HEADER, ns);
            }
            let resp = builder.send().await.unwrap();
            let status = resp.status();
            if status.is_success() {
                assert_eq!(resp.headers()[header::CONTENT_TYPE], PROTOBUF_CONTENT_TYPE);
                let bytes = resp.bytes().await.unwrap();
                Ok(Resp::decode(bytes).unwrap())
            } else {
                let code = resp.headers()[GRPC_CODE_HEADER]
                    .to_str()
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
                let msg = resp.text().await.unwrap();
                Err((status, code, msg))
            }
        }

        async fn connect_ws(
            &self,
            path: &str,
            token: Option<&str>,
            namespace: Option<&str>,
        ) -> Result<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            (StatusCode, i32, String),
        > {
            let mut req = format!("{}{path}", self.base_ws)
                .into_client_request()
                .unwrap();
            if let Some(token) = token {
                req.headers_mut().insert(
                    header::AUTHORIZATION,
                    format!("Bearer {token}").parse().unwrap(),
                );
            }
            if let Some(ns) = namespace {
                req.headers_mut()
                    .insert(NAMESPACE_HEADER, ns.parse().unwrap());
            }
            match tokio_tungstenite::connect_async(req).await {
                Ok((stream, _)) => Ok(stream),
                Err(tungstenite::Error::Http(resp)) => {
                    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap();
                    let code = resp.headers()[GRPC_CODE_HEADER]
                        .to_str()
                        .unwrap()
                        .parse::<i32>()
                        .unwrap();
                    let body = resp
                        .body()
                        .as_ref()
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_default();
                    Err((status, code, body))
                }
                Err(other) => panic!("unexpected ws connect error: {other}"),
            }
        }
    }

    fn job_spec(interface: &str, input: Vec<u8>) -> pb::JobSpec {
        pb::JobSpec {
            demand: Some(pb::Demand {
                interface: interface.into(),
                ..Default::default()
            }),
            inputs: if input.is_empty() {
                Vec::new()
            } else {
                vec![pb::Object {
                    body: Some(pb::object::Body::Inline(input.into())),
                    metadata: HashMap::new(),
                }]
            },
            policy: Some(pb::ExecutionPolicy {
                durability: pb::Durability::Ephemeral as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn recv_ws_envelope<M: ProstMessage + Default>(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Option<Result<M, (u8, String)>> {
        while let Some(msg) = ws.next().await {
            match msg.unwrap() {
                TsMessage::Binary(bytes) => {
                    let prefix = bytes[0];
                    let payload = &bytes[1..];
                    return Some(match prefix {
                        0x00 => Ok(M::decode(payload).unwrap()),
                        0x01 => Err((
                            payload[0],
                            String::from_utf8(payload[1..].to_vec()).unwrap(),
                        )),
                        other => panic!("unexpected envelope prefix {other:#x}"),
                    });
                }
                TsMessage::Ping(_) | TsMessage::Pong(_) => continue,
                TsMessage::Close(_) => return None,
                other => panic!("unexpected ws message: {other:?}"),
            }
        }
        None
    }

    async fn spawn_ws_worker(
        server: &TestServer,
        token: Option<&str>,
        interface: &str,
    ) -> tokio::task::JoinHandle<()> {
        let mut session = server
            .connect_ws("/ws/v2/worker/session", token, None)
            .await
            .unwrap();
        let hello = pb::WorkerMessage {
            msg: Some(pb::worker_message::Msg::Hello(pb::Hello {
                worker_id: "ws-worker-1".into(),
                interfaces: vec![interface.into()],
                max_concurrency: 2,
                ..Default::default()
            })),
        };
        session
            .send(TsMessage::Binary(hello.encode_to_vec()))
            .await
            .unwrap();
        let welcome = recv_ws_envelope::<pb::ServerMessage>(&mut session)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            welcome.msg,
            Some(pb::server_message::Msg::Welcome(_))
        ));
        let credit = pb::WorkerMessage {
            msg: Some(pb::worker_message::Msg::Credit(pb::Credit {
                free_slots: 2,
            })),
        };
        session
            .send(TsMessage::Binary(credit.encode_to_vec()))
            .await
            .unwrap();

        let base_ws = server.base_ws.clone();
        let token = token.map(str::to_owned);
        tokio::spawn(async move {
            while let Some(Ok(server_msg)) =
                recv_ws_envelope::<pb::ServerMessage>(&mut session).await
            {
                if let Some(pb::server_message::Msg::Assign(assign)) = server_msg.msg {
                    let reply = pb::WorkerMessage {
                        msg: Some(pb::worker_message::Msg::AssignReply(pb::AssignReply {
                            lease_id: assign.lease_id.clone(),
                            accepted: true,
                            reject_reason: String::new(),
                        })),
                    };
                    session
                        .send(TsMessage::Binary(reply.encode_to_vec()))
                        .await
                        .unwrap();

                    let mut req = format!("{base_ws}/ws/v2/worker/attempt")
                        .into_client_request()
                        .unwrap();
                    if let Some(token) = &token {
                        req.headers_mut().insert(
                            header::AUTHORIZATION,
                            format!("Bearer {token}").parse().unwrap(),
                        );
                    }
                    let (mut attempt_ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
                    let open = pb::AttemptUp {
                        msg: Some(pb::attempt_up::Msg::Open(pb::AttemptOpen {
                            lease_id: assign.lease_id.clone(),
                            generation: assign.generation,
                            resume_after_sequence: 0,
                        })),
                    };
                    attempt_ws
                        .send(TsMessage::Binary(open.encode_to_vec()))
                        .await
                        .unwrap();
                    let opened = recv_ws_envelope::<pb::AttemptDown>(&mut attempt_ws)
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(matches!(opened.msg, Some(pb::attempt_down::Msg::Opened(_))));

                    let mut collected = Vec::new();
                    if assign.inputs_via_attempt {
                        loop {
                            let down = recv_ws_envelope::<pb::AttemptDown>(&mut attempt_ws)
                                .await
                                .unwrap()
                                .unwrap();
                            if let Some(pb::attempt_down::Msg::Input(chunk)) = down.msg {
                                if chunk.last {
                                    break;
                                }
                                collected.extend_from_slice(&chunk.data);
                            }
                        }
                    } else {
                        for obj in assign.inline_inputs {
                            if let Some(pb::object::Body::Inline(data)) = obj.body {
                                collected.extend_from_slice(&data);
                            }
                        }
                    }

                    let frame = pb::AttemptUp {
                        msg: Some(pb::attempt_up::Msg::Frame(pb::Frame {
                            sequence: 1,
                            data: b"stream-chunk".to_vec().into(),
                            metadata: HashMap::new(),
                            progress: false,
                        })),
                    };
                    attempt_ws
                        .send(TsMessage::Binary(frame.encode_to_vec()))
                        .await
                        .unwrap();
                    let ack = recv_ws_envelope::<pb::AttemptDown>(&mut attempt_ws)
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(matches!(ack.msg, Some(pb::attempt_down::Msg::Ack(_))));

                    let result = pb::AttemptUp {
                        msg: Some(pb::attempt_up::Msg::Result(pb::ResultChunk {
                            index: 0,
                            data: collected.into(),
                            last: true,
                            metadata: HashMap::new(),
                            uri: String::new(),
                        })),
                    };
                    attempt_ws
                        .send(TsMessage::Binary(result.encode_to_vec()))
                        .await
                        .unwrap();
                    let ack2 = recv_ws_envelope::<pb::AttemptDown>(&mut attempt_ws)
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(matches!(ack2.msg, Some(pb::attempt_down::Msg::Ack(_))));

                    let finish = pb::AttemptUp {
                        msg: Some(pb::attempt_up::Msg::Finish(pb::Finish {
                            outcome: Some(pb::finish::Outcome::Succeeded(pb::Succeeded {})),
                            handler_duration_ms: None,
                        })),
                    };
                    attempt_ws
                        .send(TsMessage::Binary(finish.encode_to_vec()))
                        .await
                        .unwrap();
                    let finished = recv_ws_envelope::<pb::AttemptDown>(&mut attempt_ws)
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(matches!(
                        finished.msg,
                        Some(pb::attempt_down::Msg::Finished(_))
                    ));
                    // Server closes attempt WS once AttemptDown channel ends.
                    assert!(
                        recv_ws_envelope::<pb::AttemptDown>(&mut attempt_ws)
                            .await
                            .is_none()
                    );
                }
            }
        })
    }

    #[test]
    fn grpc_code_maps_to_expected_http_statuses() {
        let cases = [
            (tonic::Code::InvalidArgument, StatusCode::BAD_REQUEST),
            (tonic::Code::OutOfRange, StatusCode::BAD_REQUEST),
            (tonic::Code::Unauthenticated, StatusCode::UNAUTHORIZED),
            (tonic::Code::PermissionDenied, StatusCode::FORBIDDEN),
            (tonic::Code::NotFound, StatusCode::NOT_FOUND),
            (tonic::Code::AlreadyExists, StatusCode::CONFLICT),
            (tonic::Code::Aborted, StatusCode::CONFLICT),
            (
                tonic::Code::FailedPrecondition,
                StatusCode::PRECONDITION_FAILED,
            ),
            (
                tonic::Code::ResourceExhausted,
                StatusCode::TOO_MANY_REQUESTS,
            ),
            (tonic::Code::Unimplemented, StatusCode::NOT_IMPLEMENTED),
            (tonic::Code::Unavailable, StatusCode::SERVICE_UNAVAILABLE),
            (tonic::Code::DeadlineExceeded, StatusCode::GATEWAY_TIMEOUT),
            (tonic::Code::Internal, StatusCode::INTERNAL_SERVER_ERROR),
        ];
        for (code, expected_http) in cases {
            let resp = status_response(Status::new(code, "detail"));
            assert_eq!(resp.status(), expected_http);
            assert_eq!(
                resp.headers()[GRPC_CODE_HEADER],
                (code as i32).to_string().as_str()
            );
        }
    }

    #[tokio::test]
    async fn unary_rpc_routes_and_auth_isolation() {
        let auth =
            AuthPolicy::new(["op-client".into()], ["op-worker".into()]).with_scoped_tokens([
                ScopedToken {
                    token: "tenant-client".into(),
                    role: Role::Client,
                    namespace: "tenant-a".into(),
                    subject: "tenant".into(),
                    forced_pool: None,
                },
            ]);
        let server = TestServer::start(RuntimeConfig::default(), auth).await;

        // Unauthenticated -> 401 + Unauthenticated (16)
        let err = server
            .unary::<pb::GetJobRequest, pb::Job>(
                "/rpc/v2/client/get_job",
                None,
                None,
                &pb::GetJobRequest {
                    job_id: "missing".into(),
                    include_outputs: false,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::UNAUTHORIZED);
        assert_eq!(err.1, tonic::Code::Unauthenticated as i32);

        // Worker token on client unary route -> 403 + PermissionDenied (7)
        let err = server
            .unary::<pb::SubmitRequest, pb::SubmitResponse>(
                "/rpc/v2/client/submit",
                Some("op-worker"),
                None,
                &pb::SubmitRequest {
                    spec: Some(job_spec("demo.unary", b"hi".to_vec())),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert_eq!(err.1, tonic::Code::PermissionDenied as i32);

        // Scoped tenant client token succeeds and isolates namespace.
        let submit_resp: pb::SubmitResponse = server
            .unary(
                "/rpc/v2/client/submit",
                Some("tenant-client"),
                Some("spoofed"),
                &pb::SubmitRequest {
                    job_id: "job-1".into(),
                    spec: Some(job_spec("demo.unary", b"hello".to_vec())),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(submit_resp.created);
        let job = submit_resp.job.unwrap();
        assert_eq!(job.job_id, "job-1");
        assert_eq!(job.namespace, "tenant-a");

        // GetJob from default namespace does not see tenant-a job -> 404 NotFound (5)
        let err = server
            .unary::<pb::GetJobRequest, pb::Job>(
                "/rpc/v2/client/get_job",
                Some("op-client"),
                None,
                &pb::GetJobRequest {
                    job_id: "job-1".into(),
                    include_outputs: false,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::NOT_FOUND);
        assert_eq!(err.1, tonic::Code::NotFound as i32);

        let listed: pb::ListJobsResponse = server
            .unary(
                "/rpc/v2/client/list_jobs",
                Some("tenant-client"),
                None,
                &pb::ListJobsRequest {
                    states: vec![pb::JobState::Queued as i32],
                    limit: 10,
                    page_token: String::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(listed.jobs.len(), 1);

        let cancelled: pb::Job = server
            .unary(
                "/rpc/v2/client/cancel",
                Some("tenant-client"),
                None,
                &pb::CancelRequest {
                    job_id: "job-1".into(),
                    reason: "done".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(cancelled.state, pb::JobState::Cancelled as i32);
    }

    #[tokio::test]
    async fn unary_accepts_payloads_above_axum_default_2mib_limit() {
        let server = TestServer::start(RuntimeConfig::default(), AuthPolicy::default()).await;
        let payload = vec![b'x'; 3 * 1024 * 1024];
        let resp: pb::SubmitResponse = server
            .unary(
                "/rpc/v2/client/submit",
                None,
                None,
                &pb::SubmitRequest {
                    spec: Some(job_spec("demo.big", payload)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(resp.created);
    }

    #[tokio::test]
    async fn ws_pre_upgrade_rejects_auth_and_standby_without_upgrading() {
        struct StandbyBackend;
        impl JournalBackend for StandbyBackend {
            fn try_acquire_leadership(&self) -> BoxFuture<'_, anyhow::Result<Option<Leadership>>> {
                Box::pin(async { Ok(None) })
            }
            fn standby_poll_interval(&self) -> Duration {
                Duration::from_secs(60)
            }
            fn enqueue(
                &self,
                _term: u64,
                _op: JournalOp,
            ) -> Result<tokio::sync::oneshot::Receiver<CommitOutcome>, crate::error::RuntimeError>
            {
                unreachable!()
            }
            fn spec_hash<'a>(
                &'a self,
                _: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<String>>> {
                Box::pin(async { Ok(None) })
            }
            fn receipt<'a>(
                &'a self,
                _: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<(String, String)>>> {
                Box::pin(async { Ok(None) })
            }
            fn load_job<'a>(
                &'a self,
                _: &'a str,
                _: bool,
                _: Arc<crate::runtime::events::ByteBudget>,
            ) -> BoxFuture<'a, anyhow::Result<Option<crate::journal::StoredJob>>> {
                Box::pin(async { Ok(None) })
            }
            fn load_job_preview<'a>(
                &'a self,
                _: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<crate::journal::StoredJobPreview>>>
            {
                Box::pin(async { Ok(None) })
            }
            fn scan_jobs<'a>(
                &'a self,
                _: u64,
                _: u64,
                _: Option<(u64, String)>,
                _: Option<&'a str>,
                _: usize,
            ) -> BoxFuture<'a, anyhow::Result<Vec<crate::journal::StoredJob>>> {
                Box::pin(async { Ok(Vec::new()) })
            }
            fn purge<'a>(&'a self, _: u64, _: u64) -> BoxFuture<'a, anyhow::Result<usize>> {
                Box::pin(async { Ok(0) })
            }
            fn stats(&self) -> api::JournalStats {
                api::JournalStats::default()
            }
            fn close(&self) -> BoxFuture<'_, ()> {
                Box::pin(async {})
            }
        }

        let auth = AuthPolicy::new(["c-tok".into()], ["w-tok".into()]);
        let server = TestServer::start(RuntimeConfig::default(), auth.clone()).await;

        for path in [
            "/ws/v2/worker/session",
            "/ws/v2/worker/attempt",
            "/ws/v2/client/invoke",
            "/ws/v2/client/watch",
        ] {
            let err = server.connect_ws(path, None, None).await.unwrap_err();
            assert_eq!(err.0, StatusCode::UNAUTHORIZED);
            assert_eq!(err.1, tonic::Code::Unauthenticated as i32);
        }

        // Role mismatch rejected pre-upgrade with 403 + PermissionDenied.
        let err = server
            .connect_ws("/ws/v2/worker/session", Some("c-tok"), None)
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert_eq!(err.1, tonic::Code::PermissionDenied as i32);

        let err = server
            .connect_ws("/ws/v2/client/invoke", Some("w-tok"), None)
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert_eq!(err.1, tonic::Code::PermissionDenied as i32);

        // Standby replica rejects pre-upgrade with 503 + Unavailable.
        let standby_runtime =
            Runtime::start_with_backend(RuntimeConfig::default(), Some(Arc::new(StandbyBackend)))
                .await
                .unwrap();
        let standby = TestServer::start_with_runtime(
            standby_runtime,
            AuthPolicy::default(),
            WS_PING_INTERVAL,
        )
        .await;
        let err = standby
            .connect_ws("/ws/v2/client/invoke", None, None)
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(err.1, tonic::Code::Unavailable as i32);
    }

    #[tokio::test]
    async fn ws_worker_session_attempt_and_streaming_invoke_round_trip() {
        let auth = AuthPolicy::new(["client-tok".into()], ["worker-tok".into()]);
        let server = TestServer::start(RuntimeConfig::default(), auth).await;
        let worker_task = spawn_ws_worker(&server, Some("worker-tok"), "ws.echo").await;

        let mut invoke = server
            .connect_ws("/ws/v2/client/invoke", Some("client-tok"), None)
            .await
            .unwrap();
        let start = pb::InvokeRequest {
            msg: Some(pb::invoke_request::Msg::Start(pb::InvokeStart {
                spec: Some(job_spec("ws.echo", b"base-".to_vec())),
                streaming_input: true,
            })),
        };
        invoke
            .send(TsMessage::Binary(start.encode_to_vec()))
            .await
            .unwrap();

        let accepted = recv_ws_envelope::<pb::ExecutionEvent>(&mut invoke)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            accepted.event,
            Some(pb::execution_event::Event::Accepted(_))
        ));

        let chunk = pb::InvokeRequest {
            msg: Some(pb::invoke_request::Msg::Input(pb::InputChunk {
                data: b"tail".to_vec().into(),
                metadata: HashMap::new(),
            })),
        };
        invoke
            .send(TsMessage::Binary(chunk.encode_to_vec()))
            .await
            .unwrap();
        let input_end = pb::InvokeRequest {
            msg: Some(pb::invoke_request::Msg::InputEnd(pb::InputEnd {})),
        };
        invoke
            .send(TsMessage::Binary(input_end.encode_to_vec()))
            .await
            .unwrap();

        let mut output_frames = Vec::new();
        let mut finished_outputs = Vec::new();
        while let Some(res) = recv_ws_envelope::<pb::ExecutionEvent>(&mut invoke).await {
            let ev = res.unwrap();
            match ev.event.unwrap() {
                pb::execution_event::Event::Output(out) => {
                    output_frames.extend_from_slice(&out.data);
                }
                pb::execution_event::Event::Finished(fin) => {
                    assert_eq!(fin.state, pb::JobState::Succeeded as i32);
                    for obj in fin.outputs {
                        if let Some(pb::object::Body::Inline(bytes)) = obj.body {
                            finished_outputs.extend_from_slice(&bytes);
                        }
                    }
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(output_frames, b"stream-chunk");
        assert_eq!(finished_outputs, b"base-tail");
        // Immediately after Finished, server closes WS cleanly.
        assert!(
            recv_ws_envelope::<pb::ExecutionEvent>(&mut invoke)
                .await
                .is_none()
        );
        worker_task.abort();
    }

    #[tokio::test]
    async fn ws_invoke_disconnect_after_input_end_cancels_job() {
        let server = TestServer::start(RuntimeConfig::default(), AuthPolicy::default()).await;

        // Connect a worker session that accepts an assignment but never completes it.
        let mut session = server
            .connect_ws("/ws/v2/worker/session", None, None)
            .await
            .unwrap();
        let hello = pb::WorkerMessage {
            msg: Some(pb::worker_message::Msg::Hello(pb::Hello {
                worker_id: "hold-worker".into(),
                interfaces: vec!["ws.cancel".into()],
                max_concurrency: 1,
                ..Default::default()
            })),
        };
        session
            .send(TsMessage::Binary(hello.encode_to_vec()))
            .await
            .unwrap();
        let _welcome = recv_ws_envelope::<pb::ServerMessage>(&mut session)
            .await
            .unwrap()
            .unwrap();
        let credit = pb::WorkerMessage {
            msg: Some(pb::worker_message::Msg::Credit(pb::Credit {
                free_slots: 1,
            })),
        };
        session
            .send(TsMessage::Binary(credit.encode_to_vec()))
            .await
            .unwrap();

        let mut invoke = server
            .connect_ws("/ws/v2/client/invoke", None, None)
            .await
            .unwrap();
        let start = pb::InvokeRequest {
            msg: Some(pb::invoke_request::Msg::Start(pb::InvokeStart {
                spec: Some(job_spec("ws.cancel", Vec::new())),
                streaming_input: true,
            })),
        };
        invoke
            .send(TsMessage::Binary(start.encode_to_vec()))
            .await
            .unwrap();
        let accepted = recv_ws_envelope::<pb::ExecutionEvent>(&mut invoke)
            .await
            .unwrap()
            .unwrap();
        let job_id = accepted.job_id.clone();

        // Send InputEnd so input_done becomes true, then close WS!
        let end = pb::InvokeRequest {
            msg: Some(pb::invoke_request::Msg::InputEnd(pb::InputEnd {})),
        };
        invoke
            .send(TsMessage::Binary(end.encode_to_vec()))
            .await
            .unwrap();
        // Wait for worker to receive Assign so job is running.
        let assign = recv_ws_envelope::<pb::ServerMessage>(&mut session)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            assign.msg,
            Some(pb::server_message::Msg::Assign(_))
        ));

        invoke.send(TsMessage::Close(None)).await.unwrap();
        drop(invoke);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if server.runtime.get_job(&job_id, false).await.unwrap().state
                    == JobState::Cancelled
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("invoke WS close must cancel running job");
    }

    #[tokio::test]
    async fn ws_watch_streams_events_and_errors_use_0x01_envelope() {
        let server = TestServer::start(RuntimeConfig::default(), AuthPolicy::default()).await;

        // Watch non-existent job -> 0x01 error frame with NotFound (5) + Close.
        let mut bad_watch = server
            .connect_ws("/ws/v2/client/watch", None, None)
            .await
            .unwrap();
        bad_watch
            .send(TsMessage::Binary(
                pb::WatchRequest {
                    job_id: "missing-job".into(),
                    after_sequence: 0,
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();
        let (code, msg) = recv_ws_envelope::<pb::ExecutionEvent>(&mut bad_watch)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(code, tonic::Code::NotFound as u8);
        assert!(msg.contains("missing-job"));
        assert!(
            recv_ws_envelope::<pb::ExecutionEvent>(&mut bad_watch)
                .await
                .is_none()
        );

        // Submit + cancel job, then watch from sequence 0 -> Finished + Close.
        let sub: pb::SubmitResponse = server
            .unary(
                "/rpc/v2/client/submit",
                None,
                None,
                &pb::SubmitRequest {
                    job_id: "watch-job".into(),
                    spec: Some(job_spec("ws.watch", b"payload".to_vec())),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(sub.created);
        let _: pb::Job = server
            .unary(
                "/rpc/v2/client/cancel",
                None,
                None,
                &pb::CancelRequest {
                    job_id: "watch-job".into(),
                    reason: "cancelled for watch test".into(),
                },
            )
            .await
            .unwrap();

        let mut watch = server
            .connect_ws("/ws/v2/client/watch", None, None)
            .await
            .unwrap();
        watch
            .send(TsMessage::Binary(
                pb::WatchRequest {
                    job_id: "watch-job".into(),
                    after_sequence: 0,
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();
        let mut saw_finished = false;
        while let Some(res) = recv_ws_envelope::<pb::ExecutionEvent>(&mut watch).await {
            if let Some(pb::execution_event::Event::Finished(fin)) = res.unwrap().event {
                assert_eq!(fin.state, pb::JobState::Cancelled as i32);
                saw_finished = true;
                break;
            }
        }
        assert!(saw_finished);
        assert!(
            recv_ws_envelope::<pb::ExecutionEvent>(&mut watch)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn ws_sends_periodic_pings_and_times_out_missing_hello() {
        let cfg = RuntimeConfig {
            heartbeat_timeout: Duration::from_millis(80),
            ..Default::default()
        };
        let runtime = Runtime::start(cfg).await.unwrap();
        let server = TestServer::start_with_runtime(
            runtime,
            AuthPolicy::default(),
            Duration::from_millis(20),
        )
        .await;

        // Worker session that doesn't send Hello times out with 0x01 DeadlineExceeded (4) + Close,
        // and receives at least one Ping before timing out.
        let mut ws = server
            .connect_ws("/ws/v2/worker/session", None, None)
            .await
            .unwrap();
        let mut saw_ping = false;
        let mut saw_timeout_err = false;
        while let Some(msg) = ws.next().await {
            match msg.unwrap() {
                TsMessage::Ping(_) => saw_ping = true,
                TsMessage::Binary(bytes) => {
                    assert_eq!(bytes[0], 0x01);
                    assert_eq!(bytes[1], tonic::Code::DeadlineExceeded as u8);
                    saw_timeout_err = true;
                }
                TsMessage::Close(_) => break,
                _ => {}
            }
        }
        assert!(saw_ping);
        assert!(saw_timeout_err);
    }

    #[tokio::test]
    async fn ws_invoke_non_streaming_disconnect_cancels_and_stale_attempt_fences() {
        let server = TestServer::start(RuntimeConfig::default(), AuthPolicy::default()).await;

        // Stale lease on /ws/v2/worker/attempt returns 0x00 || AttemptFinished(Fenced) + Close.
        let mut attempt = server
            .connect_ws("/ws/v2/worker/attempt", None, None)
            .await
            .unwrap();
        let open = pb::AttemptUp {
            msg: Some(pb::attempt_up::Msg::Open(pb::AttemptOpen {
                lease_id: "nonexistent-lease".into(),
                generation: 999,
                resume_after_sequence: 0,
            })),
        };
        attempt
            .send(TsMessage::Binary(open.encode_to_vec()))
            .await
            .unwrap();
        let down = recv_ws_envelope::<pb::AttemptDown>(&mut attempt)
            .await
            .unwrap()
            .unwrap();
        match down.msg {
            Some(pb::attempt_down::Msg::Finished(fin)) => {
                assert_eq!(fin.status, pb::attempt_finished::Status::Fenced as i32);
            }
            other => panic!("expected Fenced AttemptFinished, got {other:?}"),
        }
        assert!(
            recv_ws_envelope::<pb::AttemptDown>(&mut attempt)
                .await
                .is_none()
        );

        // Non-streaming invoke that closes WS before completion cancels the job.
        let mut invoke = server
            .connect_ws("/ws/v2/client/invoke", None, None)
            .await
            .unwrap();
        let start = pb::InvokeRequest {
            msg: Some(pb::invoke_request::Msg::Start(pb::InvokeStart {
                spec: Some(job_spec("ws.nonstream.cancel", b"hi".to_vec())),
                streaming_input: false,
            })),
        };
        invoke
            .send(TsMessage::Binary(start.encode_to_vec()))
            .await
            .unwrap();
        let accepted = recv_ws_envelope::<pb::ExecutionEvent>(&mut invoke)
            .await
            .unwrap()
            .unwrap();
        let job_id = accepted.job_id.clone();

        drop(invoke);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if server.runtime.get_job(&job_id, false).await.unwrap().state
                    == JobState::Cancelled
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("non-streaming invoke WS disconnect must cancel queued job");
    }

    #[tokio::test]
    async fn http_pooled_execute_completes_and_cross_tenant_attempt_is_fenced() {
        let policy = AuthPolicy::new(vec!["op-c".into()], Vec::new()).with_scoped_tokens([
            crate::auth::ScopedToken {
                token: "c-alpha".into(),
                role: Role::Client,
                namespace: "alpha".into(),
                subject: "c-a".into(),
                forced_pool: None,
            },
            crate::auth::ScopedToken {
                token: "w-alpha".into(),
                role: Role::Worker,
                namespace: "alpha".into(),
                subject: "w-a".into(),
                forced_pool: None,
            },
            crate::auth::ScopedToken {
                token: "w-beta".into(),
                role: Role::Worker,
                namespace: "beta".into(),
                subject: "w-b".into(),
                forced_pool: None,
            },
        ]);
        let server = TestServer::start(RuntimeConfig::default(), policy).await;

        // Connect worker in alpha via WS session.
        let mut ws_worker = server
            .connect_ws("/ws/v2/worker/session", Some("w-alpha"), None)
            .await
            .unwrap();
        ws_worker
            .send(TsMessage::Binary(
                pb::WorkerMessage {
                    msg: Some(pb::worker_message::Msg::Hello(pb::Hello {
                        worker_id: "w1".into(),
                        interfaces: vec!["fast.echo".into()],
                        max_concurrency: 2,
                        ..Default::default()
                    })),
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();
        let _welcome = recv_ws_envelope::<pb::ServerMessage>(&mut ws_worker)
            .await
            .unwrap()
            .unwrap();
        ws_worker
            .send(TsMessage::Binary(
                pb::WorkerMessage {
                    msg: Some(pb::worker_message::Msg::Credit(pb::Credit {
                        free_slots: 2,
                    })),
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();

        // Call POST /rpc/v2/client/execute in namespace alpha.
        let http = reqwest::Client::new();
        let start = pb::InvokeStart {
            spec: Some(job_spec("fast.echo", b"ping".to_vec())),
            streaming_input: false,
        };
        let url = format!("{}/rpc/v2/client/execute", server.base_http);
        let req_task = tokio::spawn(async move {
            http.post(url)
                .header("content-type", PROTOBUF_CONTENT_TYPE)
                .header("authorization", "Bearer c-alpha")
                .body(start.encode_to_vec())
                .send()
                .await
                .unwrap()
        });

        let assign = match recv_ws_envelope::<pb::ServerMessage>(&mut ws_worker)
            .await
            .unwrap()
            .unwrap()
            .msg
        {
            Some(pb::server_message::Msg::Assign(assign)) => assign,
            other => panic!("expected Assign, got {other:?}"),
        };

        // Cross-tenant worker (beta) tries to open alpha's lease via /ws/v2/worker/attempt.
        let mut bad_attempt = server
            .connect_ws("/ws/v2/worker/attempt", Some("w-beta"), None)
            .await
            .unwrap();
        bad_attempt
            .send(TsMessage::Binary(
                pb::AttemptUp {
                    msg: Some(pb::attempt_up::Msg::Open(pb::AttemptOpen {
                        lease_id: assign.lease_id.clone(),
                        generation: assign.generation,
                        resume_after_sequence: 0,
                    })),
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();
        let bad_down = recv_ws_envelope::<pb::AttemptDown>(&mut bad_attempt)
            .await
            .unwrap()
            .unwrap();
        match bad_down.msg {
            Some(pb::attempt_down::Msg::Finished(fin)) => {
                assert_eq!(fin.status, pb::attempt_finished::Status::Fenced as i32);
            }
            other => panic!("expected Fenced, got {other:?}"),
        }

        // Open with w-alpha and finish with "pong".
        let mut ok_attempt = server
            .connect_ws("/ws/v2/worker/attempt", Some("w-alpha"), None)
            .await
            .unwrap();
        ok_attempt
            .send(TsMessage::Binary(
                pb::AttemptUp {
                    msg: Some(pb::attempt_up::Msg::Open(pb::AttemptOpen {
                        lease_id: assign.lease_id,
                        generation: assign.generation,
                        resume_after_sequence: 0,
                    })),
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();
        let _opened = recv_ws_envelope::<pb::AttemptDown>(&mut ok_attempt)
            .await
            .unwrap()
            .unwrap();
        ok_attempt
            .send(TsMessage::Binary(
                pb::AttemptUp {
                    msg: Some(pb::attempt_up::Msg::Result(pb::ResultChunk {
                        index: 0,
                        data: b"pong".to_vec().into(),
                        last: true,
                        metadata: HashMap::new(),
                        uri: String::new(),
                    })),
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();
        let _ack = recv_ws_envelope::<pb::AttemptDown>(&mut ok_attempt)
            .await
            .unwrap()
            .unwrap();
        ok_attempt
            .send(TsMessage::Binary(
                pb::AttemptUp {
                    msg: Some(pb::attempt_up::Msg::Finish(pb::Finish {
                        outcome: Some(pb::finish::Outcome::Succeeded(pb::Succeeded {})),
                        handler_duration_ms: None,
                    })),
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();
        while let Some(Ok(down)) = recv_ws_envelope::<pb::AttemptDown>(&mut ok_attempt).await {
            if matches!(down.msg, Some(pb::attempt_down::Msg::Finished(_))) {
                break;
            }
        }
        let resp = req_task.await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let body = resp.bytes().await.unwrap();
        let ev = pb::ExecutionEvent::decode(body).unwrap();
        match ev.event {
            Some(pb::execution_event::Event::Finished(fin)) => {
                assert_eq!(fin.state, pb::JobState::Succeeded as i32);
                assert_eq!(
                    fin.outputs[0].body,
                    Some(pb::object::Body::Inline(b"pong".to_vec().into()))
                );
            }
            other => panic!("expected Finished, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn revoked_signed_client_token_terminates_active_execute_invoke_and_watch_streams() {
        let key = b"test-client-stream-rev-key-32b!!".to_vec();
        let auth = AuthPolicy::default().with_signing_keys([key.clone()]);
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let server =
            TestServer::start_with_runtime(runtime.clone(), auth.clone(), WS_PING_INTERVAL).await;

        let now_s = crate::now_ms() / 1000;
        let token = crate::auth::sign_token(
            &key,
            &crate::auth::SignedTokenClaims {
                jti: "jti-stream-rev".into(),
                ns: "tenant-s".into(),
                role: "client".into(),
                sub: "gh:streamuser".into(),
                iat: now_s,
                exp: Some(now_s + 3600),
                fp: None,
            },
        )
        .unwrap();

        // Submit a queued job in tenant-s so /ws/v2/client/watch stays open waiting.
        let sub: pb::SubmitResponse = server
            .unary(
                "/rpc/v2/client/submit",
                Some(&token),
                None,
                &pb::SubmitRequest {
                    job_id: "watch-rev-1".into(),
                    spec: Some(job_spec("unserved.rev", b"x".to_vec())),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(sub.created);

        // 1. Open active POST /rpc/v2/client/execute (blocked waiting for worker).
        let http = server.http.clone();
        let exec_url = format!("{}/rpc/v2/client/execute", server.base_http);
        let exec_tok = token.clone();
        let exec_task = tokio::spawn(async move {
            let start = pb::InvokeStart {
                spec: Some(job_spec("unserved.rev", b"hi".to_vec())),
                streaming_input: false,
            };
            http.post(exec_url)
                .header(header::CONTENT_TYPE, PROTOBUF_CONTENT_TYPE)
                .bearer_auth(exec_tok)
                .body(start.encode_to_vec())
                .send()
                .await
                .unwrap()
        });

        // 2. Open active /ws/v2/client/invoke.
        let mut invoke_ws = server
            .connect_ws("/ws/v2/client/invoke", Some(&token), None)
            .await
            .unwrap();
        invoke_ws
            .send(TsMessage::Binary(
                pb::InvokeRequest {
                    msg: Some(pb::invoke_request::Msg::Start(pb::InvokeStart {
                        spec: Some(job_spec("unserved.rev", b"hi".to_vec())),
                        streaming_input: false,
                    })),
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();
        let accepted = recv_ws_envelope::<pb::ExecutionEvent>(&mut invoke_ws)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            accepted.event,
            Some(pb::execution_event::Event::Accepted(_))
        ));

        // 3. Open active /ws/v2/client/watch.
        let mut watch_ws = server
            .connect_ws("/ws/v2/client/watch", Some(&token), None)
            .await
            .unwrap();
        watch_ws
            .send(TsMessage::Binary(
                pb::WatchRequest {
                    job_id: "watch-rev-1".into(),
                    after_sequence: 0,
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();
        let _watch_acc = recv_ws_envelope::<pb::ExecutionEvent>(&mut watch_ws)
            .await
            .unwrap()
            .unwrap();

        // Wait briefly so rpc_execute has entered its select loop, then revoke the JTI!
        tokio::time::sleep(Duration::from_millis(30)).await;
        auth.apply_revocations(crate::auth::RevocationSnapshot {
            version: 1,
            revoked_jtis: vec!["jti-stream-rev".into()],
        });

        let exec_resp = tokio::time::timeout(Duration::from_secs(2), exec_task)
            .await
            .expect("rpc_execute must terminate promptly on revocation")
            .unwrap();
        assert_eq!(exec_resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            exec_resp.headers()[GRPC_CODE_HEADER],
            (tonic::Code::Unauthenticated as i32).to_string().as_str()
        );

        let (inv_code, _) = tokio::time::timeout(
            Duration::from_secs(2),
            recv_ws_envelope::<pb::ExecutionEvent>(&mut invoke_ws),
        )
        .await
        .expect("ws invoke must terminate promptly on revocation")
        .unwrap()
        .unwrap_err();
        assert_eq!(inv_code, tonic::Code::Unauthenticated as u8);

        let (watch_code, _) = tokio::time::timeout(
            Duration::from_secs(2),
            recv_ws_envelope::<pb::ExecutionEvent>(&mut watch_ws),
        )
        .await
        .expect("ws watch must terminate promptly on revocation")
        .unwrap()
        .unwrap_err();
        assert_eq!(watch_code, tonic::Code::Unauthenticated as u8);
    }
}
