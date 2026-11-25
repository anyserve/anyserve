#![allow(
    clippy::result_large_err,
    reason = "HTTP+WS transport preserves typed tonic statuses"
)]
//! HTTP unary + WebSocket streaming transport for `ws://` and `wss://` endpoints.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use futures::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use tonic::{Code, Status};

use crate::BearerAuth;
use crate::execution::{ExecutionEvents, Invocation, RpcStream, v2};

const MAX_FRAME_SIZE: usize = 32 * 1024 * 1024;
const STREAM_CHANNEL_CAPACITY: usize = 64;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_millis(250);

const TAG_MESSAGE: u8 = 0x00;
const TAG_ERROR: u8 = 0x01;

const NAMESPACE_HEADER: &str = "x-anyserve-namespace";
const GRPC_CODE_HEADER: &str = "x-anyserve-grpc-code";

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug)]
pub(crate) enum ExecuteError {
    /// Pre-admission failure safe to fail over to another endpoint (TCP connect
    /// refused or explicit server `x-anyserve-grpc-code: 14` standby/unavailable).
    Retryable(Status),
    /// Mid-flight or terminal failure that must NOT be retried across endpoints.
    Fatal(Status),
}

#[derive(Clone)]
pub(crate) struct HttpWsClient {
    http_base: String,
    ws_base: String,
    http: reqwest::Client,
    auth: BearerAuth,
}

impl HttpWsClient {
    pub(crate) fn new(ws_endpoint: &str, auth: BearerAuth, http: reqwest::Client) -> Result<Self> {
        let ws_base = ws_endpoint.trim_end_matches('/').to_owned();
        let http_base = ws_to_http_base(&ws_base)?;
        Ok(Self {
            http_base,
            ws_base,
            http,
            auth,
        })
    }

    async fn post_proto<Req: ProstMessage>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<(http::StatusCode, http::HeaderMap, bytes::Bytes), reqwest::Error> {
        let url = format!("{}{path}", self.http_base);
        let mut builder = self
            .http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
            .body(req.encode_to_vec());
        if let Some(auth) = self.auth.authorization_header() {
            builder = builder.header(reqwest::header::AUTHORIZATION, auth);
        }
        if let Some(ns) = self.auth.namespace_header() {
            builder = builder.header(NAMESPACE_HEADER, ns);
        }
        let resp = builder.send().await?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.bytes().await?;
        Ok((status, headers, body))
    }

    pub(crate) async fn unary<Req, Resp>(&self, path: &str, req: &Req) -> Result<Resp, Status>
    where
        Req: ProstMessage,
        Resp: ProstMessage + Default,
    {
        let (status, headers, body) = self
            .post_proto(path, req)
            .await
            .map_err(map_reqwest_error)?;
        if !status.is_success() {
            return Err(status_from_http_parts(status, &headers, &body));
        }
        Resp::decode(body)
            .map_err(|err| Status::internal(format!("decode protobuf response from {path}: {err}")))
    }

    pub(crate) async fn invoke(&self, start: v2::InvokeRequest) -> Result<ExecutionEvents, Status> {
        let rx = open_ws_stream::<v2::InvokeRequest, v2::InvokeRequest, v2::ExecutionEvent>(
            &self.ws_base,
            "/ws/v2/client/invoke",
            &self.auth,
            &start,
            None,
            false,
        )
        .await?;
        Ok(ExecutionEvents::new(RpcStream::Ws(rx)))
    }

    pub(crate) async fn execute(
        &self,
        start: &v2::InvokeStart,
    ) -> Result<v2::ExecutionEvent, ExecuteError> {
        let (status, headers, body) = self
            .post_proto("/rpc/v2/client/execute", start)
            .await
            .map_err(|err| {
                let is_connect = err.is_connect();
                let status = map_reqwest_error(err);
                if is_connect {
                    ExecuteError::Retryable(status)
                } else {
                    ExecuteError::Fatal(status)
                }
            })?;
        if !status.is_success() {
            let grpc_status = status_from_http_parts(status, &headers, &body);
            let retryable = explicit_grpc_code(&headers) == Some(Code::Unavailable)
                && !grpc_status
                    .message()
                    .contains("execution ended without a terminal event");
            return Err(if retryable {
                ExecuteError::Retryable(grpc_status)
            } else {
                ExecuteError::Fatal(grpc_status)
            });
        }
        if !is_protobuf_content_type(&headers) || body.is_empty() {
            return Err(ExecuteError::Fatal(Status::internal(
                "unexpected empty or non-protobuf response from /rpc/v2/client/execute",
            )));
        }
        let event = v2::ExecutionEvent::decode(body).map_err(|err| {
            ExecuteError::Fatal(Status::internal(format!(
                "decode protobuf response from /rpc/v2/client/execute: {err}"
            )))
        })?;
        if !matches!(event.event, Some(v2::execution_event::Event::Finished(_))) {
            return Err(ExecuteError::Fatal(Status::internal(
                "response from /rpc/v2/client/execute did not contain a terminal Finished event",
            )));
        }
        Ok(event)
    }

    pub(crate) async fn invoke_stream(
        &self,
        start: v2::InvokeRequest,
        send_queue: usize,
    ) -> Result<Invocation, Status> {
        let (input_tx, input_rx) = mpsc::channel(send_queue);
        let rx = open_ws_stream::<v2::InvokeRequest, v2::InvokeRequest, v2::ExecutionEvent>(
            &self.ws_base,
            "/ws/v2/client/invoke",
            &self.auth,
            &start,
            Some(input_rx),
            false,
        )
        .await?;
        Ok(Invocation {
            input: input_tx,
            events: ExecutionEvents::new(RpcStream::Ws(rx)),
        })
    }

    pub(crate) async fn watch(&self, req: v2::WatchRequest) -> Result<ExecutionEvents, Status> {
        let rx = open_ws_stream::<v2::WatchRequest, v2::WatchRequest, v2::ExecutionEvent>(
            &self.ws_base,
            "/ws/v2/client/watch",
            &self.auth,
            &req,
            None,
            false,
        )
        .await?;
        Ok(ExecutionEvents::new(RpcStream::Ws(rx)))
    }
}

pub(crate) fn build_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(20)))
        .tcp_keepalive_interval(Some(Duration::from_secs(20)))
        .build()
        .context("build anyserve HTTP client")
}

/// Whether reqwest will route through a proxy, so a direct TCP dial proves nothing.
fn has_proxy_env() -> bool {
    const VARS: [&str; 6] = [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ];
    VARS.iter()
        .any(|key| std::env::var(key).is_ok_and(|val| !val.trim().is_empty()))
}

/// Fails fast when the endpoint is unreachable; skipped behind a proxy.
pub(crate) async fn probe_tcp(endpoint: &str) -> Result<()> {
    if has_proxy_env() {
        return Ok(());
    }
    let uri: http::Uri = endpoint
        .parse()
        .with_context(|| format!("parse anyserve endpoint '{endpoint}'"))?;
    let authority = uri
        .authority()
        .ok_or_else(|| anyhow!("anyserve endpoint '{endpoint}' is missing an authority"))?;
    let default_port = match uri.scheme_str() {
        Some(s) if s.eq_ignore_ascii_case("wss") || s.eq_ignore_ascii_case("https") => 443,
        _ => 80,
    };
    let port = authority.port_u16().unwrap_or(default_port);
    let addr = format!("{}:{port}", authority.host());
    tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr))
        .await
        .with_context(|| format!("connect timeout probing '{endpoint}'"))?
        .with_context(|| format!("connect anyserve endpoint '{endpoint}'"))?;
    Ok(())
}

pub(crate) async fn open_worker_session(
    ws_endpoint: &str,
    auth: &BearerAuth,
    hello: &v2::Hello,
    send_queue: usize,
) -> Result<
    (
        mpsc::Sender<v2::WorkerMessage>,
        mpsc::Receiver<Result<v2::ServerMessage, Status>>,
    ),
    Status,
> {
    let ws_base = ws_endpoint.trim_end_matches('/');
    let (control_tx, control_rx) = mpsc::channel(send_queue);
    let initial = v2::WorkerMessage {
        msg: Some(v2::worker_message::Msg::Hello(hello.clone())),
    };
    let messages_rx = open_ws_stream::<v2::WorkerMessage, v2::WorkerMessage, v2::ServerMessage>(
        ws_base,
        "/ws/v2/worker/session",
        auth,
        &initial,
        Some(control_rx),
        true,
    )
    .await?;
    Ok((control_tx, messages_rx))
}

pub(crate) async fn open_worker_attempt(
    ws_endpoint: &str,
    auth: &BearerAuth,
    open: v2::AttemptOpen,
    send_queue: usize,
) -> Result<
    (
        mpsc::Sender<v2::AttemptUp>,
        mpsc::Receiver<Result<v2::AttemptDown, Status>>,
    ),
    Status,
> {
    let ws_base = ws_endpoint.trim_end_matches('/');
    let (send_tx, send_rx) = mpsc::channel(send_queue);
    let initial = v2::AttemptUp {
        msg: Some(v2::attempt_up::Msg::Open(open)),
    };
    let events_rx = open_ws_stream::<v2::AttemptUp, v2::AttemptUp, v2::AttemptDown>(
        ws_base,
        "/ws/v2/worker/attempt",
        auth,
        &initial,
        Some(send_rx),
        false,
    )
    .await?;
    Ok((send_tx, events_rx))
}

fn ws_to_http_base(ws_base: &str) -> Result<String> {
    let lower = ws_base.to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix("wss://") {
        let suffix = &ws_base[ws_base.len() - rest.len()..];
        Ok(format!("https://{suffix}"))
    } else if let Some(rest) = lower.strip_prefix("ws://") {
        let suffix = &ws_base[ws_base.len() - rest.len()..];
        Ok(format!("http://{suffix}"))
    } else {
        Err(anyhow!(
            "expected ws:// or wss:// endpoint, got '{ws_base}'"
        ))
    }
}

fn ws_config() -> WebSocketConfig {
    WebSocketConfig {
        max_frame_size: Some(MAX_FRAME_SIZE),
        max_message_size: Some(anyserve_proto::MAX_MESSAGE_SIZE),
        ..WebSocketConfig::default()
    }
}

async fn connect_ws(ws_base: &str, path: &str, auth: &BearerAuth) -> Result<WsStream, Status> {
    let url = format!("{ws_base}{path}");
    let mut request = url
        .into_client_request()
        .map_err(|err| Status::invalid_argument(format!("invalid websocket URL: {err}")))?;
    if let Some(auth_val) = auth.authorization_header() {
        let header = http::HeaderValue::from_str(auth_val)
            .map_err(|_| Status::invalid_argument("invalid authorization header"))?;
        request
            .headers_mut()
            .insert(http::header::AUTHORIZATION, header);
    }
    if let Some(ns_val) = auth.namespace_header() {
        let header = http::HeaderValue::from_str(ns_val)
            .map_err(|_| Status::invalid_argument("invalid namespace header"))?;
        request.headers_mut().insert(
            http::header::HeaderName::from_static(NAMESPACE_HEADER),
            header,
        );
    }
    let (stream, _resp) = tokio::time::timeout(
        CONNECT_TIMEOUT,
        connect_async_with_config(request, Some(ws_config()), true),
    )
    .await
    .map_err(|_| Status::unavailable(format!("websocket connect timeout to {path}")))?
    .map_err(map_ws_connect_error)?;
    Ok(stream)
}

async fn open_ws_stream<Init, Up, Down>(
    ws_base: &str,
    path: &str,
    auth: &BearerAuth,
    initial: &Init,
    outbound: Option<mpsc::Receiver<Up>>,
    close_on_outbound_end: bool,
) -> Result<mpsc::Receiver<Result<Down, Status>>, Status>
where
    Init: ProstMessage,
    Up: ProstMessage + Send + 'static,
    Down: ProstMessage + Default + Send + 'static,
{
    let mut ws = connect_ws(ws_base, path, auth).await?;
    ws.send(Message::Binary(initial.encode_to_vec()))
        .await
        .map_err(|err| {
            Status::unavailable(format!("send initial websocket frame to {path}: {err}"))
        })?;
    let (down_tx, down_rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
    tokio::spawn(run_ws_pump::<Up, Down>(
        ws,
        outbound,
        down_tx,
        close_on_outbound_end,
    ));
    Ok(down_rx)
}

async fn run_ws_pump<Up, Down>(
    ws: WsStream,
    mut outbound: Option<mpsc::Receiver<Up>>,
    down_tx: mpsc::Sender<Result<Down, Status>>,
    close_on_outbound_end: bool,
) where
    Up: ProstMessage + Send + 'static,
    Down: ProstMessage + Default + Send + 'static,
{
    let (mut ws_tx, mut ws_rx) = ws.split();
    let mut send_close = false;
    loop {
        tokio::select! {
            biased;
            _ = down_tx.closed() => {
                send_close = true;
                break;
            }
            maybe_up = recv_optional(&mut outbound), if outbound.is_some() => {
                match maybe_up {
                    Some(msg) => {
                        if let Err(err) = ws_tx.send(Message::Binary(msg.encode_to_vec())).await {
                            let _ = down_tx.send(Err(map_ws_stream_error(err))).await;
                            break;
                        }
                    }
                    None => {
                        outbound = None;
                        if close_on_outbound_end {
                            send_close = true;
                            break;
                        }
                    }
                }
            }
            maybe_frame = ws_rx.next() => {
                match maybe_frame {
                    Some(Ok(Message::Binary(data))) => match decode_envelope::<Down>(&data) {
                        Ok(msg) => {
                            if down_tx.send(Ok(msg)).await.is_err() {
                                send_close = true;
                                break;
                            }
                        }
                        Err(status) => {
                            let _ = down_tx.send(Err(status)).await;
                            break;
                        }
                    },
                    Some(Ok(Message::Ping(_))) => {
                        // tungstenite queues the Pong reply automatically on read;
                        // flushing pushes it out promptly without duplicating it.
                        let _ = ws_tx.flush().await;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    Some(Ok(Message::Text(_))) => {
                        let _ = down_tx
                            .send(Err(Status::internal("unexpected text websocket frame")))
                            .await;
                        send_close = true;
                        break;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(err)) => {
                        let _ = down_tx.send(Err(map_ws_stream_error(err))).await;
                        break;
                    }
                }
            }
        }
    }
    if send_close {
        let _ = tokio::time::timeout(CLOSE_TIMEOUT, async {
            let _ = ws_tx.send(Message::Close(None)).await;
            let _ = ws_tx.close().await;
        })
        .await;
    }
}

async fn recv_optional<T>(rx: &mut Option<mpsc::Receiver<T>>) -> Option<T> {
    match rx.as_mut() {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

fn decode_envelope<T: ProstMessage + Default>(data: &[u8]) -> Result<T, Status> {
    let Some((&tag, payload)) = data.split_first() else {
        return Err(Status::internal("empty websocket binary frame"));
    };
    match tag {
        TAG_MESSAGE => T::decode(payload)
            .map_err(|err| Status::internal(format!("decode websocket protobuf frame: {err}"))),
        TAG_ERROR => {
            let Some((&code_byte, msg_bytes)) = payload.split_first() else {
                return Err(Status::internal("truncated websocket error frame"));
            };
            let code = if code_byte == 0 {
                Code::Unknown
            } else {
                Code::from_i32(i32::from(code_byte))
            };
            let message = String::from_utf8_lossy(msg_bytes).into_owned();
            Err(Status::new(code, message))
        }
        other => Err(Status::internal(format!(
            "unknown websocket frame tag: {other:#x}"
        ))),
    }
}

fn explicit_grpc_code(headers: &http::HeaderMap) -> Option<Code> {
    headers
        .get(GRPC_CODE_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|raw| raw.trim().parse::<i32>().ok())
        .filter(|&code| code > 0)
        .map(Code::from_i32)
}

fn is_protobuf_content_type(headers: &http::HeaderMap) -> bool {
    headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|raw| raw.split(';').next().unwrap_or("").trim())
        .is_some_and(|ct| {
            ct.eq_ignore_ascii_case("application/x-protobuf")
                || ct.eq_ignore_ascii_case("application/octet-stream")
        })
}

pub(crate) fn status_from_http_parts(
    status: http::StatusCode,
    headers: &http::HeaderMap,
    body: &[u8],
) -> Status {
    let code = explicit_grpc_code(headers).unwrap_or_else(|| fallback_code_from_http(status));
    let message = if body.is_empty() {
        status
            .canonical_reason()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("HTTP {}", status.as_u16()))
    } else {
        String::from_utf8_lossy(body).into_owned()
    };
    Status::new(code, message)
}

fn fallback_code_from_http(status: http::StatusCode) -> Code {
    match status.as_u16() {
        400 => Code::InvalidArgument,
        401 => Code::Unauthenticated,
        403 => Code::PermissionDenied,
        404 => Code::NotFound,
        409 => Code::AlreadyExists,
        412 => Code::FailedPrecondition,
        413 | 429 => Code::ResourceExhausted,
        501 => Code::Unimplemented,
        502 | 503 => Code::Unavailable,
        504 => Code::DeadlineExceeded,
        _ => Code::Internal,
    }
}

fn map_reqwest_error(err: reqwest::Error) -> Status {
    if err.is_builder() {
        Status::invalid_argument(err.to_string())
    } else if err.is_connect() || err.is_timeout() || err.is_request() || err.is_body() {
        Status::unavailable(err.to_string())
    } else {
        Status::internal(err.to_string())
    }
}

fn map_ws_connect_error(err: tungstenite::Error) -> Status {
    match err {
        tungstenite::Error::Http(response) => status_from_http_parts(
            response.status(),
            response.headers(),
            response.body().as_deref().unwrap_or(&[]),
        ),
        tungstenite::Error::Url(err) => Status::invalid_argument(err.to_string()),
        tungstenite::Error::Io(_)
        | tungstenite::Error::Tls(_)
        | tungstenite::Error::ConnectionClosed
        | tungstenite::Error::AlreadyClosed
        | tungstenite::Error::HttpFormat(_)
        | tungstenite::Error::Protocol(_) => Status::unavailable(err.to_string()),
        other => Status::internal(other.to_string()),
    }
}

fn map_ws_stream_error(err: tungstenite::Error) -> Status {
    match err {
        tungstenite::Error::Io(_)
        | tungstenite::Error::Tls(_)
        | tungstenite::Error::ConnectionClosed
        | tungstenite::Error::AlreadyClosed
        | tungstenite::Error::Protocol(
            tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => Status::unavailable(format!("websocket stream error: {err}")),
        tungstenite::Error::Capacity(err) => Status::resource_exhausted(err.to_string()),
        other => Status::internal(format!("websocket stream error: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::extract::ws::{Message as AxumMsg, WebSocket, WebSocketUpgrade};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::Response;
    use axum::routing::{get, post};
    use tokio::sync::Notify;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;
    use crate::execution::{Attempt, AttemptClient, ExecutionClient, WorkerSession};
    use crate::{is_ws_endpoint, parse_endpoints};

    #[derive(Clone, Default)]
    struct MockState {
        name: String,
        unavailable: Arc<AtomicBool>,
        require_client_token: Option<String>,
        require_worker_token: Option<String>,
        expected_namespace: Option<String>,
        calls: Arc<AtomicUsize>,
        live_stream: bool,
        send_ping: bool,
        error_frame: Option<(u8, String)>,
        trailer_error_after_finished: bool,
        ws_dropped: Arc<Notify>,
        ws_input_chunks: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
        attempt_frames: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
        attempt_results: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
        not_found_on_get: bool,
    }

    impl MockState {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_owned(),
                ..Default::default()
            }
        }
    }

    struct TestServer {
        ws_url: String,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn http_error(status: StatusCode, grpc_code: Option<Code>, msg: &str) -> Response {
        let mut builder = Response::builder().status(status);
        if let Some(code) = grpc_code {
            builder = builder.header(GRPC_CODE_HEADER, (code as i32).to_string());
        }
        builder
            .body(axum::body::Body::from(msg.to_owned()))
            .unwrap()
    }

    fn check_auth(state: &MockState, headers: &HeaderMap, is_worker: bool) -> Result<(), Response> {
        if state.unavailable.load(Ordering::SeqCst) {
            return Err(http_error(
                StatusCode::SERVICE_UNAVAILABLE,
                Some(Code::Unavailable),
                "standby replica",
            ));
        }
        if let Some(expected_ns) = &state.expected_namespace {
            let actual = headers.get(NAMESPACE_HEADER).and_then(|v| v.to_str().ok());
            if actual != Some(expected_ns.as_str()) {
                return Err(http_error(
                    StatusCode::BAD_REQUEST,
                    Some(Code::InvalidArgument),
                    "unexpected namespace",
                ));
            }
        }
        let expected = if is_worker {
            state.require_worker_token.as_deref()
        } else {
            state.require_client_token.as_deref()
        };
        let other = if is_worker {
            state.require_client_token.as_deref()
        } else {
            state.require_worker_token.as_deref()
        };
        if let Some(expected_tok) = expected {
            let provided = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "));
            match provided {
                Some(tok) if tok == expected_tok => {}
                Some(tok) if other == Some(tok) => {
                    return Err(http_error(
                        StatusCode::FORBIDDEN,
                        Some(Code::PermissionDenied),
                        "wrong token role",
                    ));
                }
                _ => {
                    return Err(http_error(
                        StatusCode::UNAUTHORIZED,
                        Some(Code::Unauthenticated),
                        "missing or invalid bearer token",
                    ));
                }
            }
        }
        Ok(())
    }

    fn envelope_ok<M: ProstMessage>(msg: &M) -> AxumMsg {
        let mut buf = Vec::with_capacity(1 + msg.encoded_len());
        buf.push(TAG_MESSAGE);
        msg.encode(&mut buf).unwrap();
        AxumMsg::Binary(buf)
    }

    fn envelope_err(code: u8, message: &str) -> AxumMsg {
        let mut buf = Vec::with_capacity(2 + message.len());
        buf.push(TAG_ERROR);
        buf.push(code);
        buf.extend_from_slice(message.as_bytes());
        AxumMsg::Binary(buf)
    }

    fn proto_response<M: ProstMessage>(msg: &M) -> Response {
        Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, "application/x-protobuf")
            .body(axum::body::Body::from(msg.encode_to_vec()))
            .unwrap()
    }

    async fn handle_submit(
        State(state): State<MockState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        state.calls.fetch_add(1, Ordering::SeqCst);
        if let Err(resp) = check_auth(&state, &headers, false) {
            return resp;
        }
        assert_eq!(
            headers
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/x-protobuf")
        );
        let req = v2::SubmitRequest::decode(body).unwrap();
        let job_id = if req.job_id.is_empty() {
            format!("job-{}", state.name)
        } else {
            req.job_id
        };
        proto_response(&v2::SubmitResponse {
            job: Some(v2::Job {
                job_id,
                namespace: state.name.clone(),
                state: v2::JobState::Queued as i32,
                ..Default::default()
            }),
            created: true,
        })
    }

    async fn handle_get_job(
        State(state): State<MockState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        state.calls.fetch_add(1, Ordering::SeqCst);
        if let Err(resp) = check_auth(&state, &headers, false) {
            return resp;
        }
        if state.not_found_on_get {
            return http_error(StatusCode::NOT_FOUND, Some(Code::NotFound), "job missing");
        }
        let req = v2::GetJobRequest::decode(body).unwrap();
        proto_response(&v2::Job {
            job_id: req.job_id,
            state: v2::JobState::Succeeded as i32,
            ..Default::default()
        })
    }

    async fn handle_cancel(
        State(state): State<MockState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        state.calls.fetch_add(1, Ordering::SeqCst);
        if let Err(resp) = check_auth(&state, &headers, false) {
            return resp;
        }
        let req = v2::CancelRequest::decode(body).unwrap();
        proto_response(&v2::Job {
            job_id: req.job_id,
            state: v2::JobState::Cancelled as i32,
            error: Some(v2::ErrorInfo {
                code: "cancelled".into(),
                message: req.reason,
                retryable: false,
            }),
            ..Default::default()
        })
    }

    async fn handle_list_jobs(
        State(state): State<MockState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        state.calls.fetch_add(1, Ordering::SeqCst);
        if let Err(resp) = check_auth(&state, &headers, false) {
            return resp;
        }
        let req = v2::ListJobsRequest::decode(body).unwrap();
        proto_response(&v2::ListJobsResponse {
            jobs: vec![v2::Job {
                job_id: format!("listed-limit-{}", req.limit),
                ..Default::default()
            }],
            next_page_token: "next-tok".into(),
        })
    }

    async fn handle_invoke(
        State(state): State<MockState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        state.calls.fetch_add(1, Ordering::SeqCst);
        if let Err(resp) = check_auth(&state, &headers, false) {
            return resp;
        }
        ws.on_upgrade(move |socket| serve_invoke_ws(socket, state))
    }

    async fn serve_invoke_ws(mut socket: WebSocket, state: MockState) {
        let Some(Ok(AxumMsg::Binary(first))) = socket.recv().await else {
            return;
        };
        let req = v2::InvokeRequest::decode(first.as_slice()).unwrap();
        let streaming_input = match req.msg {
            Some(v2::invoke_request::Msg::Start(start)) => start.streaming_input,
            _ => panic!("expected InvokeStart"),
        };
        if let Some((code, msg)) = &state.error_frame {
            let _ = socket.send(envelope_err(*code, msg)).await;
            let _ = socket.send(AxumMsg::Close(None)).await;
            return;
        }
        if state.send_ping {
            let _ = socket.send(AxumMsg::Ping(b"keepalive".to_vec())).await;
        }
        let _ = socket
            .send(envelope_ok(&v2::ExecutionEvent {
                job_id: state.name.clone(),
                event: Some(v2::execution_event::Event::Accepted(v2::Accepted {})),
                ..Default::default()
            }))
            .await;

        if state.live_stream {
            loop {
                tokio::select! {
                    biased;
                    msg = socket.recv() => {
                        match msg {
                            Some(Ok(AxumMsg::Close(_))) | Some(Err(_)) | None => break,
                            Some(Ok(AxumMsg::Binary(_))) => {}
                            Some(Ok(_)) => {}
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(20)) => {
                        if socket.send(envelope_ok(&v2::ExecutionEvent {
                            job_id: state.name.clone(),
                            event: Some(v2::execution_event::Event::Output(v2::Output::default())),
                            ..Default::default()
                        })).await.is_err() {
                            break;
                        }
                    }
                }
            }
            state.ws_dropped.notify_one();
            return;
        }

        if streaming_input {
            while let Some(Ok(msg)) = socket.recv().await {
                match msg {
                    AxumMsg::Binary(bytes) => {
                        let req = v2::InvokeRequest::decode(bytes.as_slice()).unwrap();
                        match req.msg {
                            Some(v2::invoke_request::Msg::Input(chunk)) => {
                                state
                                    .ws_input_chunks
                                    .lock()
                                    .unwrap()
                                    .push(chunk.data.to_vec());
                            }
                            Some(v2::invoke_request::Msg::InputEnd(_)) => break,
                            _ => {}
                        }
                    }
                    AxumMsg::Close(_) => return,
                    _ => {}
                }
            }
        }

        let _ = socket
            .send(envelope_ok(&v2::ExecutionEvent {
                job_id: state.name.clone(),
                event: Some(v2::execution_event::Event::Output(v2::Output {
                    data: b"out".to_vec().into(),
                    ..Default::default()
                })),
                ..Default::default()
            }))
            .await;
        let _ = socket
            .send(envelope_ok(&v2::ExecutionEvent {
                job_id: state.name.clone(),
                event: Some(v2::execution_event::Event::Finished(
                    v2::ExecutionFinished {
                        state: v2::JobState::Succeeded as i32,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }))
            .await;
        if state.trailer_error_after_finished {
            let _ = socket
                .send(envelope_err(Code::Unavailable as u8, "late stream failure"))
                .await;
        }
        let _ = socket.send(AxumMsg::Close(None)).await;
    }

    async fn handle_watch(
        State(state): State<MockState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        state.calls.fetch_add(1, Ordering::SeqCst);
        if let Err(resp) = check_auth(&state, &headers, false) {
            return resp;
        }
        ws.on_upgrade(move |socket| serve_watch_ws(socket, state))
    }

    async fn serve_watch_ws(mut socket: WebSocket, state: MockState) {
        let Some(Ok(AxumMsg::Binary(first))) = socket.recv().await else {
            return;
        };
        let req = v2::WatchRequest::decode(first.as_slice()).unwrap();
        let _ = socket
            .send(envelope_ok(&v2::ExecutionEvent {
                job_id: req.job_id.clone(),
                event: Some(v2::execution_event::Event::Accepted(v2::Accepted {})),
                ..Default::default()
            }))
            .await;
        if state.live_stream {
            loop {
                tokio::select! {
                    msg = socket.recv() => {
                        match msg {
                            Some(Ok(AxumMsg::Close(_))) | Some(Err(_)) | None => break,
                            _ => {}
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(20)) => {
                        if socket.send(envelope_ok(&v2::ExecutionEvent {
                            job_id: req.job_id.clone(),
                            event: Some(v2::execution_event::Event::Output(v2::Output::default())),
                            ..Default::default()
                        })).await.is_err() {
                            break;
                        }
                    }
                }
            }
            state.ws_dropped.notify_one();
            return;
        }
        let _ = socket
            .send(envelope_ok(&v2::ExecutionEvent {
                job_id: req.job_id,
                event: Some(v2::execution_event::Event::Finished(
                    v2::ExecutionFinished {
                        state: v2::JobState::Succeeded as i32,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }))
            .await;
        let _ = socket.send(AxumMsg::Close(None)).await;
    }

    async fn handle_worker_session(
        State(state): State<MockState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        state.calls.fetch_add(1, Ordering::SeqCst);
        if let Err(resp) = check_auth(&state, &headers, true) {
            return resp;
        }
        ws.on_upgrade(move |socket| serve_worker_session_ws(socket, state))
    }

    async fn serve_worker_session_ws(mut socket: WebSocket, state: MockState) {
        let Some(Ok(AxumMsg::Binary(first))) = socket.recv().await else {
            return;
        };
        let msg = v2::WorkerMessage::decode(first.as_slice()).unwrap();
        let Some(v2::worker_message::Msg::Hello(hello)) = msg.msg else {
            panic!("expected Hello");
        };
        if let Some((code, err)) = &state.error_frame {
            let _ = socket.send(envelope_err(*code, err)).await;
            let _ = socket.send(AxumMsg::Close(None)).await;
            return;
        }
        if state.send_ping {
            let _ = socket.send(AxumMsg::Ping(b"session-ping".to_vec())).await;
        }
        let _ = socket
            .send(envelope_ok(&v2::ServerMessage {
                msg: Some(v2::server_message::Msg::Welcome(v2::Welcome {
                    session_id: format!("sess-{}", hello.worker_id),
                    generation: 1,
                    heartbeat_interval_ms: 1000,
                    heartbeat_timeout_ms: 5000,
                    limits: Some(v2::Limits {
                        max_chunk_bytes: 1024,
                        attempt_window_bytes: 4096,
                        ..Default::default()
                    }),
                    resumed_lease_ids: Vec::new(),
                })),
            }))
            .await;

        while let Some(Ok(msg)) = socket.recv().await {
            match msg {
                AxumMsg::Binary(bytes) => {
                    let wm = v2::WorkerMessage::decode(bytes.as_slice()).unwrap();
                    if matches!(wm.msg, Some(v2::worker_message::Msg::Credit(_))) {
                        let _ = socket
                            .send(envelope_ok(&v2::ServerMessage {
                                msg: Some(v2::server_message::Msg::Assign(v2::Assign {
                                    job_id: "job-ws-1".into(),
                                    lease_id: "lease-ws-1".into(),
                                    attempt_id: "att-1".into(),
                                    interface: "echo.v1".into(),
                                    generation: 1,
                                    inputs_via_attempt: true,
                                    total_deadline_ms: 5000,
                                    execution_deadline_ms: 5000,
                                    ..Default::default()
                                })),
                            }))
                            .await;
                    }
                }
                AxumMsg::Pong(_) => {}
                AxumMsg::Close(_) => break,
                _ => {}
            }
        }
    }

    async fn handle_worker_attempt(
        State(state): State<MockState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        state.calls.fetch_add(1, Ordering::SeqCst);
        if let Err(resp) = check_auth(&state, &headers, true) {
            return resp;
        }
        ws.on_upgrade(move |socket| serve_worker_attempt_ws(socket, state))
    }

    async fn serve_worker_attempt_ws(mut socket: WebSocket, state: MockState) {
        let Some(Ok(AxumMsg::Binary(first))) = socket.recv().await else {
            return;
        };
        let up = v2::AttemptUp::decode(first.as_slice()).unwrap();
        assert!(matches!(up.msg, Some(v2::attempt_up::Msg::Open(_))));
        if let Some((code, err)) = &state.error_frame {
            let _ = socket.send(envelope_err(*code, err)).await;
            let _ = socket.send(AxumMsg::Close(None)).await;
            return;
        }
        let _ = socket
            .send(envelope_ok(&v2::AttemptDown {
                msg: Some(v2::attempt_down::Msg::Opened(v2::AttemptOpened {
                    resume_from_sequence: 0,
                    terminal_replay: false,
                })),
            }))
            .await;
        let _ = socket
            .send(envelope_ok(&v2::AttemptDown {
                msg: Some(v2::attempt_down::Msg::Input(v2::InputData {
                    object_index: Some(0),
                    data: b"hello-input".to_vec().into(),
                    object_end: true,
                    ..Default::default()
                })),
            }))
            .await;
        let _ = socket
            .send(envelope_ok(&v2::AttemptDown {
                msg: Some(v2::attempt_down::Msg::Input(v2::InputData {
                    last: true,
                    ..Default::default()
                })),
            }))
            .await;

        while let Some(Ok(msg)) = socket.recv().await {
            match msg {
                AxumMsg::Binary(bytes) => {
                    let up = v2::AttemptUp::decode(bytes.as_slice()).unwrap();
                    match up.msg {
                        Some(v2::attempt_up::Msg::Frame(frame)) => {
                            state
                                .attempt_frames
                                .lock()
                                .unwrap()
                                .push(frame.data.to_vec());
                            let _ = socket
                                .send(envelope_ok(&v2::AttemptDown {
                                    msg: Some(v2::attempt_down::Msg::Ack(v2::Ack {
                                        up_to_sequence: frame.sequence,
                                        result_bytes: 0,
                                    })),
                                }))
                                .await;
                        }
                        Some(v2::attempt_up::Msg::Result(chunk)) => {
                            state
                                .attempt_results
                                .lock()
                                .unwrap()
                                .push(chunk.data.to_vec());
                        }
                        Some(v2::attempt_up::Msg::Finish(_)) => {
                            let _ = socket
                                .send(envelope_ok(&v2::AttemptDown {
                                    msg: Some(v2::attempt_down::Msg::Finished(
                                        v2::AttemptFinished {
                                            status: v2::attempt_finished::Status::Committed as i32,
                                            reason: String::new(),
                                        },
                                    )),
                                }))
                                .await;
                            let _ = socket.send(AxumMsg::Close(None)).await;
                            break;
                        }
                        _ => {}
                    }
                }
                AxumMsg::Close(_) => break,
                _ => {}
            }
        }
    }

    async fn spawn_mock(state: MockState) -> TestServer {
        let app = Router::new()
            .route("/rpc/v2/client/submit", post(handle_submit))
            .route("/rpc/v2/client/get_job", post(handle_get_job))
            .route("/rpc/v2/client/cancel", post(handle_cancel))
            .route("/rpc/v2/client/list_jobs", post(handle_list_jobs))
            .route("/ws/v2/client/invoke", get(handle_invoke))
            .route("/ws/v2/client/watch", get(handle_watch))
            .route("/ws/v2/worker/session", get(handle_worker_session))
            .route("/ws/v2/worker/attempt", get(handle_worker_attempt))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        TestServer {
            ws_url: format!("ws://{addr}"),
            task,
        }
    }

    #[test]
    fn endpoint_parsing_and_auth_helpers() {
        assert_eq!(
            parse_endpoints(" http://a:1 , https://b:2, ws://c:3/ , wss://d.example/v2 ").unwrap(),
            vec![
                "http://a:1",
                "https://b:2",
                "ws://c:3/",
                "wss://d.example/v2"
            ]
        );
        assert!(is_ws_endpoint("ws://localhost:3001"));
        assert!(is_ws_endpoint("WSS://example.com"));
        assert!(!is_ws_endpoint("http://localhost:3001"));
        assert!(!is_ws_endpoint("https://example.com"));

        assert!(parse_endpoints("").is_err());
        assert!(parse_endpoints("   ,  ").is_err());
        assert!(parse_endpoints("ftp://example.com").is_err());
        assert!(parse_endpoints("ws://").is_err());
        assert!(parse_endpoints("ws:///only-path").is_err());
        assert!(parse_endpoints("localhost:3001").is_err());

        assert_eq!(
            ws_to_http_base("ws://127.0.0.1:3001").unwrap(),
            "http://127.0.0.1:3001"
        );
        assert_eq!(
            ws_to_http_base("WSS://anyserve.example.com/sub").unwrap(),
            "https://anyserve.example.com/sub"
        );
        assert!(ws_to_http_base("http://127.0.0.1:3001").is_err());

        let auth = BearerAuth::with_namespace(Some(" tok-1 "), Some(" ns-1 ")).unwrap();
        assert_eq!(auth.authorization_header(), Some("Bearer tok-1"));
        assert_eq!(auth.namespace_header(), Some("ns-1"));

        let empty = BearerAuth::with_namespace(Some("  "), None).unwrap();
        assert_eq!(empty.authorization_header(), None);
        assert_eq!(empty.namespace_header(), None);
    }

    #[test]
    fn http_status_and_grpc_code_header_decoding() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            GRPC_CODE_HEADER,
            http::HeaderValue::from_static("14"), // Unavailable
        );
        let status = status_from_http_parts(
            http::StatusCode::BAD_GATEWAY,
            &headers,
            b"leader stepping down",
        );
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), "leader stepping down");

        let empty_headers = http::HeaderMap::new();
        let cases = [
            (http::StatusCode::BAD_REQUEST, Code::InvalidArgument),
            (http::StatusCode::UNAUTHORIZED, Code::Unauthenticated),
            (http::StatusCode::FORBIDDEN, Code::PermissionDenied),
            (http::StatusCode::NOT_FOUND, Code::NotFound),
            (http::StatusCode::CONFLICT, Code::AlreadyExists),
            (
                http::StatusCode::PRECONDITION_FAILED,
                Code::FailedPrecondition,
            ),
            (http::StatusCode::PAYLOAD_TOO_LARGE, Code::ResourceExhausted),
            (http::StatusCode::TOO_MANY_REQUESTS, Code::ResourceExhausted),
            (http::StatusCode::NOT_IMPLEMENTED, Code::Unimplemented),
            (http::StatusCode::BAD_GATEWAY, Code::Unavailable),
            (http::StatusCode::SERVICE_UNAVAILABLE, Code::Unavailable),
            (http::StatusCode::GATEWAY_TIMEOUT, Code::DeadlineExceeded),
            (http::StatusCode::INTERNAL_SERVER_ERROR, Code::Internal),
            (http::StatusCode::IM_A_TEAPOT, Code::Internal),
        ];
        for (http_status, expected_code) in cases {
            let decoded = status_from_http_parts(http_status, &empty_headers, b"err");
            assert_eq!(decoded.code(), expected_code, "http status {http_status}");
            assert_eq!(decoded.message(), "err");
        }
    }

    #[tokio::test]
    async fn ws_unary_endpoints_round_trip_and_headers() {
        let mut state = MockState::new("unary");
        state.require_client_token = Some("secret-client".into());
        state.expected_namespace = Some("tenant-x".into());
        let server = spawn_mock(state).await;

        let mut client = ExecutionClient::connect_with_options(
            &format!("{}/", server.ws_url),
            Some("secret-client"),
            Some("tenant-x"),
        )
        .await
        .unwrap();

        let sub = client
            .submit(Some("job-1".into()), v2::JobSpec::default())
            .await
            .unwrap();
        assert!(sub.created);
        assert_eq!(sub.job.unwrap().job_id, "job-1");

        let got = client.get("job-1", true).await.unwrap();
        assert_eq!(got.job_id, "job-1");
        assert_eq!(got.state, v2::JobState::Succeeded as i32);

        let cancelled = client.cancel("job-1", "user abort").await.unwrap();
        assert_eq!(cancelled.state, v2::JobState::Cancelled as i32);
        assert_eq!(cancelled.error.unwrap().message, "user abort");

        let listed = client
            .list(v2::ListJobsRequest {
                limit: 25,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(listed.jobs[0].job_id, "listed-limit-25");
        assert_eq!(listed.next_page_token, "next-tok");
    }

    #[tokio::test]
    async fn ws_invoke_invoke_stream_and_watch_streaming_and_trailer_errors() {
        let mut state = MockState::new("stream");
        state.send_ping = true;
        let server = spawn_mock(state.clone()).await;

        let mut client = ExecutionClient::connect(&server.ws_url).await.unwrap();

        // Non-streaming invoke
        let mut events = client.invoke(v2::JobSpec::default()).await.unwrap();
        assert!(matches!(
            events.message().await.unwrap().unwrap().event,
            Some(v2::execution_event::Event::Accepted(_))
        ));
        assert!(matches!(
            events.message().await.unwrap().unwrap().event,
            Some(v2::execution_event::Event::Output(_))
        ));
        assert!(matches!(
            events.message().await.unwrap().unwrap().event,
            Some(v2::execution_event::Event::Finished(_))
        ));
        assert!(events.message().await.unwrap().is_none());

        // Duplex invoke_stream: drop input after InputEnd and verify WS stays open for Finished.
        let invocation = client.invoke_stream(v2::JobSpec::default()).await.unwrap();
        invocation.send(b"part-1".as_slice()).await.unwrap();
        invocation.send(b"part-2".as_slice()).await.unwrap();
        invocation.finish_input().await.unwrap();
        drop(invocation.input);
        let collected: Vec<_> = invocation
            .events
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(collected.len(), 3);
        assert!(matches!(
            collected.last().unwrap().event,
            Some(v2::execution_event::Event::Finished(_))
        ));
        assert_eq!(
            *state.ws_input_chunks.lock().unwrap(),
            vec![b"part-1".to_vec(), b"part-2".to_vec()]
        );

        // Watch
        let mut watched = client.watch("job-w", 0).await.unwrap();
        assert!(matches!(
            watched.message().await.unwrap().unwrap().event,
            Some(v2::execution_event::Event::Accepted(_))
        ));
        assert!(matches!(
            watched.message().await.unwrap().unwrap().event,
            Some(v2::execution_event::Event::Finished(_))
        ));

        // 0x01 error frame after Finished replaces Finished (matching trailer error semantics).
        let mut err_state = MockState::new("late-err");
        err_state.trailer_error_after_finished = true;
        let err_server = spawn_mock(err_state).await;
        let mut err_client = ExecutionClient::connect(&err_server.ws_url).await.unwrap();
        let mut err_events = err_client.invoke(v2::JobSpec::default()).await.unwrap();
        assert!(matches!(
            err_events.message().await.unwrap().unwrap().event,
            Some(v2::execution_event::Event::Accepted(_))
        ));
        assert!(matches!(
            err_events.message().await.unwrap().unwrap().event,
            Some(v2::execution_event::Event::Output(_))
        ));
        let status = err_events.message().await.unwrap_err();
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), "late stream failure");
    }

    #[tokio::test]
    async fn ws_early_drop_cancels_server_stream_immediately() {
        let mut state = MockState::new("live");
        state.live_stream = true;
        let server = spawn_mock(state.clone()).await;
        let mut client = ExecutionClient::connect(&server.ws_url).await.unwrap();

        // invoke() early drop
        let mut events = client.invoke(v2::JobSpec::default()).await.unwrap();
        assert!(events.message().await.unwrap().is_some());
        drop(events);
        tokio::time::timeout(Duration::from_secs(2), state.ws_dropped.notified())
            .await
            .expect("invoke WS not closed on early drop");

        // invoke_stream() early drop
        let mut inv = client.invoke_stream(v2::JobSpec::default()).await.unwrap();
        assert!(inv.events.message().await.unwrap().is_some());
        drop(inv.input);
        drop(inv.events);
        tokio::time::timeout(Duration::from_secs(2), state.ws_dropped.notified())
            .await
            .expect("invoke_stream WS not closed on early drop");

        // watch() early drop
        let mut watched = client.watch("live-job", 0).await.unwrap();
        assert!(watched.message().await.unwrap().is_some());
        drop(watched);
        tokio::time::timeout(Duration::from_secs(2), state.ws_dropped.notified())
            .await
            .expect("watch WS not closed on early drop");
    }

    #[tokio::test]
    async fn ws_0x01_error_frames_and_worker_attempt_round_trip() {
        // 1. Immediate 0x01 error frames on invoke, worker session, and worker attempt
        let mut err_state = MockState::new("err");
        err_state.error_frame = Some((Code::PermissionDenied as u8, "quota exhausted".into()));
        let err_server = spawn_mock(err_state).await;

        let mut client = ExecutionClient::connect(&err_server.ws_url).await.unwrap();
        let mut events = client.invoke(v2::JobSpec::default()).await.unwrap();
        let status = events.message().await.unwrap_err();
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(status.message(), "quota exhausted");

        let mut session = WorkerSession::connect(&err_server.ws_url, None, v2::Hello::default())
            .await
            .unwrap();
        let ws_err = session.messages.message().await.unwrap_err();
        assert_eq!(ws_err.code(), Code::PermissionDenied);
        assert_eq!(ws_err.message(), "quota exhausted");

        let mut attempt_client = AttemptClient::connect(&err_server.ws_url, None)
            .await
            .unwrap();
        let mut link = attempt_client.open("lease-1".into(), 1, 0).await.unwrap();
        let att_err = link.events.message().await.unwrap_err();
        assert_eq!(att_err.code(), Code::PermissionDenied);
        assert_eq!(att_err.message(), "quota exhausted");

        // 2. Full WorkerSession + Attempt round-trip over ws:// with Ping
        let mut state = MockState::new("worker-ok");
        state.require_worker_token = Some("worker-tok".into());
        state.expected_namespace = Some("ns-w".into());
        state.send_ping = true;
        let server = spawn_mock(state.clone()).await;

        let mut session = WorkerSession::connect_with_namespace(
            &server.ws_url,
            Some("worker-tok"),
            Some("ns-w"),
            v2::Hello {
                worker_id: "w-1".into(),
                interfaces: vec!["echo.v1".into()],
                max_concurrency: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let Some(v2::server_message::Msg::Welcome(welcome)) =
            session.messages.message().await.unwrap().unwrap().msg
        else {
            panic!("expected Welcome");
        };
        assert_eq!(welcome.session_id, "sess-w-1");
        let limits = welcome.limits.unwrap();

        session
            .control
            .send(v2::WorkerMessage {
                msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                    free_slots: 1,
                })),
            })
            .await
            .unwrap();
        let Some(v2::server_message::Msg::Assign(assign)) =
            session.messages.message().await.unwrap().unwrap().msg
        else {
            panic!("expected Assign");
        };

        let mut attempt =
            Attempt::open(&mut session.data, &assign, &limits, Duration::from_secs(2))
                .await
                .unwrap();
        assert_eq!(attempt.inputs.len(), 1);
        assert!(matches!(
            attempt.inputs[0].body,
            Some(v2::object::Body::Inline(ref b)) if b.as_ref() == b"hello-input"
        ));

        attempt
            .frame(Bytes::from_static(b"chunk-a"), HashMap::new(), true)
            .await
            .unwrap();
        attempt
            .finish(
                &[v2::Object {
                    body: Some(v2::object::Body::Inline(Bytes::from_static(b"done-out"))),
                    ..Default::default()
                }],
                v2::finish::Outcome::Succeeded(v2::Succeeded {}),
            )
            .await
            .unwrap();

        assert_eq!(
            *state.attempt_frames.lock().unwrap(),
            vec![b"chunk-a".to_vec()]
        );
        assert_eq!(
            *state.attempt_results.lock().unwrap(),
            vec![b"done-out".to_vec()]
        );
    }

    #[tokio::test]
    async fn ws_auth_401_and_403_do_not_failover() {
        let mut auth_state = MockState::new("auth");
        auth_state.require_client_token = Some("client-tok".into());
        auth_state.require_worker_token = Some("worker-tok".into());
        let auth_server = spawn_mock(auth_state.clone()).await;

        let backup_state = MockState::new("backup");
        let backup_server = spawn_mock(backup_state.clone()).await;

        let endpoints = format!("{}, {}", auth_server.ws_url, backup_server.ws_url);

        // Missing token -> 401 Unauthenticated, never fails over to backup
        let mut unauth = ExecutionClient::connect(&endpoints).await.unwrap();
        assert_eq!(
            unauth
                .submit(None, v2::JobSpec::default())
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        assert_eq!(
            unauth
                .invoke(v2::JobSpec::default())
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        assert_eq!(backup_state.calls.load(Ordering::SeqCst), 0);

        // Worker token on client endpoint -> 403 PermissionDenied, never fails over
        let mut wrong_role = ExecutionClient::connect_with_token(&endpoints, Some("worker-tok"))
            .await
            .unwrap();
        assert_eq!(
            wrong_role
                .submit(None, v2::JobSpec::default())
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            wrong_role
                .invoke(v2::JobSpec::default())
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(backup_state.calls.load(Ordering::SeqCst), 0);

        // Client token on WorkerSession WS handshake -> 403 PermissionDenied, preserves tonic::Status
        let err = WorkerSession::connect(&endpoints, Some("client-tok"), v2::Hello::default())
            .await
            .err()
            .unwrap();
        assert_eq!(
            err.downcast_ref::<Status>().unwrap().code(),
            Code::PermissionDenied
        );
        assert_eq!(backup_state.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ws_multi_endpoint_503_and_closed_port_failover() {
        let closed_port = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().port()
        };
        let closed_ws = format!("ws://127.0.0.1:{closed_port}");
        let closed_http = format!("http://127.0.0.1:{closed_port}");

        let standby_state = MockState::new("standby");
        standby_state.unavailable.store(true, Ordering::SeqCst);
        let standby = spawn_mock(standby_state.clone()).await;

        let mut leader1_state = MockState::new("leader1");
        leader1_state.not_found_on_get = true;
        let leader1 = spawn_mock(leader1_state.clone()).await;

        let leader2_state = MockState::new("leader2");
        let leader2 = spawn_mock(leader2_state.clone()).await;

        let endpoints = format!(
            "{closed_ws}, {}, {}, {}",
            standby.ws_url, leader1.ws_url, leader2.ws_url
        );

        let mut client = ExecutionClient::connect(&endpoints).await.unwrap();
        // Probing skips closed_ws + standby (503) and sticks to leader1.
        for i in 0..3 {
            let res = client
                .submit(Some(format!("j-{i}")), v2::JobSpec::default())
                .await
                .unwrap();
            assert_eq!(res.job.unwrap().namespace, "leader1");
        }
        assert_eq!(leader1_state.calls.load(Ordering::SeqCst), 3);
        assert_eq!(leader2_state.calls.load(Ordering::SeqCst), 0);

        // Non-Unavailable error stays pinned to leader1.
        assert_eq!(
            client.get("missing", false).await.unwrap_err().code(),
            Code::NotFound
        );
        assert_eq!(leader2_state.calls.load(Ordering::SeqCst), 0);

        // Leader1 steps down (503) -> next WS + Unary RPCs fail over and stick to leader2.
        leader1_state.unavailable.store(true, Ordering::SeqCst);
        let mut events = client.invoke(v2::JobSpec::default()).await.unwrap();
        let first = events.message().await.unwrap().unwrap();
        assert_eq!(first.job_id, "leader2");
        assert_eq!(leader2_state.calls.load(Ordering::SeqCst), 1);
        let after = client
            .submit(Some("after".into()), v2::JobSpec::default())
            .await
            .unwrap();
        assert_eq!(after.job.unwrap().namespace, "leader2");

        // WorkerSession skips closed port + 503 WS handshake and connects to leader2.
        let mut worker = WorkerSession::connect(&endpoints, None, v2::Hello::default())
            .await
            .unwrap();
        assert!(matches!(
            worker.messages.message().await.unwrap().unwrap().msg,
            Some(v2::server_message::Msg::Welcome(_))
        ));

        // Mixed dead gRPC + standby WS + live gRPC failover check
        #[derive(Clone)]
        struct DummyGrpc;
        #[tonic::async_trait]
        impl v2::client_server::Client for DummyGrpc {
            type InvokeStream =
                tokio_stream::wrappers::ReceiverStream<Result<v2::ExecutionEvent, Status>>;
            type WatchStream = Self::InvokeStream;
            async fn submit(
                &self,
                _: tonic::Request<v2::SubmitRequest>,
            ) -> Result<tonic::Response<v2::SubmitResponse>, Status> {
                Ok(tonic::Response::new(v2::SubmitResponse {
                    job: Some(v2::Job {
                        namespace: "grpc-leader".into(),
                        ..Default::default()
                    }),
                    created: true,
                }))
            }
            async fn invoke(
                &self,
                _: tonic::Request<tonic::Streaming<v2::InvokeRequest>>,
            ) -> Result<tonic::Response<Self::InvokeStream>, Status> {
                Err(Status::unimplemented("unused"))
            }
            async fn watch(
                &self,
                _: tonic::Request<v2::WatchRequest>,
            ) -> Result<tonic::Response<Self::WatchStream>, Status> {
                Err(Status::unimplemented("unused"))
            }
            async fn get_job(
                &self,
                _: tonic::Request<v2::GetJobRequest>,
            ) -> Result<tonic::Response<v2::Job>, Status> {
                Err(Status::unimplemented("unused"))
            }
            async fn cancel(
                &self,
                _: tonic::Request<v2::CancelRequest>,
            ) -> Result<tonic::Response<v2::Job>, Status> {
                Err(Status::unimplemented("unused"))
            }
            async fn list_jobs(
                &self,
                _: tonic::Request<v2::ListJobsRequest>,
            ) -> Result<tonic::Response<v2::ListJobsResponse>, Status> {
                Err(Status::unimplemented("unused"))
            }
        }
        let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let grpc_url = format!("http://{}", grpc_listener.local_addr().unwrap());
        let grpc_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(v2::client_server::ClientServer::new(DummyGrpc))
                .serve_with_incoming(TcpListenerStream::new(grpc_listener))
                .await
                .unwrap();
        });
        let mixed = format!("{closed_http}, {}, {grpc_url}", standby.ws_url);
        let mut mixed_client = ExecutionClient::connect(&mixed).await.unwrap();
        let resp = mixed_client
            .submit(None, v2::JobSpec::default())
            .await
            .unwrap();
        assert_eq!(resp.job.unwrap().namespace, "grpc-leader");
        grpc_task.abort();

        // Mixed closed gRPC -> live WS
        let mixed_to_ws = format!("{closed_http}, {}", leader2.ws_url);
        let mut mixed_ws_client = ExecutionClient::connect(&mixed_to_ws).await.unwrap();
        let resp = mixed_ws_client
            .submit(None, v2::JobSpec::default())
            .await
            .unwrap();
        assert_eq!(resp.job.unwrap().namespace, "leader2");
    }

    #[tokio::test]
    async fn execute_pooled_http_ws_fallback_validation_and_failover_safety() {
        #[derive(Clone, Copy)]
        enum ExecMode {
            OkFinished,
            NotImplemented,
            NotFound,
            Empty200,
            Html200,
            NonTerminal200,
            GatewayTimeout,
            Standby503WithHeader,
            RawProxy503NoHeader,
            MidFlightDrop,
        }

        async fn spawn_exec_mock(mode: ExecMode, name: &str) -> (TestServer, Arc<AtomicUsize>) {
            let calls = Arc::new(AtomicUsize::new(0));
            let calls_clone = Arc::clone(&calls);
            let name_owned = name.to_owned();
            let ws_state = MockState::new(name);
            let app = Router::new()
                .route(
                    "/rpc/v2/client/execute",
                    post(move |body: Bytes| {
                        let calls = Arc::clone(&calls_clone);
                        let name = name_owned.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            let _ = v2::InvokeStart::decode(body);
                            match mode {
                                ExecMode::OkFinished => proto_response(&v2::ExecutionEvent {
                                    job_id: format!("exec-{name}"),
                                    event: Some(v2::execution_event::Event::Finished(
                                        v2::ExecutionFinished {
                                            state: v2::JobState::Succeeded as i32,
                                            ..Default::default()
                                        },
                                    )),
                                    ..Default::default()
                                }),
                                ExecMode::NotImplemented => {
                                    http_error(StatusCode::NOT_IMPLEMENTED, None, "unimplemented")
                                }
                                ExecMode::NotFound => {
                                    http_error(StatusCode::NOT_FOUND, None, "not found")
                                }
                                ExecMode::Empty200 => Response::builder()
                                    .status(StatusCode::OK)
                                    .header(
                                        axum::http::header::CONTENT_TYPE,
                                        "application/x-protobuf",
                                    )
                                    .body(axum::body::Body::empty())
                                    .unwrap(),
                                ExecMode::Html200 => Response::builder()
                                    .status(StatusCode::OK)
                                    .header(axum::http::header::CONTENT_TYPE, "text/html")
                                    .body(axum::body::Body::from("<html>ok</html>"))
                                    .unwrap(),
                                ExecMode::NonTerminal200 => proto_response(&v2::ExecutionEvent {
                                    job_id: "not-done".into(),
                                    event: Some(v2::execution_event::Event::Accepted(
                                        v2::Accepted {},
                                    )),
                                    ..Default::default()
                                }),
                                ExecMode::GatewayTimeout => {
                                    http_error(StatusCode::GATEWAY_TIMEOUT, None, "proxy timeout")
                                }
                                ExecMode::Standby503WithHeader => http_error(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    Some(Code::Unavailable),
                                    "standby replica",
                                ),
                                ExecMode::RawProxy503NoHeader => http_error(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    None,
                                    "upstream overloaded",
                                ),
                                ExecMode::MidFlightDrop => unreachable!(),
                            }
                        }
                    }),
                )
                .route("/ws/v2/client/invoke", get(handle_invoke))
                .with_state(ws_state);

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let task = if matches!(mode, ExecMode::MidFlightDrop) {
                let calls_drop = Arc::clone(&calls);
                tokio::spawn(async move {
                    use tokio::io::AsyncReadExt;
                    while let Ok((mut sock, _)) = listener.accept().await {
                        let mut buf = [0u8; 1024];
                        let n = sock.read(&mut buf).await.unwrap_or(0);
                        if n > 0 {
                            calls_drop.fetch_add(1, Ordering::SeqCst);
                        }
                        drop(sock);
                    }
                })
            } else {
                tokio::spawn(async move {
                    axum::serve(listener, app).await.unwrap();
                })
            };
            (
                TestServer {
                    ws_url: format!("ws://{addr}"),
                    task,
                },
                calls,
            )
        }

        // 1. Pooled HTTP execute success.
        let (ok_srv, ok_calls) = spawn_exec_mock(ExecMode::OkFinished, "ok").await;
        let mut client = ExecutionClient::connect(&ok_srv.ws_url).await.unwrap();
        let ev = client.execute(v2::JobSpec::default()).await.unwrap();
        assert_eq!(ev.job_id, "exec-ok");
        assert_eq!(ok_calls.load(Ordering::SeqCst), 1);

        // 2. 501 and 404 fall back cleanly to WS invoke.
        for (mode, name) in [
            (ExecMode::NotImplemented, "fb501"),
            (ExecMode::NotFound, "fb404"),
        ] {
            let (srv, _) = spawn_exec_mock(mode, name).await;
            let mut c = ExecutionClient::connect(&srv.ws_url).await.unwrap();
            let ev = c.execute(v2::JobSpec::default()).await.unwrap();
            assert_eq!(ev.job_id, name);
            assert!(matches!(
                ev.event,
                Some(v2::execution_event::Event::Finished(_))
            ));
        }

        // 3. Empty 200, HTML 200, and non-terminal 200 rejected with Code::Internal.
        for mode in [
            ExecMode::Empty200,
            ExecMode::Html200,
            ExecMode::NonTerminal200,
        ] {
            let (srv, _) = spawn_exec_mock(mode, "bad200").await;
            let mut c = ExecutionClient::connect(&srv.ws_url).await.unwrap();
            assert_eq!(
                c.execute(v2::JobSpec::default()).await.unwrap_err().code(),
                Code::Internal
            );
        }

        // 4. Standby 503 + x-anyserve-grpc-code: 14 fails over to healthy leader2.
        let (standby, _) = spawn_exec_mock(ExecMode::Standby503WithHeader, "standby").await;
        let (leader2, l2_calls) = spawn_exec_mock(ExecMode::OkFinished, "leader2").await;
        let mut c = ExecutionClient::connect(&format!("{},{}", standby.ws_url, leader2.ws_url))
            .await
            .unwrap();
        let ev = c.execute(v2::JobSpec::default()).await.unwrap();
        assert_eq!(ev.job_id, "exec-leader2");
        assert_eq!(l2_calls.load(Ordering::SeqCst), 1);

        // 5. 504 => DeadlineExceeded, raw proxy 503 (no grpc code header), and mid-flight TCP drop
        //    must NEVER retry on leader2!
        for (mode, expected_code) in [
            (ExecMode::GatewayTimeout, Code::DeadlineExceeded),
            (ExecMode::RawProxy503NoHeader, Code::Unavailable),
            (ExecMode::MidFlightDrop, Code::Unavailable),
        ] {
            let (bad1, bad1_calls) = spawn_exec_mock(mode, "bad1").await;
            let (backup, backup_calls) = spawn_exec_mock(ExecMode::OkFinished, "backup").await;
            let mut c = ExecutionClient::connect(&format!("{},{}", bad1.ws_url, backup.ws_url))
                .await
                .unwrap();
            let err = c.execute(v2::JobSpec::default()).await.unwrap_err();
            assert_eq!(err.code(), expected_code);
            assert_eq!(bad1_calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                backup_calls.load(Ordering::SeqCst),
                0,
                "execute must not duplicate across endpoints after non-standby failure"
            );
        }
    }
}
