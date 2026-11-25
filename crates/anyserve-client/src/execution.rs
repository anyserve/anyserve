#![allow(
    clippy::result_large_err,
    reason = "native RPCs preserve typed tonic statuses"
)]
//! Native execution API. Multi-endpoint lists probe sequentially and stick to
//! the active leader, never spraying RPCs with client-side round-robin.
//! The generated protocol types are public so custom engines need no core crate.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status};

pub use anyserve_proto::v2;
use v2::client_client::ClientClient;
use v2::worker_client::WorkerClient;

use crate::{
    BearerAuth, NAMESPACE_ENV, TOKEN_ENV, connect_channel, is_ws_endpoint, parse_endpoints,
    ws_transport::{self, HttpWsClient},
};

mod attempt;
pub use attempt::Attempt;
mod events;
pub use events::{ExecutionEvents, RpcStream};

const SEND_QUEUE: usize = 8;
const CONTROL_MESSAGE_BYTES: usize = 256 * 1024;
type Transport = InterceptedService<Channel, BearerAuth>;

#[derive(Clone)]
enum SlotClient {
    Grpc(ClientClient<Transport>),
    HttpWs(HttpWsClient),
}

#[derive(Clone)]
pub struct ExecutionClient {
    endpoints: Arc<Vec<String>>,
    auth: BearerAuth,
    http: reqwest::Client,
    slot: Arc<Mutex<(usize, SlotClient)>>,
}

fn build_client(channel: Channel, auth: BearerAuth) -> ClientClient<Transport> {
    ClientClient::with_interceptor(channel, auth)
        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
}

async fn connect_slot(
    endpoint: &str,
    auth: &BearerAuth,
    http: &reqwest::Client,
) -> Result<SlotClient> {
    if is_ws_endpoint(endpoint) {
        ws_transport::probe_tcp(endpoint).await?;
        Ok(SlotClient::HttpWs(HttpWsClient::new(
            endpoint,
            auth.clone(),
            http.clone(),
        )?))
    } else {
        let ch = connect_channel(endpoint).await?;
        Ok(SlotClient::Grpc(build_client(ch, auth.clone())))
    }
}

impl ExecutionClient {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let token = std::env::var(TOKEN_ENV).ok();
        let namespace = std::env::var(NAMESPACE_ENV).ok();
        Self::connect_with_options(endpoint, token.as_deref(), namespace.as_deref()).await
    }

    pub async fn connect_with_token(endpoint: &str, token: Option<&str>) -> Result<Self> {
        Self::connect_with_options(endpoint, token, None).await
    }

    pub async fn connect_with_options(
        endpoint: &str,
        token: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<Self> {
        let endpoints = parse_endpoints(endpoint)?;
        let auth = BearerAuth::with_namespace(token, namespace)?;
        let http = ws_transport::build_http_client()?;
        let mut last_err = None;
        for (idx, ep) in endpoints.iter().enumerate() {
            match connect_slot(ep, &auth, &http).await {
                Ok(client) => {
                    return Ok(Self {
                        endpoints: Arc::new(endpoints),
                        auth,
                        http,
                        slot: Arc::new(Mutex::new((idx, client))),
                    });
                }
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.expect("endpoints is non-empty"))
    }

    async fn call_with<R: Clone, T, F, Fut>(&self, req: R, mut op: F) -> Result<T, Status>
    where
        F: FnMut(SlotClient, R) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        self.call_with_retry(req, |slot, r| {
            let fut = op(slot, r);
            async move {
                fut.await.map_err(|status| {
                    let retryable = status.code() == Code::Unavailable;
                    (status, retryable)
                })
            }
        })
        .await
    }

    async fn call_with_retry<R: Clone, T, F, Fut>(&self, req: R, mut op: F) -> Result<T, Status>
    where
        F: FnMut(SlotClient, R) -> Fut,
        Fut: std::future::Future<Output = Result<T, (Status, bool)>>,
    {
        let (start_idx, primary) = self.slot.lock().expect("client slot poisoned").clone();
        if self.endpoints.len() == 1 {
            return op(primary, req).await.map_err(|(status, _)| status);
        }
        let mut last_status = match op(primary, req.clone()).await {
            Ok(val) => return Ok(val),
            Err((status, true)) => status,
            Err((status, false)) => return Err(status),
        };
        let n = self.endpoints.len();
        for offset in 1..n {
            let idx = (start_idx + offset) % n;
            let Ok(candidate) = connect_slot(&self.endpoints[idx], &self.auth, &self.http).await
            else {
                continue;
            };
            match op(candidate.clone(), req.clone()).await {
                Ok(val) => {
                    *self.slot.lock().expect("client slot poisoned") = (idx, candidate);
                    return Ok(val);
                }
                Err((status, true)) => {
                    last_status = status;
                }
                Err((status, false)) => {
                    *self.slot.lock().expect("client slot poisoned") = (idx, candidate);
                    return Err(status);
                }
            }
        }
        Err(last_status)
    }

    /// Acknowledgement means durable input was committed. Reuse the job ID to
    /// recover an acknowledgement lost to a network failure.
    pub async fn submit(
        &mut self,
        job_id: Option<String>,
        spec: v2::JobSpec,
    ) -> Result<v2::SubmitResponse, Status> {
        self.submit_with_policy(job_id, spec, v2::ConflictPolicy::UseExisting)
            .await
    }

    /// Submits a job with an explicit conflict policy (`UseExisting` or `Fail`).
    pub async fn submit_with_policy(
        &mut self,
        job_id: Option<String>,
        spec: v2::JobSpec,
        conflict_policy: v2::ConflictPolicy,
    ) -> Result<v2::SubmitResponse, Status> {
        let req = v2::SubmitRequest {
            job_id: job_id.unwrap_or_default(),
            conflict_policy: conflict_policy as i32,
            spec: Some(spec),
        };
        self.call_with(req, |slot, r| async move {
            match slot {
                SlotClient::Grpc(mut c) => Ok(c.submit(r).await?.into_inner()),
                SlotClient::HttpWs(ws) => ws.unary("/rpc/v2/client/submit", &r).await,
            }
        })
        .await
    }

    /// Executes a non-streaming ephemeral job and waits for its terminal `ExecutionEvent`.
    /// Over `ws://`/`wss://`, prefers pooled `POST /rpc/v2/client/execute` and falls back
    /// to WebSocket `invoke` when the server returns `Unimplemented` or `NotFound`.
    /// Across multiple endpoints, fails over only on explicit pre-admission unavailability
    /// (standby / connect refusal), never retrying after mid-flight transport drops or 504.
    pub async fn execute(&mut self, spec: v2::JobSpec) -> Result<v2::ExecutionEvent, Status> {
        let start = v2::InvokeStart {
            spec: Some(spec),
            streaming_input: false,
        };
        let mut events = self
            .call_with_retry(start, |slot, s| async move {
                match slot {
                    SlotClient::HttpWs(ws) => match ws.execute(&s).await {
                        Ok(event) => Ok(Ok(event)),
                        Err(ws_transport::ExecuteError::Retryable(status)) => Err((status, true)),
                        Err(ws_transport::ExecuteError::Fatal(status))
                            if matches!(status.code(), Code::Unimplemented | Code::NotFound) =>
                        {
                            let req = v2::InvokeRequest {
                                msg: Some(v2::invoke_request::Msg::Start(s)),
                            };
                            ws.invoke(req)
                                .await
                                .map(Err)
                                .map_err(|status| (status, false))
                        }
                        Err(ws_transport::ExecuteError::Fatal(status)) => Err((status, false)),
                    },
                    SlotClient::Grpc(mut c) => {
                        let req = v2::InvokeRequest {
                            msg: Some(v2::invoke_request::Msg::Start(s)),
                        };
                        let stream = c
                            .invoke(tokio_stream::iter([req]))
                            .await
                            .map_err(|status| {
                                let retryable = status.code() == Code::Unavailable;
                                (status, retryable)
                            })?
                            .into_inner();
                        Ok(Err(ExecutionEvents::new(RpcStream::Grpc(stream))))
                    }
                }
            })
            .await?;
        match events {
            Ok(terminal) => Ok(terminal),
            Err(ref mut stream) => {
                while let Some(ev) = stream.next().await {
                    let ev = ev?;
                    if matches!(ev.event, Some(v2::execution_event::Event::Finished(_))) {
                        return Ok(ev);
                    }
                }
                Err(Status::unavailable(
                    "execution ended without a terminal event",
                ))
            }
        }
    }

    /// An ephemeral invocation. Dropping before Finished cancels execution.
    /// Finished is returned only after consuming the response trailer.
    pub async fn invoke(&mut self, spec: v2::JobSpec) -> Result<ExecutionEvents, Status> {
        let request = v2::InvokeRequest {
            msg: Some(v2::invoke_request::Msg::Start(v2::InvokeStart {
                spec: Some(spec),
                streaming_input: false,
            })),
        };
        self.call_with(request, |slot, r| async move {
            match slot {
                SlotClient::Grpc(mut c) => Ok(ExecutionEvents::new(RpcStream::Grpc(
                    c.invoke(tokio_stream::iter([r])).await?.into_inner(),
                ))),
                SlotClient::HttpWs(ws) => ws.invoke(r).await,
            }
        })
        .await
    }

    pub async fn invoke_stream(&mut self, spec: v2::JobSpec) -> Result<Invocation, Status> {
        self.call_with(spec, |slot, s| async move {
            let start = v2::InvokeRequest {
                msg: Some(v2::invoke_request::Msg::Start(v2::InvokeStart {
                    spec: Some(s),
                    streaming_input: true,
                })),
            };
            match slot {
                SlotClient::Grpc(mut c) => {
                    let (input, requests) = mpsc::channel(SEND_QUEUE);
                    input
                        .send(start)
                        .await
                        .map_err(|_| Status::unavailable("invocation input closed"))?;
                    let events = c.invoke(ReceiverStream::new(requests)).await?.into_inner();
                    Ok(Invocation {
                        input,
                        events: ExecutionEvents::new(RpcStream::Grpc(events)),
                    })
                }
                SlotClient::HttpWs(ws) => ws.invoke_stream(start, SEND_QUEUE).await,
            }
        })
        .await
    }

    pub async fn watch(
        &mut self,
        job_id: impl Into<String>,
        after_sequence: u64,
    ) -> Result<ExecutionEvents, Status> {
        let req = v2::WatchRequest {
            job_id: job_id.into(),
            after_sequence,
        };
        self.call_with(req, |slot, r| async move {
            match slot {
                SlotClient::Grpc(mut c) => Ok(ExecutionEvents::new(RpcStream::Grpc(
                    c.watch(r).await?.into_inner(),
                ))),
                SlotClient::HttpWs(ws) => ws.watch(r).await,
            }
        })
        .await
    }

    pub async fn get(
        &mut self,
        job_id: impl Into<String>,
        include_outputs: bool,
    ) -> Result<v2::Job, Status> {
        let req = v2::GetJobRequest {
            job_id: job_id.into(),
            include_outputs,
        };
        self.call_with(req, |slot, r| async move {
            match slot {
                SlotClient::Grpc(mut c) => Ok(c.get_job(r).await?.into_inner()),
                SlotClient::HttpWs(ws) => ws.unary("/rpc/v2/client/get_job", &r).await,
            }
        })
        .await
    }

    pub async fn cancel(
        &mut self,
        job_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<v2::Job, Status> {
        let req = v2::CancelRequest {
            job_id: job_id.into(),
            reason: reason.into(),
        };
        self.call_with(req, |slot, r| async move {
            match slot {
                SlotClient::Grpc(mut c) => Ok(c.cancel(r).await?.into_inner()),
                SlotClient::HttpWs(ws) => ws.unary("/rpc/v2/client/cancel", &r).await,
            }
        })
        .await
    }

    pub async fn list(
        &mut self,
        request: v2::ListJobsRequest,
    ) -> Result<v2::ListJobsResponse, Status> {
        self.call_with(request, |slot, r| async move {
            match slot {
                SlotClient::Grpc(mut c) => Ok(c.list_jobs(r).await?.into_inner()),
                SlotClient::HttpWs(ws) => ws.unary("/rpc/v2/client/list_jobs", &r).await,
            }
        })
        .await
    }
}

pub struct Invocation {
    pub input: mpsc::Sender<v2::InvokeRequest>,
    pub events: ExecutionEvents,
}

impl Invocation {
    pub async fn send(&self, data: impl Into<bytes::Bytes>) -> Result<(), Status> {
        self.input
            .send(v2::InvokeRequest {
                msg: Some(v2::invoke_request::Msg::Input(v2::InputChunk {
                    data: data.into(),
                    metadata: Default::default(),
                })),
            })
            .await
            .map_err(|_| Status::unavailable("invocation input closed"))
    }

    pub async fn finish_input(&self) -> Result<(), Status> {
        self.input
            .send(v2::InvokeRequest {
                msg: Some(v2::invoke_request::Msg::InputEnd(v2::InputEnd {})),
            })
            .await
            .map_err(|_| Status::unavailable("invocation input closed"))
    }
}

pub struct WorkerSession {
    pub control: mpsc::Sender<v2::WorkerMessage>,
    pub messages: RpcStream<v2::ServerMessage>,
    pub data: AttemptClient,
}

impl WorkerSession {
    pub async fn connect(endpoint: &str, token: Option<&str>, hello: v2::Hello) -> Result<Self> {
        Self::connect_with_namespace(endpoint, token, None, hello).await
    }

    pub async fn connect_with_namespace(
        endpoint: &str,
        token: Option<&str>,
        namespace: Option<&str>,
        hello: v2::Hello,
    ) -> Result<Self> {
        let endpoints = parse_endpoints(endpoint)?;
        let auth = BearerAuth::with_namespace(token, namespace)?;
        let mut last_err = None;
        for ep in &endpoints {
            if is_ws_endpoint(ep) {
                match ws_transport::open_worker_session(ep, &auth, &hello, SEND_QUEUE).await {
                    Ok((control, messages)) => {
                        let data = AttemptClient::from_ws_endpoint(ep, auth);
                        return Ok(Self {
                            control,
                            messages: RpcStream::Ws(messages),
                            data,
                        });
                    }
                    Err(status) if status.code() == Code::Unavailable && endpoints.len() > 1 => {
                        last_err = Some(status.into());
                    }
                    Err(status) => return Err(status.into()),
                }
                continue;
            }
            let channel = match connect_channel(ep).await {
                Ok(ch) => ch,
                Err(err) => {
                    last_err = Some(err);
                    continue;
                }
            };
            let mut control_client = WorkerClient::with_interceptor(channel, auth.clone())
                .max_decoding_message_size(CONTROL_MESSAGE_BYTES)
                .max_encoding_message_size(CONTROL_MESSAGE_BYTES);
            let (control, requests) = mpsc::channel(SEND_QUEUE);
            control
                .send(v2::WorkerMessage {
                    msg: Some(v2::worker_message::Msg::Hello(hello.clone())),
                })
                .await?;
            match control_client.session(ReceiverStream::new(requests)).await {
                Ok(response) => {
                    let data_channel = connect_channel(ep).await?;
                    let data =
                        AttemptClient::from_channel_with_namespace(data_channel, token, namespace)?;
                    return Ok(Self {
                        control,
                        messages: RpcStream::Grpc(response.into_inner()),
                        data,
                    });
                }
                Err(status) if status.code() == Code::Unavailable && endpoints.len() > 1 => {
                    last_err = Some(status.into());
                }
                Err(status) => return Err(status.into()),
            }
        }
        Err(last_err.expect("endpoints is non-empty"))
    }

    pub async fn drain(&self, preempting: bool) -> Result<(), Status> {
        self.control
            .send(v2::WorkerMessage {
                msg: Some(v2::worker_message::Msg::Draining(v2::Draining {
                    preempting,
                })),
            })
            .await
            .map_err(|_| Status::unavailable("worker control stream closed"))
    }
}

#[derive(Clone)]
enum AttemptTransport {
    Grpc(WorkerClient<Transport>),
    HttpWs { ws_base: String, auth: BearerAuth },
}

#[derive(Clone)]
pub struct AttemptClient {
    inner: AttemptTransport,
}

impl AttemptClient {
    pub async fn connect(endpoint: &str, token: Option<&str>) -> Result<Self> {
        Self::connect_with_namespace(endpoint, token, None).await
    }

    pub async fn connect_with_namespace(
        endpoint: &str,
        token: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<Self> {
        let endpoints = parse_endpoints(endpoint)?;
        let auth = BearerAuth::with_namespace(token, namespace)?;
        let mut last_err = None;
        for ep in &endpoints {
            if is_ws_endpoint(ep) {
                match ws_transport::probe_tcp(ep).await {
                    Ok(()) => return Ok(Self::from_ws_endpoint(ep, auth)),
                    Err(err) => last_err = Some(err),
                }
            } else {
                match connect_channel(ep).await {
                    Ok(ch) => return Self::from_channel_with_namespace(ch, token, namespace),
                    Err(err) => last_err = Some(err),
                }
            }
        }
        Err(last_err.expect("endpoints is non-empty"))
    }

    fn from_channel_with_namespace(
        channel: Channel,
        token: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<Self> {
        let inner =
            WorkerClient::with_interceptor(channel, BearerAuth::with_namespace(token, namespace)?)
                .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE);
        Ok(Self {
            inner: AttemptTransport::Grpc(inner),
        })
    }

    fn from_ws_endpoint(endpoint: &str, auth: BearerAuth) -> Self {
        Self {
            inner: AttemptTransport::HttpWs {
                ws_base: endpoint.trim_end_matches('/').to_owned(),
                auth,
            },
        }
    }

    pub async fn open(
        &mut self,
        lease_id: String,
        generation: u64,
        resume_after_sequence: u64,
    ) -> Result<AttemptLink, Status> {
        let open = v2::AttemptOpen {
            lease_id,
            generation,
            resume_after_sequence,
        };
        match &mut self.inner {
            AttemptTransport::Grpc(inner) => {
                let (send, requests) = mpsc::channel(SEND_QUEUE);
                send.send(v2::AttemptUp {
                    msg: Some(v2::attempt_up::Msg::Open(open)),
                })
                .await
                .map_err(|_| Status::unavailable("attempt data link closed"))?;
                let events = inner
                    .attempt(ReceiverStream::new(requests))
                    .await?
                    .into_inner();
                Ok(AttemptLink {
                    send,
                    events: RpcStream::Grpc(events),
                })
            }
            AttemptTransport::HttpWs { ws_base, auth } => {
                let (send, events) =
                    ws_transport::open_worker_attempt(ws_base, auth, open, SEND_QUEUE).await?;
                Ok(AttemptLink {
                    send,
                    events: RpcStream::Ws(events),
                })
            }
        }
    }
}

/// Bounded transport. A custom worker must also obey Welcome.limits and wait
/// for Ack before exceeding its advertised application window.
pub struct AttemptLink {
    pub send: mpsc::Sender<v2::AttemptUp>,
    pub events: RpcStream<v2::AttemptDown>,
}
