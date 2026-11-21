#![allow(
    clippy::result_large_err,
    reason = "native RPCs preserve typed tonic statuses"
)]
//! Native execution API. Multi-endpoint lists probe sequentially and stick to
//! the active leader, never spraying RPCs with client-side round-robin.
//! The generated protocol types are public so custom engines need no core crate.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status, Streaming};

pub use anyserve_proto::v2;
use v2::client_client::ClientClient;
use v2::worker_client::WorkerClient;

use crate::{BearerAuth, NAMESPACE_ENV, TOKEN_ENV, connect_channel, parse_endpoints};

mod attempt;
pub use attempt::Attempt;
mod events;
pub use events::ExecutionEvents;

const SEND_QUEUE: usize = 8;
const CONTROL_MESSAGE_BYTES: usize = 256 * 1024;
type Transport = InterceptedService<Channel, BearerAuth>;

#[derive(Clone)]
pub struct ExecutionClient {
    endpoints: Arc<Vec<String>>,
    auth: BearerAuth,
    slot: Arc<Mutex<(usize, ClientClient<Transport>)>>,
}

fn build_client(channel: Channel, auth: BearerAuth) -> ClientClient<Transport> {
    ClientClient::with_interceptor(channel, auth)
        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
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
        let mut last_err = None;
        for (idx, ep) in endpoints.iter().enumerate() {
            match connect_channel(ep).await {
                Ok(ch) => {
                    let client = build_client(ch, auth.clone());
                    return Ok(Self {
                        endpoints: Arc::new(endpoints),
                        auth,
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
        F: FnMut(ClientClient<Transport>, R) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        let (start_idx, primary) = self.slot.lock().expect("client slot poisoned").clone();
        if self.endpoints.len() == 1 {
            return op(primary, req).await;
        }
        let mut last_status = match op(primary, req.clone()).await {
            Ok(val) => return Ok(val),
            Err(status) if status.code() == Code::Unavailable => status,
            Err(status) => return Err(status),
        };
        let n = self.endpoints.len();
        for offset in 1..n {
            let idx = (start_idx + offset) % n;
            let Ok(ch) = connect_channel(&self.endpoints[idx]).await else {
                continue;
            };
            let candidate = build_client(ch, self.auth.clone());
            match op(candidate.clone(), req.clone()).await {
                Ok(val) => {
                    *self.slot.lock().expect("client slot poisoned") = (idx, candidate);
                    return Ok(val);
                }
                Err(status) if status.code() == Code::Unavailable => {
                    last_status = status;
                }
                Err(status) => {
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
        let req = v2::SubmitRequest {
            job_id: job_id.unwrap_or_default(),
            conflict_policy: v2::ConflictPolicy::UseExisting as i32,
            spec: Some(spec),
        };
        self.call_with(req, |mut c, r| async move {
            Ok(c.submit(r).await?.into_inner())
        })
        .await
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
        self.call_with(request, |mut c, r| async move {
            Ok(ExecutionEvents::new(
                c.invoke(tokio_stream::iter([r])).await?.into_inner(),
            ))
        })
        .await
    }

    pub async fn invoke_stream(&mut self, spec: v2::JobSpec) -> Result<Invocation, Status> {
        self.call_with(spec, |mut c, s| async move {
            let (input, requests) = mpsc::channel(SEND_QUEUE);
            input
                .send(v2::InvokeRequest {
                    msg: Some(v2::invoke_request::Msg::Start(v2::InvokeStart {
                        spec: Some(s),
                        streaming_input: true,
                    })),
                })
                .await
                .map_err(|_| Status::unavailable("invocation input closed"))?;
            let events = c.invoke(ReceiverStream::new(requests)).await?.into_inner();
            Ok(Invocation {
                input,
                events: ExecutionEvents::new(events),
            })
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
        self.call_with(req, |mut c, r| async move {
            Ok(ExecutionEvents::new(c.watch(r).await?.into_inner()))
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
        self.call_with(req, |mut c, r| async move {
            Ok(c.get_job(r).await?.into_inner())
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
        self.call_with(req, |mut c, r| async move {
            Ok(c.cancel(r).await?.into_inner())
        })
        .await
    }

    pub async fn list(
        &mut self,
        request: v2::ListJobsRequest,
    ) -> Result<v2::ListJobsResponse, Status> {
        self.call_with(request, |mut c, r| async move {
            Ok(c.list_jobs(r).await?.into_inner())
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
    pub messages: Streaming<v2::ServerMessage>,
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
                        messages: response.into_inner(),
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
pub struct AttemptClient {
    inner: WorkerClient<Transport>,
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
        let mut last_err = None;
        for ep in &endpoints {
            match connect_channel(ep).await {
                Ok(ch) => return Self::from_channel_with_namespace(ch, token, namespace),
                Err(err) => last_err = Some(err),
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
        Ok(Self { inner })
    }

    pub async fn open(
        &mut self,
        lease_id: String,
        generation: u64,
        resume_after_sequence: u64,
    ) -> Result<AttemptLink, Status> {
        let (send, requests) = mpsc::channel(SEND_QUEUE);
        send.send(v2::AttemptUp {
            msg: Some(v2::attempt_up::Msg::Open(v2::AttemptOpen {
                lease_id,
                generation,
                resume_after_sequence,
            })),
        })
        .await
        .map_err(|_| Status::unavailable("attempt data link closed"))?;
        let events = self
            .inner
            .attempt(ReceiverStream::new(requests))
            .await?
            .into_inner();
        Ok(AttemptLink { send, events })
    }
}

/// Bounded transport. A custom worker must also obey Welcome.limits and wait
/// for Ack before exceeding its advertised application window.
pub struct AttemptLink {
    pub send: mpsc::Sender<v2::AttemptUp>,
    pub events: Streaming<v2::AttemptDown>,
}
