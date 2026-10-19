#![allow(
    clippy::result_large_err,
    reason = "native RPCs preserve typed tonic statuses"
)]
//! Native execution API. Worker control and data use separate TCP connections.
//! The generated protocol types are public so custom engines need no core crate.

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Status, Streaming};

pub use anyserve_proto::v2;
use v2::client_client::ClientClient;
use v2::worker_client::WorkerClient;

use crate::{BearerAuth, TOKEN_ENV, connect_channel};

mod attempt;
pub use attempt::Attempt;
mod events;
pub use events::ExecutionEvents;

const SEND_QUEUE: usize = 8;
const CONTROL_MESSAGE_BYTES: usize = 256 * 1024;
type Transport = InterceptedService<Channel, BearerAuth>;

#[derive(Clone)]
pub struct ExecutionClient {
    inner: ClientClient<Transport>,
}

impl ExecutionClient {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let token = std::env::var(TOKEN_ENV).ok();
        Self::connect_with_token(endpoint, token.as_deref()).await
    }

    pub async fn connect_with_token(endpoint: &str, token: Option<&str>) -> Result<Self> {
        let inner = ClientClient::with_interceptor(
            connect_channel(endpoint).await?,
            BearerAuth::new(token)?,
        )
        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE);
        Ok(Self { inner })
    }

    /// Acknowledgement means durable input was committed. Reuse the job ID to
    /// recover an acknowledgement lost to a network failure.
    pub async fn submit(
        &mut self,
        job_id: Option<String>,
        spec: v2::JobSpec,
    ) -> Result<v2::SubmitResponse, Status> {
        Ok(self
            .inner
            .submit(v2::SubmitRequest {
                job_id: job_id.unwrap_or_default(),
                conflict_policy: v2::ConflictPolicy::UseExisting as i32,
                spec: Some(spec),
            })
            .await?
            .into_inner())
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
        Ok(ExecutionEvents::new(
            self.inner
                .invoke(tokio_stream::iter([request]))
                .await?
                .into_inner(),
        ))
    }

    pub async fn invoke_stream(&mut self, spec: v2::JobSpec) -> Result<Invocation, Status> {
        let (input, requests) = mpsc::channel(SEND_QUEUE);
        input
            .send(v2::InvokeRequest {
                msg: Some(v2::invoke_request::Msg::Start(v2::InvokeStart {
                    spec: Some(spec),
                    streaming_input: true,
                })),
            })
            .await
            .map_err(|_| Status::unavailable("invocation input closed"))?;
        let events = self
            .inner
            .invoke(ReceiverStream::new(requests))
            .await?
            .into_inner();
        Ok(Invocation {
            input,
            events: ExecutionEvents::new(events),
        })
    }

    pub async fn watch(
        &mut self,
        job_id: impl Into<String>,
        after_sequence: u64,
    ) -> Result<ExecutionEvents, Status> {
        Ok(ExecutionEvents::new(
            self.inner
                .watch(v2::WatchRequest {
                    job_id: job_id.into(),
                    after_sequence,
                })
                .await?
                .into_inner(),
        ))
    }

    pub async fn get(
        &mut self,
        job_id: impl Into<String>,
        include_outputs: bool,
    ) -> Result<v2::Job, Status> {
        Ok(self
            .inner
            .get_job(v2::GetJobRequest {
                job_id: job_id.into(),
                include_outputs,
            })
            .await?
            .into_inner())
    }

    pub async fn cancel(
        &mut self,
        job_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<v2::Job, Status> {
        Ok(self
            .inner
            .cancel(v2::CancelRequest {
                job_id: job_id.into(),
                reason: reason.into(),
            })
            .await?
            .into_inner())
    }

    pub async fn list(
        &mut self,
        request: v2::ListJobsRequest,
    ) -> Result<v2::ListJobsResponse, Status> {
        Ok(self.inner.list_jobs(request).await?.into_inner())
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
        // Two connect() calls are intentional: cloned channels share a connection.
        let mut control_client = WorkerClient::with_interceptor(
            connect_channel(endpoint).await?,
            BearerAuth::new(token)?,
        )
        .max_decoding_message_size(CONTROL_MESSAGE_BYTES)
        .max_encoding_message_size(CONTROL_MESSAGE_BYTES);
        let data = AttemptClient::connect(endpoint, token).await?;
        let (control, requests) = mpsc::channel(SEND_QUEUE);
        control
            .send(v2::WorkerMessage {
                msg: Some(v2::worker_message::Msg::Hello(hello)),
            })
            .await?;
        let messages = control_client
            .session(ReceiverStream::new(requests))
            .await?
            .into_inner();
        Ok(Self {
            control,
            messages,
            data,
        })
    }
}

#[derive(Clone)]
pub struct AttemptClient {
    inner: WorkerClient<Transport>,
}

impl AttemptClient {
    pub async fn connect(endpoint: &str, token: Option<&str>) -> Result<Self> {
        let inner = WorkerClient::with_interceptor(
            connect_channel(endpoint).await?,
            BearerAuth::new(token)?,
        )
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
