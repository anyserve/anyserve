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
#[cfg(feature = "direct")]
pub use attempt::{DirectAttempt, DirectAttemptError};
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
    #[cfg(feature = "direct")]
    direct: Option<Arc<crate::direct::LazyDirectEndpoint>>,
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
                        #[cfg(feature = "direct")]
                        direct: None,
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

    /// Enables optional `iroh` direct data-plane transfers for ephemeral
    /// [`ExecutionClient::invoke_direct`] calls. The underlying `iroh` endpoint
    /// is bound lazily on first use and reused across invocations.
    #[cfg(feature = "direct")]
    pub fn with_direct(mut self, config: crate::direct::DirectConfig) -> Self {
        self.direct = Some(Arc::new(crate::direct::LazyDirectEndpoint::new(config)));
        self
    }

    /// Returns the lazily initialized [`crate::direct::DirectEndpoint`] if
    /// configured via [`ExecutionClient::with_direct`].
    #[cfg(feature = "direct")]
    pub async fn direct_endpoint(
        &self,
    ) -> Result<Option<Arc<crate::direct::DirectEndpoint>>, crate::direct::DirectError> {
        match &self.direct {
            Some(lazy) => lazy.get_or_init().await.map(Some),
            None => Ok(None),
        }
    }

    /// Ephemeral invocation with optional `iroh` direct data-plane offloading
    /// for static large inputs (`>= DirectConfig::threshold_bytes`) and direct
    /// terminal outputs (`ResultChunk.uri`).
    ///
    /// - Strictly limited to ephemeral `Invoke` (durable policies are rejected).
    /// - Sets `metadata["anyserve.direct"] = "1"` so direct workers know the
    ///   caller can resolve `anyserve+iroh://` outputs; when any input is staged
    ///   over direct, also sets `demand.required_attributes["anyserve.direct"] = "1"`
    ///   so the scheduler routes only to direct-capable workers (queuing until
    ///   `queue_timeout` if none are online).
    /// - Memory cost: `spec` is cloned for inline fallback **only** when total
    ///   staged inline input bytes `<= inline_fallback_limit_bytes` (default
    ///   `8 MiB`). Larger non-fallbackable payloads are never cloned or retained
    ///   in a fallback copy.
    /// - Fallback stream semantics: if the worker reports pre-handler
    ///   `direct_public_relay_throttled` and total staged input bytes
    ///   `<= inline_fallback_limit_bytes`, [`DirectExecutionEvents::message`]
    ///   intercepts that terminal failure and transparently opens one new inline
    ///   `Invoke` (stripping `anyserve.direct`). Callers may therefore have
    ///   observed `Accepted`/`Assigned` events for the initial throttled
    ///   `job_id` before subsequent events carry the fallback `job_id`; inspect
    ///   [`DirectExecutionEvents::fell_back_inline`] and
    ///   [`DirectExecutionEvents::fallback_from_job_id`] to detect this transition.
    ///   Any server error (e.g. `max_inline_object_bytes`) is propagated unchanged.
    #[cfg(feature = "direct")]
    pub async fn invoke_direct(
        &mut self,
        spec: v2::JobSpec,
    ) -> Result<DirectExecutionEvents, Status> {
        let lazy = self.direct.clone().ok_or_else(|| {
            Status::failed_precondition(
                "direct data plane is not configured on ExecutionClient (call with_direct)",
            )
        })?;
        if spec
            .policy
            .as_ref()
            .is_some_and(|p| p.durability == v2::Durability::Durable as i32)
        {
            return Err(Status::invalid_argument(
                "invoke_direct only supports ephemeral invocations (Submit/durable stays off by default)",
            ));
        }

        let endpoint = lazy
            .get_or_init()
            .await
            .map_err(crate::direct::DirectError::into_status)?;
        let threshold = endpoint.config().threshold_bytes;
        let inline_fallback_limit = endpoint.config().inline_fallback_limit_bytes;
        let max_fetches =
            crate::direct::derive_input_max_fetches(spec.policy.as_ref().map(|p| p.max_attempts));

        let staged_inline_total: usize = spec
            .inputs
            .iter()
            .filter_map(|obj| match &obj.body {
                Some(v2::object::Body::Inline(data)) if data.len() >= threshold => Some(data.len()),
                _ => None,
            })
            .fold(0usize, usize::saturating_add);
        let fallback_spec = (inline_fallback_limit > 0
            && staged_inline_total > 0
            && staged_inline_total <= inline_fallback_limit)
            .then(|| spec.clone());

        let mut direct_spec = spec;
        let mut guards = Vec::new();
        let mut total_staged_bytes = 0usize;

        for obj in &mut direct_spec.inputs {
            let inline_len = match &obj.body {
                Some(v2::object::Body::Inline(data)) if data.len() >= threshold => data.len(),
                _ => continue,
            };
            let data = match obj.body.take() {
                Some(v2::object::Body::Inline(data)) => data,
                other => {
                    obj.body = other;
                    continue;
                }
            };
            let (uri, sha256, size_bytes, guard) = endpoint
                .stage_input(data, max_fetches)
                .map_err(crate::direct::DirectError::into_status)?;
            obj.body = Some(v2::object::Body::Uri(uri));
            obj.metadata
                .insert(crate::OBJECT_SHA256_METADATA_KEY.to_owned(), sha256);
            obj.metadata.insert(
                crate::OBJECT_SIZE_METADATA_KEY.to_owned(),
                size_bytes.to_string(),
            );
            total_staged_bytes = total_staged_bytes.saturating_add(inline_len);
            guards.push(guard);
        }

        direct_spec.metadata.insert(
            crate::DIRECT_CAPABILITY_KEY.to_owned(),
            crate::DIRECT_CAPABILITY_VALUE.to_owned(),
        );
        if !guards.is_empty() {
            direct_spec
                .demand
                .get_or_insert_with(Default::default)
                .required_attributes
                .insert(
                    crate::DIRECT_CAPABILITY_KEY.to_owned(),
                    crate::DIRECT_CAPABILITY_VALUE.to_owned(),
                );
        }

        let events = self.invoke(direct_spec).await?;

        Ok(DirectExecutionEvents {
            events,
            client: self.clone(),
            endpoint,
            input_guards: guards,
            fallback_spec,
            total_staged_bytes,
            fallback_attempted: false,
            fell_back_inline: false,
            fallback_from_job_id: None,
            cancel: crate::direct::DirectCancel::new(),
            output_reports: Vec::new(),
        })
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

/// Event stream returned by [`ExecutionClient::invoke_direct`].
///
/// Holds input capability-token leases until completion/cancellation/drop,
/// transparently resolves `anyserve+iroh://` terminal outputs with SHA-256
/// verification, and handles bounded pre-handler inline fallback on
/// `direct_public_relay_throttled`.
///
/// **Inline fallback stream transition**: if automatic inline fallback fires
/// (`<= inline_fallback_limit_bytes`), callers may already have observed
/// `Accepted`/`Assigned` events for the initial throttled `job_id` before
/// subsequent events switch to the fallback `Invoke`'s `job_id`. Use
/// [`DirectExecutionEvents::fell_back_inline`] and
/// [`DirectExecutionEvents::fallback_from_job_id`] to observe that switch.
#[cfg(feature = "direct")]
pub struct DirectExecutionEvents {
    events: ExecutionEvents,
    client: ExecutionClient,
    endpoint: Arc<crate::direct::DirectEndpoint>,
    input_guards: Vec<crate::direct::InputLeaseGuard>,
    fallback_spec: Option<v2::JobSpec>,
    total_staged_bytes: usize,
    fallback_attempted: bool,
    fell_back_inline: bool,
    fallback_from_job_id: Option<String>,
    cancel: crate::direct::DirectCancel,
    output_reports: Vec<crate::direct::DirectTransferReport>,
}

#[cfg(feature = "direct")]
impl DirectExecutionEvents {
    /// Returns a cooperative cancel handle that also resets/stops any active
    /// output QUIC stream.
    pub fn cancel_handle(&self) -> crate::direct::DirectCancel {
        self.cancel.clone()
    }

    /// Returns telemetry reports for all resolved direct outputs so far.
    pub fn output_reports(&self) -> &[crate::direct::DirectTransferReport] {
        &self.output_reports
    }

    /// Returns `true` if this invocation fell back to inline relay after
    /// hitting `direct_public_relay_throttled`.
    pub fn fell_back_inline(&self) -> bool {
        self.fell_back_inline
    }

    /// When [`DirectExecutionEvents::fell_back_inline`] is `true`, returns the
    /// initial throttled `job_id` whose terminal failure triggered the inline
    /// fallback `Invoke` stream.
    pub fn fallback_from_job_id(&self) -> Option<&str> {
        self.fallback_from_job_id.as_deref()
    }

    /// Reads the next execution event, resolving `anyserve+iroh://` terminal
    /// outputs and handling pre-handler `direct_public_relay_throttled` fallback.
    pub async fn message(&mut self) -> Result<Option<v2::ExecutionEvent>, Status> {
        loop {
            let Some(mut event) = self.events.message().await? else {
                self.input_guards.clear();
                return Ok(None);
            };
            if let Some(v2::execution_event::Event::Finished(ref mut fin)) = event.event {
                // Terminal state reached: revoke all staged input tokens immediately.
                self.input_guards.clear();

                let is_throttled = fin.state == v2::JobState::Failed as i32
                    && fin.error.as_ref().is_some_and(|err| {
                        err.code == crate::ERROR_CODE_DIRECT_PUBLIC_RELAY_THROTTLED
                    });
                if is_throttled && !self.fallback_attempted {
                    self.fallback_attempted = true;
                    if let Some(mut inline_spec) = self.fallback_spec.take() {
                        inline_spec.metadata.remove(crate::DIRECT_CAPABILITY_KEY);
                        if let Some(demand) = inline_spec.demand.as_mut() {
                            demand
                                .required_attributes
                                .remove(crate::DIRECT_CAPABILITY_KEY);
                        }
                        self.fallback_from_job_id = Some(event.job_id.clone());
                        // Propagate any server rejection (e.g. max_inline_*) verbatim.
                        self.events = self.client.invoke(inline_spec).await?;
                        self.fell_back_inline = true;
                        continue;
                    }
                    return Err(crate::direct::DirectError::PublicRelayThrottled {
                        detail: format!(
                            "staged input ({} B) exceeds inline_fallback_limit_bytes ({} B)",
                            self.total_staged_bytes,
                            self.endpoint.config().inline_fallback_limit_bytes
                        ),
                    }
                    .into_status());
                }

                if fin.state == v2::JobState::Succeeded as i32 {
                    for obj in &mut fin.outputs {
                        let uri = match &obj.body {
                            Some(v2::object::Body::Uri(u)) if crate::is_direct_uri(u) => u.clone(),
                            _ => continue,
                        };
                        let (bytes, report) = self
                            .endpoint
                            .fetch_object(&uri, &obj.metadata, &self.cancel)
                            .await
                            .map_err(crate::direct::DirectError::into_status)?;
                        obj.body = Some(v2::object::Body::Inline(bytes));
                        self.output_reports.push(report);
                    }
                }
            }
            return Ok(Some(event));
        }
    }
}

#[cfg(feature = "direct")]
impl Drop for DirectExecutionEvents {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.input_guards.clear();
    }
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

/// Safe worker session bound to a [`crate::direct::DirectEndpoint`].
///
/// Connecting via [`DirectWorkerSession::connect`] automatically injects
/// `anyserve.direct = "1"` into `Hello.attributes`, and
/// [`DirectWorkerSession::open_attempt`] opens assignments through
/// [`Attempt::open_with_direct`] so direct inputs are resolved pre-handler and
/// failures commit non-retryably before any user handler can execute.
#[cfg(feature = "direct")]
pub struct DirectWorkerSession {
    pub session: WorkerSession,
    direct: Arc<crate::direct::DirectEndpoint>,
}

#[cfg(feature = "direct")]
impl DirectWorkerSession {
    pub async fn connect(
        endpoint: &str,
        token: Option<&str>,
        hello: v2::Hello,
        direct: Arc<crate::direct::DirectEndpoint>,
    ) -> Result<Self> {
        Self::connect_with_namespace(endpoint, token, None, hello, direct).await
    }

    pub async fn connect_with_namespace(
        endpoint: &str,
        token: Option<&str>,
        namespace: Option<&str>,
        mut hello: v2::Hello,
        direct: Arc<crate::direct::DirectEndpoint>,
    ) -> Result<Self> {
        crate::direct::DirectEndpoint::apply_worker_attributes(&mut hello);
        let session =
            WorkerSession::connect_with_namespace(endpoint, token, namespace, hello).await?;
        Ok(Self { session, direct })
    }

    pub fn direct_endpoint(&self) -> &Arc<crate::direct::DirectEndpoint> {
        &self.direct
    }

    /// Opens an assignment safely via [`Attempt::open_with_direct`].
    pub async fn open_attempt(
        &mut self,
        assignment: &v2::Assign,
        limits: &v2::Limits,
        heartbeat_timeout: std::time::Duration,
        cancel: &crate::direct::DirectCancel,
    ) -> Result<DirectAttempt, DirectAttemptError> {
        Attempt::open_with_direct(
            &mut self.session.data,
            assignment,
            limits,
            heartbeat_timeout,
            Arc::clone(&self.direct),
            cancel,
        )
        .await
    }

    pub fn drain(
        &self,
        preempting: bool,
    ) -> impl std::future::Future<Output = Result<(), Status>> + Send + 'static {
        let control = self.session.control.clone();
        async move {
            control
                .send(v2::WorkerMessage {
                    msg: Some(v2::worker_message::Msg::Draining(v2::Draining {
                        preempting,
                    })),
                })
                .await
                .map_err(|_| Status::unavailable("worker control stream closed"))
        }
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
