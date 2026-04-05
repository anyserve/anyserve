use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_stream::stream;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};

use anyserve_core::kernel::{Kernel, OpenStreamCommand};
use anyserve_core::model::{
    Attributes, Capacity, ExecutionPolicy, Frame, FrameKind, JobRecord, JobSpec, JobState,
    ObjectRef, StreamDirection, StreamScope, WorkerRecord,
};
use anyserve_core::notify::ClusterSubscription;

use crate::serve_config::ServeOpenAIConfig;

const INPUT_STREAM: &str = "input.default";
const OUTPUT_STREAM: &str = "output.default";

#[derive(Clone)]
struct AppState {
    kernel: Arc<Kernel>,
    config: Arc<ServeOpenAIConfig>,
}

#[derive(Serialize)]
struct OpenAIErrorEnvelope {
    error: OpenAIErrorBody,
}

#[derive(Serialize)]
struct OpenAIErrorBody {
    message: String,
    #[serde(rename = "type")]
    error_type: String,
    code: String,
}

#[derive(Debug)]
struct OpenAIError {
    status: StatusCode,
    message: String,
    error_type: &'static str,
    code: &'static str,
}

struct JobCancellationGuard {
    kernel: Arc<Kernel>,
    job_id: String,
    active: bool,
}

impl JobCancellationGuard {
    fn new(kernel: Arc<Kernel>, job_id: String) -> Self {
        Self {
            kernel,
            job_id,
            active: true,
        }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for JobCancellationGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        let kernel = Arc::clone(&self.kernel);
        let job_id = self.job_id.clone();
        tokio::spawn(async move {
            let _ = kernel.cancel_job(&job_id).await;
        });
    }
}

impl OpenAIError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            error_type: "invalid_request_error",
            code: "invalid_request",
        }
    }

    fn gateway_timeout(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: message.into(),
            error_type: "timeout_error",
            code: "gateway_timeout",
        }
    }

    fn bad_gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: message.into(),
            error_type: "upstream_error",
            code: "bad_gateway",
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
            error_type: "server_error",
            code: "internal_error",
        }
    }
}

impl IntoResponse for OpenAIError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(OpenAIErrorEnvelope {
                error: OpenAIErrorBody {
                    message: self.message,
                    error_type: self.error_type.to_string(),
                    code: self.code.to_string(),
                },
            }),
        )
            .into_response()
    }
}

pub async fn run_server(
    listen: std::net::SocketAddr,
    kernel: Arc<Kernel>,
    config: ServeOpenAIConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let state = AppState {
        kernel,
        config: Arc::new(config),
    };
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/embeddings", post(embeddings))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind openai listener {listen}"))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run openai http server")
}

async fn healthz() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn readyz(State(state): State<AppState>) -> Response {
    match readiness_snapshot(&state).await {
        Ok(snapshot) => {
            let status = if snapshot.chat_ready && snapshot.embeddings_ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (
                status,
                Json(json!({
                    "status": if status == StatusCode::OK { "ready" } else { "not_ready" },
                    "chat_ready": snapshot.chat_ready,
                    "embeddings_ready": snapshot.embeddings_ready,
                    "matching_chat_workers": snapshot.matching_chat_workers,
                    "matching_embeddings_workers": snapshot.matching_embeddings_workers,
                })),
            )
                .into_response()
        }
        Err(error) => error.into_response(),
    }
}

async fn list_models(State(state): State<AppState>) -> impl IntoResponse {
    let data = state
        .config
        .models
        .iter()
        .map(|id| {
            json!({
                "id": id,
                "object": "model",
                "created": 0,
                "owned_by": "anyserve",
            })
        })
        .collect::<Vec<_>>();
    Json(json!({
        "object": "list",
        "data": data,
    }))
}

async fn chat_completions(
    State(state): State<AppState>,
    Json(request): Json<Value>,
) -> Result<Response, OpenAIError> {
    let stream = request
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let payload = serde_json::to_vec(&request)
        .map_err(|error| OpenAIError::bad_request(format!("serialize request: {error}")))?;

    let job = submit_request(&state, &state.config.chat_interface, payload).await?;
    if stream {
        return Ok(streaming_response(state, job.job_id));
    }

    let output = wait_for_output_bytes(&state, job.job_id).await?;
    let response_json: Value = serde_json::from_slice(&output).map_err(|error| {
        OpenAIError::bad_gateway(format!("worker returned invalid json: {error}"))
    })?;

    Ok((StatusCode::OK, Json(response_json)).into_response())
}

async fn embeddings(
    State(state): State<AppState>,
    Json(request): Json<Value>,
) -> Result<Response, OpenAIError> {
    let payload = serde_json::to_vec(&request)
        .map_err(|error| OpenAIError::bad_request(format!("serialize request: {error}")))?;
    let job = submit_request(&state, &state.config.embeddings_interface, payload).await?;
    let output = wait_for_output_bytes(&state, job.job_id).await?;
    let response_json: Value = serde_json::from_slice(&output).map_err(|error| {
        OpenAIError::bad_gateway(format!("worker returned invalid json: {error}"))
    })?;

    Ok((StatusCode::OK, Json(response_json)).into_response())
}

async fn submit_request(
    state: &AppState,
    interface_name: &str,
    payload: Vec<u8>,
) -> Result<JobRecord, OpenAIError> {
    if payload.len() <= state.config.inline_request_limit_bytes {
        return state
            .kernel
            .submit_job(
                None,
                JobSpec {
                    interface_name: interface_name.to_string(),
                    inputs: Vec::new(),
                    params: payload,
                    demand: state.config.demand.clone(),
                    policy: ExecutionPolicy::default(),
                    metadata: Attributes::from([("source".to_string(), "openai".to_string())]),
                },
            )
            .await
            .map_err(|error| OpenAIError::internal(format!("submit job: {error}")));
    }

    let job = state
        .kernel
        .submit_job(
            None,
            JobSpec {
                interface_name: interface_name.to_string(),
                inputs: Vec::new(),
                params: Vec::new(),
                demand: state.config.demand.clone(),
                policy: ExecutionPolicy::default(),
                metadata: Attributes::from([
                    ("source".to_string(), "openai".to_string()),
                    ("input_mode".to_string(), "stream".to_string()),
                ]),
            },
        )
        .await
        .map_err(|error| OpenAIError::internal(format!("submit job: {error}")))?;

    if let Err(error) = attach_streamed_input(state, &job.job_id, payload).await {
        return Err(cancel_submitted_job(state, &job.job_id, error).await);
    }

    Ok(job)
}

async fn attach_streamed_input(
    state: &AppState,
    job_id: &str,
    payload: Vec<u8>,
) -> Result<(), String> {
    let input_stream = state
        .kernel
        .open_stream(OpenStreamCommand {
            job_id: job_id.to_string(),
            attempt_id: None,
            worker_id: None,
            lease_id: None,
            stream_name: INPUT_STREAM.to_string(),
            scope: StreamScope::Job,
            direction: StreamDirection::ClientToWorker,
            metadata: Attributes::new(),
        })
        .await
        .map_err(|error| format!("open input stream: {error}"))?;

    state
        .kernel
        .push_frame(
            &input_stream.stream_id,
            None,
            None,
            FrameKind::Data,
            payload,
            Attributes::new(),
        )
        .await
        .map_err(|error| format!("push input frame: {error}"))?;

    state
        .kernel
        .close_stream(&input_stream.stream_id, None, None, Attributes::new())
        .await
        .map_err(|error| format!("close input stream: {error}"))?;

    Ok(())
}

async fn cancel_submitted_job(state: &AppState, job_id: &str, error: String) -> OpenAIError {
    match state.kernel.cancel_job(job_id).await {
        Ok(_) => OpenAIError::internal(error),
        Err(cleanup_error) => OpenAIError::internal(format!(
            "{error}; cleanup cancel job '{job_id}': {cleanup_error}"
        )),
    }
}

fn streaming_response(state: AppState, job_id: String) -> Response {
    let kernel = Arc::clone(&state.kernel);
    let deadline = Instant::now() + Duration::from_secs(state.config.request_timeout_secs);
    let poll_interval = Duration::from_millis(kernel.watch_poll_interval_ms());
    let stream = stream! {
        let mut cancel_guard = JobCancellationGuard::new(Arc::clone(&kernel), job_id.clone());
        let mut job_stream_updates = match kernel.subscribe_job_streams(&job_id).await {
            Ok(updates) => updates,
            Err(error) => {
                yield Err::<Vec<u8>, io::Error>(io::Error::other(format!("subscribe job streams: {error}")));
                return;
            }
        };
        let mut cluster_job_stream_updates = match kernel.subscribe_cluster_job_streams(&job_id).await {
            Ok(updates) => updates,
            Err(error) => {
                yield Err::<Vec<u8>, io::Error>(io::Error::other(format!("subscribe cluster job streams: {error}")));
                return;
            }
        };
        let mut job_event_updates = match kernel.subscribe_job_events(&job_id).await {
            Ok(updates) => updates,
            Err(error) => {
                yield Err::<Vec<u8>, io::Error>(io::Error::other(format!("subscribe job events: {error}")));
                return;
            }
        };
        let mut cluster_job_event_updates = match kernel.subscribe_cluster_job_events(&job_id).await {
            Ok(updates) => updates,
            Err(error) => {
                yield Err::<Vec<u8>, io::Error>(io::Error::other(format!("subscribe cluster job events: {error}")));
                return;
            }
        };
        let mut output_stream_id: Option<String> = None;
        let mut after_sequence = 0u64;

        loop {
            if Instant::now() > deadline {
                let _ = kernel.cancel_job(&job_id).await;
                cancel_guard.disarm();
                yield Err::<Vec<u8>, io::Error>(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "request timed out waiting for worker stream",
                ));
                break;
            }

            if output_stream_id.is_none() {
                let streams = match kernel.list_streams(&job_id).await {
                    Ok(streams) => streams,
                    Err(error) => {
                        yield Err(io::Error::other(format!("list streams: {error}")));
                        break;
                    }
                };
                output_stream_id = streams
                    .into_iter()
                    .find(|stream| stream.stream_name == OUTPUT_STREAM)
                    .map(|stream| stream.stream_id);
            }

            if let Some(stream_id) = output_stream_id.as_ref() {
                let frames = match kernel.pull_frames(stream_id, after_sequence).await {
                    Ok(frames) => frames,
                    Err(error) => {
                        yield Err(io::Error::other(format!("pull frames: {error}")));
                        break;
                    }
                };
                let chunks = match extract_data_chunks(frames, &mut after_sequence) {
                    Ok(chunks) => chunks,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                for chunk in chunks {
                    yield Ok(chunk);
                }

                let stream = match kernel.get_stream(stream_id).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        yield Err(io::Error::other(format!("get stream: {error}")));
                        break;
                    }
                };
                let job = match kernel.get_job(&job_id).await {
                    Ok(job) => job,
                    Err(error) => {
                        yield Err(io::Error::other(format!("get job: {error}")));
                        break;
                    }
                };
                if job.state == JobState::Failed {
                    cancel_guard.disarm();
                    yield Err(io::Error::other(
                        job.last_error
                            .unwrap_or_else(|| "worker reported failure".to_string()),
                    ));
                    break;
                }
                if job.state == JobState::Cancelled {
                    cancel_guard.disarm();
                    yield Err(io::Error::other("job was cancelled"));
                    break;
                }
                if job.state.is_terminal() && stream.state.is_terminal() {
                    let remaining = match kernel.pull_frames(stream_id, after_sequence).await {
                        Ok(frames) => frames,
                        Err(error) => {
                            yield Err(io::Error::other(format!("pull frames: {error}")));
                            break;
                        }
                    };
                    let chunks = match extract_data_chunks(remaining, &mut after_sequence) {
                        Ok(chunks) => chunks,
                        Err(error) => {
                            yield Err(error);
                            return;
                        }
                    };
                    for chunk in chunks {
                        yield Ok(chunk);
                    }
                    cancel_guard.disarm();
                    break;
                }
            } else {
                let job = match kernel.get_job(&job_id).await {
                    Ok(job) => job,
                    Err(error) => {
                        yield Err(io::Error::other(format!("get job: {error}")));
                        break;
                    }
                };
                if job.state == JobState::Failed {
                    cancel_guard.disarm();
                    yield Err(io::Error::other(
                        job.last_error
                            .unwrap_or_else(|| "worker reported failure".to_string()),
                    ));
                    break;
                }
                if job.state == JobState::Cancelled {
                    cancel_guard.disarm();
                    yield Err(io::Error::other("job was cancelled"));
                    break;
                }
                if job.state.is_terminal() {
                    let streams = match kernel.list_streams(&job_id).await {
                        Ok(streams) => streams,
                        Err(error) => {
                            yield Err(io::Error::other(format!("list streams: {error}")));
                            break;
                        }
                    };
                    if let Some(stream_id) = streams
                        .into_iter()
                        .find(|stream| stream.stream_name == OUTPUT_STREAM)
                        .map(|stream| stream.stream_id)
                    {
                        output_stream_id = Some(stream_id);
                        continue;
                    }
                    cancel_guard.disarm();
                    yield Err(io::Error::other("job completed without output stream"));
                    break;
                }
            }

            let wait_result = if let Some(stream_id) = output_stream_id.as_ref() {
                wait_for_stream_or_job_change(
                    kernel.as_ref(),
                    stream_id,
                    after_sequence,
                    deadline,
                    poll_interval,
                    &mut job_event_updates,
                    &mut cluster_job_event_updates,
                    "request timed out waiting for worker stream",
                )
                .await
            } else {
                wait_for_job_or_stream_change(
                    deadline,
                    poll_interval,
                    &mut job_stream_updates,
                    &mut cluster_job_stream_updates,
                    &mut job_event_updates,
                    &mut cluster_job_event_updates,
                    "request timed out waiting for worker stream",
                )
                .await
            };

            if let Err(error) = wait_result {
                if error.kind() == io::ErrorKind::TimedOut {
                    let _ = kernel.cancel_job(&job_id).await;
                    cancel_guard.disarm();
                }
                yield Err(error);
                break;
            }
        }
    };

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

struct ReadinessSnapshot {
    chat_ready: bool,
    embeddings_ready: bool,
    matching_chat_workers: usize,
    matching_embeddings_workers: usize,
}

async fn readiness_snapshot(state: &AppState) -> Result<ReadinessSnapshot, OpenAIError> {
    let now_ms = now_ms();
    let workers = state
        .kernel
        .list_workers()
        .await
        .map_err(|error| OpenAIError::internal(format!("list workers: {error}")))?;
    let matching_chat_workers = workers
        .iter()
        .filter(|worker| {
            worker_can_serve(
                worker,
                &state.config.chat_interface,
                &state.config.demand,
                now_ms,
            )
        })
        .count();
    let matching_embeddings_workers = workers
        .iter()
        .filter(|worker| {
            worker_can_serve(
                worker,
                &state.config.embeddings_interface,
                &state.config.demand,
                now_ms,
            )
        })
        .count();

    Ok(ReadinessSnapshot {
        chat_ready: matching_chat_workers > 0,
        embeddings_ready: matching_embeddings_workers > 0,
        matching_chat_workers,
        matching_embeddings_workers,
    })
}

fn worker_can_serve(
    worker: &WorkerRecord,
    interface_name: &str,
    demand: &anyserve_core::model::Demand,
    now_ms: u64,
) -> bool {
    worker.expires_at_ms > now_ms
        && worker.spec.interfaces.contains(interface_name)
        && required_attributes_match(worker, demand)
        && required_capacity_match(&worker.status.available_capacity, &demand.required_capacity)
}

fn required_attributes_match(worker: &WorkerRecord, demand: &anyserve_core::model::Demand) -> bool {
    demand
        .required_attributes
        .iter()
        .all(|(key, expected)| worker.spec.attributes.get(key) == Some(expected))
}

fn required_capacity_match(available: &Capacity, required: &Capacity) -> bool {
    required
        .iter()
        .all(|(key, value)| available.get(key).copied().unwrap_or_default() >= *value)
}

async fn wait_for_output_bytes(state: &AppState, job_id: String) -> Result<Vec<u8>, OpenAIError> {
    match tokio::time::timeout(
        Duration::from_secs(state.config.request_timeout_secs),
        collect_output_bytes(Arc::clone(&state.kernel), job_id.clone()),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            cancel_job_best_effort(Arc::clone(&state.kernel), job_id).await;
            Err(OpenAIError::gateway_timeout(
                "request timed out waiting for worker",
            ))
        }
    }
}

async fn cancel_job_best_effort(kernel: Arc<Kernel>, job_id: String) {
    let _ = kernel.cancel_job(&job_id).await;
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be valid")
        .as_millis() as u64
}

async fn collect_output_bytes(kernel: Arc<Kernel>, job_id: String) -> Result<Vec<u8>, OpenAIError> {
    let mut local_updates = kernel
        .subscribe_job_events(&job_id)
        .await
        .map_err(|error| OpenAIError::internal(format!("subscribe job events: {error}")))?;
    let mut cluster_updates = kernel
        .subscribe_cluster_job_events(&job_id)
        .await
        .map_err(|error| OpenAIError::internal(format!("subscribe cluster job events: {error}")))?;
    let poll_interval = Duration::from_millis(kernel.watch_poll_interval_ms());

    loop {
        let job = kernel
            .get_job(&job_id)
            .await
            .map_err(|error| OpenAIError::internal(format!("get job: {error}")))?;

        match job.state {
            JobState::Succeeded => {
                if let Some(output) = read_output_from_job(&job)? {
                    return Ok(output);
                }
                return read_output_stream(kernel, &job.job_id).await;
            }
            JobState::Failed => {
                return Err(OpenAIError::bad_gateway(
                    job.last_error
                        .unwrap_or_else(|| "worker reported failure".to_string()),
                ));
            }
            JobState::Cancelled => return Err(OpenAIError::bad_gateway("job was cancelled")),
            JobState::Pending | JobState::Leased | JobState::Running => {
                wait_for_job_event_change(poll_interval, &mut local_updates, &mut cluster_updates)
                    .await;
            }
        }
    }
}

fn read_output_from_job(job: &JobRecord) -> Result<Option<Vec<u8>>, OpenAIError> {
    for output in &job.outputs {
        match output {
            ObjectRef::Inline { content, .. } if !content.is_empty() => {
                return Ok(Some(content.clone()));
            }
            ObjectRef::Inline { .. } => continue,
            ObjectRef::Uri { uri, .. } => {
                return Err(OpenAIError::bad_gateway(format!(
                    "worker returned unsupported uri output: {uri}"
                )));
            }
        }
    }

    Ok(None)
}

fn extract_data_chunks(frames: Vec<Frame>, after_sequence: &mut u64) -> io::Result<Vec<Vec<u8>>> {
    let mut chunks = Vec::new();
    for frame in frames {
        *after_sequence = frame.sequence;
        match frame.kind {
            FrameKind::Data => chunks.push(frame.payload),
            FrameKind::Error => {
                return Err(io::Error::other(
                    String::from_utf8_lossy(&frame.payload).into_owned(),
                ));
            }
            _ => {}
        }
    }
    Ok(chunks)
}

async fn wait_for_job_or_stream_change(
    deadline: Instant,
    poll_interval: Duration,
    local_job_stream_updates: &mut watch::Receiver<u64>,
    cluster_job_stream_updates: &mut ClusterSubscription,
    local_job_event_updates: &mut watch::Receiver<u64>,
    cluster_job_event_updates: &mut ClusterSubscription,
    timeout_message: &'static str,
) -> io::Result<()> {
    let poll_deadline = (Instant::now() + poll_interval).min(deadline);
    tokio::select! {
        _ = sleep_until(deadline) => Err(io::Error::new(io::ErrorKind::TimedOut, timeout_message)),
        _ = sleep_until(poll_deadline) => Ok(()),
        result = local_job_stream_updates.changed() => {
            result.map_err(|_| io::Error::other("job stream watcher closed"))
        }
        result = cluster_job_stream_updates.changed() => {
            result.map_err(|error| io::Error::other(format!("cluster job stream watcher closed: {error}")))
        }
        result = local_job_event_updates.changed() => {
            result.map_err(|_| io::Error::other("job event watcher closed"))
        }
        result = cluster_job_event_updates.changed() => {
            result.map_err(|error| io::Error::other(format!("cluster job event watcher closed: {error}")))
        }
    }
}

async fn wait_for_stream_or_job_change(
    kernel: &Kernel,
    stream_id: &str,
    after_sequence: u64,
    deadline: Instant,
    poll_interval: Duration,
    local_job_event_updates: &mut watch::Receiver<u64>,
    cluster_job_event_updates: &mut ClusterSubscription,
    timeout_message: &'static str,
) -> io::Result<()> {
    tokio::select! {
        _ = sleep_until(deadline) => Err(io::Error::new(io::ErrorKind::TimedOut, timeout_message)),
        result = kernel.wait_for_frames(stream_id, after_sequence, poll_interval) => {
            result
                .map(|_| ())
                .map_err(|error| io::Error::other(format!("wait for frames: {error}")))
        }
        result = local_job_event_updates.changed() => {
            result.map_err(|_| io::Error::other("job event watcher closed"))
        }
        result = cluster_job_event_updates.changed() => {
            result.map_err(|error| io::Error::other(format!("cluster job event watcher closed: {error}")))
        }
    }
}

async fn wait_for_job_event_change(
    poll_interval: Duration,
    local_updates: &mut watch::Receiver<u64>,
    cluster_updates: &mut ClusterSubscription,
) {
    tokio::select! {
        result = local_updates.changed() => {
            if result.is_err() {
                sleep_until(Instant::now() + poll_interval).await;
            }
        }
        result = cluster_updates.changed() => {
            if result.is_err() {
                sleep_until(Instant::now() + poll_interval).await;
            }
        }
        _ = sleep_until(Instant::now() + poll_interval) => {}
    }
}

async fn read_output_stream(kernel: Arc<Kernel>, job_id: &str) -> Result<Vec<u8>, OpenAIError> {
    let streams = kernel
        .list_streams(job_id)
        .await
        .map_err(|error| OpenAIError::internal(format!("list output streams: {error}")))?;
    let Some(output_stream) = streams
        .iter()
        .find(|stream| stream.stream_name == OUTPUT_STREAM)
    else {
        return Err(OpenAIError::bad_gateway(
            "worker completed without output.default stream",
        ));
    };

    let frames = kernel
        .pull_frames(&output_stream.stream_id, 0)
        .await
        .map_err(|error| OpenAIError::internal(format!("pull output frames: {error}")))?;

    let mut payload = Vec::new();
    for frame in frames {
        match frame.kind {
            FrameKind::Data => payload.extend(frame.payload),
            FrameKind::Error => {
                let message = String::from_utf8_lossy(&frame.payload).into_owned();
                return Err(OpenAIError::bad_gateway(if message.is_empty() {
                    "worker returned error frame".to_string()
                } else {
                    message
                }));
            }
            _ => {}
        }
    }

    if payload.is_empty() {
        return Err(OpenAIError::bad_gateway(
            "worker completed without response payload",
        ));
    }

    Ok(payload)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use tokio::sync::watch;

    use super::{AppState, read_output_from_job, submit_request};
    use crate::serve_config::ServeOpenAIConfig;
    use anyserve_core::frame::{FramePlane, MemoryFramePlane};
    use anyserve_core::kernel::Kernel;
    use anyserve_core::model::{
        Attributes, Demand, Frame, FrameKind, JobRecord, JobSpec, JobState, ObjectRef, StreamRecord,
    };
    use anyserve_core::notify::NoopClusterNotifier;
    use anyserve_core::scheduler::BasicScheduler;
    use anyserve_core::store::MemoryStateStore;

    #[test]
    fn read_output_from_job_prefers_inline_output() {
        let job = JobRecord {
            spec: JobSpec::default(),
            outputs: vec![ObjectRef::Inline {
                content: br#"{"ok":true}"#.to_vec(),
                metadata: Attributes::from([(
                    "content_type".to_string(),
                    "application/json".to_string(),
                )]),
            }],
            ..JobRecord::default()
        };

        let output = read_output_from_job(&job).expect("inline output should be supported");
        assert_eq!(output, Some(br#"{"ok":true}"#.to_vec()));
    }

    #[tokio::test]
    async fn streamed_submit_cancels_job_when_input_upload_fails() {
        let state = test_state(Arc::new(FailingAppendFramePlane {
            inner: MemoryFramePlane::new(),
        }));

        let error = submit_request(&state, "llm.chat.v1", b"hello".to_vec())
            .await
            .expect_err("streamed submit should fail");
        assert!(error.message.contains("push input frame"));

        let jobs = state.kernel.list_jobs().await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, JobState::Cancelled);

        let streams = state.kernel.list_streams(&jobs[0].job_id).await.unwrap();
        assert_eq!(streams.len(), 1);
        assert!(streams[0].state.is_terminal());
    }

    fn test_state(frame_plane: Arc<dyn FramePlane>) -> AppState {
        AppState {
            kernel: Arc::new(Kernel::new(
                Arc::new(MemoryStateStore::new()),
                frame_plane,
                Arc::new(NoopClusterNotifier),
                Arc::new(BasicScheduler),
                30,
                30,
                250,
            )),
            config: Arc::new(ServeOpenAIConfig {
                listen: "127.0.0.1:8080".to_string(),
                models: vec!["demo".to_string()],
                chat_interface: "llm.chat.v1".to_string(),
                embeddings_interface: "llm.embed.v1".to_string(),
                request_timeout_secs: 30,
                inline_request_limit_bytes: 1,
                demand: Demand::default(),
            }),
        }
    }

    struct FailingAppendFramePlane {
        inner: MemoryFramePlane,
    }

    #[async_trait]
    impl FramePlane for FailingAppendFramePlane {
        async fn stream_created(&self, stream: &StreamRecord) -> Result<()> {
            self.inner.stream_created(stream).await
        }

        async fn stream_updated(&self, stream: &StreamRecord) -> Result<()> {
            self.inner.stream_updated(stream).await
        }

        async fn append_frame(
            &self,
            _stream: &StreamRecord,
            _kind: FrameKind,
            _payload: Vec<u8>,
            _metadata: Attributes,
            _now_ms: u64,
        ) -> Result<Frame> {
            Err(anyhow!("synthetic append failure"))
        }

        async fn frames_after(&self, stream_id: &str, after_sequence: u64) -> Result<Vec<Frame>> {
            self.inner.frames_after(stream_id, after_sequence).await
        }

        async fn wait_for_frames(
            &self,
            stream_id: &str,
            after_sequence: u64,
            timeout: Duration,
        ) -> Result<Vec<Frame>> {
            self.inner
                .wait_for_frames(stream_id, after_sequence, timeout)
                .await
        }

        async fn latest_sequence(&self, stream_id: &str) -> Result<Option<u64>> {
            self.inner.latest_sequence(stream_id).await
        }

        async fn subscribe_stream_updates(&self, stream_id: &str) -> watch::Receiver<u64> {
            self.inner.subscribe_stream_updates(stream_id).await
        }
    }
}
