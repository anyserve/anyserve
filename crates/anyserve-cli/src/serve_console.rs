use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    ORIGIN, VARY,
};
use axum::http::{HeaderValue, Method, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::watch;

use anyserve_core::kernel::Kernel;
use anyserve_core::model::{
    AttemptRecord, Demand, EventKind, ExecutionPolicy, FrameKind, JobEvent, JobRecord, JobState,
    ObjectRef, StreamRecord, WorkerRecord,
};
use anyserve_core::store::JobListSummary as StoreJobListSummary;

use crate::serve_config::{RuntimeMode, ServeConsoleConfig};

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 200;
const INLINE_PREVIEW_LIMIT_BYTES: usize = 4 * 1024;

#[derive(Clone)]
struct AppState {
    kernel: Arc<Kernel>,
    allowed_origins: Arc<BTreeSet<String>>,
    runtime: RuntimeModeSummary,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: ErrorBody {
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    message: String,
}

#[derive(Clone, Serialize)]
struct RuntimeModeSummary {
    code: &'static str,
    label: &'static str,
    storage_backend: &'static str,
    frame_backend: &'static str,
}

#[derive(Serialize)]
struct QueueSummary {
    pending: usize,
    leased: usize,
    running: usize,
    active: usize,
}

#[derive(Serialize)]
struct HistorySummary {
    succeeded: usize,
    failed: usize,
    cancelled: usize,
    total: usize,
}

#[derive(Serialize)]
struct WorkerRollup {
    total: usize,
    healthy: usize,
    expired: usize,
    active_leases: u64,
    total_capacity: BTreeMap<String, i64>,
    available_capacity: BTreeMap<String, i64>,
    last_seen_at_ms: Option<u64>,
}

#[derive(Serialize)]
struct JobsResponse {
    jobs: Vec<JobSummary>,
    total: usize,
    limit: usize,
    offset: usize,
}

#[derive(Serialize)]
struct JobSummary {
    job_id: String,
    state: JobState,
    interface_name: String,
    source: Option<String>,
    created_at_ms: u64,
    updated_at_ms: u64,
    duration_ms: Option<u64>,
    current_attempt_id: Option<String>,
    latest_worker_id: Option<String>,
    attempt_count: usize,
    last_error: Option<String>,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct JobDetailResponse {
    job: JobDetail,
    attempts: Vec<AttemptSummary>,
    streams: Vec<StreamSummary>,
    events: Vec<JobEventSummary>,
    latest_worker: Option<WorkerSummary>,
}

#[derive(Serialize)]
struct JobDetail {
    job_id: String,
    state: JobState,
    interface_name: String,
    source: Option<String>,
    created_at_ms: u64,
    updated_at_ms: u64,
    duration_ms: Option<u64>,
    current_attempt_id: Option<String>,
    lease_id: Option<String>,
    version: u64,
    last_error: Option<String>,
    metadata: BTreeMap<String, String>,
    demand: Demand,
    policy: ExecutionPolicy,
    params_size_bytes: usize,
    params_preview: Option<String>,
    params_truncated: bool,
    output_preview: Option<InlinePreviewSummary>,
    inputs: Vec<ObjectRefSummary>,
    outputs: Vec<ObjectRefSummary>,
}

#[derive(Serialize)]
struct InlinePreviewSummary {
    size_bytes: usize,
    content_type: Option<String>,
    preview: Option<String>,
    truncated: bool,
}

#[derive(Serialize)]
struct ObjectRefSummary {
    kind: &'static str,
    size_bytes: Option<usize>,
    uri: Option<String>,
    content_type: Option<String>,
    preview: Option<String>,
    truncated: bool,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct AttemptSummary {
    attempt_id: String,
    state: anyserve_core::model::AttemptState,
    worker_id: Option<String>,
    lease_id: Option<String>,
    created_at_ms: u64,
    started_at_ms: Option<u64>,
    finished_at_ms: Option<u64>,
    duration_ms: Option<u64>,
    last_error: Option<String>,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct StreamSummary {
    stream_id: String,
    stream_name: String,
    scope: anyserve_core::model::StreamScope,
    direction: anyserve_core::model::StreamDirection,
    state: anyserve_core::model::StreamState,
    attempt_id: Option<String>,
    lease_id: Option<String>,
    created_at_ms: u64,
    closed_at_ms: Option<u64>,
    last_sequence: u64,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct JobEventSummary {
    sequence: u64,
    kind: EventKind,
    created_at_ms: u64,
    payload_size_bytes: usize,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct WorkersResponse {
    workers: Vec<WorkerSummary>,
    total: usize,
    healthy: usize,
}

#[derive(Clone, Serialize)]
struct WorkerSummary {
    worker_id: String,
    healthy: bool,
    interfaces: Vec<String>,
    attributes: BTreeMap<String, String>,
    total_capacity: BTreeMap<String, i64>,
    available_capacity: BTreeMap<String, i64>,
    max_active_leases: u32,
    active_leases: u32,
    registered_at_ms: u64,
    last_seen_at_ms: u64,
    expires_at_ms: u64,
    spec_metadata: BTreeMap<String, String>,
    status_metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct CancelJobResponse {
    job: JobSummary,
}

#[derive(Deserialize)]
struct JobsQuery {
    states: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

pub async fn run_server(
    addr: SocketAddr,
    kernel: Arc<Kernel>,
    config: ServeConsoleConfig,
    runtime_mode: RuntimeMode,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let state = AppState {
        kernel,
        allowed_origins: Arc::new(config.allowed_origins.into_iter().collect()),
        runtime: runtime_mode_summary(runtime_mode),
    };
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind console listener {addr}"))?;

    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run console http server")
}

fn router(state: AppState) -> Router {
    let middleware_state = state.clone();
    Router::new()
        .route("/healthz", get(healthz))
        .route("/api/overview", get(overview))
        .route("/api/jobs", get(list_jobs))
        .route("/api/jobs/:job_id", get(get_job))
        .route("/api/jobs/:job_id/cancel", post(cancel_job))
        .route("/api/workers", get(list_workers))
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            middleware_state,
            cors_middleware,
        ))
}

async fn healthz() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn overview(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let counts = state
        .kernel
        .job_state_counts()
        .await
        .map_err(internal_error("count jobs"))?;
    let workers = state
        .kernel
        .list_workers()
        .await
        .map_err(internal_error("list workers"))?;
    let now_ms = now_ms();

    let mut queue = QueueSummary {
        pending: 0,
        leased: 0,
        running: 0,
        active: 0,
    };
    let mut history = HistorySummary {
        succeeded: 0,
        failed: 0,
        cancelled: 0,
        total: 0,
    };

    queue.pending = counts.pending;
    queue.leased = counts.leased;
    queue.running = counts.running;
    queue.active = queue.pending + queue.leased + queue.running;
    history.succeeded = counts.succeeded;
    history.failed = counts.failed;
    history.cancelled = counts.cancelled;
    history.total = history.succeeded + history.failed + history.cancelled;

    let healthy_workers = workers
        .iter()
        .filter(|worker| worker.expires_at_ms > now_ms)
        .collect::<Vec<_>>();

    let worker_rollup = WorkerRollup {
        total: workers.len(),
        healthy: healthy_workers.len(),
        expired: workers.len().saturating_sub(healthy_workers.len()),
        active_leases: workers
            .iter()
            .map(|worker| worker.status.active_leases as u64)
            .sum(),
        total_capacity: aggregate_capacity(
            healthy_workers
                .iter()
                .map(|worker| &worker.spec.total_capacity),
        ),
        available_capacity: aggregate_capacity(
            healthy_workers
                .iter()
                .map(|worker| &worker.status.available_capacity),
        ),
        last_seen_at_ms: workers
            .iter()
            .map(|worker| worker.status.last_seen_at_ms)
            .max(),
    };

    Ok(Json(json!({
        "runtime": {
            "code": state.runtime.code,
            "label": state.runtime.label,
            "storage_backend": state.runtime.storage_backend,
            "frame_backend": state.runtime.frame_backend,
        },
        "queue": queue,
        "history": history,
        "workers": worker_rollup,
        "refreshed_at_ms": now_ms,
    })))
}

async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<JobsQuery>,
) -> Result<Json<JobsResponse>, ApiError> {
    let selected_states = parse_states(query.states.as_deref())?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let offset = query.offset.unwrap_or(0);
    let page = state
        .kernel
        .list_job_summary_page(selected_states.as_deref().unwrap_or(&[]), limit, offset)
        .await
        .map_err(internal_error("list job summaries"))?;

    Ok(Json(JobsResponse {
        jobs: page.jobs.iter().map(job_summary_from_store).collect(),
        total: page.total,
        limit,
        offset,
    }))
}

async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobDetailResponse>, ApiError> {
    let job = state
        .kernel
        .get_job(&job_id)
        .await
        .map_err(not_found_if_missing("load job"))?;
    let attempts = state
        .kernel
        .list_attempts(&job_id)
        .await
        .map_err(internal_error("list attempts"))?;
    let streams = state
        .kernel
        .list_streams(&job_id)
        .await
        .map_err(internal_error("list streams"))?;
    let events = state
        .kernel
        .list_job_events(&job_id)
        .await
        .map_err(internal_error("list events"))?;

    let workers = state
        .kernel
        .list_workers()
        .await
        .map_err(internal_error("list workers"))?;
    let latest_worker = latest_worker_id(&attempts).and_then(|worker_id| {
        workers
            .iter()
            .find(|worker| worker.worker_id == worker_id)
            .cloned()
    });

    Ok(Json(JobDetailResponse {
        job: JobDetail {
            job_id: job.job_id.clone(),
            state: job.state,
            interface_name: job.spec.interface_name.clone(),
            source: metadata_value(&job.spec.metadata, "source"),
            created_at_ms: job.created_at_ms,
            updated_at_ms: job.updated_at_ms,
            duration_ms: job_duration_ms(&job),
            current_attempt_id: non_empty(&job.current_attempt_id),
            lease_id: non_empty(&job.lease_id),
            version: job.version,
            last_error: non_empty(&job.last_error),
            metadata: job.spec.metadata.clone(),
            demand: job.spec.demand.clone(),
            policy: job.spec.policy.clone(),
            params_size_bytes: job.spec.params.len(),
            params_preview: inline_preview(&job.spec.params),
            params_truncated: job.spec.params.len() > INLINE_PREVIEW_LIMIT_BYTES,
            output_preview: read_output_preview(&state.kernel, &streams)
                .await
                .map_err(internal_error("read output preview"))?,
            inputs: job.spec.inputs.iter().map(object_ref_summary).collect(),
            outputs: job.outputs.iter().map(object_ref_summary).collect(),
        },
        attempts: attempts.iter().map(attempt_summary).collect(),
        streams: streams.iter().map(stream_summary).collect(),
        events: events.iter().map(event_summary).collect(),
        latest_worker: latest_worker
            .as_ref()
            .map(|worker| worker_summary(worker, now_ms())),
    }))
}

async fn cancel_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<CancelJobResponse>, ApiError> {
    let job = state
        .kernel
        .cancel_job(&job_id)
        .await
        .map_err(not_found_if_missing("cancel job"))?;
    let attempts = state
        .kernel
        .list_attempts(&job_id)
        .await
        .map_err(internal_error("list attempts"))?;
    Ok(Json(CancelJobResponse {
        job: job_summary(&job, &attempts),
    }))
}

async fn list_workers(State(state): State<AppState>) -> Result<Json<WorkersResponse>, ApiError> {
    let workers = state
        .kernel
        .list_workers()
        .await
        .map_err(internal_error("list workers"))?;
    let now_ms = now_ms();

    Ok(Json(WorkersResponse {
        healthy: workers
            .iter()
            .filter(|worker| worker.expires_at_ms > now_ms)
            .count(),
        total: workers.len(),
        workers: workers
            .iter()
            .map(|worker| worker_summary(worker, now_ms))
            .collect(),
    }))
}

async fn cors_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let origin = request
        .headers()
        .get(ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let is_preflight = request.method() == Method::OPTIONS;
    let response = if is_preflight {
        StatusCode::NO_CONTENT.into_response()
    } else {
        next.run(request).await
    };

    let mut response = response;
    let headers = response.headers_mut();
    headers.insert(VARY, HeaderValue::from_static("Origin"));
    headers.insert(
        ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    headers.insert(
        ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Accept, Content-Type"),
    );

    if let Some(origin) = origin
        .as_deref()
        .filter(|origin| state.allowed_origins.contains(*origin))
        .and_then(|origin| HeaderValue::from_str(origin).ok())
    {
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }

    response
}

fn runtime_mode_summary(mode: RuntimeMode) -> RuntimeModeSummary {
    match mode {
        RuntimeMode::Memory => RuntimeModeSummary {
            code: mode.code(),
            label: mode.label(),
            storage_backend: "memory",
            frame_backend: "memory",
        },
        RuntimeMode::MemoryPlusPostgres => RuntimeModeSummary {
            code: mode.code(),
            label: mode.label(),
            storage_backend: "postgresql",
            frame_backend: "memory",
        },
        RuntimeMode::MemoryPlusSqlite => RuntimeModeSummary {
            code: mode.code(),
            label: mode.label(),
            storage_backend: "sqlite",
            frame_backend: "memory",
        },
        RuntimeMode::PostgresAndRedis => RuntimeModeSummary {
            code: mode.code(),
            label: mode.label(),
            storage_backend: "postgresql",
            frame_backend: "redis",
        },
    }
}

fn parse_states(raw: Option<&str>) -> Result<Option<Vec<JobState>>, ApiError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let mut states = Vec::new();
    for value in raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let state = match value {
            "pending" => JobState::Pending,
            "leased" => JobState::Leased,
            "running" => JobState::Running,
            "succeeded" => JobState::Succeeded,
            "failed" => JobState::Failed,
            "cancelled" => JobState::Cancelled,
            other => {
                return Err(ApiError::bad_request(format!(
                    "unsupported state filter '{other}'"
                )));
            }
        };
        if !states.contains(&state) {
            states.push(state);
        }
    }
    Ok(Some(states))
}

fn aggregate_capacity<'a>(
    capacities: impl Iterator<Item = &'a BTreeMap<String, i64>>,
) -> BTreeMap<String, i64> {
    let mut total = BTreeMap::new();
    for capacity in capacities {
        for (name, value) in capacity {
            *total.entry(name.clone()).or_default() += value;
        }
    }
    total
}

fn job_summary(job: &JobRecord, attempts: &[AttemptRecord]) -> JobSummary {
    JobSummary {
        job_id: job.job_id.clone(),
        state: job.state,
        interface_name: job.spec.interface_name.clone(),
        source: metadata_value(&job.spec.metadata, "source"),
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        duration_ms: job_duration_ms(job),
        current_attempt_id: non_empty(&job.current_attempt_id),
        latest_worker_id: latest_worker_id(attempts),
        attempt_count: attempts.len(),
        last_error: non_empty(&job.last_error),
        metadata: job.spec.metadata.clone(),
    }
}

fn job_summary_from_store(job: &StoreJobListSummary) -> JobSummary {
    JobSummary {
        job_id: job.job_id.clone(),
        state: job.state,
        interface_name: job.interface_name.clone(),
        source: job.source.clone(),
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        duration_ms: job
            .state
            .is_terminal()
            .then(|| job.updated_at_ms.saturating_sub(job.created_at_ms)),
        current_attempt_id: non_empty(&job.current_attempt_id),
        latest_worker_id: job.latest_worker_id.clone(),
        attempt_count: job.attempt_count,
        last_error: non_empty(&job.last_error),
        metadata: job.metadata.clone(),
    }
}

fn worker_summary(worker: &WorkerRecord, now_ms: u64) -> WorkerSummary {
    WorkerSummary {
        worker_id: worker.worker_id.clone(),
        healthy: worker.expires_at_ms > now_ms,
        interfaces: worker.spec.interfaces.iter().cloned().collect(),
        attributes: worker.spec.attributes.clone(),
        total_capacity: worker.spec.total_capacity.clone(),
        available_capacity: worker.status.available_capacity.clone(),
        max_active_leases: worker.spec.max_active_leases,
        active_leases: worker.status.active_leases,
        registered_at_ms: worker.registered_at_ms,
        last_seen_at_ms: worker.status.last_seen_at_ms,
        expires_at_ms: worker.expires_at_ms,
        spec_metadata: worker.spec.metadata.clone(),
        status_metadata: worker.status.metadata.clone(),
    }
}

fn attempt_summary(attempt: &AttemptRecord) -> AttemptSummary {
    AttemptSummary {
        attempt_id: attempt.attempt_id.clone(),
        state: attempt.state,
        worker_id: non_empty_string(&attempt.worker_id),
        lease_id: non_empty_string(&attempt.lease_id),
        created_at_ms: attempt.created_at_ms,
        started_at_ms: attempt.started_at_ms,
        finished_at_ms: attempt.finished_at_ms,
        duration_ms: attempt_duration_ms(attempt),
        last_error: non_empty(&attempt.last_error),
        metadata: attempt.metadata.clone(),
    }
}

fn stream_summary(stream: &StreamRecord) -> StreamSummary {
    StreamSummary {
        stream_id: stream.stream_id.clone(),
        stream_name: stream.stream_name.clone(),
        scope: stream.scope,
        direction: stream.direction,
        state: stream.state,
        attempt_id: non_empty(&stream.attempt_id),
        lease_id: non_empty(&stream.lease_id),
        created_at_ms: stream.created_at_ms,
        closed_at_ms: stream.closed_at_ms,
        last_sequence: stream.last_sequence,
        metadata: stream.metadata.clone(),
    }
}

fn event_summary(event: &JobEvent) -> JobEventSummary {
    JobEventSummary {
        sequence: event.sequence,
        kind: event.kind,
        created_at_ms: event.created_at_ms,
        payload_size_bytes: event.payload.len(),
        metadata: event.metadata.clone(),
    }
}

fn object_ref_summary(value: &ObjectRef) -> ObjectRefSummary {
    match value {
        ObjectRef::Inline { content, metadata } => ObjectRefSummary {
            kind: "inline",
            size_bytes: Some(content.len()),
            uri: None,
            content_type: metadata_value(metadata, "content_type"),
            preview: inline_preview(content),
            truncated: content.len() > INLINE_PREVIEW_LIMIT_BYTES,
            metadata: metadata.clone(),
        },
        ObjectRef::Uri { uri, metadata } => ObjectRefSummary {
            kind: "uri",
            size_bytes: None,
            uri: Some(uri.clone()),
            content_type: metadata_value(metadata, "content_type"),
            preview: None,
            truncated: false,
            metadata: metadata.clone(),
        },
    }
}

async fn read_output_preview(
    kernel: &Kernel,
    streams: &[StreamRecord],
) -> Result<Option<InlinePreviewSummary>> {
    let Some(output_stream) = streams.iter().find(|stream| stream.stream_name == "output.default")
    else {
        return Ok(None);
    };

    let frames = kernel.pull_frames(&output_stream.stream_id, 0).await?;
    let mut payload = Vec::new();
    let mut content_type = metadata_value(&output_stream.metadata, "content_type");

    for frame in frames {
        match frame.kind {
            FrameKind::Data => {
                if content_type.is_none() {
                    content_type = metadata_value(&frame.metadata, "content_type");
                }
                payload.extend(frame.payload);
            }
            FrameKind::Error => {
                if payload.is_empty() {
                    payload.extend(frame.payload);
                }
            }
            _ => {}
        }
    }

    if payload.is_empty() {
        return Ok(None);
    }

    Ok(Some(InlinePreviewSummary {
        size_bytes: payload.len(),
        content_type,
        preview: inline_preview(&payload),
        truncated: payload.len() > INLINE_PREVIEW_LIMIT_BYTES,
    }))
}

fn inline_preview(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() || !is_probably_text(bytes) {
        return None;
    }

    let preview = &bytes[..bytes.len().min(INLINE_PREVIEW_LIMIT_BYTES)];
    Some(String::from_utf8_lossy(preview).trim().to_string())
}

fn is_probably_text(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }

    let sample = &bytes[..bytes.len().min(512)];
    let binary_like = sample
        .iter()
        .filter(|byte| matches!(byte, 0..=8 | 11..=12 | 14..=31))
        .count();

    binary_like * 10 <= sample.len()
}

fn metadata_value(metadata: &BTreeMap<String, String>, key: &str) -> Option<String> {
    metadata.get(key).and_then(|value| non_empty_string(value))
}

fn latest_worker_id(attempts: &[AttemptRecord]) -> Option<String> {
    attempts
        .iter()
        .max_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.attempt_id.cmp(&right.attempt_id))
        })
        .and_then(|attempt| non_empty_string(&attempt.worker_id))
}

fn attempt_duration_ms(attempt: &AttemptRecord) -> Option<u64> {
    attempt.finished_at_ms.map(|finished| {
        finished.saturating_sub(attempt.started_at_ms.unwrap_or(attempt.created_at_ms))
    })
}

fn job_duration_ms(job: &JobRecord) -> Option<u64> {
    job.state
        .is_terminal()
        .then(|| job.updated_at_ms.saturating_sub(job.created_at_ms))
}

fn non_empty(value: &Option<String>) -> Option<String> {
    value.as_deref().and_then(non_empty_string)
}

fn non_empty_string(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.to_string())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be valid")
        .as_millis() as u64
}

fn internal_error(context: &'static str) -> impl Fn(anyhow::Error) -> ApiError {
    move |error| ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("{context}: {error}"),
    }
}

fn not_found_if_missing(context: &'static str) -> impl Fn(anyhow::Error) -> ApiError {
    move |error| {
        let message = error.to_string();
        if message.contains("does not exist") {
            ApiError::not_found(message)
        } else {
            ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("{context}: {message}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AppState, StatusCode, parse_states, router, runtime_mode_summary};
    use anyserve_core::frame::MemoryFramePlane;
    use anyserve_core::kernel::Kernel;
    use anyserve_core::model::{Attributes, JobSpec, WorkerSpec};
    use anyserve_core::notify::NoopClusterNotifier;
    use anyserve_core::scheduler::BasicScheduler;
    use anyserve_core::store::MemoryStateStore;
    use serde_json::Value;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use crate::serve_config::RuntimeMode;

    #[test]
    fn parse_states_rejects_invalid_values() {
        let error = parse_states(Some("pending,nope")).expect_err("should reject invalid state");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn overview_reports_runtime_queue_history_and_workers() {
        let kernel = test_kernel();
        register_worker(&kernel, "worker-overview").await;
        kernel
            .submit_job(None, test_job_spec("demo.queue.v1"))
            .await
            .unwrap();
        let cancelled = kernel
            .submit_job(None, test_job_spec("demo.history.v1"))
            .await
            .unwrap();
        kernel.cancel_job(&cancelled.job_id).await.unwrap();

        let (base_url, handle) = spawn_test_server(kernel).await;
        let response = reqwest::get(format!("{base_url}/api/overview"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body: Value = response.json().await.unwrap();
        assert_eq!(body["runtime"]["code"], "memory");
        assert_eq!(body["queue"]["pending"], 1);
        assert_eq!(body["history"]["cancelled"], 1);
        assert_eq!(body["workers"]["healthy"], 1);

        handle.abort();
    }

    #[tokio::test]
    async fn jobs_endpoint_filters_and_paginates() {
        let kernel = test_kernel();
        let first = kernel
            .submit_job(None, test_job_spec("demo.first.v1"))
            .await
            .unwrap();
        let second = kernel
            .submit_job(None, test_job_spec("demo.second.v1"))
            .await
            .unwrap();
        kernel.cancel_job(&second.job_id).await.unwrap();
        kernel
            .submit_job(None, test_job_spec("demo.third.v1"))
            .await
            .unwrap();

        let (base_url, handle) = spawn_test_server(kernel).await;
        let response = reqwest::get(format!(
            "{base_url}/api/jobs?states=cancelled&limit=1&offset=0"
        ))
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body: Value = response.json().await.unwrap();
        assert_eq!(body["total"], 1);
        assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
        assert_eq!(body["jobs"][0]["state"], "cancelled");
        assert_eq!(body["jobs"][0]["job_id"], second.job_id);
        assert_ne!(body["jobs"][0]["job_id"], first.job_id);

        handle.abort();
    }

    #[tokio::test]
    async fn job_detail_and_cancel_endpoint_return_payloads() {
        let kernel = test_kernel();
        let job = kernel
            .submit_job(None, test_job_spec("demo.detail.v1"))
            .await
            .unwrap();

        let (base_url, handle) = spawn_test_server(kernel.clone()).await;
        let client = reqwest::Client::new();

        let detail = client
            .get(format!("{base_url}/api/jobs/{}", job.job_id))
            .send()
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);
        let body: Value = detail.json().await.unwrap();
        assert!(body["events"].as_array().unwrap().len() >= 1);
        assert_eq!(body["job"]["params_size_bytes"], 5);
        assert_eq!(body["job"]["inputs"][0]["kind"], "inline");
        assert!(body["job"]["inputs"][0].get("content").is_none());

        let cancel = client
            .post(format!("{base_url}/api/jobs/{}/cancel", job.job_id))
            .send()
            .await
            .unwrap();
        assert_eq!(cancel.status(), StatusCode::OK);
        let cancel_body: Value = cancel.json().await.unwrap();
        assert_eq!(cancel_body["job"]["state"], "cancelled");

        handle.abort();
    }

    #[tokio::test]
    async fn workers_endpoint_reports_health() {
        let kernel = test_kernel();
        register_worker(&kernel, "worker-health").await;

        let (base_url, handle) = spawn_test_server(kernel).await;
        let response = reqwest::get(format!("{base_url}/api/workers"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body: Value = response.json().await.unwrap();
        assert_eq!(body["total"], 1);
        assert_eq!(body["healthy"], 1);
        assert_eq!(body["workers"][0]["worker_id"], "worker-health");
        assert_eq!(body["workers"][0]["healthy"], true);

        handle.abort();
    }

    fn test_kernel() -> Arc<Kernel> {
        Arc::new(Kernel::new(
            Arc::new(MemoryStateStore::new()),
            Arc::new(MemoryFramePlane::new()),
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            250,
        ))
    }

    async fn spawn_test_server(kernel: Arc<Kernel>) -> (String, tokio::task::JoinHandle<()>) {
        let state = AppState {
            kernel,
            allowed_origins: Arc::new(BTreeSet::new()),
            runtime: runtime_mode_summary(RuntimeMode::Memory),
        };
        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), handle)
    }

    fn test_job_spec(interface_name: &str) -> JobSpec {
        JobSpec {
            interface_name: interface_name.to_string(),
            inputs: vec![anyserve_core::model::ObjectRef::inline(b"hello".to_vec())],
            params: b"world".to_vec(),
            metadata: Attributes::from([("source".to_string(), "runtime".to_string())]),
            ..JobSpec::default()
        }
    }

    async fn register_worker(kernel: &Arc<Kernel>, worker_id: &str) {
        let worker = kernel
            .register_worker(
                Some(worker_id.to_string()),
                WorkerSpec {
                    interfaces: BTreeSet::from(["demo.queue.v1".to_string()]),
                    attributes: BTreeMap::from([("region".to_string(), "local".to_string())]),
                    total_capacity: BTreeMap::from([("slot".to_string(), 4)]),
                    max_active_leases: 4,
                    metadata: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        kernel
            .heartbeat_worker(
                &worker.worker_id,
                BTreeMap::from([("slot".to_string(), 4)]),
                0,
                BTreeMap::new(),
            )
            .await
            .unwrap();
    }
}
