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
use serde_json::json;

use anyserve_core::model::JobState;

use super::api::{
    ApiError, AppState, CancelJobResponse, HistorySummary, JobDetail, JobDetailResponse, JobsQuery,
    JobsResponse, QueueSummary, WorkerRollup, WorkersResponse,
};
use super::snapshot::{
    aggregate_capacity, attempt_summary, event_summary, inline_preview, job_duration_ms,
    job_summary, job_summary_from_store, latest_worker_id, metadata_value, non_empty,
    object_ref_summary, read_output_preview, stream_summary, worker_summary,
};

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 200;
const INLINE_PREVIEW_LIMIT_BYTES: usize = 4 * 1024;

pub(crate) fn router(state: AppState) -> Router {
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

    let queue = QueueSummary {
        pending: counts.pending,
        leased: counts.leased,
        running: counts.running,
        active: counts.pending + counts.leased + counts.running,
    };
    let history = HistorySummary {
        succeeded: counts.succeeded,
        failed: counts.failed,
        cancelled: counts.cancelled,
        total: counts.succeeded + counts.failed + counts.cancelled,
    };

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

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
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
