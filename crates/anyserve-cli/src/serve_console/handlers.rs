use super::api::{
    AnalyticsJob, AnalyticsQuery, AnalyticsResponse, AnalyticsSource, ApiError, AppState,
    CancelJobResponse, JobDetail, JobDetailResponse, JobsQuery, JobsResponse, WorkersResponse,
};
use super::snapshot::{
    INLINE_PREVIEW_LIMIT_BYTES, aggregate_capacity, attempts_from_events, event_summary,
    inline_preview, job_summary, object_preview_summary, object_ref_summary, output_preview,
    worker_summary,
};
use anyserve_core::auth::AuthPolicy;
use anyserve_core::error::{ErrorKind, RuntimeError};
use anyserve_core::now_ms;
use anyserve_core::runtime::api::{Durability, JobFilter, JobPreview, JobScan, JobState};
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
use std::sync::Arc;

pub(crate) fn router(state: AppState, auth: Arc<AuthPolicy>) -> Router {
    let middleware_state = state.clone();
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/api/overview", get(overview))
        .route("/api/jobs", get(list_jobs))
        .route("/api/analytics/jobs", get(analytics_jobs))
        .route("/api/jobs/:job_id", get(get_job))
        .route("/api/jobs/:job_id/cancel", post(cancel_job))
        .route("/api/workers", get(list_workers))
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            auth,
            crate::serve_auth::require_client_token,
        ))
        .layer(middleware::from_fn_with_state(
            middleware_state,
            cors_middleware,
        ))
}
async fn analytics_jobs(
    State(state): State<AppState>,
    Query(query): Query<AnalyticsQuery>,
) -> Result<Json<AnalyticsResponse>, ApiError> {
    let page = state
        .runtime
        .scan_job_metadata(JobScan {
            archived: matches!(query.source, AnalyticsSource::Archive),
            from_ms: query.from_ms,
            to_ms: query.to_ms,
            limit: query.limit.unwrap_or(250),
            cursor: query.cursor,
        })
        .await
        .map_err(api_error)?;
    Ok(Json(AnalyticsResponse {
        jobs: page
            .jobs
            .iter()
            .map(|job| AnalyticsJob {
                summary: job_summary(job),
                durability: match job.durability {
                    Durability::Durable => "durable",
                    Durability::Ephemeral => "ephemeral",
                },
            })
            .collect(),
        next_cursor: page.next_page_token,
    }))
}
async fn healthz(State(state): State<AppState>) -> StatusCode {
    if state.runtime.health().live {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn readyz(State(state): State<AppState>) -> StatusCode {
    if state.runtime.health().ready {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        super::metrics::render(&state.runtime),
    )
}
async fn overview(State(state): State<AppState>) -> Json<serde_json::Value> {
    let stats = state.runtime.stats();
    let workers: Vec<_> = state
        .runtime
        .list_workers()
        .iter()
        .map(worker_summary)
        .collect();
    let healthy: Vec<_> = workers.iter().filter(|worker| worker.healthy).collect();
    Json(json!({
        "runtime": state.mode,
        "queue": {"queued": stats.queued, "running": stats.running, "active": stats.queued + stats.running},
        "history": {
            "succeeded": stats.succeeded,
            "failed": stats.failed,
            "cancelled": stats.cancelled,
            "total": stats.succeeded + stats.failed + stats.cancelled
        },
        "workers": {
            "total": workers.len(), "healthy": healthy.len(), "unavailable": workers.len() - healthy.len(),
            "active_leases": workers.iter().map(|worker| u64::from(worker.active_leases)).sum::<u64>(),
            "total_capacity": aggregate_capacity(healthy.iter().map(|worker| &worker.total_capacity)),
            "last_seen_at_ms": workers.iter().map(|worker| worker.last_seen_at_ms).max()
        },
        "interfaces": stats.interfaces.iter().map(|(name, interface)| json!({
            "name": name,
            "queued": interface.queued,
            "running": interface.running,
            "oldest_queued_age_ms": interface.oldest_queued_age_ms,
            "sessions": interface.sessions,
            "free_slots": interface.free_slots
        })).collect::<Vec<_>>(),
        "refreshed_at_ms": now_ms()
    }))
}
async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<JobsQuery>,
) -> Result<Json<JobsResponse>, ApiError> {
    let page = state
        .runtime
        .list_jobs(JobFilter {
            states: parse_states(query.states.as_deref())?,
            limit: query.limit.unwrap_or(50).clamp(1, 200),
            page_token: query.offset.map(|offset| offset.to_string()),
            ..Default::default()
        })
        .await
        .map_err(api_error)?;
    Ok(Json(JobsResponse {
        jobs: page.jobs.iter().map(job_summary).collect(),
        next_page_token: page.next_page_token,
    }))
}
async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobDetailResponse>, ApiError> {
    let JobPreview {
        job,
        spec,
        events,
        outputs,
    } = state
        .runtime
        .inspect_job_preview(&job_id)
        .await
        .map_err(api_error)?;
    let attempts = attempts_from_events(&events);
    let incomplete = attempts.len() < job.attempt_count as usize
        || events.first().is_some_and(|event| {
            !matches!(event.kind, anyserve_core::runtime::api::EventKind::Accepted)
        });
    let latest_worker_id = job
        .current_worker_id
        .as_ref()
        .or_else(|| attempts.last().map(|attempt| &attempt.worker_id));
    let latest_worker = state
        .runtime
        .list_workers()
        .iter()
        .find(|worker| Some(&worker.worker_id) == latest_worker_id)
        .map(worker_summary);
    Ok(Json(JobDetailResponse {
        job: JobDetail {
            summary: job_summary(&job),
            demand: spec.as_ref().map(|spec| spec.demand.clone()),
            policy: spec.as_ref().map(|spec| spec.policy.clone()),
            params_size_bytes: spec.as_ref().map(|spec| spec.params.len()),
            params_preview: spec.as_ref().and_then(|spec| inline_preview(&spec.params)),
            params_truncated: spec
                .as_ref()
                .is_some_and(|spec| spec.params.len() > INLINE_PREVIEW_LIMIT_BYTES),
            inputs: spec
                .as_ref()
                .map(|spec| spec.inputs.iter().map(object_ref_summary).collect())
                .unwrap_or_default(),
            outputs: outputs.iter().map(object_preview_summary).collect(),
            output_preview: output_preview(&events, &outputs),
        },
        attempts,
        events: events.iter().map(event_summary).collect(),
        history_incomplete: incomplete,
        latest_worker,
    }))
}
async fn cancel_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<CancelJobResponse>, ApiError> {
    let job = state
        .runtime
        .cancel(&job_id, "cancelled from console".into())
        .await
        .map_err(api_error)?;
    Ok(Json(CancelJobResponse {
        job: job_summary(&job),
    }))
}
async fn list_workers(State(state): State<AppState>) -> Json<WorkersResponse> {
    let workers: Vec<_> = state
        .runtime
        .list_workers()
        .iter()
        .map(worker_summary)
        .collect();
    Json(WorkersResponse {
        healthy: workers.iter().filter(|worker| worker.healthy).count(),
        total: workers.len(),
        workers,
    })
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
        HeaderValue::from_static("Accept, Content-Type, Authorization"),
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

pub(super) fn parse_states(raw: Option<&str>) -> Result<Vec<JobState>, ApiError> {
    let mut states = Vec::new();
    for value in raw
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let state = match value {
            "queued" => JobState::Queued,
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
    Ok(states)
}
fn api_error(error: RuntimeError) -> ApiError {
    let status = match error.kind {
        ErrorKind::InvalidArgument => StatusCode::BAD_REQUEST,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Conflict | ErrorKind::StaleLease => StatusCode::CONFLICT,
        ErrorKind::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        ErrorKind::PersistenceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorKind::CursorExpired { .. } => StatusCode::GONE,
    };
    ApiError {
        status,
        message: error.to_string(),
    }
}
