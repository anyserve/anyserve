use std::collections::BTreeSet;
use std::sync::Arc;

use anyserve_core::auth::AuthPolicy;
use anyserve_core::error::{ErrorKind, RuntimeError};
use anyserve_core::now_ms;
use anyserve_core::runtime::api::{JobFilter, JobPreview, JobScan, JobState};
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

use super::api::{
    AnalyticsQuery, AnalyticsResponse, AnalyticsSource, ApiError, AppState, BudgetsSummary,
    CancelJobResponse, ControlPlaneSummary, HistorySummary, InterfaceSummary, JobDetail,
    JobDetailResponse, JobsQuery, JobsResponse, NamespaceQuery, OverviewResponse, PoolSummary,
    QueueSummary, WorkersResponse,
};
use super::snapshot::{
    INLINE_PREVIEW_LIMIT_BYTES, attempts_from_events, duration_ms, event_summary, health_summary,
    inline_preview, interface_policy_summary, job_summary, journal_stats_summary,
    object_preview_summary, object_ref_summary, output_preview, worker_rollup, worker_summary,
};

pub(crate) fn router(state: AppState, auth: Arc<AuthPolicy>) -> Router {
    let allowed_origins = Arc::clone(&state.allowed_origins);
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
            allowed_origins,
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
            cursor: normalize_optional_filter(query.cursor),
            namespace: normalize_optional_filter(query.namespace),
        })
        .await
        .map_err(api_error)?;
    Ok(Json(AnalyticsResponse {
        jobs: page.jobs.into_iter().map(job_summary).collect(),
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

async fn overview(State(state): State<AppState>) -> Result<Json<OverviewResponse>, ApiError> {
    state.runtime.ensure_active_leader().map_err(api_error)?;
    let health = state.runtime.health();
    let stats = state.runtime.stats();
    let config = state.runtime.config();
    let workers = state.runtime.list_workers();

    let interface_names: BTreeSet<&str> = stats
        .interfaces
        .keys()
        .chain(config.interfaces.keys())
        .map(String::as_str)
        .collect();
    let interfaces = interface_names
        .into_iter()
        .map(|name| {
            let current = stats.interfaces.get(name);
            InterfaceSummary {
                name: name.to_owned(),
                queued: current.map_or(0, |s| s.queued),
                running: current.map_or(0, |s| s.running),
                oldest_queued_age_ms: current.map_or(0, |s| s.oldest_queued_age_ms),
                sessions: current.map_or(0, |s| s.sessions),
                free_slots: current.map_or(0, |s| s.free_slots),
                policy: config.interfaces.get(name).map(interface_policy_summary),
            }
        })
        .collect();

    let pools = stats
        .pools
        .into_iter()
        .map(|((namespace, interface, pool), current)| PoolSummary {
            namespace,
            interface,
            pool,
            sessions: current.sessions,
            free_slots: current.free_slots,
            active_leases: current.active_leases,
            desired_additional_slots: current.desired_additional_slots,
        })
        .collect();

    Ok(Json(OverviewResponse {
        runtime: state.mode,
        health: health_summary(health),
        control_plane: ControlPlaneSummary {
            grpc_listen: state.control_plane.grpc_listen.clone(),
            grpc_tls_enabled: state.control_plane.grpc_tls_enabled,
            console_listen: state.control_plane.console_listen.clone(),
            auth_enabled: state.control_plane.auth_enabled,
            generation: stats.generation,
            retained_jobs: stats.retained_jobs,
            sessions: stats.sessions,
            max_sessions: config.max_sessions,
            priority_levels: config.priority_levels,
            default_max_attempts: config.default_max_attempts,
            heartbeat_interval_ms: duration_ms(config.heartbeat_interval),
            heartbeat_timeout_ms: duration_ms(config.heartbeat_timeout),
            reconnect_grace_ms: config.reconnect_grace.map(duration_ms),
        },
        budgets: BudgetsSummary {
            input_bytes: stats.input_bytes,
            max_pending_bytes: config.max_pending_bytes,
            result_bytes: stats.result_bytes,
            max_result_bytes: config.max_result_bytes,
            event_bytes: stats.event_bytes,
            max_event_bytes: config.max_event_bytes,
        },
        journal_stats: stats.journal.map(journal_stats_summary),
        queue: QueueSummary {
            queued: stats.queued,
            running: stats.running,
            active: stats.queued + stats.running,
        },
        history: HistorySummary {
            succeeded: stats.succeeded,
            failed: stats.failed,
            cancelled: stats.cancelled,
            total: stats.succeeded + stats.failed + stats.cancelled,
        },
        workers: worker_rollup(&workers),
        interfaces,
        pools,
        refreshed_at_ms: now_ms(),
    }))
}

async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<JobsQuery>,
) -> Result<Json<JobsResponse>, ApiError> {
    let page_token = normalize_optional_filter(query.page_token).or_else(|| {
        query
            .offset
            .filter(|o| *o > 0)
            .map(|offset| offset.to_string())
    });
    let page = state
        .runtime
        .list_jobs(JobFilter {
            states: parse_states(query.states.as_deref())?,
            interface: normalize_optional_filter(query.interface),
            namespace: normalize_optional_filter(query.namespace),
            limit: query.limit.unwrap_or(50).clamp(1, 200),
            page_token,
        })
        .await
        .map_err(api_error)?;
    Ok(Json(JobsResponse {
        jobs: page.jobs.into_iter().map(job_summary).collect(),
        next_page_token: page.next_page_token,
    }))
}

async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    Query(query): Query<NamespaceQuery>,
) -> Result<Json<JobDetailResponse>, ApiError> {
    let namespace = normalize_optional_filter(query.namespace);
    let JobPreview {
        job,
        spec,
        events,
        outputs,
    } = match namespace.as_deref() {
        Some(ns) => state.runtime.inspect_job_preview_in(ns, &job_id).await,
        None => state.runtime.inspect_job_preview(&job_id).await,
    }
    .map_err(api_error)?;

    let attempts = attempts_from_events(&events);
    let incomplete = attempts.len() < job.attempt_count as usize
        || events.first().is_some_and(|event| {
            !matches!(event.kind, anyserve_core::runtime::api::EventKind::Accepted)
        });
    let latest_worker_id = job
        .current_worker_id
        .as_deref()
        .or_else(|| attempts.last().map(|attempt| attempt.worker_id.as_str()));
    let latest_worker = latest_worker_id.and_then(|worker_id| {
        state
            .runtime
            .list_workers()
            .into_iter()
            .find(|worker| worker.namespace == job.namespace && worker.worker_id == worker_id)
            .map(worker_summary)
    });
    let output_preview = output_preview(&events, &outputs);
    let event_summaries = events.iter().map(event_summary).collect();
    let output_summaries = outputs.iter().map(object_preview_summary).collect();
    let (demand, policy, params_size_bytes, params_preview, params_truncated, inputs) =
        match spec.as_ref() {
            Some(spec) => (
                Some(spec.demand.clone()),
                Some(spec.policy.clone()),
                Some(spec.params.len()),
                inline_preview(&spec.params),
                spec.params.len() > INLINE_PREVIEW_LIMIT_BYTES,
                spec.inputs.iter().map(object_ref_summary).collect(),
            ),
            None => (None, None, None, None, false, Vec::new()),
        };
    Ok(Json(JobDetailResponse {
        job: JobDetail {
            summary: job_summary(job),
            demand,
            policy,
            params_size_bytes,
            params_preview,
            params_truncated,
            inputs,
            outputs: output_summaries,
            output_preview,
        },
        attempts,
        events: event_summaries,
        history_incomplete: incomplete,
        latest_worker,
    }))
}

async fn cancel_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    Query(query): Query<NamespaceQuery>,
) -> Result<Json<CancelJobResponse>, ApiError> {
    let namespace = normalize_optional_filter(query.namespace);
    let job = match namespace.as_deref() {
        Some(ns) => {
            state
                .runtime
                .cancel_in(ns, &job_id, "cancelled from console".into())
                .await
        }
        None => {
            state
                .runtime
                .cancel(&job_id, "cancelled from console".into())
                .await
        }
    }
    .map_err(api_error)?;
    Ok(Json(CancelJobResponse {
        job: job_summary(job),
    }))
}

async fn list_workers(State(state): State<AppState>) -> Result<Json<WorkersResponse>, ApiError> {
    state.runtime.ensure_active_leader().map_err(api_error)?;
    let workers_view = state.runtime.list_workers();
    let mut healthy = 0usize;
    let mut draining = 0usize;
    let mut disconnected = 0usize;
    let mut workers = Vec::with_capacity(workers_view.len());
    for worker in workers_view {
        let summary = worker_summary(worker);
        if summary.healthy {
            healthy += 1;
        }
        if summary.draining {
            draining += 1;
        }
        if !summary.connected {
            disconnected += 1;
        }
        workers.push(summary);
    }
    Ok(Json(WorkersResponse {
        total: workers.len(),
        healthy,
        draining,
        disconnected,
        workers,
    }))
}

async fn cors_middleware(
    State(allowed_origins): State<Arc<BTreeSet<String>>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let allowed_origin = (!allowed_origins.is_empty())
        .then(|| {
            request
                .headers()
                .get(ORIGIN)
                .filter(|value| {
                    value
                        .to_str()
                        .ok()
                        .is_some_and(|origin| allowed_origins.contains(origin))
                })
                .cloned()
        })
        .flatten();

    let mut response = if request.method() == Method::OPTIONS {
        StatusCode::NO_CONTENT.into_response()
    } else {
        next.run(request).await
    };

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

    if let Some(origin) = allowed_origin {
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }

    response
}

fn normalize_optional_filter(raw: Option<String>) -> Option<String> {
    raw.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
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
