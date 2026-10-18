//! Synchronous HTTP invoke gateway.
//!
//! `POST /v1/invoke/{interface}` turns one HTTP request into one job, waits for
//! a worker (in whichever cluster wins placement) to finish it, and relays the
//! upstream status, content type, and body. Central services integrate with a
//! single HTTP call instead of driving the job/stream RPCs themselves.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tokio::sync::watch;

use anyserve_core::auth::AuthPolicy;
use anyserve_core::kernel::{Kernel, QUEUE_TIMEOUT_REASON};
use anyserve_core::model::{Attributes, JobRecord, JobSpec, JobState, ObjectRef};
use anyserve_core::now_ms;

use crate::edge_routing::{
    TIMEOUT_HEADER, apply_routing_headers, header_number, require_client_token,
};
use crate::serve_config::ServeInvokeConfig;

mod metrics;

use metrics::InvokeMetrics;

pub(crate) const FORWARD_HEADER_PREFIX: &str = "http.header.";

#[derive(Clone)]
struct InvokeState {
    kernel: Arc<Kernel>,
    config: Arc<ServeInvokeConfig>,
    metrics: Arc<InvokeMetrics>,
}

pub async fn run_server(
    listen: std::net::SocketAddr,
    kernel: Arc<Kernel>,
    config: ServeInvokeConfig,
    auth: Arc<AuthPolicy>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind invoke listener {listen}"))?;
    axum::serve(listener, router(kernel, config, auth))
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run invoke http server")
}

fn router(kernel: Arc<Kernel>, config: ServeInvokeConfig, auth: Arc<AuthPolicy>) -> Router {
    let max_body_bytes = config.max_body_bytes;
    let state = InvokeState {
        kernel,
        config: Arc::new(config),
        metrics: Arc::new(InvokeMetrics::default()),
    };
    Router::new()
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .route("/readyz", axum::routing::get(readyz))
        .route("/metrics", axum::routing::get(metrics_handler))
        .route("/v1/invoke/*interface", axum::routing::post(invoke))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(state)
        .layer(axum::middleware::from_fn_with_state(
            auth,
            require_client_token,
        ))
}

async fn readyz(State(state): State<InvokeState>) -> Response {
    let now = now_ms();
    match state.kernel.list_workers().await {
        Ok(workers) => {
            let live = workers
                .iter()
                .filter(|worker| worker.expires_at_ms > now)
                .count();
            let status = if live > 0 {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (
                status,
                axum::Json(serde_json::json!({ "live_workers": live })),
            )
                .into_response()
        }
        Err(error) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &error.to_string(),
        ),
    }
}

async fn metrics_handler(State(state): State<InvokeState>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(&state.kernel).await,
    )
        .into_response()
}

async fn invoke(
    State(state): State<InvokeState>,
    Path(interface): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = std::time::Instant::now();
    let response = invoke_job(&state, interface.clone(), headers, body).await;
    state
        .metrics
        .observe(&interface, response.status().as_u16(), started.elapsed());
    response
}

async fn invoke_job(
    state: &InvokeState,
    interface: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let config = &state.config;
    let mut demand = config.demand.clone();
    let mut policy = config.defaults.policy();
    if let Err(message) = apply_routing_headers(&headers, &mut demand, &mut policy) {
        return error_response(StatusCode::BAD_REQUEST, "bad_request", &message);
    }
    let timeout_ms = match header_number::<u64>(&headers, TIMEOUT_HEADER) {
        Ok(value) => value.unwrap_or(config.default_timeout_ms).max(1),
        Err(message) => return error_response(StatusCode::BAD_REQUEST, "bad_request", &message),
    };
    // A job nobody picks up should not outlive the caller's patience.
    if policy.queue_timeout_ms == 0 || policy.queue_timeout_ms > timeout_ms {
        policy.queue_timeout_ms = timeout_ms;
    }

    let mut input_metadata = Attributes::new();
    if let Some(content_type) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        input_metadata.insert("content_type".to_string(), content_type.to_string());
    }
    for name in &config.forward_headers {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) {
            input_metadata.insert(format!("{FORWARD_HEADER_PREFIX}{name}"), value.to_string());
        }
    }

    let job = match state
        .kernel
        .submit_job(
            None,
            JobSpec {
                interface_name: interface,
                inputs: vec![ObjectRef::Inline {
                    content: body.to_vec(),
                    metadata: input_metadata,
                }],
                params: Vec::new(),
                demand,
                policy,
                metadata: Attributes::from([("source".to_string(), "invoke".to_string())]),
            },
        )
        .await
    {
        Ok(job) => job,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "submit_failed",
                &format!("{error:#}"),
            );
        }
    };

    match tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        state.kernel.wait_for_terminal(&job.job_id),
    )
    .await
    {
        Ok(Ok(job)) => job_response(&state.kernel, job).await,
        Ok(Err(error)) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &format!("{error:#}"),
        ),
        Err(_) => {
            let _ = state.kernel.cancel_job(&job.job_id).await;
            with_job_id(
                error_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    "timeout",
                    &format!("job did not finish within {timeout_ms} ms"),
                ),
                &job.job_id,
            )
        }
    }
}

async fn job_response(kernel: &Kernel, job: JobRecord) -> Response {
    let job_id = job.job_id.clone();
    let worker_id = match job.current_attempt_id.as_deref() {
        Some(attempt_id) => kernel
            .get_attempt(attempt_id)
            .await
            .ok()
            .map(|attempt| attempt.worker_id),
        None => None,
    };
    let attempts = kernel
        .list_attempts(&job_id)
        .await
        .map(|attempts| attempts.len())
        .unwrap_or_default();

    let mut response = match job.state {
        JobState::Succeeded => success_response(&job),
        JobState::Failed => {
            let message = job
                .last_error
                .clone()
                .unwrap_or_else(|| "worker reported failure".to_string());
            let queue_timeout = kernel
                .watch_job(&job_id, 0)
                .await
                .ok()
                .and_then(|events| events.last().cloned())
                .is_some_and(|event| {
                    event.metadata.get("reason").map(String::as_str) == Some(QUEUE_TIMEOUT_REASON)
                });
            if queue_timeout {
                error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    QUEUE_TIMEOUT_REASON,
                    &message,
                )
            } else {
                error_response(StatusCode::BAD_GATEWAY, "worker_failed", &message)
            }
        }
        _ => error_response(StatusCode::BAD_GATEWAY, "cancelled", "job was cancelled"),
    };

    let headers = response.headers_mut();
    if let Some(worker_id) = worker_id
        && let Ok(value) = HeaderValue::from_str(&worker_id)
    {
        headers.insert(HeaderName::from_static("x-anyserve-worker-id"), value);
    }
    if let Ok(value) = HeaderValue::from_str(&attempts.to_string()) {
        headers.insert(HeaderName::from_static("x-anyserve-attempts"), value);
    }
    with_job_id(response, &job_id)
}

fn success_response(job: &JobRecord) -> Response {
    let Some(ObjectRef::Inline { content, metadata }) = job.outputs.first() else {
        return StatusCode::NO_CONTENT.into_response();
    };
    let status = metadata
        .get("http_status")
        .and_then(|value| value.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
        .unwrap_or(StatusCode::OK);
    let content_type = metadata
        .get("content_type")
        .cloned()
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let mut response = Response::new(Body::from(content.clone()));
    *response.status_mut() = status;
    if let Ok(value) = HeaderValue::from_str(&content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response {
    let mut response = (
        status,
        axum::Json(serde_json::json!({ "error": { "code": code, "message": message } })),
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(code) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-anyserve-error"), value);
    }
    response
}

fn with_job_id(mut response: Response, job_id: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(job_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-anyserve-job-id"), value);
    }
    response
}

#[cfg(test)]
mod tests;
