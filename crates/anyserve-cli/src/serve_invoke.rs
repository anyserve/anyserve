//! Generic HTTP request/reply adapter over the execution runtime.
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
use anyserve_core::runtime::{Runtime, api};

use crate::edge_routing::{
    TIMEOUT_HEADER, apply_routing_headers, header_number, require_client_token,
};
use crate::serve_config::ServeInvokeConfig;
use crate::serve_execution::{ExecutionResult, collect, execution_status, runtime_status};

mod metrics;
use metrics::InvokeMetrics;
pub(crate) const FORWARD_HEADER_PREFIX: &str = "http.header.";

#[derive(Clone)]
struct InvokeState {
    runtime: Runtime,
    config: Arc<ServeInvokeConfig>,
    metrics: Arc<InvokeMetrics>,
}

pub async fn run_server(
    listen: std::net::SocketAddr,
    runtime: Runtime,
    config: ServeInvokeConfig,
    auth: Arc<AuthPolicy>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind invoke listener {listen}"))?;
    axum::serve(listener, router(runtime, config, auth))
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run invoke HTTP server")
}

fn router(runtime: Runtime, config: ServeInvokeConfig, auth: Arc<AuthPolicy>) -> Router {
    let max_body_bytes = config.max_body_bytes;
    let state = InvokeState {
        runtime,
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
    let live = state
        .runtime
        .list_workers()
        .iter()
        .filter(|worker| {
            crate::serve_execution::worker_can_serve(worker, None, &state.config.demand)
        })
        .count();
    (
        if state.runtime.health().ready && live > 0 {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        axum::Json(serde_json::json!({ "live_workers": live })),
    )
        .into_response()
}
async fn metrics_handler(State(state): State<InvokeState>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(&state.runtime),
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
    let mut demand = state.config.demand.clone();
    demand.interface = interface;
    let mut policy = state.config.defaults.policy();
    if let Err(message) = apply_routing_headers(&headers, &mut demand, &mut policy) {
        return error_response(StatusCode::BAD_REQUEST, "bad_request", &message);
    }
    let timeout_ms = match header_number::<u64>(&headers, TIMEOUT_HEADER) {
        Ok(value) => value.unwrap_or(state.config.default_timeout_ms).max(1),
        Err(message) => return error_response(StatusCode::BAD_REQUEST, "bad_request", &message),
    };
    let timeout = Duration::from_millis(timeout_ms);
    policy.total_timeout = Some(timeout);
    if policy.queue_timeout.is_none_or(|queue| queue > timeout) {
        policy.queue_timeout = Some(timeout);
    }
    let mut metadata = api::Metadata::new();
    if let Some(value) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        metadata.insert("content_type".to_owned(), value.to_owned());
    }
    for name in &state.config.forward_headers {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) {
            metadata.insert(format!("{FORWARD_HEADER_PREFIX}{name}"), value.to_owned());
        }
    }
    let execution = match state
        .runtime
        .invoke(api::InvokeRequest {
            spec: api::JobSpec {
                demand,
                inputs: vec![api::Object::Inline {
                    data: body,
                    metadata,
                }],
                policy,
                metadata: api::Metadata::from([("source".to_owned(), "invoke".to_owned())]),
                ..Default::default()
            },
            delivery: api::Delivery::Buffered,
            input: api::InputMode::Complete,
        })
        .await
    {
        Ok(execution) => execution,
        Err(error) => {
            return error_response(runtime_status(&error), "submit_failed", &error.to_string());
        }
    };
    let job_id = execution.job_id().to_owned();
    let response = match tokio::time::timeout(timeout, collect(execution)).await {
        Ok(Ok(result)) => job_response(result),
        Ok(Err(error)) => {
            error_response(runtime_status(&error), "runtime_error", &error.to_string())
        }
        Err(_) => error_response(
            StatusCode::GATEWAY_TIMEOUT,
            "timeout",
            "request execution deadline exceeded",
        ),
    };
    with_header(response, "x-anyserve-job-id", &job_id)
}

fn job_response(result: ExecutionResult) -> Response {
    let response = if result.state == api::JobState::Succeeded {
        success_response(&result.outputs)
    } else {
        let error = result.error.as_ref();
        error_response(
            execution_status(error),
            error
                .map(|error| error.code.as_str())
                .unwrap_or("cancelled"),
            error
                .map(|error| error.message.as_str())
                .unwrap_or("job was cancelled"),
        )
    };
    let response = with_header(
        response,
        "x-anyserve-attempts",
        &result.attempts.to_string(),
    );
    with_header(response, "x-anyserve-worker-id", &result.worker_id)
}

fn success_response(outputs: &[api::Object]) -> Response {
    let Some(output) = outputs.first() else {
        return StatusCode::NO_CONTENT.into_response();
    };
    let api::Object::Inline { data, metadata } = output else {
        return error_response(
            StatusCode::BAD_GATEWAY,
            "unsupported_output",
            "HTTP invoke requires inline output",
        );
    };
    let status = metadata
        .get("http_status")
        .and_then(|value| value.parse::<u16>().ok())
        .and_then(|code| StatusCode::from_u16(code).ok())
        .unwrap_or(StatusCode::OK);
    let mut response = Response::new(Body::from(data.clone()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        metadata
            .get("content_type")
            .and_then(|value| HeaderValue::from_str(value).ok())
            .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream")),
    );
    response
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response {
    let response = (
        status,
        axum::Json(serde_json::json!({ "error": { "code": code, "message": message } })),
    )
        .into_response();
    with_header(response, "x-anyserve-error", code)
}
fn with_header(mut response: Response, name: &'static str, value: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(value) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(name), value);
    }
    response
}
#[cfg(test)]
mod tests;
