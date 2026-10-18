use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use anyserve_core::runtime::api::{self, Demand, ExecutionPolicy};
use std::time::Duration;

use crate::edge_routing::{TIMEOUT_HEADER, apply_routing_headers, header_number};

use super::state::{AppState, OpenAIError};
use super::streaming::{streaming_response, wait_for_output_bytes};

pub(super) async fn healthz() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

pub(super) async fn list_models(State(state): State<AppState>) -> impl IntoResponse {
    let data = state
        .config
        .models
        .iter()
        .map(|id| {
            serde_json::json!({
                "id": id,
                "object": "model",
                "created": 0,
                "owned_by": "anyserve",
            })
        })
        .collect::<Vec<_>>();
    Json(serde_json::json!({
        "object": "list",
        "data": data,
    }))
}

pub(super) async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Result<Response, OpenAIError> {
    let stream = request
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let payload = serde_json::to_vec(&request)
        .map_err(|error| OpenAIError::bad_request(format!("serialize request: {error}")))?;

    let delivery = if stream {
        api::Delivery::Streamed
    } else {
        api::Delivery::Buffered
    };
    let (execution, timeout) = submit_request(
        &state,
        &headers,
        &state.config.chat_interface,
        payload,
        delivery,
    )
    .await?;
    if stream {
        return streaming_response(execution, timeout).await;
    }
    let output = wait_for_output_bytes(timeout, execution).await?;
    let response_json: Value = serde_json::from_slice(&output).map_err(|error| {
        OpenAIError::bad_gateway(format!("worker returned invalid json: {error}"))
    })?;

    Ok((StatusCode::OK, Json(response_json)).into_response())
}

pub(super) async fn embeddings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Result<Response, OpenAIError> {
    let payload = serde_json::to_vec(&request)
        .map_err(|error| OpenAIError::bad_request(format!("serialize request: {error}")))?;
    let (execution, timeout) = submit_request(
        &state,
        &headers,
        &state.config.embeddings_interface,
        payload,
        api::Delivery::Buffered,
    )
    .await?;
    let output = wait_for_output_bytes(timeout, execution).await?;
    let response_json: Value = serde_json::from_slice(&output).map_err(|error| {
        OpenAIError::bad_gateway(format!("worker returned invalid json: {error}"))
    })?;

    Ok((StatusCode::OK, Json(response_json)).into_response())
}

fn placement(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(Demand, ExecutionPolicy), OpenAIError> {
    let mut demand = state.config.demand.clone();
    let mut policy = state.config.policy.policy();
    apply_routing_headers(headers, &mut demand, &mut policy).map_err(OpenAIError::bad_request)?;
    Ok((demand, policy))
}

pub(super) async fn submit_request(
    state: &AppState,
    headers: &HeaderMap,
    interface_name: &str,
    payload: Vec<u8>,
    delivery: api::Delivery,
) -> Result<(api::Execution, Duration), OpenAIError> {
    let (mut demand, mut policy) = placement(state, headers)?;
    demand.interface = interface_name.to_owned();
    let timeout_ms = header_number::<u64>(headers, TIMEOUT_HEADER)
        .map_err(OpenAIError::bad_request)?
        .unwrap_or(state.config.request_timeout_secs.saturating_mul(1000))
        .max(1);
    policy.total_timeout = Some(Duration::from_millis(timeout_ms));
    if policy
        .queue_timeout
        .is_none_or(|queue| queue > Duration::from_millis(timeout_ms))
    {
        policy.queue_timeout = Some(Duration::from_millis(timeout_ms));
    }
    let timeout = Duration::from_millis(timeout_ms);
    let execution = state
        .runtime
        .invoke(api::InvokeRequest {
            spec: api::JobSpec {
                demand,
                inputs: vec![api::Object::Inline {
                    data: payload.into(),
                    metadata: api::Metadata::from([(
                        "content_type".to_owned(),
                        "application/json".to_owned(),
                    )]),
                }],
                policy,
                metadata: api::Metadata::from([("source".to_owned(), "openai".to_owned())]),
                ..Default::default()
            },
            input: api::InputMode::Complete,
            delivery,
        })
        .await
        .map_err(OpenAIError::runtime)?;
    Ok((execution, timeout))
}
