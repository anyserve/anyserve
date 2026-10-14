use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use anyserve_core::model::{Attributes, Demand, ExecutionPolicy, JobRecord, JobSpec, ObjectRef};

use crate::edge_routing::apply_routing_headers;

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

    let job = submit_request(&state, &headers, &state.config.chat_interface, payload).await?;
    if stream {
        return Ok(streaming_response(state, job.job_id));
    }

    let output = wait_for_output_bytes(&state, job.job_id).await?;
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
    let job = submit_request(
        &state,
        &headers,
        &state.config.embeddings_interface,
        payload,
    )
    .await?;
    let output = wait_for_output_bytes(&state, job.job_id).await?;
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
) -> Result<JobRecord, OpenAIError> {
    let (demand, policy) = placement(state, headers)?;
    state
        .kernel
        .submit_job(
            None,
            JobSpec {
                interface_name: interface_name.to_string(),
                inputs: vec![ObjectRef::Inline {
                    content: payload,
                    metadata: Attributes::from([(
                        "content_type".to_string(),
                        "application/json".to_string(),
                    )]),
                }],
                params: Vec::new(),
                demand,
                policy,
                metadata: Attributes::from([("source".to_string(), "openai".to_string())]),
            },
        )
        .await
        .map_err(|error| OpenAIError::internal(format!("submit job: {error}")))
}
