use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use anyserve_core::kernel::OpenStreamCommand;
use anyserve_core::model::{
    Attributes, ExecutionPolicy, FrameKind, JobRecord, JobSpec, StreamDirection, StreamScope,
};

use super::state::{AppState, OpenAIError};
use super::streaming::{cancel_submitted_job, streaming_response, wait_for_output_bytes};

const INPUT_STREAM: &str = "input.default";

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

pub(super) async fn embeddings(
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

pub(super) async fn submit_request(
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
        return Err(cancel_submitted_job(state.kernel.clone(), job.job_id, error).await);
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
