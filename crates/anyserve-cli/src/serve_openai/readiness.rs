use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use anyserve_core::model::{Capacity, WorkerRecord};

use super::state::{AppState, OpenAIError};

pub(super) struct ReadinessSnapshot {
    pub(super) chat_ready: bool,
    pub(super) embeddings_ready: bool,
    pub(super) matching_chat_workers: usize,
    pub(super) matching_embeddings_workers: usize,
}

pub(super) async fn readyz(state: axum::extract::State<AppState>) -> Response {
    let axum::extract::State(state) = state;
    match readiness_snapshot(&state).await {
        Ok(snapshot) => {
            let status = if snapshot.chat_ready && snapshot.embeddings_ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (
                status,
                Json(serde_json::json!({
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

pub(super) async fn readiness_snapshot(state: &AppState) -> Result<ReadinessSnapshot, OpenAIError> {
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

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be valid")
        .as_millis() as u64
}
