use super::state::AppState;
use crate::serve_execution::worker_can_serve;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

pub(super) async fn readyz(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Response {
    let workers = state.runtime.list_workers();
    let matching = |interface: &str| {
        workers
            .iter()
            .filter(|worker| worker_can_serve(worker, Some(interface), &state.config.demand))
            .count()
    };
    let chat = matching(&state.config.chat_interface);
    let embeddings = matching(&state.config.embeddings_interface);
    let ready = state.runtime.health().ready;
    let status = if ready && chat > 0 {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "status": if status == StatusCode::OK { "ready" } else { "not_ready" },
            "chat_ready": ready && chat > 0, "embeddings_ready": ready && embeddings > 0,
            "matching_chat_workers": chat, "matching_embeddings_workers": embeddings,
        })),
    )
        .into_response()
}
