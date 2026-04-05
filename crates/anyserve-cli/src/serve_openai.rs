mod errors;
mod handlers;
mod readiness;
mod state;
mod streaming;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use tokio::sync::watch;

use anyserve_core::kernel::Kernel;

use crate::serve_config::ServeOpenAIConfig;

use handlers::{chat_completions, embeddings, healthz, list_models};
use readiness::readyz;
use state::AppState;

pub async fn run_server(
    listen: std::net::SocketAddr,
    kernel: Arc<Kernel>,
    config: ServeOpenAIConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let state = AppState {
        kernel,
        config: Arc::new(config),
    };
    let app = Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .route("/readyz", axum::routing::get(readyz))
        .route("/v1/models", axum::routing::get(list_models))
        .route(
            "/v1/chat/completions",
            axum::routing::post(chat_completions),
        )
        .route("/v1/embeddings", axum::routing::post(embeddings))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind openai listener {listen}"))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run openai http server")
}
