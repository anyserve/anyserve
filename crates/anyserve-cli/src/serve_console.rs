mod api;
mod handlers;
mod snapshot;
#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::serve;
use tokio::sync::watch;

use anyserve_core::auth::AuthPolicy;
use anyserve_core::runtime::Runtime;

use crate::serve_config::{RuntimeMode, ServeConsoleConfig};

use api::{AppState, runtime_mode_summary};
use handlers::router;

pub async fn run_server(
    addr: SocketAddr,
    runtime: Runtime,
    config: ServeConsoleConfig,
    runtime_mode: RuntimeMode,
    auth: Arc<AuthPolicy>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let state = AppState {
        runtime,
        allowed_origins: Arc::new(config.allowed_origins.into_iter().collect()),
        mode: runtime_mode_summary(runtime_mode),
    };
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind console listener {addr}"))?;

    serve(listener, router(state, auth))
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run console http server")
}
