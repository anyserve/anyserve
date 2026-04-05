mod api;
mod handlers;
mod snapshot;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::serve;
use tokio::sync::watch;

use anyserve_core::kernel::Kernel;

use crate::serve_config::{RuntimeMode, ServeConsoleConfig};

use api::{AppState, runtime_mode_summary};
use handlers::router;

pub async fn run_server(
    addr: SocketAddr,
    kernel: Arc<Kernel>,
    config: ServeConsoleConfig,
    runtime_mode: RuntimeMode,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let state = AppState {
        kernel,
        allowed_origins: Arc::new(config.allowed_origins.into_iter().collect()),
        runtime: runtime_mode_summary(runtime_mode),
    };
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind console listener {addr}"))?;

    serve(listener, router(state))
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run console http server")
}
