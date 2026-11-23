mod api;
#[cfg(any(feature = "embed-console", test))]
mod embedded_ui;
mod handlers;
mod metrics;
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

use api::{AppState, ControlPlaneStatic, runtime_mode_summary};
use handlers::router;

#[derive(Clone, Debug)]
pub struct ControlPlaneMetadata {
    pub grpc_listen: String,
    pub grpc_tls_enabled: bool,
    pub public_endpoint: Option<String>,
}

pub(crate) fn format_host_port(host: &str, port: u16) -> String {
    let host = host.trim();
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

pub async fn run_server(
    addr: SocketAddr,
    runtime: Runtime,
    config: ServeConsoleConfig,
    control_plane: ControlPlaneMetadata,
    runtime_mode: RuntimeMode,
    auth: Arc<AuthPolicy>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind console listener {addr}"))?;
    let console_listen = if addr.port() == 0 {
        listener
            .local_addr()
            .map(|local| local.to_string())
            .unwrap_or(config.listen)
    } else {
        config.listen
    };

    let state = AppState {
        runtime,
        allowed_origins: Arc::new(
            config
                .allowed_origins
                .into_iter()
                .map(|origin| origin.trim().to_string())
                .filter(|origin| !origin.is_empty())
                .collect(),
        ),
        mode: runtime_mode_summary(runtime_mode),
        control_plane: Arc::new(ControlPlaneStatic {
            grpc_listen: control_plane.grpc_listen,
            grpc_tls_enabled: control_plane.grpc_tls_enabled,
            console_listen,
            public_endpoint: control_plane.public_endpoint,
            auth_enabled: auth.is_enabled(),
        }),
    };

    serve(listener, router(state, auth))
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run console http server")
}
