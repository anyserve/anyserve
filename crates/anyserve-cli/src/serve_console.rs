mod api;
#[cfg(any(feature = "embed-console", test))]
mod embedded_ui;
mod handlers;
mod metrics;
pub(crate) mod settings;
mod snapshot;
#[cfg(test)]
mod tests;

#[cfg(test)]
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

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
    pub grpc_enabled: bool,
    pub grpc_listen: String,
    pub grpc_tls_enabled: bool,
    pub public_endpoint: Option<String>,
    pub auth_portal_url: Option<String>,
    pub revocation_sync_ok: Option<Arc<AtomicBool>>,
    pub settings_path: Option<PathBuf>,
}

pub(crate) fn format_host_port(host: &str, port: u16) -> String {
    let host = host.trim();
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
pub async fn run_server(
    addr: SocketAddr,
    runtime: Runtime,
    config: ServeConsoleConfig,
    control_plane: ControlPlaneMetadata,
    runtime_mode: RuntimeMode,
    auth: Arc<AuthPolicy>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind console listener {addr}"))?;
    run_server_with_listener(
        listener,
        runtime,
        config,
        control_plane,
        runtime_mode,
        auth,
        shutdown,
    )
    .await
}

pub async fn run_server_with_listener(
    listener: tokio::net::TcpListener,
    runtime: Runtime,
    config: ServeConsoleConfig,
    control_plane: ControlPlaneMetadata,
    runtime_mode: RuntimeMode,
    auth: Arc<AuthPolicy>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let console_listen = listener
        .local_addr()
        .map(|local| local.to_string())
        .unwrap_or(config.listen);

    let state = AppState {
        runtime,
        auth,
        revocation_sync_ok: control_plane.revocation_sync_ok,
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
            grpc_enabled: control_plane.grpc_enabled,
            grpc_listen: control_plane.grpc_listen,
            grpc_tls_enabled: control_plane.grpc_tls_enabled,
            console_listen,
            public_endpoint: control_plane.public_endpoint,
            auth_portal_url: control_plane.auth_portal_url,
        }),
        settings: settings::SettingsStore::new(control_plane.settings_path),
    };

    serve(listener, router(state))
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run console http server")
}
