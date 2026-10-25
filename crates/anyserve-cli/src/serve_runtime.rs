use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use anyserve_core::auth::{AuthLayer, AuthPolicy};
use anyserve_core::client_service::ClientGrpcService;
use anyserve_core::runtime::Runtime;
use anyserve_core::worker_service::WorkerGrpcService;
use anyserve_proto::v2::{client_server::ClientServer, worker_server::WorkerServer};
use clap::Args;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{info, warn};

use crate::serve_config::{GrpcConfig, ResolvedServeConfig, ServeOverrides};
use crate::serve_console::run_server as run_console_server;

#[cfg(test)]
mod tests;

const MAX_PENDING_RESET_STREAMS: usize = 1024;
#[derive(Args, Debug)]
pub(crate) struct ServeArgs {
    #[arg(long = "config", value_name = "PATH")]
    config: Option<PathBuf>,
    #[arg(long = "grpc-host")]
    grpc_host: Option<String>,
    #[arg(long = "grpc-port")]
    grpc_port: Option<u16>,
    #[arg(
        long = "grpc-tls-enabled",
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    grpc_tls_enabled: Option<bool>,
    #[arg(long = "grpc-cert-file")]
    grpc_cert_file: Option<String>,
    #[arg(long = "grpc-key-file")]
    grpc_key_file: Option<String>,
    /// Disable durable jobs and keep the runtime entirely in memory.
    #[arg(long, conflicts_with = "journal_path")]
    ephemeral: bool,
    /// SQLite recovery journal (default: .anyserve/journal.sqlite).
    #[arg(long = "journal", value_name = "PATH")]
    journal_path: Option<PathBuf>,
}

pub(crate) async fn run(args: ServeArgs) -> Result<()> {
    let config = ResolvedServeConfig::load(ServeOverrides {
        config: args.config,
        grpc_host: args.grpc_host,
        grpc_port: args.grpc_port,
        grpc_tls_enabled: args.grpc_tls_enabled,
        grpc_cert_file: args.grpc_cert_file,
        grpc_key_file: args.grpc_key_file,
        ephemeral: args.ephemeral,
        journal_path: args.journal_path,
    })?;
    let grpc_config = config.grpc.clone();
    let runtime = Runtime::start(config.runtime.clone())
        .await
        .context("start execution runtime")?;
    let auth = Arc::new(config.auth.clone());
    if !auth.is_enabled() {
        warn!(
            "auth is disabled; any client can reach the control plane (set [auth] tokens before exposing it beyond a trusted network)"
        );
    }

    let grpc_addr = socket_addr(&grpc_config.host, grpc_config.port)?;
    let console_addr = config
        .console
        .as_ref()
        .map(|console| socket_addr_from_bind(&console.listen))
        .transpose()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn(run_grpc_server(
        grpc_addr,
        runtime.clone(),
        grpc_config,
        AuthPolicy::clone(&auth),
        shutdown_rx,
    ));
    if let (Some(adapter), Some(addr)) = (config.console.clone(), console_addr) {
        servers.spawn(run_console_server(
            addr,
            runtime.clone(),
            adapter,
            config.runtime_mode(),
            Arc::clone(&auth),
            shutdown_tx.subscribe(),
        ));
    }

    let tls = config.grpc.tls_enabled;
    if let Some(config_path) = config.config_path.as_ref() {
        let config_path = config_path.display().to_string();
        info!(grpc = %grpc_addr, tls, config = %config_path, "control plane started");
    } else {
        info!(grpc = %grpc_addr, tls, "control plane started");
    }
    if let Some(console_addr) = console_addr {
        info!(console = %console_addr, "console api started");
    }

    let failure = tokio::select! {
        signal = shutdown_signal() => { signal?; None }
        ended = servers.join_next() => Some(match ended {
            Some(Ok(Err(error))) => error,
            Some(Err(error)) => error.into(),
            _ => anyhow::anyhow!("a configured server stopped unexpectedly"),
        }),
    };
    let _ = shutdown_tx.send(true);
    runtime.shutdown().await;
    let drained = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(joined) = servers.join_next().await {
            if let Ok(Err(error)) = joined {
                warn!(%error, "server stopped during shutdown");
            }
        }
    })
    .await;
    if drained.is_err() {
        servers.abort_all();
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("install SIGTERM handler")?;
        tokio::select! { signal = tokio::signal::ctrl_c() => { signal.context("listen for SIGINT")?; }, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .context("listen for shutdown")?;
    Ok(())
}

async fn run_grpc_server(
    addr: SocketAddr,
    runtime: Runtime,
    config: GrpcConfig,
    auth: AuthPolicy,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let client_service = ClientServer::new(ClientGrpcService::new(runtime.clone()))
        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE);
    let worker_service = WorkerServer::new(WorkerGrpcService::new(runtime))
        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE);
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<ClientServer<ClientGrpcService>>()
        .await;
    health_reporter
        .set_serving::<WorkerServer<WorkerGrpcService>>()
        .await;

    let mut builder = Server::builder();
    if config.tls_enabled {
        let cert_file = config
            .cert_file
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--grpc-cert-file is required when TLS is enabled"))?;
        let key_file = config
            .key_file
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--grpc-key-file is required when TLS is enabled"))?;
        let identity = Identity::from_pem(
            tokio::fs::read(cert_file)
                .await
                .context("read tls certificate")?,
            tokio::fs::read(key_file)
                .await
                .context("read tls private key")?,
        );
        builder = builder
            .tls_config(ServerTlsConfig::new().identity(identity))
            .context("configure grpc tls")?;
    }

    builder
        .tcp_nodelay(true)
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .initial_connection_window_size(Some(32 * 1024 * 1024))
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(20)))
        // A draining worker cancels its attempt streams at once; the
        // default rapid-reset guard (20) would drop the whole connection.
        .http2_max_pending_accept_reset_streams(Some(MAX_PENDING_RESET_STREAMS))
        .layer(AuthLayer::new(auth))
        .add_service(health_service)
        .add_service(client_service)
        .add_service(worker_service)
        .serve_with_shutdown(addr, async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run grpc server")
}

fn socket_addr(host: &str, port: u16) -> Result<SocketAddr> {
    let mut addrs = (host, port)
        .to_socket_addrs()
        .with_context(|| format!("resolve socket address {host}:{port}"))?;
    addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("no socket addresses resolved for {host}:{port}"))
}

fn socket_addr_from_bind(value: &str) -> Result<SocketAddr> {
    let mut addrs = value
        .to_socket_addrs()
        .with_context(|| format!("resolve socket address {value}"))?;
    addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("no socket addresses resolved for {value}"))
}
