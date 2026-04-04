use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use anyhow::{Context, Result};
use anyserve_core::config::GrpcConfig;
use anyserve_core::kernel::Kernel;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::service::ControlPlaneGrpcService;
use anyserve_core::store::{MemoryStateStore, MemoryStreamStore};
use anyserve_proto::controlplane::control_plane_service_server::ControlPlaneServiceServer;
use clap::{Args, Parser, Subcommand};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "anyserve",
    version,
    about = "A zero-dependency control plane for generic distributed execution."
)]
struct Cli {
    #[arg(short, long, global = true)]
    verbose: bool,
    #[arg(long, default_value = "info", global = true)]
    log_level: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Serve(ServeArgs),
}

#[derive(Args, Debug)]
struct ServeArgs {
    #[arg(long = "grpc-host", default_value = "0.0.0.0")]
    grpc_host: String,
    #[arg(long = "grpc-port", default_value_t = 50_052)]
    grpc_port: u16,
    #[arg(long = "grpc-tls-enabled")]
    grpc_tls_enabled: bool,
    #[arg(long = "grpc-cert-file")]
    grpc_cert_file: Option<String>,
    #[arg(long = "grpc-key-file")]
    grpc_key_file: Option<String>,
    #[arg(long = "heartbeat-ttl-secs", default_value_t = 30)]
    heartbeat_ttl_secs: u64,
    #[arg(long = "default-lease-ttl-secs", default_value_t = 30)]
    default_lease_ttl_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose, &cli.log_level)?;

    match cli.command {
        Commands::Serve(args) => serve(args).await,
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    let grpc_config = GrpcConfig {
        host: args.grpc_host,
        port: args.grpc_port,
        tls_enabled: args.grpc_tls_enabled,
        cert_file: args.grpc_cert_file,
        key_file: args.grpc_key_file,
    };

    let kernel = Arc::new(Kernel::new(
        Arc::new(MemoryStateStore::new()),
        Arc::new(MemoryStreamStore::new()),
        Arc::new(BasicScheduler),
        args.heartbeat_ttl_secs,
        args.default_lease_ttl_secs,
    ));

    let grpc_addr = socket_addr(&grpc_config.host, grpc_config.port)?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let grpc_task = tokio::spawn(run_grpc_server(grpc_addr, kernel, grpc_config, shutdown_rx));

    info!(grpc = %grpc_addr, "control plane started");

    tokio::signal::ctrl_c()
        .await
        .context("listen for shutdown signal")?;
    let _ = shutdown_tx.send(true);

    grpc_task.await.context("join grpc task")??;
    Ok(())
}

async fn run_grpc_server(
    addr: SocketAddr,
    kernel: Arc<Kernel>,
    config: GrpcConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let service = ControlPlaneGrpcService::new(kernel);
    let grpc_service = ControlPlaneServiceServer::new(service);
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<ControlPlaneServiceServer<ControlPlaneGrpcService>>()
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
        .add_service(health_service)
        .add_service(grpc_service)
        .serve_with_shutdown(addr, async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run grpc server")
}

fn init_tracing(verbose: bool, log_level: &str) -> Result<()> {
    let level = if verbose { "debug" } else { log_level };
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(level))
        .context("build log filter")?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|error| anyhow::anyhow!("initialize tracing: {error}"))
}

fn socket_addr(host: &str, port: u16) -> Result<SocketAddr> {
    let mut addrs = (host, port)
        .to_socket_addrs()
        .with_context(|| format!("resolve socket address {host}:{port}"))?;
    addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("no socket addresses resolved for {host}:{port}"))
}
