use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use anyserve_core::config::{GrpcConfig, HttpConfig};
use anyserve_core::consts::DEFAULT_QUEUE;
use anyserve_core::meta::{Format, Queue, connect};
use anyserve_core::service::InferenceService;
use anyserve_proto::inference::grpc_inference_service_server::GrpcInferenceServiceServer;
use axum::Router;
use axum::routing::get;
use clap::{Args, Parser, Subcommand};
use tokio::net::TcpListener;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(
    name = "anyserve",
    version,
    about = "Serve anywhere. Infer everywhere. Anyserve."
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
    Init(InitArgs),
    Serve(ServeArgs),
    Queue(QueueCommand),
}

#[derive(Args, Debug)]
struct InitArgs {
    meta_uri: String,
    name: String,
    #[arg(long)]
    force: bool,
}

#[derive(Args, Debug)]
struct ServeArgs {
    meta_uri: String,
    #[arg(long = "http-host", default_value = "0.0.0.0")]
    http_host: String,
    #[arg(long = "http-port", default_value_t = 8848)]
    http_port: u16,
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
}

#[derive(Args, Debug)]
struct QueueCommand {
    #[command(subcommand)]
    command: QueueSubcommand,
}

#[derive(Subcommand, Debug)]
enum QueueSubcommand {
    Create(QueueCreateArgs),
    List(QueueListArgs),
    Stats(QueueStatsArgs),
    Remove(QueueRemoveArgs),
}

#[derive(Args, Debug)]
struct QueueCreateArgs {
    meta_uri: String,
    name: String,
    #[arg(long)]
    index: String,
    #[arg(long)]
    streaming: Option<String>,
    #[arg(long)]
    storage: Option<String>,
}

#[derive(Args, Debug)]
struct QueueListArgs {
    meta_uri: String,
}

#[derive(Args, Debug)]
struct QueueStatsArgs {
    meta_uri: String,
    name: String,
}

#[derive(Args, Debug)]
struct QueueRemoveArgs {
    meta_uri: String,
    name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose, &cli.log_level)?;

    match cli.command {
        Commands::Init(args) => init(args).await,
        Commands::Serve(args) => serve(args).await,
        Commands::Queue(queue_command) => handle_queue(queue_command).await,
    }
}

async fn init(args: InitArgs) -> Result<()> {
    let meta = connect(&args.meta_uri).await?;

    let (format, force) = match meta.load().await {
        Ok(existing) => {
            if !args.force {
                bail!("backend already initialized; pass --force to overwrite");
            }
            warn!("meta backend already initialized, overwriting existing format");
            (
                Format {
                    name: args.name,
                    uuid: existing.uuid,
                },
                true,
            )
        }
        Err(_) => (
            Format {
                name: args.name,
                uuid: Uuid::new_v4().to_string(),
            },
            args.force,
        ),
    };

    meta.init(&format, force).await?;
    info!(
        name = format.name,
        uuid = format.uuid,
        "initialized metadata backend"
    );

    if !meta
        .list_queues()
        .await?
        .iter()
        .any(|queue| queue.name == DEFAULT_QUEUE)
    {
        meta.create_queue(Queue {
            name: DEFAULT_QUEUE.to_string(),
            ..Queue::default()
        })
        .await?;
        info!("created default queue");
    }

    Ok(())
}

async fn serve(args: ServeArgs) -> Result<()> {
    let meta = connect(&args.meta_uri).await?;
    let format = meta.load().await?;
    info!(
        name = format.name,
        uuid = format.uuid,
        "loaded metadata format"
    );

    let http_config = HttpConfig {
        host: args.http_host,
        port: args.http_port,
    };
    let grpc_config = GrpcConfig {
        host: args.grpc_host,
        port: args.grpc_port,
        tls_enabled: args.grpc_tls_enabled,
        cert_file: args.grpc_cert_file,
        key_file: args.grpc_key_file,
    };

    let http_listener = TcpListener::bind(socket_addr(&http_config.host, http_config.port)?)
        .await
        .context("bind http listener")?;
    let grpc_addr = socket_addr(&grpc_config.host, grpc_config.port)?;
    let http_addr = http_listener.local_addr().context("http local addr")?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let http_task = tokio::spawn(run_http_server(http_listener, shutdown_rx.clone()));
    let grpc_task = tokio::spawn(run_grpc_server(
        grpc_addr,
        Arc::clone(&meta),
        grpc_config,
        shutdown_rx,
    ));

    info!(
        http = %http_addr,
        grpc = %grpc_addr,
        "servers started"
    );

    tokio::signal::ctrl_c()
        .await
        .context("listen for shutdown signal")?;
    let _ = shutdown_tx.send(true);

    http_task.await.context("join http task")??;
    grpc_task.await.context("join grpc task")??;
    Ok(())
}

async fn handle_queue(command: QueueCommand) -> Result<()> {
    match command.command {
        QueueSubcommand::Create(args) => queue_create(args).await,
        QueueSubcommand::List(args) => queue_list(args).await,
        QueueSubcommand::Stats(args) => queue_stats(args).await,
        QueueSubcommand::Remove(args) => queue_remove(args).await,
    }
}

async fn queue_create(args: QueueCreateArgs) -> Result<()> {
    let meta = connect(&args.meta_uri).await?;
    let format = meta.load().await?;
    info!(
        name = format.name,
        uuid = format.uuid,
        "loaded metadata format"
    );

    meta.create_queue(Queue {
        name: args.name,
        index: normalize_index(&args.index)?,
        streaming: args.streaming.unwrap_or_default(),
        storage: args.storage.unwrap_or_default(),
    })
    .await?;
    Ok(())
}

async fn queue_list(args: QueueListArgs) -> Result<()> {
    let meta = connect(&args.meta_uri).await?;
    let queues = meta.list_queues().await?;
    println!(
        "{:<3} {:<20} {:<24} {:<24} Storage",
        "#", "Name", "Index", "Streaming"
    );
    for (index, queue) in queues.iter().enumerate() {
        println!(
            "{:<3} {:<20} {:<24} {:<24} {}",
            index, queue.name, queue.index, queue.streaming, queue.storage
        );
    }
    Ok(())
}

async fn queue_stats(args: QueueStatsArgs) -> Result<()> {
    let meta = connect(&args.meta_uri).await?;
    let stats = meta.queue_stats(&args.name).await?;
    println!("{}", serde_json::to_string_pretty(&stats)?);
    Ok(())
}

async fn queue_remove(args: QueueRemoveArgs) -> Result<()> {
    let meta = connect(&args.meta_uri).await?;
    meta.delete_queue(&args.name).await
}

async fn run_http_server(
    listener: TcpListener,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let app = Router::new()
        .route("/", get(|| async { "Hello, anyserve!" }))
        .route("/healthz", get(|| async { "ok" }));

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .context("run http server")
}

async fn run_grpc_server(
    addr: SocketAddr,
    meta: Arc<dyn anyserve_core::meta::MetaStore>,
    config: GrpcConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let service = InferenceService::new(meta);
    let grpc_service = GrpcInferenceServiceServer::new(service);
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<GrpcInferenceServiceServer<InferenceService>>()
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

fn normalize_index(index: &str) -> Result<String> {
    let values: Vec<String> = index
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            if value.starts_with('@') {
                value.to_string()
            } else {
                format!("@{value}")
            }
        })
        .collect();

    if values.is_empty() {
        bail!("index is required");
    }

    Ok(values.join(","))
}
