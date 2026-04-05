use std::collections::BTreeSet;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use anyserve_core::frame::{FramePlane, MemoryFramePlane, RedisFramePlane};
use anyserve_core::kernel::Kernel;
use anyserve_core::notify::{ClusterNotifier, NoopClusterNotifier, RedisClusterNotifier};
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::service::ControlPlaneGrpcService;
use anyserve_core::sql_store::SqlStateStore;
use anyserve_core::store::{MemoryStateStore, StateStore};
use anyserve_proto::controlplane::control_plane_service_server::ControlPlaneServiceServer;
use clap::Args;
use sqlx::{AnyConnection, Connection};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{info, warn};

use crate::serve_config::{
    FrameBackend, GrpcConfig, ResolvedServeConfig, ServeOverrides, StorageBackend,
};
use crate::serve_console::run_server as run_console_server;
use crate::serve_openai::run_server as run_serve_openai;

const POSTGRES_MEMORY_SINGLETON_LOCK_ID: i64 = 0x4153_5256_5047_4d4d;

struct PostgresMemorySingletonLock {
    _connection: AnyConnection,
}

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
    #[arg(long = "heartbeat-ttl-secs")]
    heartbeat_ttl_secs: Option<u64>,
    #[arg(long = "default-lease-ttl-secs")]
    default_lease_ttl_secs: Option<u64>,
}

pub(crate) async fn run(args: ServeArgs) -> Result<()> {
    let config = ResolvedServeConfig::load(ServeOverrides {
        config: args.config,
        grpc_host: args.grpc_host,
        grpc_port: args.grpc_port,
        grpc_tls_enabled: args.grpc_tls_enabled,
        grpc_cert_file: args.grpc_cert_file,
        grpc_key_file: args.grpc_key_file,
        heartbeat_ttl_secs: args.heartbeat_ttl_secs,
        default_lease_ttl_secs: args.default_lease_ttl_secs,
    })?;
    if config.storage.backend == StorageBackend::Postgres
        && config.frames.backend == FrameBackend::Memory
    {
        warn!(
            "postgres + memory mode is intended for a single control-plane instance; frame data stays in memory and is not shared or persisted"
        );
    }
    let grpc_config = config.grpc.clone();
    let _postgres_memory_lock = match (config.storage.backend, config.frames.backend) {
        (StorageBackend::Postgres, FrameBackend::Memory) => {
            let dsn = config
                .storage
                .dsn
                .as_deref()
                .context("storage dsn is required")?;
            Some(acquire_postgres_memory_singleton_lock(dsn).await?)
        }
        _ => None,
    };
    let state_store: Arc<dyn StateStore> = match config.storage.backend {
        StorageBackend::Memory => Arc::new(MemoryStateStore::new()),
        StorageBackend::Sqlite | StorageBackend::Postgres => {
            let dsn = config
                .storage
                .dsn
                .as_deref()
                .context("storage dsn is required")?;
            Arc::new(SqlStateStore::connect(dsn).await?)
        }
    };
    let frame_plane: Arc<dyn FramePlane> = match config.frames.backend {
        FrameBackend::Memory => Arc::new(MemoryFramePlane::new()),
        FrameBackend::Redis => Arc::new(RedisFramePlane::new(
            config
                .frames
                .redis_url
                .as_deref()
                .context("frames redis url is required")?,
            config.frames.closed_retention_secs,
        )?),
    };
    let notifier: Arc<dyn ClusterNotifier> = match config.frames.backend {
        FrameBackend::Memory => Arc::new(NoopClusterNotifier),
        FrameBackend::Redis => Arc::new(RedisClusterNotifier::new(
            config
                .frames
                .redis_url
                .as_deref()
                .context("frames redis url is required")?,
        )?),
    };

    let kernel = Arc::new(Kernel::new(
        state_store,
        frame_plane,
        notifier,
        Arc::new(BasicScheduler),
        config.heartbeat_ttl_secs,
        config.default_lease_ttl_secs,
        config.frames.watch_poll_interval_ms,
    ));

    let grpc_addr = socket_addr(&grpc_config.host, grpc_config.port)?;
    let openai_addr = config
        .openai
        .as_ref()
        .map(|openai| socket_addr_from_bind(&openai.listen))
        .transpose()?;
    let console_addr = config
        .console
        .as_ref()
        .map(|console| socket_addr_from_bind(&console.listen))
        .transpose()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let grpc_task = tokio::spawn(run_grpc_server(
        grpc_addr,
        Arc::clone(&kernel),
        grpc_config,
        shutdown_rx,
    ));
    let openai_task =
        if let (Some(openai_config), Some(openai_addr)) = (config.openai.clone(), openai_addr) {
            Some(tokio::spawn(run_serve_openai(
                openai_addr,
                Arc::clone(&kernel),
                openai_config,
                shutdown_tx.subscribe(),
            )))
        } else {
            None
        };
    let console_task = if let (Some(console_config), Some(console_addr)) =
        (config.console.clone(), console_addr)
    {
        Some(tokio::spawn(run_console_server(
            console_addr,
            Arc::clone(&kernel),
            console_config,
            config.runtime_mode(),
            shutdown_tx.subscribe(),
        )))
    } else {
        None
    };

    print_startup_access_points(
        &grpc_addr,
        config.grpc.tls_enabled,
        config.config_path.as_deref(),
        openai_addr.as_ref(),
        console_addr.as_ref(),
    );
    if let Some(config_path) = config.config_path.as_ref() {
        let config_path = config_path.display().to_string();
        info!(grpc = %grpc_addr, config = %config_path, "control plane started");
    } else {
        info!(grpc = %grpc_addr, "control plane started");
    }
    if let Some(openai_addr) = openai_addr {
        info!(openai = %openai_addr, "openai gateway started");
    }
    if let Some(console_addr) = console_addr {
        info!(console = %console_addr, "console api started");
    }

    tokio::signal::ctrl_c()
        .await
        .context("listen for shutdown signal")?;
    let _ = shutdown_tx.send(true);

    grpc_task.await.context("join grpc task")??;
    if let Some(openai_task) = openai_task {
        openai_task.await.context("join openai task")??;
    }
    if let Some(console_task) = console_task {
        console_task.await.context("join console task")??;
    }
    Ok(())
}

async fn acquire_postgres_memory_singleton_lock(dsn: &str) -> Result<PostgresMemorySingletonLock> {
    sqlx::any::install_default_drivers();
    let mut connection = AnyConnection::connect(dsn)
        .await
        .with_context(|| format!("connect postgres singleton lock session {dsn}"))?;
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(POSTGRES_MEMORY_SINGLETON_LOCK_ID)
        .fetch_one(&mut connection)
        .await
        .context("acquire postgres + memory singleton advisory lock")?;
    if !acquired {
        bail!("postgres + memory only supports a single control-plane instance");
    }
    Ok(PostgresMemorySingletonLock {
        _connection: connection,
    })
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

fn print_startup_access_points(
    addr: &SocketAddr,
    tls_enabled: bool,
    config_path: Option<&Path>,
    openai_addr: Option<&SocketAddr>,
    console_addr: Option<&SocketAddr>,
) {
    let scheme = if tls_enabled { "https" } else { "http" };
    let grpc_access_points = access_points(addr, scheme);
    let mut entries = vec![
        ("grpc", addr.to_string()),
        ("scheme", scheme.to_string()),
        ("access", grpc_access_points.join(", ")),
    ];
    if let Some(config_path) = config_path {
        entries.push(("config", config_path.display().to_string()));
    }
    if let Some(openai_addr) = openai_addr {
        let access = access_points(openai_addr, "http")
            .into_iter()
            .map(|value| format!("{value}/v1"))
            .collect::<Vec<_>>()
            .join(", ");
        entries.push(("openai", access));
    }
    if let Some(console_addr) = console_addr {
        entries.push(("console", access_points(console_addr, "http").join(", ")));
    }

    println!("Control Plane Ready");
    println!("{}", "=".repeat("Control Plane Ready".len()));
    let width = entries.iter().map(|(key, _)| key.len()).max().unwrap_or(0);
    for (key, value) in entries {
        println!("{key:width$} : {value}", width = width);
    }
    println!();
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

fn access_points(addr: &SocketAddr, scheme: &str) -> Vec<String> {
    let mut access_points = BTreeSet::new();
    match addr.ip() {
        std::net::IpAddr::V4(ip) if ip.is_unspecified() => {
            access_points.insert(format!("{scheme}://127.0.0.1:{}", addr.port()));
            if let Some(local_ip) = detect_local_ipv4() {
                access_points.insert(format!("{scheme}://{local_ip}:{}", addr.port()));
            }
        }
        std::net::IpAddr::V6(ip) if ip.is_unspecified() => {
            access_points.insert(format!("{scheme}://[::1]:{}", addr.port()));
            if let Some(local_ip) = detect_local_ipv4() {
                access_points.insert(format!("{scheme}://{local_ip}:{}", addr.port()));
            }
        }
        std::net::IpAddr::V4(ip) if ip.is_loopback() => {
            access_points.insert(format!("{scheme}://{ip}:{}", addr.port()));
        }
        std::net::IpAddr::V6(ip) if ip.is_loopback() => {
            access_points.insert(format!("{scheme}://[{ip}]:{}", addr.port()));
        }
        std::net::IpAddr::V4(ip) => {
            access_points.insert(format!("{scheme}://{ip}:{}", addr.port()));
        }
        std::net::IpAddr::V6(ip) => {
            access_points.insert(format!("{scheme}://[{ip}]:{}", addr.port()));
        }
    }

    access_points.into_iter().collect()
}

fn detect_local_ipv4() -> Option<std::net::Ipv4Addr> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    match socket.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(ip) if !ip.is_loopback() && !ip.is_unspecified() => Some(ip),
        _ => None,
    }
}
