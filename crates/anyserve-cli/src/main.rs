use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{self, BufRead, IsTerminal, Write};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use anyserve_client::{
    AnyserveClient, AttemptRecord, AttemptState, JobRecord, JobState, StreamDirection,
    StreamRecord, StreamScope, StreamState,
};
use anyserve_core::config::GrpcConfig;
use anyserve_core::frame::{FramePlane, MemoryFramePlane, RedisFramePlane};
use anyserve_core::kernel::Kernel;
use anyserve_core::notify::{ClusterNotifier, NoopClusterNotifier, RedisClusterNotifier};
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::service::ControlPlaneGrpcService;
use anyserve_core::sql_store::SqlStateStore;
use anyserve_core::store::{MemoryStateStore, StateStore};
use anyserve_proto::controlplane::control_plane_service_server::ControlPlaneServiceServer;
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use sqlx::{AnyConnection, Connection};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod serve_config;
mod serve_openai;
mod serve_openai_worker;

use serve_config::{FrameBackend, ResolvedServeConfig, ServeOverrides, StorageBackend};
use serve_openai::run_server as run_serve_openai;
use serve_openai_worker::ServeOpenAIWorkerArgs;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:50052";
const POSTGRES_MEMORY_SINGLETON_LOCK_ID: i64 = 0x4153_5256_5047_4d4d;

struct PostgresMemorySingletonLock {
    _connection: AnyConnection,
}

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
    #[arg(long, default_value = DEFAULT_ENDPOINT, global = true)]
    endpoint: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Serve(ServeArgs),
    Worker(ServeOpenAIWorkerArgs),
    Job {
        #[command(subcommand)]
        command: JobCommands,
    },
}

#[derive(Subcommand, Debug)]
enum JobCommands {
    #[command(alias = "list")]
    Ls(JobLsArgs),
    Inspect(JobInspectArgs),
    Cancel(JobCancelArgs),
}

#[derive(Args, Clone, Debug)]
struct OutputArgs {
    #[arg(short = 'o', long = "output", value_enum, default_value = "text")]
    output: OutputFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum JobStateFilter {
    Pending,
    Leased,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Args, Debug)]
struct JobLsArgs {
    #[arg(long = "state", value_enum)]
    states: Vec<JobStateFilter>,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Args, Debug)]
struct JobInspectArgs {
    job_id: String,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Args, Debug)]
struct JobCancelArgs {
    #[arg(
        value_name = "JOB_ID",
        num_args = 1..,
        required_unless_present = "stdin",
        conflicts_with = "stdin"
    )]
    job_ids: Vec<String>,
    #[arg(long, conflicts_with = "job_ids")]
    stdin: bool,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Args, Debug)]
struct ServeArgs {
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

#[derive(Clone, Copy, Debug, Default)]
struct JobCounts {
    pending: usize,
    leased: usize,
    running: usize,
    succeeded: usize,
    failed: usize,
    cancelled: usize,
}

#[derive(Serialize)]
struct JobJsonRecord {
    job_id: String,
    state: String,
    interface_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_attempt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_id: Option<String>,
    version: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    outputs_count: usize,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct AttemptJsonRecord {
    attempt_id: String,
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_id: Option<String>,
    created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    finished_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct StreamJsonRecord {
    stream_id: String,
    stream_name: String,
    scope: String,
    direction: String,
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_id: Option<String>,
    created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_at_ms: Option<u64>,
    last_sequence: u64,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct JobInspectJsonRecord {
    job: JobJsonRecord,
    attempts: Vec<AttemptJsonRecord>,
    streams: Vec<StreamJsonRecord>,
}

#[derive(Serialize)]
struct CancelJsonResult {
    job_id: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let Cli {
        verbose,
        log_level,
        endpoint,
        command,
    } = Cli::parse();
    init_tracing(verbose, &log_level)?;

    match command {
        Commands::Serve(args) => serve(args).await,
        Commands::Worker(args) => serve_openai_worker::run(&endpoint, args).await,
        Commands::Job { command } => run_job_command(&endpoint, command).await,
    }
}

async fn run_job_command(endpoint: &str, command: JobCommands) -> Result<()> {
    let mut client = AnyserveClient::connect(endpoint.to_string()).await?;
    match command {
        JobCommands::Ls(args) => run_job_ls(endpoint, &mut client, args).await,
        JobCommands::Inspect(args) => run_job_inspect(endpoint, &mut client, args).await,
        JobCommands::Cancel(args) => run_job_cancel(endpoint, &mut client, args).await,
    }
}

async fn run_job_ls(endpoint: &str, client: &mut AnyserveClient, args: JobLsArgs) -> Result<()> {
    let jobs = client.list_jobs().await?;
    let filtered_jobs = filter_jobs_by_state(jobs, &args.states);

    if args.output.output == OutputFormat::Json {
        let payload = filtered_jobs
            .iter()
            .map(job_json_record)
            .collect::<Vec<_>>();
        return write_json(&payload);
    }

    let counts = count_jobs(&filtered_jobs);
    print_section("Control Plane");
    let mut entries = vec![
        ("endpoint", endpoint.to_string()),
        ("jobs", format!("{} total", filtered_jobs.len())),
        ("states", format_job_counts(counts)),
    ];
    if !args.states.is_empty() {
        entries.push(("filter", format_state_filters(&args.states)));
    }
    print_key_values(&entries);
    println!();

    print_section("Jobs");
    if filtered_jobs.is_empty() {
        println!("No jobs found.");
        return Ok(());
    }

    let rows = filtered_jobs
        .iter()
        .map(|job| {
            vec![
                ellipsize(&job.job_id, 18),
                job_state_name(job.state()).to_string(),
                ellipsize(interface_name(job), 24),
                ellipsize(optional_text(&job.current_attempt_id), 18),
                ellipsize(optional_text(&job.last_error), 24),
                format_age(job.updated_at_ms),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "JOB ID",
            "STATE",
            "INTERFACE",
            "ATTEMPT",
            "LAST ERROR",
            "UPDATED",
        ],
        &rows,
    );
    Ok(())
}

async fn run_job_inspect(
    endpoint: &str,
    client: &mut AnyserveClient,
    args: JobInspectArgs,
) -> Result<()> {
    let job = client.get_job(args.job_id.clone()).await?;
    let attempts = client.list_attempts(args.job_id.clone()).await?;
    let streams = client.list_streams(args.job_id).await?;

    if args.output.output == OutputFormat::Json {
        let payload = JobInspectJsonRecord {
            job: job_json_record(&job),
            attempts: attempts.iter().map(attempt_json_record).collect(),
            streams: streams.iter().map(stream_json_record).collect(),
        };
        return write_json(&payload);
    }

    print_section("Job");
    print_key_values(&[
        ("endpoint", endpoint.to_string()),
        ("job_id", job.job_id.clone()),
        ("state", job_state_name(job.state()).to_string()),
        ("interface", interface_name(&job).to_string()),
        (
            "current_attempt",
            optional_text(&job.current_attempt_id).to_string(),
        ),
        ("lease_id", optional_text(&job.lease_id).to_string()),
        ("created_at", format_timestamp(job.created_at_ms)),
        ("updated_at", format_timestamp(job.updated_at_ms)),
        ("outputs", job.outputs.len().to_string()),
        ("last_error", optional_text(&job.last_error).to_string()),
        ("metadata", format_string_map(job_metadata(&job))),
    ]);
    println!();

    print_section("Attempts");
    if attempts.is_empty() {
        println!("No attempts recorded.");
    } else {
        let rows = attempts
            .iter()
            .map(|attempt| {
                vec![
                    ellipsize(&attempt.attempt_id, 18),
                    attempt_state_name(attempt.state()).to_string(),
                    ellipsize(optional_text(&attempt.worker_id), 18),
                    ellipsize(optional_text(&attempt.lease_id), 18),
                    format_age(attempt.created_at_ms),
                    format_optional_timestamp(attempt.finished_at_ms),
                ]
            })
            .collect::<Vec<_>>();
        print_table(
            &[
                "ATTEMPT ID",
                "STATE",
                "WORKER",
                "LEASE",
                "CREATED",
                "FINISHED",
            ],
            &rows,
        );
    }
    println!();

    print_section("Streams");
    if streams.is_empty() {
        println!("No streams recorded.");
    } else {
        let rows = streams
            .iter()
            .map(|stream| {
                vec![
                    ellipsize(&stream.stream_id, 18),
                    ellipsize(&stream.stream_name, 20),
                    stream_scope_name(stream.scope()).to_string(),
                    stream_direction_name(stream.direction()).to_string(),
                    stream_state_name(stream.state()).to_string(),
                    format_optional_timestamp(stream.closed_at_ms),
                ]
            })
            .collect::<Vec<_>>();
        print_table(
            &["STREAM ID", "NAME", "SCOPE", "DIRECTION", "STATE", "CLOSED"],
            &rows,
        );
    }

    Ok(())
}

async fn run_job_cancel(
    endpoint: &str,
    client: &mut AnyserveClient,
    args: JobCancelArgs,
) -> Result<()> {
    let job_ids = collect_cancel_job_ids(&args)?;
    let batch_mode = args.stdin || job_ids.len() > 1;

    if args.output.output == OutputFormat::Text && !batch_mode {
        let job = client.cancel_job(job_ids[0].clone()).await?;
        let state = job_state_name(job.state()).to_string();
        let updated_at = format_timestamp(job.updated_at_ms);

        print_section("Cancel Result");
        print_key_values(&[
            ("endpoint", endpoint.to_string()),
            ("job_id", job.job_id),
            ("state", state),
            ("updated_at", updated_at),
        ]);
        return Ok(());
    }

    let mut failures = 0usize;
    for job_id in job_ids {
        let result = match client.cancel_job(job_id.clone()).await {
            Ok(job) => cancel_result_from_job(job),
            Err(error) => {
                failures += 1;
                CancelJsonResult {
                    job_id,
                    ok: false,
                    state: None,
                    updated_at_ms: None,
                    error: Some(error.to_string()),
                }
            }
        };

        match args.output.output {
            OutputFormat::Json => write_json_line(&result)?,
            OutputFormat::Text => print_cancel_result_text(&result),
        }
    }

    if failures > 0 {
        bail!("failed to cancel {failures} job(s)");
    }

    Ok(())
}

async fn serve(args: ServeArgs) -> Result<()> {
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

    print_startup_access_points(
        &grpc_addr,
        config.grpc.tls_enabled,
        config.config_path.as_deref(),
        openai_addr.as_ref(),
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

    tokio::signal::ctrl_c()
        .await
        .context("listen for shutdown signal")?;
    let _ = shutdown_tx.send(true);

    grpc_task.await.context("join grpc task")??;
    if let Some(openai_task) = openai_task {
        openai_task.await.context("join openai task")??;
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
    config_path: Option<&std::path::Path>,
    openai_addr: Option<&SocketAddr>,
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

    print_section("Control Plane Ready");
    print_key_values(&entries);
    println!();
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

fn collect_cancel_job_ids(args: &JobCancelArgs) -> Result<Vec<String>> {
    if args.stdin {
        return read_job_ids_from_stdin();
    }

    let job_ids = args
        .job_ids
        .iter()
        .filter_map(|job_id| normalize_job_id(job_id))
        .collect::<Vec<_>>();
    if job_ids.is_empty() {
        bail!("no job ids provided");
    }
    Ok(job_ids)
}

fn read_job_ids_from_stdin() -> Result<Vec<String>> {
    let stdin = io::stdin();
    if stdin.is_terminal() {
        bail!("--stdin requires piped input");
    }
    read_job_ids_from_reader(stdin.lock())
}

fn read_job_ids_from_reader<R: BufRead>(reader: R) -> Result<Vec<String>> {
    let mut job_ids = Vec::new();
    for line in reader.lines() {
        let line = line.context("read stdin line")?;
        if let Some(job_id) = normalize_job_id(&line) {
            job_ids.push(job_id);
        }
    }

    if job_ids.is_empty() {
        bail!("no job ids provided");
    }

    Ok(job_ids)
}

fn normalize_job_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn count_jobs(jobs: &[JobRecord]) -> JobCounts {
    let mut counts = JobCounts::default();
    for job in jobs {
        match job.state() {
            JobState::Pending => counts.pending += 1,
            JobState::Leased => counts.leased += 1,
            JobState::Running => counts.running += 1,
            JobState::Succeeded => counts.succeeded += 1,
            JobState::Failed => counts.failed += 1,
            JobState::Cancelled => counts.cancelled += 1,
            JobState::Unspecified => {}
        }
    }
    counts
}

fn filter_jobs_by_state(jobs: Vec<JobRecord>, states: &[JobStateFilter]) -> Vec<JobRecord> {
    if states.is_empty() {
        return jobs;
    }

    jobs.into_iter()
        .filter(|job| states.iter().any(|state| job_matches_state(job, *state)))
        .collect()
}

fn job_matches_state(job: &JobRecord, state: JobStateFilter) -> bool {
    matches!(
        (job.state(), state),
        (JobState::Pending, JobStateFilter::Pending)
            | (JobState::Leased, JobStateFilter::Leased)
            | (JobState::Running, JobStateFilter::Running)
            | (JobState::Succeeded, JobStateFilter::Succeeded)
            | (JobState::Failed, JobStateFilter::Failed)
            | (JobState::Cancelled, JobStateFilter::Cancelled)
    )
}

fn format_state_filters(states: &[JobStateFilter]) -> String {
    states
        .iter()
        .map(|state| job_state_filter_name(*state))
        .collect::<Vec<_>>()
        .join(",")
}

fn cancel_result_from_job(job: JobRecord) -> CancelJsonResult {
    let state = job_state_name(job.state()).to_string();
    let updated_at_ms = job.updated_at_ms;
    CancelJsonResult {
        job_id: job.job_id,
        ok: true,
        state: Some(state),
        updated_at_ms: Some(updated_at_ms),
        error: None,
    }
}

fn job_json_record(job: &JobRecord) -> JobJsonRecord {
    JobJsonRecord {
        job_id: job.job_id.clone(),
        state: job_state_name(job.state()).to_string(),
        interface_name: interface_name(job).to_string(),
        current_attempt_id: optional_string(&job.current_attempt_id),
        lease_id: optional_string(&job.lease_id),
        version: job.version,
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        last_error: optional_string(&job.last_error),
        outputs_count: job.outputs.len(),
        metadata: sorted_map(job_metadata(job)),
    }
}

fn attempt_json_record(attempt: &AttemptRecord) -> AttemptJsonRecord {
    AttemptJsonRecord {
        attempt_id: attempt.attempt_id.clone(),
        state: attempt_state_name(attempt.state()).to_string(),
        worker_id: optional_string(&attempt.worker_id),
        lease_id: optional_string(&attempt.lease_id),
        created_at_ms: attempt.created_at_ms,
        started_at_ms: optional_timestamp(attempt.started_at_ms),
        finished_at_ms: optional_timestamp(attempt.finished_at_ms),
        last_error: optional_string(&attempt.last_error),
        metadata: sorted_map(&attempt.metadata),
    }
}

fn stream_json_record(stream: &StreamRecord) -> StreamJsonRecord {
    StreamJsonRecord {
        stream_id: stream.stream_id.clone(),
        stream_name: stream.stream_name.clone(),
        scope: stream_scope_name(stream.scope()).to_string(),
        direction: stream_direction_name(stream.direction()).to_string(),
        state: stream_state_name(stream.state()).to_string(),
        attempt_id: optional_string(&stream.attempt_id),
        lease_id: optional_string(&stream.lease_id),
        created_at_ms: stream.created_at_ms,
        closed_at_ms: optional_timestamp(stream.closed_at_ms),
        last_sequence: stream.last_sequence,
        metadata: sorted_map(&stream.metadata),
    }
}

fn sorted_map(map: &HashMap<String, String>) -> BTreeMap<String, String> {
    map.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn optional_string(value: &str) -> Option<String> {
    normalize_job_id(value)
}

fn optional_timestamp(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

fn write_json<T: Serialize>(value: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer(&mut handle, value).context("serialize json output")?;
    handle.write_all(b"\n").context("flush json newline")
}

fn write_json_line<T: Serialize>(value: &T) -> Result<()> {
    write_json(value)
}

fn print_cancel_result_text(result: &CancelJsonResult) {
    match (
        &result.ok,
        &result.state,
        &result.updated_at_ms,
        &result.error,
    ) {
        (true, Some(state), Some(updated_at_ms), _) => {
            println!(
                "cancelled {} state={} updated_at_ms={}",
                result.job_id, state, updated_at_ms
            );
        }
        (false, _, _, Some(error)) => {
            println!("failed {} error={}", result.job_id, error);
        }
        _ => {
            println!("failed {} error=unknown", result.job_id);
        }
    }
}

fn format_job_counts(counts: JobCounts) -> String {
    format!(
        "pending {} | leased {} | running {} | succeeded {} | failed {} | cancelled {}",
        counts.pending,
        counts.leased,
        counts.running,
        counts.succeeded,
        counts.failed,
        counts.cancelled
    )
}

fn print_section(title: &str) {
    println!("{title}");
    println!("{}", "=".repeat(title.len()));
}

fn print_key_values(entries: &[(&str, String)]) {
    let width = entries.iter().map(|(key, _)| key.len()).max().unwrap_or(0);
    for (key, value) in entries {
        println!("{key:width$} : {value}", width = width);
    }
}

fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            if let Some(width) = widths.get_mut(index) {
                *width = (*width).max(cell.len());
            }
        }
    }

    let separator = format!(
        "+{}+",
        widths
            .iter()
            .map(|width| "-".repeat(width + 2))
            .collect::<Vec<_>>()
            .join("+")
    );

    println!("{separator}");
    print!("|");
    for (header, width) in headers.iter().zip(widths.iter()) {
        print!(" {:width$} |", header, width = *width);
    }
    println!();
    println!("{separator}");

    for row in rows {
        print!("|");
        for (cell, width) in row.iter().zip(widths.iter()) {
            print!(" {:width$} |", cell, width = *width);
        }
        println!();
    }
    println!("{separator}");
}

fn interface_name(job: &JobRecord) -> &str {
    job.spec
        .as_ref()
        .map(|spec| spec.interface_name.as_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("-")
}

fn job_metadata(job: &JobRecord) -> &HashMap<String, String> {
    static EMPTY: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
    job.spec
        .as_ref()
        .map(|spec| &spec.metadata)
        .unwrap_or_else(|| EMPTY.get_or_init(HashMap::new))
}

fn format_string_map(map: &HashMap<String, String>) -> String {
    if map.is_empty() {
        return "-".to_string();
    }

    let mut entries = map
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    entries.sort();
    entries.join(", ")
}

fn optional_text(value: &str) -> &str {
    if value.trim().is_empty() { "-" } else { value }
}

fn ellipsize(value: &str, limit: usize) -> String {
    let length = value.chars().count();
    if length <= limit {
        return value.to_string();
    }
    if limit <= 3 {
        return value.chars().take(limit).collect();
    }

    let prefix = value.chars().take(limit - 3).collect::<String>();
    format!("{prefix}...")
}

fn format_age(timestamp_ms: u64) -> String {
    if timestamp_ms == 0 {
        return "-".to_string();
    }

    let now_ms = now_ms();
    if timestamp_ms >= now_ms {
        return "just now".to_string();
    }

    let delta_ms = now_ms - timestamp_ms;
    format!("{} ago", human_duration(delta_ms))
}

fn format_timestamp(timestamp_ms: u64) -> String {
    if timestamp_ms == 0 {
        "-".to_string()
    } else {
        format!("{timestamp_ms} ({})", format_age(timestamp_ms))
    }
}

fn format_optional_timestamp(timestamp_ms: u64) -> String {
    if timestamp_ms == 0 {
        "-".to_string()
    } else {
        format_age(timestamp_ms)
    }
}

fn human_duration(duration_ms: u64) -> String {
    const SECOND: u64 = 1_000;
    const MINUTE: u64 = 60 * SECOND;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;

    if duration_ms >= DAY {
        format!("{}d", duration_ms / DAY)
    } else if duration_ms >= HOUR {
        format!("{}h", duration_ms / HOUR)
    } else if duration_ms >= MINUTE {
        format!("{}m", duration_ms / MINUTE)
    } else if duration_ms >= SECOND {
        format!("{}s", duration_ms / SECOND)
    } else {
        format!("{duration_ms}ms")
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn job_state_name(state: JobState) -> &'static str {
    match state {
        JobState::Pending => "pending",
        JobState::Leased => "leased",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
        JobState::Unspecified => "unspecified",
    }
}

fn job_state_filter_name(state: JobStateFilter) -> &'static str {
    match state {
        JobStateFilter::Pending => "pending",
        JobStateFilter::Leased => "leased",
        JobStateFilter::Running => "running",
        JobStateFilter::Succeeded => "succeeded",
        JobStateFilter::Failed => "failed",
        JobStateFilter::Cancelled => "cancelled",
    }
}

fn attempt_state_name(state: AttemptState) -> &'static str {
    match state {
        AttemptState::Created => "created",
        AttemptState::Leased => "leased",
        AttemptState::Running => "running",
        AttemptState::Succeeded => "succeeded",
        AttemptState::Failed => "failed",
        AttemptState::Expired => "expired",
        AttemptState::Cancelled => "cancelled",
        AttemptState::Unspecified => "unspecified",
    }
}

fn stream_scope_name(scope: StreamScope) -> &'static str {
    match scope {
        StreamScope::Job => "job",
        StreamScope::Attempt => "attempt",
        StreamScope::Lease => "lease",
        StreamScope::Unspecified => "unspecified",
    }
}

fn stream_direction_name(direction: StreamDirection) -> &'static str {
    match direction {
        StreamDirection::ClientToWorker => "client_to_worker",
        StreamDirection::WorkerToClient => "worker_to_client",
        StreamDirection::Bidirectional => "bidirectional",
        StreamDirection::Internal => "internal",
        StreamDirection::Unspecified => "unspecified",
    }
}

fn stream_state_name(state: StreamState) -> &'static str {
    match state {
        StreamState::Open => "open",
        StreamState::Closing => "closing",
        StreamState::Closed => "closed",
        StreamState::Error => "error",
        StreamState::Unspecified => "unspecified",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;

    use anyserve_proto::controlplane::JobSpec;
    use serde_json::json;

    use super::{
        CancelJsonResult, JobStateFilter, ellipsize, filter_jobs_by_state, human_duration,
        job_json_record, read_job_ids_from_reader,
    };
    use anyserve_client::{JobRecord, JobState};

    #[test]
    fn ellipsize_keeps_short_values() {
        assert_eq!(ellipsize("job-1", 8), "job-1");
    }

    #[test]
    fn ellipsize_truncates_long_values() {
        assert_eq!(ellipsize("1234567890abcdef", 8), "12345...");
    }

    #[test]
    fn human_duration_uses_compact_units() {
        assert_eq!(human_duration(999), "999ms");
        assert_eq!(human_duration(2_000), "2s");
        assert_eq!(human_duration(120_000), "2m");
    }

    #[test]
    fn read_job_ids_from_reader_trims_and_skips_empty_lines() {
        let input = Cursor::new("job-1\n\n  job-2 \n   \njob-3\n");
        let job_ids = read_job_ids_from_reader(input).unwrap();
        assert_eq!(job_ids, vec!["job-1", "job-2", "job-3"]);
    }

    #[test]
    fn job_json_record_serializes_machine_readable_shape() {
        let job = JobRecord {
            job_id: "job-1".to_string(),
            state: anyserve_client::JobState::Pending as i32,
            spec: Some(JobSpec {
                interface_name: "demo.echo.v1".to_string(),
                metadata: HashMap::from([("team".to_string(), "ops".to_string())]),
                ..JobSpec::default()
            }),
            version: 3,
            created_at_ms: 100,
            updated_at_ms: 200,
            ..JobRecord::default()
        };

        let value = serde_json::to_value(job_json_record(&job)).unwrap();
        assert_eq!(
            value,
            json!({
                "job_id": "job-1",
                "state": "pending",
                "interface_name": "demo.echo.v1",
                "version": 3,
                "created_at_ms": 100,
                "updated_at_ms": 200,
                "outputs_count": 0,
                "metadata": {
                    "team": "ops"
                }
            })
        );
    }

    #[test]
    fn cancel_json_result_omits_optional_fields_when_missing() {
        let result = CancelJsonResult {
            job_id: "job-1".to_string(),
            ok: false,
            state: None,
            updated_at_ms: None,
            error: Some("boom".to_string()),
        };

        let value = serde_json::to_value(result).unwrap();
        assert_eq!(
            value,
            json!({ "job_id": "job-1", "ok": false, "error": "boom" })
        );
    }

    #[test]
    fn filter_jobs_by_state_keeps_only_requested_states() {
        let pending = JobRecord {
            job_id: "job-pending".to_string(),
            state: JobState::Pending as i32,
            ..JobRecord::default()
        };
        let cancelled = JobRecord {
            job_id: "job-cancelled".to_string(),
            state: JobState::Cancelled as i32,
            ..JobRecord::default()
        };

        let filtered =
            filter_jobs_by_state(vec![pending.clone(), cancelled], &[JobStateFilter::Pending]);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].job_id, pending.job_id);
    }
}
