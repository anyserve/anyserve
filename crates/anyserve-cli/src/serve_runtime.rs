use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use anyserve_core::auth::{AuthLayer, AuthPolicy};
use anyserve_core::client_service::ClientGrpcService;
use anyserve_core::runtime::Runtime;
use anyserve_core::worker_service::WorkerGrpcService;
use anyserve_proto::v2::{client_server::ClientServer, worker_server::WorkerServer};
use clap::Args;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{info, warn};

use crate::serve_config::{
    AuthRevocationConfig, AutoTokens, GrpcConfig, ResolvedServeConfig, ServeOverrides,
};
use crate::serve_console::{
    ControlPlaneMetadata, format_host_port, run_server_with_listener as run_console_server,
};

#[cfg(test)]
mod tests;

const MAX_PENDING_RESET_STREAMS: usize = 1024;
const REVOCATION_STARTUP_TIMEOUT: Duration = Duration::from_secs(3);
const REVOCATION_MAX_BACKOFF: Duration = Duration::from_secs(30);

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
    /// Disable the gRPC listener (requires --console-listen or [console] in config).
    #[arg(long = "no-grpc")]
    no_grpc: bool,
    /// Disable durable jobs and keep the runtime entirely in memory.
    #[arg(long, conflicts_with = "journal_path")]
    ephemeral: bool,
    /// SQLite recovery journal (default: .anyserve/journal.sqlite).
    #[arg(long = "journal", value_name = "PATH")]
    journal_path: Option<PathBuf>,
    /// Maximum SQLite journal size in bytes (ANYSERVE_JOURNAL_MAX_BYTES / [journal].max_bytes).
    #[arg(long = "journal-max-bytes", value_name = "BYTES")]
    journal_max_bytes: Option<u64>,
    /// Approximate process memory limit in MiB used to scale byte budgets automatically.
    #[arg(long = "memory-limit", value_name = "MB")]
    memory_limit_mb: Option<u64>,
    /// Enable and bind the HTTP console / management API listener (e.g. 127.0.0.1:3001).
    #[arg(long = "console-listen", value_name = "ADDR")]
    console_listen: Option<String>,
    /// Public endpoint URL advertised to clients and workers (overrides [console].public_endpoint and ANYSERVE_PUBLIC_ENDPOINT).
    #[arg(long = "public-endpoint", value_name = "URL")]
    public_endpoint: Option<String>,
    /// Static client bearer token (repeatable; comma-separated values are split).
    #[arg(long = "client-token", value_name = "TOKEN")]
    client_tokens: Vec<String>,
    /// Static worker bearer token (repeatable; comma-separated values are split).
    #[arg(long = "worker-token", value_name = "TOKEN")]
    worker_tokens: Vec<String>,
    /// Generate (or reload from <journal_dir>/tokens.env) random client and worker tokens when no auth is configured.
    #[arg(long = "auto-token")]
    auto_token: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct StartupSummary<'a> {
    pub grpc_addr: Option<SocketAddr>,
    pub grpc_tls_enabled: bool,
    pub console_addr: Option<SocketAddr>,
    pub embedded_console_ui: bool,
    pub public_endpoint: Option<&'a str>,
    pub auth_enabled: bool,
    pub auto_tokens: Option<&'a AutoTokens>,
}

fn connectable_socket_addr(addr: SocketAddr) -> SocketAddr {
    match addr {
        SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
            SocketAddr::from(([127, 0, 0, 1], v4.port()))
        }
        SocketAddr::V6(v6) if v6.ip().is_unspecified() => {
            SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], v6.port()))
        }
        other => other,
    }
}

fn percent_encode_uri_component(input: &str) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(input.len());
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn console_http_base_url(console_addr: SocketAddr, public_endpoint: Option<&str>) -> String {
    if let Some(raw) = public_endpoint {
        let trimmed = raw.trim();
        let without_fragment = trimmed.split('#').next().unwrap_or(trimmed).trim();
        let base = without_fragment.trim_end_matches('/');
        if let Some((scheme, rest)) = base.split_once("://")
            && !rest.is_empty()
        {
            if scheme.eq_ignore_ascii_case("https") || scheme.eq_ignore_ascii_case("wss") {
                return format!("https://{rest}");
            }
            if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("ws") {
                return format!("http://{rest}");
            }
        }
    }
    format!("http://{}", connectable_socket_addr(console_addr))
}

fn console_login_url(summary: &StartupSummary<'_>) -> Option<String> {
    let console_addr = summary.console_addr?;
    let auto = summary.auto_tokens?;
    let token = auto.client_token.trim();
    if token.is_empty() {
        return None;
    }
    let base = console_http_base_url(console_addr, summary.public_endpoint);
    Some(format!(
        "{base}/#token={}",
        percent_encode_uri_component(token)
    ))
}

pub(crate) fn format_startup_summary(summary: &StartupSummary<'_>) -> String {
    use std::fmt::Write;

    let scheme = if summary.grpc_tls_enabled {
        "https"
    } else {
        "http"
    };
    let mut out = String::from("AnyServe relay ready\n");
    match summary.grpc_addr {
        Some(addr) => {
            let _ = writeln!(out, "  gRPC:     {scheme}://{addr}");
        }
        None => {
            let _ = writeln!(out, "  gRPC:     disabled");
        }
    }
    match summary.console_addr {
        Some(addr) => {
            let ui = if summary.embedded_console_ui {
                " (embedded UI enabled)"
            } else {
                ""
            };
            let _ = writeln!(out, "  Console:  http://{addr}{ui}");
        }
        None => {
            let _ = writeln!(out, "  Console:  disabled");
        }
    }
    if let Some(public_endpoint) = summary.public_endpoint {
        let _ = writeln!(out, "  Public:   {public_endpoint}");
    }

    if let Some(auto) = summary.auto_tokens {
        let detail = match (&auto.persisted_path, auto.reused_existing) {
            (Some(path), true) => format!("auto-token loaded from {}", path.display()),
            (Some(path), false) => format!("auto-generated, saved to {}", path.display()),
            (None, _) => "auto-generated, ephemeral".to_string(),
        };
        let _ = writeln!(out, "  Auth:     enabled ({detail})");
        let _ = writeln!(out, "  Client token: {}", auto.client_token);
        let _ = writeln!(out, "  Worker token: {}", auto.worker_token);
    } else {
        let state = if summary.auth_enabled {
            "enabled"
        } else {
            "disabled (anonymous)"
        };
        let _ = writeln!(out, "  Auth:     {state}");
    }
    if let Some(login_url) = console_login_url(summary) {
        let _ = writeln!(out, "  Console login: {login_url}");
    }

    let endpoint = summary
        .public_endpoint
        .map(str::to_owned)
        .or_else(|| {
            summary
                .grpc_addr
                .map(|a| format!("{scheme}://{}", connectable_socket_addr(a)))
        })
        .or_else(|| {
            summary
                .console_addr
                .map(|a| format!("ws://{}", connectable_socket_addr(a)))
        });
    if let Some(endpoint) = endpoint {
        let token = match (summary.auto_tokens, summary.auth_enabled) {
            (Some(auto), _) => auto.client_token.as_str(),
            (None, true) => "<client-or-worker-token>",
            (None, false) => "\"\"",
        };
        let _ = writeln!(
            out,
            "  Quickstart:\n    export ANYSERVE_ENDPOINT={endpoint}\n    export ANYSERVE_TOKEN={token}"
        );
    }

    out.trim_end_matches('\n').to_owned()
}

fn emit_startup_ready(summary: &StartupSummary<'_>, json_logs: bool) {
    if json_logs {
        let grpc_str = summary.grpc_addr.map(|a| a.to_string());
        let console_str = summary.console_addr.map(|a| a.to_string());
        let auto_token_source =
            summary
                .auto_tokens
                .map(|auto| match (&auto.persisted_path, auto.reused_existing) {
                    (Some(_), true) => "reloaded",
                    (Some(_), false) => "persisted",
                    (None, _) => "ephemeral",
                });
        let auto_token_path = summary
            .auto_tokens
            .and_then(|auto| auto.persisted_path.as_ref())
            .map(|p| p.display().to_string());
        info!(
            grpc = grpc_str.as_deref(),
            grpc_tls = summary.grpc_tls_enabled,
            console = console_str.as_deref(),
            embedded_ui = summary.embedded_console_ui,
            public_endpoint = summary.public_endpoint,
            auth_enabled = summary.auth_enabled,
            auto_token_source,
            auto_token_path = auto_token_path.as_deref(),
            "anyserve relay ready"
        );
    } else {
        eprintln!("{}", format_startup_summary(summary));
    }
}

pub(crate) fn validate_listeners(config: &ResolvedServeConfig) -> Result<()> {
    if !config.grpc.enabled && config.console.is_none() {
        anyhow::bail!("cannot disable gRPC server when console server is also disabled");
    }
    Ok(())
}

async fn load_grpc_tls_config(config: &GrpcConfig) -> Result<Option<ServerTlsConfig>> {
    if !config.tls_enabled {
        return Ok(None);
    }
    let cert_file = config
        .cert_file
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--grpc-cert-file is required when TLS is enabled"))?;
    let key_file = config
        .key_file
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--grpc-key-file is required when TLS is enabled"))?;
    let identity = Identity::from_pem(
        tokio::fs::read(cert_file)
            .await
            .context("read tls certificate")?,
        tokio::fs::read(key_file)
            .await
            .context("read tls private key")?,
    );
    Ok(Some(ServerTlsConfig::new().identity(identity)))
}

async fn bind_listeners(
    grpc_addr: Option<SocketAddr>,
    console_addr: Option<SocketAddr>,
) -> Result<(
    Option<(tokio::net::TcpListener, SocketAddr)>,
    Option<(tokio::net::TcpListener, SocketAddr)>,
)> {
    let grpc = match grpc_addr {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("bind gRPC listener {addr}"))?;
            let bound = listener.local_addr().context("read bound gRPC address")?;
            Some((listener, bound))
        }
        None => None,
    };
    let console = match console_addr {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("bind console listener {addr}"))?;
            let bound = listener
                .local_addr()
                .context("read bound console address")?;
            Some((listener, bound))
        }
        None => None,
    };
    Ok((grpc, console))
}

pub(crate) async fn run(args: ServeArgs, json_logs: bool) -> Result<()> {
    let config = ResolvedServeConfig::load(ServeOverrides {
        config: args.config,
        grpc_host: args.grpc_host,
        grpc_port: args.grpc_port,
        grpc_tls_enabled: args.grpc_tls_enabled,
        grpc_cert_file: args.grpc_cert_file,
        grpc_key_file: args.grpc_key_file,
        no_grpc: args.no_grpc,
        ephemeral: args.ephemeral,
        journal_path: args.journal_path,
        journal_max_bytes: args.journal_max_bytes,
        memory_limit_mb: args.memory_limit_mb,
        console_listen: args.console_listen,
        public_endpoint: args.public_endpoint,
        client_tokens: args.client_tokens,
        worker_tokens: args.worker_tokens,
        auto_token: args.auto_token,
    })?;
    validate_listeners(&config)?;

    let grpc_addr = config
        .grpc
        .enabled
        .then(|| socket_addr(&config.grpc.host, config.grpc.port))
        .transpose()?;
    let console_addr = config
        .console
        .as_ref()
        .map(|console| socket_addr_from_bind(&console.listen))
        .transpose()?;
    let grpc_tls = if config.grpc.enabled {
        load_grpc_tls_config(&config.grpc).await?
    } else {
        None
    };

    let settings_path = config
        .runtime
        .journal
        .as_ref()
        .filter(|j| j.postgres.is_none() && j.cloudflare.is_none())
        .map(|j| {
            j.path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| std::path::Path::new("."))
                .join("runtime-settings.json")
        });

    let runtime = Runtime::start(config.runtime.clone())
        .await
        .context("start execution runtime")?;
    if let Some(ref path) = settings_path {
        match crate::serve_console::settings::SettingsStore::load_file(path) {
            Ok(Some(saved)) => match runtime.update_live_settings(saved) {
                Ok(_) => {
                    info!(path = %path.display(), "loaded persisted runtime settings overrides")
                }
                Err(error) => warn!(
                    path = %path.display(),
                    error = %error,
                    "ignoring invalid persisted runtime settings overrides"
                ),
            },
            Ok(None) => {}
            Err(error) => warn!(
                path = %path.display(),
                error = %error,
                "failed to read persisted runtime settings overrides; using startup config"
            ),
        }
    }
    let auth = Arc::new(config.auth.clone());
    if !auth.is_enabled() {
        warn!(
            "auth is disabled; any client can reach the control plane (set [auth] tokens before exposing it beyond a trusted network)"
        );
    }

    let (grpc_bound, console_bound) = match bind_listeners(grpc_addr, console_addr).await {
        Ok(bound) => bound,
        Err(err) => {
            runtime.shutdown().await;
            return Err(err);
        }
    };
    let grpc_bound_addr = grpc_bound.as_ref().map(|(_, addr)| *addr);
    let console_bound_addr = console_bound.as_ref().map(|(_, addr)| *addr);

    let tls = config.grpc.tls_enabled;
    let grpc_listen_display = match grpc_bound_addr {
        Some(bound) if config.grpc.port == 0 => format_host_port(&config.grpc.host, bound.port()),
        _ => format_host_port(&config.grpc.host, config.grpc.port),
    };
    let runtime_mode = config.runtime_mode();
    let config_path = config.config_path;
    let console_config = config.console;
    let public_endpoint = config.public_endpoint;
    let auth_revocation = config.auth_revocation;
    let auto_tokens = config.auto_tokens;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Run one bounded startup revocation sync before serving traffic to close
    // the restart window when the auth service is reachable.
    let mut revocation_sync_ok: Option<Arc<AtomicBool>> = None;
    if let Some(rev_cfg) = auth_revocation {
        let sync_ok = Arc::new(AtomicBool::new(false));
        revocation_sync_ok = Some(Arc::clone(&sync_ok));
        match build_revocation_http_client() {
            Ok(client) => {
                let (initial_failures, initial_error) = match tokio::time::timeout(
                    REVOCATION_STARTUP_TIMEOUT,
                    poll_revocations_once(&client, &rev_cfg, &auth, &runtime, &sync_ok),
                )
                .await
                {
                    Ok(Ok(())) => (0u32, None),
                    Ok(Err(error)) => {
                        warn!(
                            url = %rev_cfg.url,
                            error = %error,
                            "initial auth revocation sync failed; continuing with background polling"
                        );
                        (1u32, Some(error))
                    }
                    Err(_) => {
                        let msg = "initial auth revocation sync timed out".to_owned();
                        warn!(url = %rev_cfg.url, "{msg}; continuing with background polling");
                        (1u32, Some(msg))
                    }
                };
                tokio::spawn(run_revocation_poller(
                    client,
                    rev_cfg,
                    Arc::clone(&auth),
                    runtime.clone(),
                    sync_ok,
                    initial_failures,
                    initial_error,
                    shutdown_tx.subscribe(),
                ));
            }
            Err(error) => {
                warn!(%error, "build HTTP client for auth revocation poller");
            }
        }
    }

    let console_meta = ControlPlaneMetadata {
        grpc_enabled: config.grpc.enabled,
        grpc_listen: grpc_listen_display,
        grpc_tls_enabled: tls,
        public_endpoint: console_config
            .as_ref()
            .and_then(|c| c.public_endpoint.clone()),
        auth_portal_url: console_config
            .as_ref()
            .and_then(|c| c.auth_portal_url.clone()),
        revocation_sync_ok,
        settings_path,
    };

    let mut servers = tokio::task::JoinSet::new();
    if let Some((listener, _)) = grpc_bound {
        servers.spawn(run_grpc_server(
            listener,
            grpc_tls,
            runtime.clone(),
            AuthPolicy::clone(&auth),
            shutdown_rx,
        ));
    } else {
        drop(shutdown_rx);
    }
    if let (Some(adapter), Some((listener, _))) = (console_config, console_bound) {
        servers.spawn(run_console_server(
            listener,
            runtime.clone(),
            adapter,
            console_meta,
            runtime_mode,
            Arc::clone(&auth),
            shutdown_tx.subscribe(),
        ));
    }

    match (grpc_bound_addr, config_path.as_ref().map(|p| p.display())) {
        (Some(grpc), Some(cfg)) => info!(%grpc, tls, config = %cfg, "control plane started"),
        (Some(grpc), None) => info!(%grpc, tls, "control plane started"),
        (None, Some(cfg)) => info!(grpc_enabled = false, config = %cfg, "control plane started"),
        (None, None) => info!(grpc_enabled = false, "control plane started"),
    }
    if let Some(console) = console_bound_addr {
        info!(
            %console,
            embedded_ui = cfg!(feature = "embed-console"),
            "console api started"
        );
    }

    let summary = StartupSummary {
        grpc_addr: grpc_bound_addr,
        grpc_tls_enabled: tls,
        console_addr: console_bound_addr,
        embedded_console_ui: cfg!(feature = "embed-console"),
        public_endpoint: public_endpoint.as_deref(),
        auth_enabled: auth.is_enabled(),
        auto_tokens: auto_tokens.as_ref(),
    };
    emit_startup_ready(&summary, json_logs);

    let mut lease_lost = false;
    let failure = tokio::select! {
        signal = shutdown_signal() => { signal?; None }
        _ = runtime.wait_for_lease_loss() => {
            lease_lost = true;
            Some(anyhow::anyhow!("leadership lease lost; exiting so standby or supervisor can recover"))
        }
        ended = servers.join_next() => Some(match ended {
            Some(Ok(Err(error))) => error,
            Some(Err(error)) => error.into(),
            _ => anyhow::anyhow!("a configured server stopped unexpectedly"),
        }),
    };
    let _ = shutdown_tx.send(true);
    if lease_lost {
        // Sever active HTTP/2 connections immediately when fenced so clients and
        // workers reconnect to the new leader without waiting for drain grace.
        servers.abort_all();
        runtime.shutdown().await;
    } else {
        // Stop accepting new connections and let in-flight RPCs (including durable
        // completions) drain before closing the runtime journal.
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
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
        runtime.shutdown().await;
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

fn build_revocation_http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
}

fn next_revocation_delay(base: Duration, consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return base;
    }
    let max_delay = REVOCATION_MAX_BACKOFF.max(base);
    let factor = 1u32 << consecutive_failures.min(10);
    base.saturating_mul(factor).min(max_delay)
}

#[allow(clippy::too_many_arguments)]
async fn run_revocation_poller(
    client: reqwest::Client,
    cfg: AuthRevocationConfig,
    auth: Arc<AuthPolicy>,
    runtime: Runtime,
    sync_ok: Arc<AtomicBool>,
    mut consecutive_failures: u32,
    mut last_error: Option<String>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        let delay = next_revocation_delay(cfg.poll_interval, consecutive_failures);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
        }
        if *shutdown.borrow() {
            break;
        }
        match poll_revocations_once(&client, &cfg, &auth, &runtime, &sync_ok).await {
            Ok(()) => {
                consecutive_failures = 0;
                last_error = None;
            }
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let changed = last_error.as_deref() != Some(error.as_str());
                if consecutive_failures == 1 || consecutive_failures.is_multiple_of(10) || changed {
                    warn!(
                        url = %cfg.url,
                        consecutive_failures,
                        error = %error,
                        "auth revocation sync failed"
                    );
                }
                last_error = Some(error);
            }
        }
    }
}

async fn poll_revocations_once(
    client: &reqwest::Client,
    cfg: &AuthRevocationConfig,
    auth: &AuthPolicy,
    runtime: &Runtime,
    sync_ok: &AtomicBool,
) -> Result<(), String> {
    let since_version = auth.revocation_version();
    let mut bearers = vec![cfg.bearer_token.clone()];
    for candidate in auth.revocation_bearers() {
        if !bearers.iter().any(|existing| existing == &candidate) {
            bearers.push(candidate);
        }
    }

    let mut result = Err("no revocation bearer configured".to_owned());
    for (idx, bearer) in bearers.iter().enumerate() {
        match client
            .get(&cfg.url)
            .query(&[("since_version", since_version)])
            .bearer_auth(bearer)
            .send()
            .await
        {
            Ok(resp) if resp.status() == reqwest::StatusCode::NOT_MODIFIED => {
                auth.mark_revocation_synced();
                result = Ok(());
                break;
            }
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<anyserve_core::auth::RevocationSnapshot>().await {
                    Ok(snapshot) => {
                        auth.apply_revocations(snapshot);
                        result = Ok(());
                    }
                    Err(error) => {
                        result = Err(format!("decode auth revocation snapshot: {error}"));
                    }
                }
                break;
            }
            Ok(resp)
                if resp.status() == reqwest::StatusCode::UNAUTHORIZED
                    && idx + 1 < bearers.len() =>
            {
                continue;
            }
            Ok(resp) => {
                result = Err(format!("auth revocation endpoint status {}", resp.status()));
                break;
            }
            Err(error) => {
                result = Err(format!("poll auth revocation endpoint: {error}"));
                break;
            }
        }
    }

    sync_ok.store(result.is_ok(), Ordering::Relaxed);
    runtime.disconnect_revoked_and_expired_sessions(auth);
    result
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

async fn set_grpc_health(reporter: &mut tonic_health::server::HealthReporter, serving: bool) {
    let status = if serving {
        tonic_health::ServingStatus::Serving
    } else {
        tonic_health::ServingStatus::NotServing
    };
    reporter.set_service_status("", status).await;
    if serving {
        reporter
            .set_serving::<ClientServer<ClientGrpcService>>()
            .await;
        reporter
            .set_serving::<WorkerServer<WorkerGrpcService>>()
            .await;
    } else {
        reporter
            .set_not_serving::<ClientServer<ClientGrpcService>>()
            .await;
        reporter
            .set_not_serving::<WorkerServer<WorkerGrpcService>>()
            .await;
    }
}

async fn run_grpc_server(
    listener: tokio::net::TcpListener,
    tls_config: Option<ServerTlsConfig>,
    runtime: Runtime,
    auth: AuthPolicy,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let mut builder = Server::builder();
    if let Some(tls_cfg) = tls_config {
        builder = builder.tls_config(tls_cfg).context("configure grpc tls")?;
    }
    let incoming = TcpIncoming::from_listener(listener, true, None)
        .map_err(|error| anyhow::anyhow!("configure gRPC TCP listener: {error}"))?;

    let client_service = ClientServer::new(ClientGrpcService::new(runtime.clone()))
        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE);
    let worker_service = WorkerGrpcService::server(runtime.clone());
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    set_grpc_health(&mut health_reporter, runtime.is_leader_active()).await;

    let mut leader_rx = runtime.subscribe_leadership();
    let health_watch = tokio::spawn(async move {
        while leader_rx.changed().await.is_ok() {
            let serving = *leader_rx.borrow();
            set_grpc_health(&mut health_reporter, serving).await;
        }
    });

    builder
        .tcp_nodelay(true)
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .initial_connection_window_size(Some(32 * 1024 * 1024))
        .http2_keepalive_interval(Some(Duration::from_secs(20)))
        // A draining worker cancels its attempt streams at once; the
        // default rapid-reset guard (20) would drop the whole connection.
        .http2_max_pending_accept_reset_streams(Some(MAX_PENDING_RESET_STREAMS))
        .layer(AuthLayer::new(auth))
        .add_service(health_service)
        .add_service(client_service)
        .add_service(worker_service)
        .serve_with_incoming_shutdown(incoming, async move {
            let _ = shutdown.changed().await;
            health_watch.abort();
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

#[cfg(test)]
mod grpc_toggle_tests {
    use super::*;
    use crate::serve_config::ServeConsoleConfig;
    use crate::test_support::Server as TestServer;
    use anyserve_client::execution::{WorkerSession, v2};
    use anyserve_core::auth::{SignedTokenClaims, internal_revocation_bearer, sign_token};
    use anyserve_core::runtime::api::RuntimeConfig;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::get;
    use axum::{Json, Router};
    use clap::Parser;
    use std::sync::Mutex;

    #[derive(Parser)]
    struct TestServeCli {
        #[command(flatten)]
        serve: ServeArgs,
    }

    #[test]
    fn disabling_grpc_requires_console_server() {
        let mut cfg = ResolvedServeConfig::load(ServeOverrides::default()).unwrap();
        assert!(validate_listeners(&cfg).is_ok());

        cfg.grpc.enabled = false;
        cfg.console = None;
        assert!(validate_listeners(&cfg).is_err());

        cfg.console = Some(ServeConsoleConfig {
            listen: "127.0.0.1:3001".into(),
            allowed_origins: Vec::new(),
            public_endpoint: Some("wss://anyserve.example.com".into()),
            auth_portal_url: None,
        });
        assert!(validate_listeners(&cfg).is_ok());
    }

    #[test]
    fn no_grpc_override_requires_console_listener() {
        let without_console = ResolvedServeConfig::load_with_env(
            ServeOverrides {
                no_grpc: true,
                ..Default::default()
            },
            |_| None,
        )
        .unwrap();
        assert!(!without_console.grpc.enabled);
        assert!(validate_listeners(&without_console).is_err());

        let with_console = ResolvedServeConfig::load_with_env(
            ServeOverrides {
                no_grpc: true,
                console_listen: Some("127.0.0.1:3001".into()),
                ..Default::default()
            },
            |_| None,
        )
        .unwrap();
        assert!(!with_console.grpc.enabled);
        assert!(validate_listeners(&with_console).is_ok());
    }

    #[test]
    fn serve_args_parse_zero_config_flags() {
        let parsed = TestServeCli::try_parse_from([
            "serve",
            "--client-token",
            "tok-c1",
            "--client-token",
            "tok-c2,tok-c3",
            "--worker-token",
            "tok-w1,tok-w2",
            "--worker-token",
            "tok-w3",
            "--public-endpoint",
            "https://relay.example.com",
            "--no-grpc",
            "--auto-token",
            "--console-listen",
            "0.0.0.0:3001",
        ])
        .unwrap();
        assert_eq!(parsed.serve.client_tokens, vec!["tok-c1", "tok-c2,tok-c3"]);
        assert_eq!(parsed.serve.worker_tokens, vec!["tok-w1,tok-w2", "tok-w3"]);
        assert_eq!(
            parsed.serve.public_endpoint.as_deref(),
            Some("https://relay.example.com")
        );
        assert!(parsed.serve.no_grpc);
        assert!(parsed.serve.auto_token);
        assert_eq!(parsed.serve.console_listen.as_deref(), Some("0.0.0.0:3001"));
    }

    #[test]
    fn startup_summary_formats_anonymous_static_auth_and_auto_token_modes() {
        let render = |grpc: Option<&str>,
                      tls: bool,
                      console: Option<&str>,
                      ui: bool,
                      public: Option<&str>,
                      auth: bool,
                      auto: Option<&AutoTokens>| {
            format_startup_summary(&StartupSummary {
                grpc_addr: grpc.map(|s| s.parse().unwrap()),
                grpc_tls_enabled: tls,
                console_addr: console.map(|s| s.parse().unwrap()),
                embedded_console_ui: ui,
                public_endpoint: public,
                auth_enabled: auth,
                auto_tokens: auto,
            })
        };

        let anonymous = render(
            Some("127.0.0.1:50052"),
            false,
            None,
            false,
            None,
            false,
            None,
        );
        for needle in [
            "gRPC:     http://127.0.0.1:50052",
            "Console:  disabled",
            "Auth:     disabled (anonymous)",
            "export ANYSERVE_ENDPOINT=http://127.0.0.1:50052",
            "export ANYSERVE_TOKEN=\"\"",
        ] {
            assert!(anonymous.contains(needle), "{anonymous}");
        }
        assert!(!anonymous.contains("Public:"));

        let static_auth = render(
            Some("0.0.0.0:50052"),
            true,
            Some("0.0.0.0:3001"),
            true,
            Some("https://relay.example.com"),
            true,
            None,
        );
        for needle in [
            "gRPC:     https://0.0.0.0:50052",
            "Console:  http://0.0.0.0:3001 (embedded UI enabled)",
            "Public:   https://relay.example.com",
            "Auth:     enabled\n",
            "export ANYSERVE_ENDPOINT=https://relay.example.com",
            "export ANYSERVE_TOKEN=<client-or-worker-token>",
        ] {
            assert!(static_auth.contains(needle), "{static_auth}");
        }
        assert!(!static_auth.contains("Client token:"));
        assert!(!static_auth.contains("Worker token:"));

        let saved = AutoTokens {
            client_token: "as_c_0123456789abcdef0123456789abcdef".into(),
            worker_token: "as_w_fedcba9876543210fedcba9876543210".into(),
            persisted_path: Some(PathBuf::from(".anyserve/tokens.env")),
            reused_existing: false,
        };
        let auto_saved = render(
            None,
            false,
            Some("0.0.0.0:3001"),
            false,
            None,
            true,
            Some(&saved),
        );
        for needle in [
            "gRPC:     disabled",
            "Console:  http://0.0.0.0:3001\n",
            "Auth:     enabled (auto-generated, saved to .anyserve/tokens.env)",
            "Client token: as_c_0123456789abcdef0123456789abcdef",
            "Worker token: as_w_fedcba9876543210fedcba9876543210",
            "export ANYSERVE_ENDPOINT=ws://127.0.0.1:3001",
            "export ANYSERVE_TOKEN=as_c_0123456789abcdef0123456789abcdef",
        ] {
            assert!(auto_saved.contains(needle), "{auto_saved}");
        }
        assert!(!auto_saved.contains("(embedded UI enabled)"));

        let reloaded = AutoTokens {
            reused_existing: true,
            ..saved.clone()
        };
        let reloaded_banner = render(
            Some("0.0.0.0:50052"),
            false,
            None,
            false,
            None,
            true,
            Some(&reloaded),
        );
        assert!(
            reloaded_banner
                .contains("Auth:     enabled (auto-token loaded from .anyserve/tokens.env)")
        );
        assert!(reloaded_banner.contains("export ANYSERVE_ENDPOINT=http://127.0.0.1:50052"));

        let ephemeral = AutoTokens {
            persisted_path: None,
            reused_existing: false,
            ..saved
        };
        let ephemeral_banner = render(
            Some("127.0.0.1:50052"),
            false,
            None,
            false,
            None,
            true,
            Some(&ephemeral),
        );
        assert!(ephemeral_banner.contains("Auth:     enabled (auto-generated, ephemeral)"));
        assert!(!anonymous.contains("Console login:"));
        assert!(!static_auth.contains("Console login:"));
        assert!(!reloaded_banner.contains("Console login:"));
        assert!(!ephemeral_banner.contains("Console login:"));
        assert!(auto_saved.contains(
            "Console login: http://127.0.0.1:3001/#token=as_c_0123456789abcdef0123456789abcdef"
        ));
    }

    #[test]
    fn startup_summary_formats_console_login_url_variants_and_encodes_token() {
        let auto = AutoTokens {
            client_token: "as_c_a+b/c=d&e#f%g h".into(),
            worker_token: "as_w_secret".into(),
            persisted_path: None,
            reused_existing: false,
        };

        let ipv6_wildcard = format_startup_summary(&StartupSummary {
            grpc_addr: None,
            grpc_tls_enabled: false,
            console_addr: Some("[::]:3001".parse().unwrap()),
            embedded_console_ui: true,
            public_endpoint: None,
            auth_enabled: true,
            auto_tokens: Some(&auto),
        });
        assert!(
            ipv6_wildcard.contains(
                "Console login: http://[::1]:3001/#token=as_c_a%2Bb%2Fc%3Dd%26e%23f%25g%20h"
            ),
            "{ipv6_wildcard}"
        );

        let wss_public = format_startup_summary(&StartupSummary {
            grpc_addr: None,
            grpc_tls_enabled: false,
            console_addr: Some("127.0.0.1:3001".parse().unwrap()),
            embedded_console_ui: true,
            public_endpoint: Some("wss://demo.trycloudflare.com/"),
            auth_enabled: true,
            auto_tokens: Some(&auto),
        });
        assert!(
            wss_public.contains(
                "Console login: https://demo.trycloudflare.com/#token=as_c_a%2Bb%2Fc%3Dd%26e%23f%25g%20h"
            ),
            "{wss_public}"
        );

        let http_public = format_startup_summary(&StartupSummary {
            grpc_addr: Some("127.0.0.1:50052".parse().unwrap()),
            grpc_tls_enabled: false,
            console_addr: Some("0.0.0.0:3001".parse().unwrap()),
            embedded_console_ui: true,
            public_endpoint: Some("http://relay.local:8080/console/"),
            auth_enabled: true,
            auto_tokens: Some(&auto),
        });
        assert!(
            http_public.contains(
                "Console login: http://relay.local:8080/console/#token=as_c_a%2Bb%2Fc%3Dd%26e%23f%25g%20h"
            ),
            "{http_public}"
        );
    }

    #[test]
    fn startup_json_log_never_leaks_auto_tokens_or_login_url() {
        #[derive(Clone, Default)]
        struct SharedBuf(Arc<Mutex<Vec<u8>>>);

        impl std::io::Write for SharedBuf {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buf = SharedBuf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();

        let auto = AutoTokens {
            client_token: "as_c_supersecretclient+token".into(),
            worker_token: "as_w_supersecretworker+token".into(),
            persisted_path: Some(PathBuf::from(".anyserve/tokens.env")),
            reused_existing: false,
        };
        let summary = StartupSummary {
            grpc_addr: None,
            grpc_tls_enabled: false,
            console_addr: Some("0.0.0.0:3001".parse().unwrap()),
            embedded_console_ui: true,
            public_endpoint: Some("wss://demo.example.com"),
            auth_enabled: true,
            auto_tokens: Some(&auto),
        };

        tracing::subscriber::with_default(subscriber, || {
            emit_startup_ready(&summary, true);
        });

        let logged = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(logged.contains("anyserve relay ready"), "{logged}");
        assert!(logged.contains("auth_enabled=true"), "{logged}");
        assert!(
            logged.contains("auto_token_source=\"persisted\""),
            "{logged}"
        );
        assert!(
            logged.contains("public_endpoint=\"wss://demo.example.com\""),
            "{logged}"
        );
        for secret in [
            "as_c_supersecretclient+token",
            "as_c_supersecretclient%2Btoken",
            "as_w_supersecretworker+token",
            "#token=",
        ] {
            assert!(!logged.contains(secret), "leaked {secret} in {logged}");
        }
    }

    #[test]
    fn revocation_backoff_doubles_and_caps_at_30s() {
        let base = Duration::from_secs(2);
        assert_eq!(next_revocation_delay(base, 0), Duration::from_secs(2));
        assert_eq!(next_revocation_delay(base, 1), Duration::from_secs(4));
        assert_eq!(next_revocation_delay(base, 2), Duration::from_secs(8));
        assert_eq!(next_revocation_delay(base, 3), Duration::from_secs(16));
        assert_eq!(next_revocation_delay(base, 4), Duration::from_secs(30));
        assert_eq!(next_revocation_delay(base, 20), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn poll_revocations_once_handles_200_304_401_key_rotation_and_error_disconnects() {
        #[derive(Clone)]
        enum MockMode {
            Snapshot {
                expected_bearer: String,
                version: u64,
                jtis: Vec<String>,
            },
            NotModified {
                expected_bearer: String,
            },
            ServerError,
        }

        let mode = Arc::new(Mutex::new(MockMode::ServerError));
        let mode_route = Arc::clone(&mode);
        let app = Router::new().route(
            "/internal/revocations",
            get(move |headers: HeaderMap| {
                let mode = mode_route.lock().unwrap().clone();
                async move {
                    let bearer = headers
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.strip_prefix("Bearer "));
                    match mode {
                        MockMode::Snapshot {
                            expected_bearer,
                            version,
                            jtis,
                        } => {
                            if bearer != Some(expected_bearer.as_str()) {
                                return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({})));
                            }
                            (
                                StatusCode::OK,
                                Json(serde_json::json!({
                                    "version": version,
                                    "revoked_jtis": jtis,
                                })),
                            )
                        }
                        MockMode::NotModified { expected_bearer } => {
                            if bearer != Some(expected_bearer.as_str()) {
                                return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({})));
                            }
                            (StatusCode::NOT_MODIFIED, Json(serde_json::json!({})))
                        }
                        MockMode::ServerError => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({"error": "db down"})),
                        ),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let new_key = b"new-signing-key-0123456789abcdef".to_vec();
        let old_key = b"old-signing-key-0123456789abcdef".to_vec();
        let new_bearer = internal_revocation_bearer(&new_key);
        let old_bearer = internal_revocation_bearer(&old_key);

        let auth = AuthPolicy::default().with_signing_keys([new_key.clone(), old_key.clone()]);
        let cfg = AuthRevocationConfig {
            url: format!("http://{addr}/internal/revocations"),
            bearer_token: new_bearer.clone(),
            poll_interval: Duration::from_secs(1),
        };
        let client = build_revocation_http_client().unwrap();
        let grpc_server = TestServer::start(RuntimeConfig::default(), auth.clone()).await;
        let runtime = grpc_server.runtime.clone();
        let sync_ok = AtomicBool::new(false);

        let connect_signed_worker = |endpoint: String, token: String, worker_id: &'static str| async move {
            let mut ws = WorkerSession::connect(
                &endpoint,
                Some(&token),
                v2::Hello {
                    worker_id: worker_id.into(),
                    interfaces: vec!["demo.v1".into()],
                    max_concurrency: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let welcome = ws.messages.message().await.unwrap().unwrap().msg;
            assert!(matches!(welcome, Some(v2::server_message::Msg::Welcome(_))));
            ws
        };

        // Connect a worker backed by signed jti-revoked.
        let tok_rev = sign_token(
            &new_key,
            &SignedTokenClaims {
                jti: "jti-revoked".into(),
                ns: "team-a".into(),
                role: "worker".into(),
                sub: "w1".into(),
                iat: 1_700_000_000,
                exp: None,
                fp: None,
            },
        )
        .unwrap();
        let _w_rev = connect_signed_worker(grpc_server.endpoint.clone(), tok_rev, "w-rev").await;
        assert_eq!(runtime.list_workers().len(), 1);

        // 1. 200 OK applies revocation, sets sync_ok=true, and disconnects revoked session.
        *mode.lock().unwrap() = MockMode::Snapshot {
            expected_bearer: new_bearer.clone(),
            version: 1,
            jtis: vec!["jti-revoked".into()],
        };
        poll_revocations_once(&client, &cfg, &auth, &runtime, &sync_ok)
            .await
            .unwrap();
        assert!(sync_ok.load(Ordering::Relaxed));
        assert!(auth.is_jti_revoked("jti-revoked"));
        assert!(runtime.list_workers().is_empty());

        // 2. 304 Not Modified keeps sync_ok=true and updates last_sync.
        *mode.lock().unwrap() = MockMode::NotModified {
            expected_bearer: new_bearer,
        };
        poll_revocations_once(&client, &cfg, &auth, &runtime, &sync_ok)
            .await
            .unwrap();
        assert!(sync_ok.load(Ordering::Relaxed));
        assert!(auth.revocation_last_sync_at_ms().is_some());

        // 3. 401 on primary bearer falls back to secondary key during signing-key rotation.
        *mode.lock().unwrap() = MockMode::Snapshot {
            expected_bearer: old_bearer,
            version: 2,
            jtis: vec!["jti-revoked".into(), "jti-rotated".into()],
        };
        poll_revocations_once(&client, &cfg, &auth, &runtime, &sync_ok)
            .await
            .unwrap();
        assert!(sync_ok.load(Ordering::Relaxed));
        assert!(auth.is_jti_revoked("jti-rotated"));
        assert_eq!(auth.revocation_version(), 2);

        // 4. HTTP 500 sets sync_ok=false while STILL disconnecting expired sessions.
        let now_secs = anyserve_core::now_ms() / 1000;
        let tok_exp = sign_token(
            &new_key,
            &SignedTokenClaims {
                jti: "jti-exp".into(),
                ns: "team-a".into(),
                role: "worker".into(),
                sub: "w-exp".into(),
                iat: now_secs,
                exp: Some(now_secs + 1),
                fp: None,
            },
        )
        .unwrap();
        let _w_exp = connect_signed_worker(grpc_server.endpoint.clone(), tok_exp, "w-exp").await;
        assert_eq!(runtime.list_workers().len(), 1);
        tokio::time::sleep(Duration::from_millis(1200)).await;

        *mode.lock().unwrap() = MockMode::ServerError;
        let err = poll_revocations_once(&client, &cfg, &auth, &runtime, &sync_ok)
            .await
            .unwrap_err();
        assert!(err.contains("500"), "{err}");
        assert!(!sync_ok.load(Ordering::Relaxed));
        assert!(
            runtime.list_workers().is_empty(),
            "expired session must still be disconnected when HTTP poll fails"
        );

        grpc_server.stop().await;
        server_task.abort();
    }
}
