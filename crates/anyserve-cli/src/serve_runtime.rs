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

use crate::serve_config::{AutoTokens, GrpcConfig, ResolvedServeConfig, ServeOverrides};
use crate::serve_console::{
    ControlPlaneMetadata, format_host_port, run_server as run_console_server,
};

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
    /// Disable the gRPC listener (requires --console-listen or [console] in config).
    #[arg(long = "no-grpc")]
    no_grpc: bool,
    /// Disable durable jobs and keep the runtime entirely in memory.
    #[arg(long, conflicts_with = "journal_path")]
    ephemeral: bool,
    /// SQLite recovery journal (default: .anyserve/journal.sqlite).
    #[arg(long = "journal", value_name = "PATH")]
    journal_path: Option<PathBuf>,
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

pub(crate) fn format_startup_summary(summary: &StartupSummary<'_>) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let _ = writeln!(out, "AnyServe relay ready");

    match summary.grpc_addr {
        Some(addr) => {
            let scheme = if summary.grpc_tls_enabled {
                "https"
            } else {
                "http"
            };
            let _ = writeln!(out, "  gRPC:     {scheme}://{addr}");
        }
        None => {
            let _ = writeln!(out, "  gRPC:     disabled");
        }
    }

    match summary.console_addr {
        Some(addr) => {
            let suffix = if summary.embedded_console_ui {
                " (embedded UI enabled)"
            } else {
                ""
            };
            let _ = writeln!(out, "  Console:  http://{addr}{suffix}");
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
    } else if summary.auth_enabled {
        let _ = writeln!(out, "  Auth:     enabled");
    } else {
        let _ = writeln!(out, "  Auth:     disabled (anonymous)");
    }

    let endpoint = summary
        .public_endpoint
        .map(str::to_owned)
        .or_else(|| {
            summary.grpc_addr.map(|addr| {
                let scheme = if summary.grpc_tls_enabled {
                    "https"
                } else {
                    "http"
                };
                format!("{scheme}://{}", connectable_socket_addr(addr))
            })
        })
        .or_else(|| {
            summary
                .console_addr
                .map(|addr| format!("ws://{}", connectable_socket_addr(addr)))
        });

    if let Some(endpoint) = endpoint {
        let _ = writeln!(out, "  Quickstart:");
        let _ = writeln!(out, "    export ANYSERVE_ENDPOINT={endpoint}");
        if let Some(auto) = summary.auto_tokens {
            let _ = writeln!(out, "    export ANYSERVE_TOKEN={}", auto.client_token);
        } else if summary.auth_enabled {
            let _ = writeln!(out, "    export ANYSERVE_TOKEN=<client-or-worker-token>");
        } else {
            let _ = writeln!(out, "    export ANYSERVE_TOKEN=\"\"");
        }
    }

    out.trim_end_matches('\n').to_owned()
}

pub(crate) fn validate_listeners(config: &ResolvedServeConfig) -> Result<()> {
    if !config.grpc.enabled && config.console.is_none() {
        anyhow::bail!("cannot disable gRPC server when console server is also disabled");
    }
    Ok(())
}

pub(crate) async fn run(args: ServeArgs) -> Result<()> {
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
        console_listen: args.console_listen,
        public_endpoint: args.public_endpoint,
        client_tokens: args.client_tokens,
        worker_tokens: args.worker_tokens,
        auto_token: args.auto_token,
    })?;
    validate_listeners(&config)?;
    let runtime = Runtime::start(config.runtime.clone())
        .await
        .context("start execution runtime")?;
    let auth = Arc::new(config.auth.clone());
    if !auth.is_enabled() {
        warn!(
            "auth is disabled; any client can reach the control plane (set [auth] tokens before exposing it beyond a trusted network)"
        );
    }

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

    let tls = config.grpc.tls_enabled;
    let console_meta = ControlPlaneMetadata {
        grpc_listen: format_host_port(&config.grpc.host, config.grpc.port),
        grpc_tls_enabled: tls,
        public_endpoint: config
            .console
            .as_ref()
            .and_then(|c| c.public_endpoint.clone()),
    };
    let runtime_mode = config.runtime_mode();
    let config_path = config.config_path;
    let console_config = config.console;
    let public_endpoint = config.public_endpoint;
    let auto_tokens = config.auto_tokens;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut servers = tokio::task::JoinSet::new();
    if let Some(grpc_addr) = grpc_addr {
        servers.spawn(run_grpc_server(
            grpc_addr,
            runtime.clone(),
            config.grpc,
            AuthPolicy::clone(&auth),
            shutdown_rx,
        ));
    } else {
        drop(shutdown_rx);
    }
    if let (Some(adapter), Some(addr)) = (console_config, console_addr) {
        servers.spawn(run_console_server(
            addr,
            runtime.clone(),
            adapter,
            console_meta,
            runtime_mode,
            Arc::clone(&auth),
            shutdown_tx.subscribe(),
        ));
    }

    if let Some(grpc_addr) = grpc_addr {
        if let Some(config_path) = config_path.as_ref() {
            let config_path = config_path.display().to_string();
            info!(grpc = %grpc_addr, tls, config = %config_path, "control plane started");
        } else {
            info!(grpc = %grpc_addr, tls, "control plane started");
        }
    } else if let Some(config_path) = config_path.as_ref() {
        let config_path = config_path.display().to_string();
        info!(grpc_enabled = false, config = %config_path, "control plane started");
    } else {
        info!(grpc_enabled = false, "control plane started");
    }
    if let Some(console_addr) = console_addr {
        info!(
            console = %console_addr,
            embedded_ui = cfg!(feature = "embed-console"),
            "console api started"
        );
    }

    eprintln!(
        "{}",
        format_startup_summary(&StartupSummary {
            grpc_addr,
            grpc_tls_enabled: tls,
            console_addr,
            embedded_console_ui: cfg!(feature = "embed-console"),
            public_endpoint: public_endpoint.as_deref(),
            auth_enabled: auth.is_enabled(),
            auto_tokens: auto_tokens.as_ref(),
        })
    );

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
    }
    runtime.shutdown().await;
    if !lease_lost {
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
    let worker_service = WorkerGrpcService::server(runtime.clone());
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    if runtime.is_leader_active() {
        health_reporter
            .set_serving::<ClientServer<ClientGrpcService>>()
            .await;
        health_reporter
            .set_serving::<WorkerServer<WorkerGrpcService>>()
            .await;
    } else {
        health_reporter
            .set_not_serving::<ClientServer<ClientGrpcService>>()
            .await;
        health_reporter
            .set_not_serving::<WorkerServer<WorkerGrpcService>>()
            .await;
    }
    let mut leader_rx = runtime.subscribe_leadership();
    let health_watch = tokio::spawn(async move {
        while leader_rx.changed().await.is_ok() {
            if *leader_rx.borrow() {
                health_reporter
                    .set_serving::<ClientServer<ClientGrpcService>>()
                    .await;
                health_reporter
                    .set_serving::<WorkerServer<WorkerGrpcService>>()
                    .await;
            } else {
                health_reporter
                    .set_not_serving::<ClientServer<ClientGrpcService>>()
                    .await;
                health_reporter
                    .set_not_serving::<WorkerServer<WorkerGrpcService>>()
                    .await;
            }
        }
    });

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
    use clap::Parser;

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
        // 1. Default anonymous mode (gRPC only, console disabled).
        let anonymous = format_startup_summary(&StartupSummary {
            grpc_addr: Some("127.0.0.1:50052".parse().unwrap()),
            grpc_tls_enabled: false,
            console_addr: None,
            embedded_console_ui: false,
            public_endpoint: None,
            auth_enabled: false,
            auto_tokens: None,
        });
        assert!(anonymous.contains("gRPC:     http://127.0.0.1:50052"));
        assert!(anonymous.contains("Console:  disabled"));
        assert!(!anonymous.contains("Public:"));
        assert!(anonymous.contains("Auth:     disabled (anonymous)"));
        assert!(anonymous.contains("export ANYSERVE_ENDPOINT=http://127.0.0.1:50052"));
        assert!(anonymous.contains("export ANYSERVE_TOKEN=\"\""));

        // 2. Static user-configured auth + TLS + embedded UI + public endpoint (never prints static tokens).
        let static_auth = format_startup_summary(&StartupSummary {
            grpc_addr: Some("0.0.0.0:50052".parse().unwrap()),
            grpc_tls_enabled: true,
            console_addr: Some("0.0.0.0:3001".parse().unwrap()),
            embedded_console_ui: true,
            public_endpoint: Some("https://relay.example.com"),
            auth_enabled: true,
            auto_tokens: None,
        });
        assert!(static_auth.contains("gRPC:     https://0.0.0.0:50052"));
        assert!(static_auth.contains("Console:  http://0.0.0.0:3001 (embedded UI enabled)"));
        assert!(static_auth.contains("Public:   https://relay.example.com"));
        assert!(static_auth.contains("Auth:     enabled\n"));
        assert!(!static_auth.contains("Client token:"));
        assert!(!static_auth.contains("Worker token:"));
        assert!(static_auth.contains("export ANYSERVE_ENDPOINT=https://relay.example.com"));
        assert!(static_auth.contains("export ANYSERVE_TOKEN=<client-or-worker-token>"));

        // 3. --no-grpc + --auto-token persisted (newly generated).
        let saved_auto = AutoTokens {
            client_token: "as_c_0123456789abcdef0123456789abcdef".into(),
            worker_token: "as_w_fedcba9876543210fedcba9876543210".into(),
            persisted_path: Some(PathBuf::from(".anyserve/tokens.env")),
            reused_existing: false,
        };
        let auto_saved_banner = format_startup_summary(&StartupSummary {
            grpc_addr: None,
            grpc_tls_enabled: false,
            console_addr: Some("0.0.0.0:3001".parse().unwrap()),
            embedded_console_ui: false,
            public_endpoint: None,
            auth_enabled: true,
            auto_tokens: Some(&saved_auto),
        });
        assert!(auto_saved_banner.contains("gRPC:     disabled"));
        assert!(auto_saved_banner.contains("Console:  http://0.0.0.0:3001\n"));
        assert!(!auto_saved_banner.contains("(embedded UI enabled)"));
        assert!(
            auto_saved_banner
                .contains("Auth:     enabled (auto-generated, saved to .anyserve/tokens.env)")
        );
        assert!(auto_saved_banner.contains("Client token: as_c_0123456789abcdef0123456789abcdef"));
        assert!(auto_saved_banner.contains("Worker token: as_w_fedcba9876543210fedcba9876543210"));
        assert!(auto_saved_banner.contains("export ANYSERVE_ENDPOINT=ws://127.0.0.1:3001"));
        assert!(
            auto_saved_banner
                .contains("export ANYSERVE_TOKEN=as_c_0123456789abcdef0123456789abcdef")
        );

        // 4. --auto-token reloaded from disk vs ephemeral.
        let reloaded_auto = AutoTokens {
            reused_existing: true,
            ..saved_auto.clone()
        };
        let reloaded_banner = format_startup_summary(&StartupSummary {
            grpc_addr: Some("0.0.0.0:50052".parse().unwrap()),
            grpc_tls_enabled: false,
            console_addr: None,
            embedded_console_ui: false,
            public_endpoint: None,
            auth_enabled: true,
            auto_tokens: Some(&reloaded_auto),
        });
        assert!(
            reloaded_banner
                .contains("Auth:     enabled (auto-token loaded from .anyserve/tokens.env)")
        );
        assert!(reloaded_banner.contains("export ANYSERVE_ENDPOINT=http://127.0.0.1:50052"));

        let ephemeral_auto = AutoTokens {
            persisted_path: None,
            reused_existing: false,
            ..saved_auto
        };
        let ephemeral_banner = format_startup_summary(&StartupSummary {
            grpc_addr: Some("127.0.0.1:50052".parse().unwrap()),
            grpc_tls_enabled: false,
            console_addr: None,
            embedded_console_ui: false,
            public_endpoint: None,
            auth_enabled: true,
            auto_tokens: Some(&ephemeral_auto),
        });
        assert!(ephemeral_banner.contains("Auth:     enabled (auto-generated, ephemeral)"));
    }
}
