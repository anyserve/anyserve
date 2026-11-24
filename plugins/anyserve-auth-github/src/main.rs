mod config;
mod github;
mod server;
mod session;
mod store;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

use config::{Args, Config};
use server::{AppState, app};
use store::SqliteStore;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    let cfg = Config::from_args(Args::parse())?;
    let store = SqliteStore::open(&cfg.db_path).await?;
    let listener = TcpListener::bind(cfg.listen)
        .await
        .with_context(|| format!("bind {}", cfg.listen))?;

    info!(
        listen = %cfg.listen,
        public_url = %cfg.public_url,
        callback_url = %cfg.callback_url(),
        "anyserve-auth-github listening"
    );
    eprintln!(
        "AnyServe GitHub Auth Plugin ready\n  Listen:   http://{}\n  Portal:   {}/tokens\n  Callback: {}",
        cfg.listen,
        cfg.public_url,
        cfg.callback_url(),
    );

    let state = AppState::new(cfg, store)?;
    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve anyserve-auth-github")
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut sigterm) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
