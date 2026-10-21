use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

mod client;
mod profile;
mod worker;

type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser)]
#[command(about = "Closed-loop control-plane benchmark and mock services")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Worker(WorkerArgs),
    Client(ClientArgs),
    /// Profile the library runtime; normal baselines use the production binary.
    Serve {
        #[arg(long, default_value = "127.0.0.1:50052")]
        listen: std::net::SocketAddr,
        #[arg(long)]
        journal: PathBuf,
        #[arg(long)]
        profile: PathBuf,
    },
}

#[derive(Args)]
struct WorkerArgs {
    #[arg(long)]
    endpoint: String,
    #[arg(long, default_value = "bench.echo.v1")]
    interface: String,
    #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u32).range(1..))]
    concurrency: u32,
    #[arg(long, default_value = "echo", value_parser = worker::parse_mode)]
    mode: worker::Mode,
}

#[derive(Args)]
struct ClientArgs {
    #[arg(long)]
    endpoint: String,
    #[arg(long, value_parser = client::parse_scenario)]
    scenario: u8,
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    concurrency: u32,
    /// Independent gRPC channels (default min(C, 8)).
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    connections: Option<u32>,
    #[arg(long, default_value = "20s", value_parser = duration)]
    duration: Duration,
    #[arg(long, default_value = "5s", value_parser = duration)]
    warmup: Duration,
    #[arg(long, default_value_t = 256)]
    payload: usize,
    #[arg(long)]
    out: PathBuf,
}

fn duration(value: &str) -> std::result::Result<Duration, String> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 0.001)
    } else {
        (value.strip_suffix('s').unwrap_or(value), 1.0)
    };
    let seconds = number.parse::<f64>().map_err(|error| error.to_string())? * multiplier;
    Duration::try_from_secs_f64(seconds).map_err(|error| error.to_string())
}

#[tokio::main]
async fn main() -> Result {
    match Cli::parse().command {
        Command::Worker(args) => worker::run(args).await,
        Command::Client(args) => client::run(args).await,
        Command::Serve {
            listen,
            journal,
            profile,
        } => profile::serve(listen, journal, profile).await,
    }
}

async fn shutdown_signal() -> Result {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = terminate.recv() => {},
    }
    Ok(())
}
