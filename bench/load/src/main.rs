use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

mod client;
mod payload;
mod profile;
mod submit;
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
    /// Persist a fixed number of verified echo results for management benchmarks.
    Seed {
        #[arg(long)]
        endpoint: String,
        #[arg(long, default_value_t = 4)]
        count: usize,
        #[arg(long, default_value_t = 16 * 1024 * 1024)]
        payload: usize,
        #[arg(long, value_enum, default_value = "repeat")]
        payload_pattern: payload::Pattern,
        #[arg(long, default_value_t = 0)]
        payload_seed: u64,
        #[arg(long)]
        out: PathBuf,
    },
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
    /// S6 total Submit attempts, including warm-up and failed calls.
    #[arg(long, default_value_t = 100_000, value_parser = clap::value_parser!(u32).range(1..))]
    max_jobs: u32,
    /// S6 shared deadline for checking results after all Submit calls finish.
    #[arg(long, default_value = "120s", value_parser = duration)]
    drain_timeout: Duration,
    #[arg(long, default_value_t = 256)]
    payload: usize,
    /// Input bytes: repeat x for historical comparisons, or deterministic high entropy.
    #[arg(long, value_enum, default_value = "repeat")]
    payload_pattern: payload::Pattern,
    /// SplitMix64 seed; ignored for repeat. Generated once before timing begins.
    #[arg(long, default_value_t = 0)]
    payload_seed: u64,
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
        Command::Seed {
            endpoint,
            count,
            payload,
            payload_pattern,
            payload_seed,
            out,
        } => {
            client::seed(
                &endpoint,
                count,
                payload,
                payload_pattern,
                payload_seed,
                &out,
            )
            .await
        }
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
