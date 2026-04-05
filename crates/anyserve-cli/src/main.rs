use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod job_commands;
mod serve_config;
mod serve_console;
mod serve_openai;
mod serve_openai_worker;
mod serve_runtime;

use job_commands::{JobCommands, run_job_command};
use serve_openai_worker::ServeOpenAIWorkerArgs;
use serve_runtime::{ServeArgs, run as run_serve};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:50052";

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
        Commands::Serve(args) => run_serve(args).await,
        Commands::Worker(args) => serve_openai_worker::run(&endpoint, args).await,
        Commands::Job { command } => run_job_command(&endpoint, command).await,
    }
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
