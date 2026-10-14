mod config;
mod helpers;
mod runtime;

use anyhow::Result;

pub use config::WorkerArgs;

pub async fn run(control_plane_endpoint: &str, args: WorkerArgs) -> Result<()> {
    runtime::run(control_plane_endpoint, args).await
}
