mod config;
mod helpers;
mod runtime;
#[cfg(test)]
mod tests;

use anyhow::Result;

pub use config::ServeOpenAIWorkerArgs;

pub async fn run(control_plane_endpoint: &str, args: ServeOpenAIWorkerArgs) -> Result<()> {
    runtime::run(control_plane_endpoint, args).await
}
