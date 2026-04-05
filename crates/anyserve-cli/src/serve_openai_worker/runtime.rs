use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use anyserve_client::WorkerRegistration;
use tokio::sync::Mutex;

use super::config::{LeaseActivity, ServeOpenAIWorkerArgs, load_config};
use super::helpers::{build_http_client, connect_client};

mod execution;
mod heartbeat;

use execution::process_grant;
use heartbeat::heartbeat_loop;

pub(crate) async fn run(control_plane_endpoint: &str, args: ServeOpenAIWorkerArgs) -> Result<()> {
    let config = load_config(&args)?;
    let http_client = build_http_client(&config)?;
    let mut client = connect_client(control_plane_endpoint).await?;

    let mut registration = WorkerRegistration::new(config.interfaces.clone());
    registration.worker_id = config.worker_id.clone();
    registration.attributes = config.attributes.clone();
    registration.total_capacity = config.capacity.clone();
    registration.max_active_leases = config.max_active_leases;
    registration.metadata = config.metadata.clone();

    let worker = client.register_worker(registration).await?;
    let worker_id = worker.worker_id.clone();

    let activity = Arc::new(Mutex::new(LeaseActivity::default()));
    tokio::spawn(heartbeat_loop(
        control_plane_endpoint.to_string(),
        worker_id.clone(),
        config.clone(),
        activity.clone(),
    ));

    loop {
        let grant = match client.poll_lease(worker_id.clone()).await {
            Ok(grant) => grant,
            Err(error) => {
                tracing::warn!(worker_id = %worker_id, error = %error, "llm worker poll lease failed");
                tokio::time::sleep(config.poll_interval).await;
                client = match connect_client(control_plane_endpoint).await {
                    Ok(client) => client,
                    Err(error) => {
                        tracing::warn!(worker_id = %worker_id, error = %error, "llm worker reconnect failed");
                        continue;
                    }
                };
                continue;
            }
        };
        let Some(grant) = grant else {
            tokio::time::sleep(config.poll_interval).await;
            continue;
        };

        {
            let mut state = activity.lock().await;
            state.active_lease_id = Some(grant.lease.lease_id.clone());
            state.active_leases = 1;
        }

        let lease_id = grant.lease.lease_id.clone();
        let job_id = grant.job.job_id.clone();
        let result = process_grant(&mut client, &http_client, &config, &worker_id, grant.job).await;

        {
            let mut state = activity.lock().await;
            state.active_lease_id = None;
            state.active_leases = 0;
        }

        if let Err(error) = result {
            tracing::error!(worker_id = %worker_id, error = %error, "llm worker failed processing lease");
            if !lease_id.trim().is_empty() {
                let _ = client
                    .fail_lease(
                        worker_id.clone(),
                        lease_id,
                        format!("worker execution failed for job {job_id}: {error}"),
                        false,
                        HashMap::from([
                            ("provider".to_string(), config.provider.clone()),
                            ("worker.kind".to_string(), config.kind.clone()),
                        ]),
                    )
                    .await;
            }
        }
    }
}
