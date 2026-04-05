use std::sync::Arc;

use anyserve_client::AnyserveClient;
use tokio::sync::Mutex;

use crate::serve_openai_worker::config::{LeaseActivity, ResolvedServeOpenAIWorkerConfig};
use crate::serve_openai_worker::helpers::{available_capacity, connect_client};

pub(super) async fn heartbeat_loop(
    control_plane_endpoint: String,
    worker_id: String,
    config: ResolvedServeOpenAIWorkerConfig,
    activity: Arc<Mutex<LeaseActivity>>,
) {
    let mut interval = tokio::time::interval(config.heartbeat_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut client: Option<AnyserveClient> = None;

    loop {
        interval.tick().await;

        let snapshot = { activity.lock().await.clone() };
        if client.is_none() {
            client = match connect_client(&control_plane_endpoint).await {
                Ok(client) => Some(client),
                Err(error) => {
                    tracing::warn!(worker_id = %worker_id, error = %error, "llm worker heartbeat reconnect failed");
                    continue;
                }
            };
        }
        let Some(client_ref) = client.as_mut() else {
            continue;
        };

        if let Err(error) = client_ref
            .heartbeat_worker(
                worker_id.clone(),
                available_capacity(&config.capacity, snapshot.active_leases),
                snapshot.active_leases,
                config.metadata.clone(),
            )
            .await
        {
            tracing::warn!(worker_id = %worker_id, error = %error, "llm worker heartbeat failed");
            client = None;
            continue;
        }

        if let Some(lease_id) = snapshot.active_lease_id {
            if let Err(error) = client_ref
                .renew_lease(worker_id.clone(), lease_id.clone())
                .await
            {
                tracing::warn!(worker_id = %worker_id, lease_id = %lease_id, error = %error, "llm worker lease renew failed");
                client = None;
            }
        }
    }
}
