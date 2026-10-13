use anyserve_client::AnyserveClient;
use tokio::sync::watch;

use super::WorkerContext;
use crate::serve_openai_worker::helpers::{available_capacity, connect_client};

pub(super) async fn heartbeat_loop(context: WorkerContext) {
    let worker_id = context.worker_id.clone();
    let mut interval = tokio::time::interval(context.config.heartbeat_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut client: Option<AnyserveClient> = None;

    loop {
        interval.tick().await;

        let active_lease_ids = context.activity.lock().await.active_lease_ids.clone();
        let active_leases = active_lease_ids.len() as u32;
        if client.is_none() {
            client = match connect_client(&context.endpoint, &context.config).await {
                Ok(client) => Some(client),
                Err(error) => {
                    tracing::warn!(worker_id = %worker_id, error = %error, "worker heartbeat reconnect failed");
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
                available_capacity(&context.config.capacity, active_leases),
                active_leases,
                context.config.metadata.clone(),
            )
            .await
        {
            if format!("{error:#}").contains("does not exist") {
                // The control plane lost its state (e.g. restarted in memory mode).
                match client_ref
                    .register_worker(context.registration.clone())
                    .await
                {
                    Ok(_) => tracing::info!(worker_id = %worker_id, "worker re-registered"),
                    Err(error) => {
                        tracing::warn!(worker_id = %worker_id, error = %error, "worker re-register failed")
                    }
                }
            } else {
                tracing::warn!(worker_id = %worker_id, error = %error, "worker heartbeat failed");
                client = None;
            }
            continue;
        }

        for lease_id in active_lease_ids {
            if let Err(error) = client_ref
                .renew_lease(worker_id.clone(), lease_id.clone())
                .await
            {
                tracing::debug!(worker_id = %worker_id, lease_id = %lease_id, error = %error, "worker lease renew failed");
            }
        }
    }
}

/// Probes the local upstream and flips `healthy` so lease loops stop taking
/// work while the backend is down.
pub(super) async fn health_loop(context: WorkerContext, url: String, healthy: watch::Sender<bool>) {
    let mut interval = tokio::time::interval(context.config.health_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let ok = match context
            .http_client
            .get(&url)
            .timeout(context.config.health_interval)
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        };
        if *healthy.borrow() != ok {
            tracing::warn!(worker_id = %context.worker_id, healthy = ok, url = %url, "upstream health changed");
            let _ = healthy.send(ok);
        }
    }
}
