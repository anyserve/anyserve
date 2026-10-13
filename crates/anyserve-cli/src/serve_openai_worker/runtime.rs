use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use anyserve_client::{AnyserveClient, LeaseGrant, WorkerRegistration};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinSet;

use super::config::{
    LeaseActivity, ResolvedServeOpenAIWorkerConfig, ServeOpenAIWorkerArgs, load_config,
};
use super::helpers::{build_http_client, connect_client};

mod execution;
mod heartbeat;

use execution::{ExecutionError, process_grant};
use heartbeat::{health_loop, heartbeat_loop};

#[derive(Clone)]
pub(super) struct WorkerContext {
    pub(super) endpoint: String,
    pub(super) worker_id: String,
    pub(super) config: Arc<ResolvedServeOpenAIWorkerConfig>,
    pub(super) http_client: reqwest::Client,
    pub(super) registration: WorkerRegistration,
    pub(super) activity: Arc<Mutex<LeaseActivity>>,
}

pub(crate) async fn run(control_plane_endpoint: &str, args: ServeOpenAIWorkerArgs) -> Result<()> {
    let config = Arc::new(load_config(&args)?);
    let http_client = build_http_client(&config)?;
    let mut client = connect_client(control_plane_endpoint, &config).await?;

    let mut registration = WorkerRegistration::new(config.interfaces.clone());
    registration.worker_id = config.worker_id.clone();
    registration.attributes = config.attributes.clone();
    registration.total_capacity = config.capacity.clone();
    registration.max_active_leases = config.max_active_leases;
    registration.metadata = config.metadata.clone();

    let worker = client.register_worker(registration.clone()).await?;
    let worker_id = worker.worker_id.clone();
    // Re-registration after a control-plane restart must keep the same id.
    registration.worker_id = Some(worker_id.clone());
    tracing::info!(
        worker_id = %worker_id,
        kind = %config.kind,
        concurrency = config.max_active_leases,
        long_poll_ms = config.long_poll.as_millis() as u64,
        "worker registered"
    );

    let context = WorkerContext {
        endpoint: control_plane_endpoint.to_string(),
        worker_id,
        config: Arc::clone(&config),
        http_client,
        registration,
        activity: Arc::new(Mutex::new(LeaseActivity::default())),
    };

    let (health_tx, health_rx) = watch::channel(true);
    if let Some(health_url) = config.health_url.clone() {
        tokio::spawn(health_loop(context.clone(), health_url, health_tx));
    }
    tokio::spawn(heartbeat_loop(context.clone()));

    let (stop_tx, stop_rx) = watch::channel(false);
    let mut lease_loops = JoinSet::new();
    for slot in 0..config.max_active_leases {
        lease_loops.spawn(lease_loop(
            context.clone(),
            client.clone(),
            health_rx.clone(),
            stop_rx.clone(),
            slot,
        ));
    }

    tokio::select! {
        _ = shutdown_signal() => {}
        Some(joined) = lease_loops.join_next() => {
            if let Err(error) = joined {
                tracing::error!(error = %error, "worker lease loop exited unexpectedly");
            }
        }
    }

    // Drain: stop taking leases, let in-flight jobs finish, then exit, so a
    // rolling restart or scale-down never strands a lease until its TTL.
    let in_flight = context.activity.lock().await.active_lease_ids.len();
    tracing::info!(worker_id = %context.worker_id, in_flight, "worker draining");
    let _ = stop_tx.send(true);
    let drained = tokio::time::timeout(DRAIN_TIMEOUT, async {
        while lease_loops.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::warn!(worker_id = %context.worker_id, "drain timed out; leases will expire and requeue");
    }
    Ok(())
}

const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

async fn lease_loop(
    context: WorkerContext,
    mut client: AnyserveClient,
    mut healthy: watch::Receiver<bool>,
    mut stop: watch::Receiver<bool>,
    slot: u32,
) {
    let config = &context.config;
    let worker_id = context.worker_id.clone();
    loop {
        if *stop.borrow() {
            return;
        }
        if !*healthy.borrow_and_update() {
            // Upstream is down: stop taking work so other clusters absorb it.
            let _ = healthy.changed().await;
            continue;
        }

        let poll = async {
            if config.long_poll.is_zero() {
                client.poll_lease(worker_id.clone()).await
            } else {
                client
                    .poll_lease_wait(worker_id.clone(), config.long_poll)
                    .await
            }
        };
        // Abandon an in-flight long-poll as soon as the upstream turns unhealthy,
        // otherwise a draining worker would keep accepting work for up to
        // `long_poll` after its backend died.
        let polled = tokio::select! {
            polled = poll => polled,
            _ = wait_unhealthy(&mut healthy) => continue,
            _ = stop.changed() => return,
        };
        let grant = match polled {
            Ok(Some(grant)) => grant,
            Ok(None) => {
                if config.long_poll.is_zero() {
                    tokio::time::sleep(config.poll_interval).await;
                }
                continue;
            }
            Err(error) => {
                tracing::warn!(worker_id = %worker_id, slot, error = %error, "worker poll lease failed");
                tokio::time::sleep(config.poll_interval).await;
                if let Ok(reconnected) = connect_client(&context.endpoint, config).await {
                    client = reconnected;
                }
                continue;
            }
        };

        if !*healthy.borrow() {
            // Lost the race with the health check: hand the job straight back.
            let _ = client
                .fail_lease(
                    worker_id.clone(),
                    grant.lease.lease_id.clone(),
                    format!("worker {worker_id} is draining: upstream unhealthy"),
                    true,
                    HashMap::new(),
                )
                .await;
            continue;
        }

        handle_grant(&context, &mut client, grant).await;
    }
}

async fn wait_unhealthy(healthy: &mut watch::Receiver<bool>) {
    while *healthy.borrow_and_update() {
        if healthy.changed().await.is_err() {
            // Health loop gone: never report unhealthy.
            std::future::pending::<()>().await;
        }
    }
}

async fn handle_grant(context: &WorkerContext, client: &mut AnyserveClient, grant: LeaseGrant) {
    let lease_id = grant.lease.lease_id.clone();
    let job_id = grant.job.job_id.clone();
    context
        .activity
        .lock()
        .await
        .active_lease_ids
        .insert(lease_id.clone());

    let result = process_grant(context, client, grant).await;

    context
        .activity
        .lock()
        .await
        .active_lease_ids
        .remove(&lease_id);

    if let Err(ExecutionError { message, retryable }) = result {
        tracing::warn!(
            worker_id = %context.worker_id,
            job_id = %job_id,
            retryable,
            error = %message,
            "worker failed lease"
        );
        let _ = client
            .fail_lease(
                context.worker_id.clone(),
                lease_id,
                format!(
                    "worker {} failed job {job_id}: {message}",
                    context.worker_id
                ),
                retryable,
                HashMap::from([
                    ("provider".to_string(), context.config.provider.clone()),
                    ("worker.kind".to_string(), context.config.kind.clone()),
                ]),
            )
            .await;
    }
}
