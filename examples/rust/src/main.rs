use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use anyserve_client::{
    AnyserveClient, EventKind, FrameKind, FrameWrite, JobState, JobSubmission, ObjectRef,
    StreamDirection, StreamOpen, WorkerRegistration, object_ref,
};
use clap::{Parser, ValueEnum};
use tokio::time::MissedTickBehavior;
use tokio_stream::StreamExt;
use tracing::info;

#[derive(Clone, Debug, ValueEnum)]
enum Mode {
    Submit,
    Worker,
}

#[derive(Parser, Debug)]
struct Cli {
    #[arg(long, value_enum, default_value = "worker")]
    mode: Mode,
    #[arg(long, default_value = "http://127.0.0.1:50052")]
    endpoint: String,
    #[arg(long)]
    job_id: Option<String>,
    #[arg(long)]
    worker_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct WorkerActivity {
    active_lease_id: Option<String>,
    active_leases: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();
    let cli = Cli::parse();

    match cli.mode {
        Mode::Submit => submit(&cli.endpoint, cli.job_id).await,
        Mode::Worker => worker(&cli.endpoint, cli.worker_id).await,
    }
}

async fn submit(endpoint: &str, job_id: Option<String>) -> Result<()> {
    let mut client = AnyserveClient::connect(endpoint.to_string()).await?;
    let mut request = JobSubmission::new("demo.echo.v1");
    request.job_id = job_id;
    request.required_attributes = HashMap::from([("runtime".to_string(), "demo".to_string())]);
    request.required_capacity = HashMap::from([("slot".to_string(), 1)]);
    request.metadata = HashMap::from([("example".to_string(), "true".to_string())]);
    request.lease_ttl_secs = 15;

    let job = client.submit_job(request).await?;
    info!(job_id = %job.job_id, "submitted job");

    let input_stream = client
        .open_stream(StreamOpen::job(
            job.job_id.clone(),
            "input.default",
            StreamDirection::ClientToWorker,
        ))
        .await?;

    client
        .push_frames(
            input_stream.stream_id.clone(),
            vec![
                FrameWrite::data(b"hello ".to_vec()),
                FrameWrite::data(b"from anyserve".to_vec()),
            ],
            None,
            None,
        )
        .await?;
    client
        .close_stream(input_stream.stream_id.clone(), None, None, HashMap::new())
        .await?;

    let mut watch = client.watch_job(job.job_id.clone(), 0).await?;
    while let Some(event) = watch.next().await {
        let event = event?;
        info!(
            job_id = %event.job_id,
            sequence = event.sequence,
            kind = event_kind_name(event.kind()),
            "job event"
        );
    }

    let streams = client.list_streams(job.job_id.clone()).await?;
    let output_stream = streams
        .into_iter()
        .find(|stream| stream.stream_name == "output.default")
        .context("demo worker did not create output.default")?;
    let mut frames = client
        .pull_frames(output_stream.stream_id, 0, false)
        .await?;
    while let Some(frame) = frames.next().await {
        let frame = frame?;
        if frame.kind() == FrameKind::Data {
            info!(
                payload = %String::from_utf8_lossy(&frame.payload),
                sequence = frame.sequence,
                "output frame"
            );
        }
    }

    let job = client.get_job(job.job_id.clone()).await?;
    if job.state() != JobState::Succeeded {
        bail!("demo job did not succeed: {}", job_state_name(job.state()));
    }
    info!(
        job_id = %job.job_id,
        state = job_state_name(job.state()),
        "final job state"
    );
    Ok(())
}

async fn worker(endpoint: &str, worker_id: Option<String>) -> Result<()> {
    let mut client = AnyserveClient::connect(endpoint.to_string()).await?;
    let mut request = WorkerRegistration::new(vec!["demo.echo.v1".to_string()]);
    request.worker_id = worker_id;
    request.attributes = HashMap::from([("runtime".to_string(), "demo".to_string())]);
    request.total_capacity = HashMap::from([("slot".to_string(), 1)]);

    let worker = client.register_worker(request).await?;
    info!(worker_id = %worker.worker_id, "registered worker");

    let activity = Arc::new(Mutex::new(WorkerActivity::default()));
    let heartbeat_endpoint = endpoint.to_string();
    let heartbeat_worker_id = worker.worker_id.clone();
    let heartbeat_activity = Arc::clone(&activity);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut heartbeat_client = AnyserveClient::connect(heartbeat_endpoint.clone())
            .await
            .ok();
        loop {
            interval.tick().await;
            let snapshot = {
                let state = heartbeat_activity
                    .lock()
                    .expect("worker activity mutex should not be poisoned");
                state.clone()
            };
            if heartbeat_client.is_none() {
                heartbeat_client = match AnyserveClient::connect(heartbeat_endpoint.clone()).await {
                    Ok(client) => Some(client),
                    Err(error) => {
                        info!(?error, "heartbeat reconnect failed");
                        continue;
                    }
                };
            }
            let client = heartbeat_client
                .as_mut()
                .expect("heartbeat client should exist after reconnect");
            if let Err(error) = client
                .heartbeat_worker(
                    heartbeat_worker_id.clone(),
                    capacity_for_active_leases(snapshot.active_leases),
                    snapshot.active_leases,
                    HashMap::new(),
                )
                .await
            {
                info!(?error, "heartbeat failed");
                heartbeat_client = None;
                continue;
            }
            let Some(lease_id) = snapshot.active_lease_id else {
                continue;
            };
            if let Err(error) = client
                .renew_lease(heartbeat_worker_id.clone(), lease_id)
                .await
            {
                info!(?error, "lease renew failed");
                heartbeat_client = None;
            }
        }
    });

    loop {
        let Some(grant) = client.poll_lease(worker.worker_id.clone()).await? else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let lease = grant.lease;
        let attempt = grant.attempt;
        let job = grant.job;
        info!(
            job_id = %job.job_id,
            attempt_id = %attempt.attempt_id,
            lease_id = %lease.lease_id,
            "received lease"
        );
        {
            let mut state = activity
                .lock()
                .expect("worker activity mutex should not be poisoned");
            state.active_lease_id = Some(lease.lease_id.clone());
            state.active_leases = 1;
        }

        let result = async {
            client
                .report_event(
                    worker.worker_id.clone(),
                    lease.lease_id.clone(),
                    EventKind::Started,
                    Vec::new(),
                    HashMap::new(),
                )
                .await?;

            let payload = read_input_payload(
                &mut client,
                &job.job_id,
                job.spec
                    .clone()
                    .and_then(|spec| spec.inputs.into_iter().next())
                    .and_then(|input| match input.reference {
                        Some(object_ref::Reference::Inline(content)) => Some(content),
                        _ => None,
                    }),
            )
            .await?;

            let output_stream = client
                .open_stream(
                    StreamOpen::job(
                        job.job_id.clone(),
                        "output.default",
                        StreamDirection::WorkerToClient,
                    )
                    .with_attempt_id(attempt.attempt_id.clone())
                    .with_worker_id(worker.worker_id.clone())
                    .with_lease_id(lease.lease_id.clone()),
                )
                .await?;

            client
                .push_frames(
                    output_stream.stream_id.clone(),
                    vec![
                        FrameWrite::data(payload.clone()).with_metadata(HashMap::from([(
                            "content_type".to_string(),
                            "text/plain".to_string(),
                        )])),
                    ],
                    Some(worker.worker_id.clone()),
                    Some(lease.lease_id.clone()),
                )
                .await?;
            client
                .close_stream(
                    output_stream.stream_id.clone(),
                    Some(worker.worker_id.clone()),
                    Some(lease.lease_id.clone()),
                    HashMap::new(),
                )
                .await?;

            client
                .report_event(
                    worker.worker_id.clone(),
                    lease.lease_id.clone(),
                    EventKind::Progress,
                    b"streamed output".to_vec(),
                    HashMap::new(),
                )
                .await?;
            client
                .report_event(
                    worker.worker_id.clone(),
                    lease.lease_id.clone(),
                    EventKind::OutputReady,
                    Vec::new(),
                    HashMap::from([("stream_name".to_string(), "output.default".to_string())]),
                )
                .await?;

            tokio::time::sleep(Duration::from_secs(1)).await;

            client
                .complete_lease(
                    worker.worker_id.clone(),
                    lease.lease_id,
                    vec![ObjectRef {
                        reference: Some(object_ref::Reference::Inline(payload)),
                        metadata: HashMap::from([("result".to_string(), "echo".to_string())]),
                    }],
                    HashMap::new(),
                )
                .await
        }
        .await;

        {
            let mut state = activity
                .lock()
                .expect("worker activity mutex should not be poisoned");
            state.active_lease_id = None;
            state.active_leases = 0;
        }

        result?;
    }
}

async fn read_input_payload(
    client: &mut AnyserveClient,
    job_id: &str,
    fallback: Option<Vec<u8>>,
) -> Result<Vec<u8>> {
    let streams = client.list_streams(job_id.to_string()).await?;

    if let Some(input_stream) = streams
        .into_iter()
        .find(|stream| stream.stream_name == "input.default")
    {
        let mut frames = client.pull_frames(input_stream.stream_id, 0, false).await?;
        let mut payload = Vec::new();
        while let Some(frame) = frames.next().await {
            let frame = frame?;
            if frame.kind() == FrameKind::Data {
                payload.extend(frame.payload);
            }
        }
        return Ok(payload);
    }

    Ok(fallback.unwrap_or_default())
}

fn event_kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Accepted => "accepted",
        EventKind::LeaseGranted => "lease_granted",
        EventKind::Started => "started",
        EventKind::Progress => "progress",
        EventKind::OutputReady => "output_ready",
        EventKind::Succeeded => "succeeded",
        EventKind::Failed => "failed",
        EventKind::Cancelled => "cancelled",
        EventKind::LeaseExpired => "lease_expired",
        EventKind::Requeued => "requeued",
        EventKind::Unspecified => "unspecified",
    }
}

fn job_state_name(state: JobState) -> &'static str {
    match state {
        JobState::Pending => "pending",
        JobState::Leased => "leased",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
        JobState::Unspecified => "unspecified",
    }
}

fn capacity_for_active_leases(active_leases: u32) -> HashMap<String, i64> {
    HashMap::from([("slot".to_string(), if active_leases == 0 { 1 } else { 0 })])
}
