use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
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
    if let Some(output_stream) = streams
        .into_iter()
        .find(|stream| stream.stream_name == "output.default")
    {
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
    }

    let job = client.get_job(job.job_id.clone()).await?;
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

    let heartbeat_endpoint = endpoint.to_string();
    let heartbeat_worker_id = worker.worker_id.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let mut client = match AnyserveClient::connect(heartbeat_endpoint.clone()).await {
                Ok(client) => client,
                Err(error) => {
                    info!(?error, "heartbeat reconnect failed");
                    continue;
                }
            };
            if let Err(error) = client
                .heartbeat_worker(
                    heartbeat_worker_id.clone(),
                    HashMap::from([("slot".to_string(), 1)]),
                    0,
                    HashMap::new(),
                )
                .await
            {
                info!(?error, "heartbeat failed");
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

        client
            .renew_lease(worker.worker_id.clone(), lease.lease_id.clone())
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
            .await?;
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
