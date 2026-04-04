use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use anyserve_proto::controlplane::control_plane_service_client::ControlPlaneServiceClient;
use anyserve_proto::controlplane::{
    CloseStreamRequest, CompleteLeaseRequest, Demand, EventKind, ExecutionPolicy, FrameKind,
    GetJobRequest, HeartbeatWorkerRequest, JobSpec, ListStreamsRequest, ObjectRef,
    OpenStreamRequest, PollLeaseRequest, PullFramesRequest, PushFramesRequest,
    RegisterWorkerRequest, RenewLeaseRequest, ReportEventRequest, ResourceQuantity,
    StreamDirection, StreamScope, SubmitJobRequest, WatchJobRequest, WorkerSpec, object_ref,
};
use clap::{Parser, ValueEnum};
use tokio::time::MissedTickBehavior;
use tokio_stream::StreamExt;
use tonic::Request;
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
    let mut client = ControlPlaneServiceClient::connect(endpoint.to_string()).await?;
    let submit = client
        .submit_job(Request::new(SubmitJobRequest {
            job_id: job_id.unwrap_or_default(),
            spec: Some(JobSpec {
                interface_name: "demo.echo.v1".to_string(),
                inputs: Vec::new(),
                params: Vec::new(),
                demand: Some(Demand {
                    required_attributes: HashMap::from([(
                        "runtime".to_string(),
                        "demo".to_string(),
                    )]),
                    preferred_attributes: HashMap::new(),
                    required_capacity: vec![ResourceQuantity {
                        name: "slot".to_string(),
                        value: 1,
                    }],
                }),
                policy: Some(ExecutionPolicy {
                    profile: "basic".to_string(),
                    priority: 0,
                    lease_ttl_secs: 15,
                }),
                metadata: HashMap::from([("example".to_string(), "true".to_string())]),
            }),
        }))
        .await?
        .into_inner();

    let job = submit.job.context("missing job in submit response")?;
    info!(job_id = %job.job_id, "submitted job");

    let input_stream = client
        .open_stream(Request::new(OpenStreamRequest {
            job_id: job.job_id.clone(),
            attempt_id: String::new(),
            worker_id: String::new(),
            lease_id: String::new(),
            stream_name: "input.default".to_string(),
            scope: StreamScope::Job as i32,
            direction: StreamDirection::ClientToWorker as i32,
            metadata: HashMap::new(),
        }))
        .await?
        .into_inner()
        .stream
        .context("missing input stream in open response")?;

    let input_frames = vec![
        PushFramesRequest {
            stream_id: input_stream.stream_id.clone(),
            worker_id: String::new(),
            lease_id: String::new(),
            kind: FrameKind::Data as i32,
            payload: b"hello ".to_vec(),
            metadata: HashMap::new(),
        },
        PushFramesRequest {
            stream_id: input_stream.stream_id.clone(),
            worker_id: String::new(),
            lease_id: String::new(),
            kind: FrameKind::Data as i32,
            payload: b"from anyserve".to_vec(),
            metadata: HashMap::new(),
        },
    ];
    client
        .push_frames(Request::new(tokio_stream::iter(input_frames)))
        .await?;
    client
        .close_stream(Request::new(CloseStreamRequest {
            stream_id: input_stream.stream_id.clone(),
            worker_id: String::new(),
            lease_id: String::new(),
            metadata: HashMap::new(),
        }))
        .await?;

    let mut watch = client
        .watch_job(Request::new(WatchJobRequest {
            job_id: job.job_id.clone(),
            after_sequence: 0,
        }))
        .await?
        .into_inner();

    while let Some(event) = watch.next().await {
        let event = event?;
        info!(
            job_id = %event.job_id,
            sequence = event.sequence,
            kind = event_kind_name(event.kind()),
            "job event"
        );
    }

    let streams = client
        .list_streams(Request::new(ListStreamsRequest {
            job_id: job.job_id.clone(),
        }))
        .await?
        .into_inner()
        .streams;
    if let Some(output_stream) = streams
        .into_iter()
        .find(|stream| stream.stream_name == "output.default")
    {
        let mut frames = client
            .pull_frames(Request::new(PullFramesRequest {
                stream_id: output_stream.stream_id,
                after_sequence: 0,
                follow: false,
            }))
            .await?
            .into_inner();
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

    let job = client
        .get_job(Request::new(GetJobRequest {
            job_id: job.job_id.clone(),
        }))
        .await?
        .into_inner();
    info!(
        job_id = %job.job_id,
        state = job_state_name(job.state()),
        "final job state"
    );
    Ok(())
}

async fn worker(endpoint: &str, worker_id: Option<String>) -> Result<()> {
    let mut client = ControlPlaneServiceClient::connect(endpoint.to_string()).await?;
    let registered = client
        .register_worker(Request::new(RegisterWorkerRequest {
            worker_id: worker_id.unwrap_or_default(),
            spec: Some(WorkerSpec {
                interfaces: vec!["demo.echo.v1".to_string()],
                attributes: HashMap::from([("runtime".to_string(), "demo".to_string())]),
                total_capacity: vec![ResourceQuantity {
                    name: "slot".to_string(),
                    value: 1,
                }],
                max_active_leases: 1,
                metadata: HashMap::new(),
            }),
        }))
        .await?
        .into_inner();

    let worker = registered
        .worker
        .context("missing worker in register response")?;
    info!(worker_id = %worker.worker_id, "registered worker");

    let heartbeat_endpoint = endpoint.to_string();
    let heartbeat_worker_id = worker.worker_id.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let mut client =
                match ControlPlaneServiceClient::connect(heartbeat_endpoint.clone()).await {
                    Ok(client) => client,
                    Err(error) => {
                        info!(?error, "heartbeat reconnect failed");
                        continue;
                    }
                };
            if let Err(error) = client
                .heartbeat_worker(Request::new(HeartbeatWorkerRequest {
                    worker_id: heartbeat_worker_id.clone(),
                    available_capacity: vec![ResourceQuantity {
                        name: "slot".to_string(),
                        value: 1,
                    }],
                    active_leases: 0,
                    metadata: HashMap::new(),
                }))
                .await
            {
                info!(?error, "heartbeat failed");
            }
        }
    });

    loop {
        let mut client = ControlPlaneServiceClient::connect(endpoint.to_string()).await?;
        let response = client
            .poll_lease(Request::new(PollLeaseRequest {
                worker_id: worker.worker_id.clone(),
            }))
            .await?
            .into_inner();

        let Some(lease) = response.lease else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let attempt = response
            .attempt
            .context("missing attempt for granted lease")?;
        let job = response.job.context("missing job for granted lease")?;
        info!(
            job_id = %job.job_id,
            attempt_id = %attempt.attempt_id,
            lease_id = %lease.lease_id,
            "received lease"
        );

        client
            .report_event(Request::new(ReportEventRequest {
                worker_id: worker.worker_id.clone(),
                lease_id: lease.lease_id.clone(),
                kind: EventKind::Started as i32,
                payload: Vec::new(),
                metadata: HashMap::new(),
            }))
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
            .open_stream(Request::new(OpenStreamRequest {
                job_id: job.job_id.clone(),
                attempt_id: attempt.attempt_id.clone(),
                worker_id: worker.worker_id.clone(),
                lease_id: lease.lease_id.clone(),
                stream_name: "output.default".to_string(),
                scope: StreamScope::Job as i32,
                direction: StreamDirection::WorkerToClient as i32,
                metadata: HashMap::new(),
            }))
            .await?
            .into_inner()
            .stream
            .context("missing output stream in open response")?;

        client
            .push_frames(Request::new(tokio_stream::iter(vec![PushFramesRequest {
                stream_id: output_stream.stream_id.clone(),
                worker_id: worker.worker_id.clone(),
                lease_id: lease.lease_id.clone(),
                kind: FrameKind::Data as i32,
                payload: payload.clone(),
                metadata: HashMap::from([("content_type".to_string(), "text/plain".to_string())]),
            }])))
            .await?;
        client
            .close_stream(Request::new(CloseStreamRequest {
                stream_id: output_stream.stream_id.clone(),
                worker_id: worker.worker_id.clone(),
                lease_id: lease.lease_id.clone(),
                metadata: HashMap::new(),
            }))
            .await?;

        client
            .report_event(Request::new(ReportEventRequest {
                worker_id: worker.worker_id.clone(),
                lease_id: lease.lease_id.clone(),
                kind: EventKind::Progress as i32,
                payload: b"streamed output".to_vec(),
                metadata: HashMap::new(),
            }))
            .await?;
        client
            .report_event(Request::new(ReportEventRequest {
                worker_id: worker.worker_id.clone(),
                lease_id: lease.lease_id.clone(),
                kind: EventKind::OutputReady as i32,
                payload: Vec::new(),
                metadata: HashMap::from([(
                    "stream_name".to_string(),
                    "output.default".to_string(),
                )]),
            }))
            .await?;

        client
            .renew_lease(Request::new(RenewLeaseRequest {
                worker_id: worker.worker_id.clone(),
                lease_id: lease.lease_id.clone(),
            }))
            .await?;

        tokio::time::sleep(Duration::from_secs(1)).await;

        client
            .complete_lease(Request::new(CompleteLeaseRequest {
                worker_id: worker.worker_id.clone(),
                lease_id: lease.lease_id,
                outputs: vec![ObjectRef {
                    reference: Some(object_ref::Reference::Inline(payload)),
                    metadata: HashMap::from([("result".to_string(), "echo".to_string())]),
                }],
                metadata: HashMap::new(),
            }))
            .await?;
    }
}

async fn read_input_payload(
    client: &mut ControlPlaneServiceClient<tonic::transport::Channel>,
    job_id: &str,
    fallback: Option<Vec<u8>>,
) -> Result<Vec<u8>> {
    let streams = client
        .list_streams(Request::new(ListStreamsRequest {
            job_id: job_id.to_string(),
        }))
        .await?
        .into_inner()
        .streams;

    if let Some(input_stream) = streams
        .into_iter()
        .find(|stream| stream.stream_name == "input.default")
    {
        let mut frames = client
            .pull_frames(Request::new(PullFramesRequest {
                stream_id: input_stream.stream_id,
                after_sequence: 0,
                follow: false,
            }))
            .await?
            .into_inner();
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

fn job_state_name(state: anyserve_proto::controlplane::JobState) -> &'static str {
    match state {
        anyserve_proto::controlplane::JobState::Pending => "pending",
        anyserve_proto::controlplane::JobState::Leased => "leased",
        anyserve_proto::controlplane::JobState::Running => "running",
        anyserve_proto::controlplane::JobState::Succeeded => "succeeded",
        anyserve_proto::controlplane::JobState::Failed => "failed",
        anyserve_proto::controlplane::JobState::Cancelled => "cancelled",
        anyserve_proto::controlplane::JobState::Unspecified => "unspecified",
    }
}
