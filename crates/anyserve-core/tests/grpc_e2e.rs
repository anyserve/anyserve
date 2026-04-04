use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use anyserve_core::kernel::Kernel;
use anyserve_core::scheduler::BasicScheduler;
use anyserve_core::service::ControlPlaneGrpcService;
use anyserve_core::store::{MemoryStateStore, MemoryStreamStore};
use anyserve_proto::controlplane::control_plane_service_client::ControlPlaneServiceClient;
use anyserve_proto::controlplane::control_plane_service_server::ControlPlaneServiceServer;
use anyserve_proto::controlplane::{
    CancelJobRequest, CompleteLeaseRequest, Demand, EventKind, ExecutionPolicy, FrameKind,
    GetJobRequest, JobSpec, JobState, ListAttemptsRequest, ListJobsRequest, ListStreamsRequest,
    OpenStreamRequest, PollLeaseRequest, PullFramesRequest, PushFramesRequest,
    RegisterWorkerRequest, ReportEventRequest, ResourceQuantity, StreamDirection, StreamScope,
    SubmitJobRequest, WatchJobRequest, WorkerSpec,
};
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::Server;

async fn spawn_server() -> Result<(
    String,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind test listener")?;
    let addr = listener
        .local_addr()
        .context("resolve test listener addr")?;

    let kernel = Arc::new(Kernel::new(
        Arc::new(MemoryStateStore::new()),
        Arc::new(MemoryStreamStore::new()),
        Arc::new(BasicScheduler),
        30,
        30,
    ));
    let service = ControlPlaneGrpcService::new(kernel);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let incoming = TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(ControlPlaneServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    println!("control plane started endpoint={addr}");
    Ok((format!("http://{addr}"), shutdown_tx, handle))
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_streaming_e2e_round_trip() -> Result<()> {
    let (endpoint, shutdown_tx, handle) = spawn_server().await?;
    let mut client = ControlPlaneServiceClient::connect(endpoint.clone())
        .await
        .context("connect submit client")?;
    let mut worker = ControlPlaneServiceClient::connect(endpoint)
        .await
        .context("connect worker client")?;

    let worker_id = worker
        .register_worker(Request::new(RegisterWorkerRequest {
            worker_id: "worker-e2e".to_string(),
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
        .await
        .context("register worker")?
        .into_inner()
        .worker
        .context("missing worker in register response")?
        .worker_id;
    println!("registered worker worker_id={worker_id}");

    let job = client
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-e2e-grpc".to_string(),
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
                metadata: HashMap::from([("source".to_string(), "grpc-e2e".to_string())]),
            }),
        }))
        .await
        .context("submit job")?
        .into_inner()
        .job
        .context("missing job in submit response")?;
    println!("submitted job job_id={}", job.job_id);

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
        .await
        .context("open input stream")?
        .into_inner()
        .stream
        .context("missing input stream in open response")?;
    println!(
        "opened input stream stream_id={} stream_name={}",
        input_stream.stream_id, input_stream.stream_name
    );

    client
        .push_frames(Request::new(tokio_stream::iter(vec![
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
                payload: b"from grpc e2e".to_vec(),
                metadata: HashMap::new(),
            },
        ])))
        .await
        .context("push input frames")?;
    println!("pushed input frames stream_id={}", input_stream.stream_id);

    client
        .close_stream(Request::new(
            anyserve_proto::controlplane::CloseStreamRequest {
                stream_id: input_stream.stream_id.clone(),
                worker_id: String::new(),
                lease_id: String::new(),
                metadata: HashMap::new(),
            },
        ))
        .await
        .context("close input stream")?;
    println!("closed input stream stream_id={}", input_stream.stream_id);

    let assignment = worker
        .poll_lease(Request::new(PollLeaseRequest {
            worker_id: worker_id.clone(),
        }))
        .await
        .context("poll lease for job")?
        .into_inner();

    let lease = assignment.lease.context("missing lease in poll response")?;
    let attempt = assignment
        .attempt
        .context("missing attempt in poll response")?;
    let leased_job = assignment.job.context("missing job in poll response")?;

    assert_eq!(leased_job.job_id, job.job_id);
    assert_eq!(
        attempt.state(),
        anyserve_proto::controlplane::AttemptState::Leased
    );
    println!(
        "received lease job_id={} attempt_id={} lease_id={}",
        leased_job.job_id, attempt.attempt_id, lease.lease_id
    );

    worker
        .report_event(Request::new(ReportEventRequest {
            worker_id: worker_id.clone(),
            lease_id: lease.lease_id.clone(),
            kind: EventKind::Started as i32,
            payload: Vec::new(),
            metadata: HashMap::new(),
        }))
        .await
        .context("report started")?;
    println!("reported event kind=started");

    worker
        .report_event(Request::new(ReportEventRequest {
            worker_id: worker_id.clone(),
            lease_id: lease.lease_id.clone(),
            kind: EventKind::Progress as i32,
            payload: Vec::new(),
            metadata: HashMap::new(),
        }))
        .await
        .context("report progress")?;
    println!("reported event kind=progress");

    let mut input_frames = worker
        .pull_frames(Request::new(PullFramesRequest {
            stream_id: input_stream.stream_id.clone(),
            after_sequence: 0,
            follow: false,
        }))
        .await
        .context("pull input frames")?
        .into_inner();

    let mut input_payload = Vec::new();
    while let Some(frame) = input_frames.next().await {
        let frame = frame.context("receive input frame")?;
        if frame.kind() == FrameKind::Data {
            input_payload.extend(frame.payload);
        }
    }
    assert_eq!(input_payload, b"hello from grpc e2e".to_vec());
    println!(
        "pulled input payload payload={}",
        String::from_utf8_lossy(&input_payload)
    );

    let output_stream = worker
        .open_stream(Request::new(OpenStreamRequest {
            job_id: job.job_id.clone(),
            attempt_id: attempt.attempt_id.clone(),
            worker_id: worker_id.clone(),
            lease_id: lease.lease_id.clone(),
            stream_name: "output.default".to_string(),
            scope: StreamScope::Lease as i32,
            direction: StreamDirection::WorkerToClient as i32,
            metadata: HashMap::new(),
        }))
        .await
        .context("open output stream")?
        .into_inner()
        .stream
        .context("missing output stream in open response")?;
    println!(
        "opened output stream stream_id={} stream_name={}",
        output_stream.stream_id, output_stream.stream_name
    );

    worker
        .push_frames(Request::new(tokio_stream::iter(vec![PushFramesRequest {
            stream_id: output_stream.stream_id.clone(),
            worker_id: worker_id.clone(),
            lease_id: lease.lease_id.clone(),
            kind: FrameKind::Data as i32,
            payload: b"stream reply".to_vec(),
            metadata: HashMap::new(),
        }])))
        .await
        .context("push output frame")?;
    println!(
        "pushed output frame stream_id={} payload=stream reply",
        output_stream.stream_id
    );

    worker
        .report_event(Request::new(ReportEventRequest {
            worker_id: worker_id.clone(),
            lease_id: lease.lease_id.clone(),
            kind: EventKind::OutputReady as i32,
            payload: Vec::new(),
            metadata: HashMap::new(),
        }))
        .await
        .context("report output ready")?;
    println!("reported event kind=output_ready");

    worker
        .complete_lease(Request::new(CompleteLeaseRequest {
            worker_id: worker_id.clone(),
            lease_id: lease.lease_id.clone(),
            outputs: Vec::new(),
            metadata: HashMap::new(),
        }))
        .await
        .context("complete lease")?;
    println!("completed lease lease_id={}", lease.lease_id);

    let mut events = client
        .watch_job(Request::new(WatchJobRequest {
            job_id: job.job_id.clone(),
            after_sequence: 0,
        }))
        .await
        .context("watch job")?
        .into_inner();
    let mut event_kinds = Vec::new();
    while let Some(event) = events.next().await {
        let event = event.context("receive job event")?;
        println!(
            "job event job_id={} sequence={} kind={}",
            event.job_id,
            event.sequence,
            event_kind_name(event.kind())
        );
        event_kinds.push(event.kind());
    }
    assert_eq!(
        event_kinds,
        vec![
            EventKind::Accepted,
            EventKind::LeaseGranted,
            EventKind::Started,
            EventKind::Progress,
            EventKind::OutputReady,
            EventKind::Succeeded,
        ]
    );

    let attempts = client
        .list_attempts(Request::new(ListAttemptsRequest {
            job_id: job.job_id.clone(),
        }))
        .await
        .context("list attempts")?
        .into_inner()
        .attempts;
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].state(),
        anyserve_proto::controlplane::AttemptState::Succeeded
    );

    let streams = client
        .list_streams(Request::new(ListStreamsRequest {
            job_id: job.job_id.clone(),
        }))
        .await
        .context("list streams")?
        .into_inner()
        .streams;
    assert!(
        streams
            .iter()
            .any(|stream| stream.stream_name == "input.default")
    );
    assert!(
        streams
            .iter()
            .any(|stream| stream.stream_name == "output.default")
    );

    let mut output_frames = client
        .pull_frames(Request::new(PullFramesRequest {
            stream_id: output_stream.stream_id.clone(),
            after_sequence: 0,
            follow: false,
        }))
        .await
        .context("pull output frames")?
        .into_inner();
    let mut output_payloads = Vec::new();
    while let Some(frame) = output_frames.next().await {
        let frame = frame.context("receive output frame")?;
        if frame.kind() == FrameKind::Data {
            let payload = String::from_utf8(frame.payload).context("decode output")?;
            println!("output frame sequence={} payload={payload}", frame.sequence);
            output_payloads.push(payload);
        }
    }
    assert_eq!(output_payloads, vec!["stream reply".to_string()]);

    let final_job = client
        .get_job(Request::new(GetJobRequest {
            job_id: job.job_id.clone(),
        }))
        .await
        .context("get final job")?
        .into_inner();
    assert_eq!(final_job.state(), JobState::Succeeded);
    assert_eq!(final_job.current_attempt_id, attempt.attempt_id);
    println!(
        "final job state job_id={} state={}",
        final_job.job_id,
        job_state_name(final_job.state())
    );

    drop(client);
    drop(worker);
    let _ = shutdown_tx.send(());
    handle.await.context("join grpc server task")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_follow_streams_wake_on_new_events_and_frames() -> Result<()> {
    let (endpoint, shutdown_tx, handle) = spawn_server().await?;
    let mut client = ControlPlaneServiceClient::connect(endpoint.clone())
        .await
        .context("connect submit client")?;
    let mut worker = ControlPlaneServiceClient::connect(endpoint)
        .await
        .context("connect worker client")?;

    let worker_id = worker
        .register_worker(Request::new(RegisterWorkerRequest {
            worker_id: "worker-follow".to_string(),
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
        .await
        .context("register worker")?
        .into_inner()
        .worker
        .context("missing worker in register response")?
        .worker_id;

    let job = client
        .submit_job(Request::new(SubmitJobRequest {
            job_id: "job-follow".to_string(),
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
                metadata: HashMap::new(),
            }),
        }))
        .await
        .context("submit job")?
        .into_inner()
        .job
        .context("missing job in submit response")?;

    let assignment = worker
        .poll_lease(Request::new(PollLeaseRequest {
            worker_id: worker_id.clone(),
        }))
        .await
        .context("poll lease for follow job")?
        .into_inner();
    let lease = assignment.lease.context("missing lease in poll response")?;
    let attempt = assignment
        .attempt
        .context("missing attempt in poll response")?;

    let mut events = client
        .watch_job(Request::new(WatchJobRequest {
            job_id: job.job_id.clone(),
            after_sequence: 2,
        }))
        .await
        .context("watch follow job")?
        .into_inner();
    tokio::time::sleep(Duration::from_millis(10)).await;

    worker
        .report_event(Request::new(ReportEventRequest {
            worker_id: worker_id.clone(),
            lease_id: lease.lease_id.clone(),
            kind: EventKind::Started as i32,
            payload: Vec::new(),
            metadata: HashMap::new(),
        }))
        .await
        .context("report started for follow job")?;

    let event = tokio::time::timeout(Duration::from_millis(90), events.next())
        .await
        .context("timed out waiting for follow job event")?
        .context("follow job event stream ended early")?
        .context("receive follow job event")?;
    assert_eq!(event.kind(), EventKind::Started);

    let output_stream = worker
        .open_stream(Request::new(OpenStreamRequest {
            job_id: job.job_id.clone(),
            attempt_id: attempt.attempt_id.clone(),
            worker_id: worker_id.clone(),
            lease_id: lease.lease_id.clone(),
            stream_name: "output.default".to_string(),
            scope: StreamScope::Lease as i32,
            direction: StreamDirection::WorkerToClient as i32,
            metadata: HashMap::new(),
        }))
        .await
        .context("open follow output stream")?
        .into_inner()
        .stream
        .context("missing follow output stream in open response")?;

    let mut frames = client
        .pull_frames(Request::new(PullFramesRequest {
            stream_id: output_stream.stream_id.clone(),
            after_sequence: 0,
            follow: true,
        }))
        .await
        .context("pull follow output frames")?
        .into_inner();
    tokio::time::sleep(Duration::from_millis(10)).await;

    worker
        .push_frames(Request::new(tokio_stream::iter(vec![PushFramesRequest {
            stream_id: output_stream.stream_id.clone(),
            worker_id: worker_id.clone(),
            lease_id: lease.lease_id.clone(),
            kind: FrameKind::Data as i32,
            payload: b"stream reply".to_vec(),
            metadata: HashMap::new(),
        }])))
        .await
        .context("push follow output frame")?;

    let frame = tokio::time::timeout(Duration::from_millis(90), frames.next())
        .await
        .context("timed out waiting for follow output frame")?
        .context("follow frame stream ended early")?
        .context("receive follow output frame")?;
    assert_eq!(frame.kind(), FrameKind::Data);
    assert_eq!(frame.payload, b"stream reply".to_vec());

    worker
        .complete_lease(Request::new(CompleteLeaseRequest {
            worker_id: worker_id.clone(),
            lease_id: lease.lease_id.clone(),
            outputs: Vec::new(),
            metadata: HashMap::new(),
        }))
        .await
        .context("complete follow lease")?;

    drop(events);
    drop(frames);
    drop(client);
    drop(worker);
    let _ = shutdown_tx.send(());
    handle.await.context("join grpc server task")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_can_list_and_cancel_jobs() -> Result<()> {
    let (endpoint, shutdown_tx, handle) = spawn_server().await?;
    let mut client = ControlPlaneServiceClient::connect(endpoint)
        .await
        .context("connect client")?;

    for job_id in ["job-list-1", "job-list-2"] {
        client
            .submit_job(Request::new(SubmitJobRequest {
                job_id: job_id.to_string(),
                spec: Some(JobSpec {
                    interface_name: "demo.echo.v1".to_string(),
                    inputs: Vec::new(),
                    params: Vec::new(),
                    demand: Some(Demand {
                        required_attributes: HashMap::new(),
                        preferred_attributes: HashMap::new(),
                        required_capacity: Vec::new(),
                    }),
                    policy: Some(ExecutionPolicy {
                        profile: "basic".to_string(),
                        priority: 0,
                        lease_ttl_secs: 15,
                    }),
                    metadata: HashMap::new(),
                }),
            }))
            .await
            .with_context(|| format!("submit job {job_id}"))?;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let jobs = client
        .list_jobs(Request::new(ListJobsRequest {}))
        .await
        .context("list jobs before cancel")?
        .into_inner()
        .jobs;
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].job_id, "job-list-2");
    assert_eq!(jobs[1].job_id, "job-list-1");

    let cancelled = client
        .cancel_job(Request::new(CancelJobRequest {
            job_id: "job-list-1".to_string(),
        }))
        .await
        .context("cancel job")?
        .into_inner();
    assert_eq!(cancelled.state(), JobState::Cancelled);

    let jobs = client
        .list_jobs(Request::new(ListJobsRequest {}))
        .await
        .context("list jobs after cancel")?
        .into_inner()
        .jobs;
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].job_id, "job-list-1");
    assert_eq!(jobs[0].state(), JobState::Cancelled);
    assert_eq!(jobs[1].job_id, "job-list-2");

    drop(client);
    let _ = shutdown_tx.send(());
    handle.await.context("join grpc server task")??;
    Ok(())
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
