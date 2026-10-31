//! End-to-end behaviour of the runtime through the v2 worker gRPC service,
//! driven by a minimal worker written against the generated client.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyserve_core::runtime::Runtime;
use anyserve_core::runtime::api::{
    self, ConflictPolicy, Durability, EventKind, ExecutionEvent, InputMode, InvokeRequest, JobSpec,
    JobState, JournalConfig, Object, RuntimeConfig, SubmitRequest, codes,
};
use anyserve_core::worker_service::WorkerGrpcService;
use anyserve_proto::v2::{self as pb, worker_client::WorkerClient};
use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::Streaming;
use tonic::transport::Channel;

const WAIT: Duration = Duration::from_secs(10);

struct Server {
    runtime: Runtime,
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(config: RuntimeConfig) -> Server {
    let runtime = Runtime::start(config).await.expect("start runtime");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let service = WorkerGrpcService::server(runtime.clone());
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    Server { runtime, url, task }
}

fn config() -> RuntimeConfig {
    RuntimeConfig {
        heartbeat_timeout: Duration::from_secs(30),
        default_initial_backoff: Duration::from_millis(10),
        default_max_backoff: Duration::from_millis(50),
        ..RuntimeConfig::default()
    }
}

fn journal_path(name: &str) -> PathBuf {
    std::env::temp_dir()
        .join(format!("anyserve-test-{}", uuid::Uuid::new_v4()))
        .join(format!("{name}.sqlite"))
}

fn spec(input: &[u8]) -> JobSpec {
    let mut spec = JobSpec::default();
    spec.demand.interface = "echo".to_string();
    spec.inputs
        .push(Object::inline(Bytes::copy_from_slice(input)));
    spec
}

struct TestWorker {
    client: WorkerClient<Channel>,
    tx: mpsc::Sender<pb::WorkerMessage>,
    rx: Streaming<pb::ServerMessage>,
    welcome: pb::Welcome,
    heartbeat: tokio::task::JoinHandle<()>,
}

impl Drop for TestWorker {
    fn drop(&mut self) {
        self.heartbeat.abort();
    }
}

fn msg(msg: pb::worker_message::Msg) -> pb::WorkerMessage {
    pb::WorkerMessage { msg: Some(msg) }
}

impl TestWorker {
    async fn connect(url: &str, worker_id: &str, attributes: &[(&str, &str)]) -> Self {
        Self::connect_with(url, worker_id, attributes, Vec::new(), String::new()).await
    }

    async fn connect_with(
        url: &str,
        worker_id: &str,
        attributes: &[(&str, &str)],
        held: Vec<String>,
        resume: String,
    ) -> Self {
        Self::connect_hello(
            url,
            pb::Hello {
                worker_id: worker_id.to_string(),
                resume_session_id: resume,
                interfaces: vec!["echo".to_string()],
                attributes: attributes
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                max_concurrency: 4,
                held_lease_ids: held,
                ..Default::default()
            },
        )
        .await
    }

    async fn connect_hello(url: &str, hello: pb::Hello) -> Self {
        let mut client = WorkerClient::connect(url.to_string()).await.unwrap();
        let (tx, outbound) = mpsc::channel(64);
        tx.send(msg(pb::worker_message::Msg::Hello(hello)))
            .await
            .unwrap();
        let mut rx = client
            .session(ReceiverStream::new(outbound))
            .await
            .unwrap()
            .into_inner();
        let welcome = match rx.message().await.unwrap().unwrap().msg {
            Some(pb::server_message::Msg::Welcome(welcome)) => welcome,
            other => panic!("expected Welcome, got {other:?}"),
        };
        let interval = Duration::from_millis(welcome.heartbeat_interval_ms.clamp(20, 100) as u64);
        let beat = tx.clone();
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if beat
                    .send(msg(pb::worker_message::Msg::Heartbeat(pb::Heartbeat {
                        load: None,
                    })))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Self {
            client,
            tx,
            rx,
            welcome,
            heartbeat,
        }
    }

    async fn next(&mut self) -> pb::server_message::Msg {
        tokio::time::timeout(WAIT, self.rx.message())
            .await
            .expect("server message in time")
            .unwrap()
            .expect("session open")
            .msg
            .unwrap()
    }

    async fn next_assign(&mut self) -> pb::Assign {
        loop {
            if let pb::server_message::Msg::Assign(assign) = self.next().await {
                return assign;
            }
        }
    }

    async fn reply(&self, lease_id: &str, accepted: bool) {
        self.tx
            .send(msg(pb::worker_message::Msg::AssignReply(pb::AssignReply {
                lease_id: lease_id.to_string(),
                accepted,
                reject_reason: String::new(),
            })))
            .await
            .unwrap();
    }

    async fn attempt(&self, lease_id: &str, generation: u64) -> Attempt {
        Attempt::open(self.client.clone(), lease_id, generation).await
    }
}

struct Attempt {
    tx: mpsc::Sender<pb::AttemptUp>,
    rx: Streaming<pb::AttemptDown>,
    opened: Option<pb::AttemptOpened>,
    fenced: Option<pb::AttemptFinished>,
}

fn up(msg: pb::attempt_up::Msg) -> pb::AttemptUp {
    pb::AttemptUp { msg: Some(msg) }
}

impl Attempt {
    async fn open(mut client: WorkerClient<Channel>, lease_id: &str, generation: u64) -> Self {
        let (tx, outbound) = mpsc::channel(64);
        tx.send(up(pb::attempt_up::Msg::Open(pb::AttemptOpen {
            lease_id: lease_id.to_string(),
            generation,
            resume_after_sequence: 0,
        })))
        .await
        .unwrap();
        let mut rx = client
            .attempt(ReceiverStream::new(outbound))
            .await
            .unwrap()
            .into_inner();
        let (opened, fenced) = match rx.message().await.unwrap().unwrap().msg {
            Some(pb::attempt_down::Msg::Opened(opened)) => (Some(opened), None),
            Some(pb::attempt_down::Msg::Finished(finished)) => (None, Some(finished)),
            other => panic!("expected Opened, got {other:?}"),
        };
        Self {
            tx,
            rx,
            opened,
            fenced,
        }
    }

    /// Reads inputs until the trailer: (objects by index, params).
    async fn inputs(&mut self) -> (Vec<Vec<u8>>, Vec<u8>) {
        let mut objects: Vec<Vec<u8>> = Vec::new();
        let mut params = Vec::new();
        let mut ended = Vec::new();
        loop {
            match self.rx.message().await.unwrap().unwrap().msg {
                Some(pb::attempt_down::Msg::Input(input)) => {
                    if input.last {
                        assert_eq!(
                            ended,
                            (0..objects.len()).collect::<Vec<_>>(),
                            "each object ends once"
                        );
                        return (objects, params);
                    }
                    if input.parameter_chunk {
                        params.extend_from_slice(&input.data);
                    } else if input.object_end {
                        let index = input.object_index.expect("object index") as usize;
                        if objects.len() <= index {
                            objects.resize(index + 1, Vec::new());
                        }
                        objects[index].extend_from_slice(&input.data);
                        ended.push(index);
                    } else {
                        let index = input.object_index.expect("object index") as usize;
                        if objects.len() <= index {
                            objects.resize(index + 1, Vec::new());
                        }
                        objects[index].extend_from_slice(&input.data);
                    }
                }
                other => panic!("expected input, got {other:?}"),
            }
        }
    }

    async fn frame(&mut self, sequence: u64, data: &[u8]) -> u64 {
        self.tx
            .send(up(pb::attempt_up::Msg::Frame(pb::Frame {
                sequence,
                data: Bytes::copy_from_slice(data),
                ..Default::default()
            })))
            .await
            .unwrap();
        loop {
            match self.rx.message().await.unwrap().unwrap().msg {
                Some(pb::attempt_down::Msg::Ack(ack)) => return ack.up_to_sequence,
                Some(pb::attempt_down::Msg::Input(_)) => continue,
                other => panic!("expected ack, got {other:?}"),
            }
        }
    }

    async fn finish(&mut self, result: &[u8], outcome: pb::finish::Outcome) -> pb::AttemptFinished {
        if matches!(outcome, pb::finish::Outcome::Succeeded(_)) {
            self.tx
                .send(up(pb::attempt_up::Msg::Result(pb::ResultChunk {
                    index: 0,
                    data: Bytes::copy_from_slice(result),
                    last: true,
                    ..Default::default()
                })))
                .await
                .unwrap();
        }
        self.tx
            .send(up(pb::attempt_up::Msg::Finish(pb::Finish {
                outcome: Some(outcome),
            })))
            .await
            .unwrap();
        loop {
            match tokio::time::timeout(WAIT, self.rx.message()).await.unwrap() {
                Ok(Some(pb::AttemptDown {
                    msg: Some(pb::attempt_down::Msg::Finished(finished)),
                })) => return finished,
                Ok(Some(_)) => continue,
                other => panic!("expected AttemptFinished, got {other:?}"),
            }
        }
    }

    async fn succeed(&mut self, result: &[u8]) -> pb::AttemptFinished {
        self.finish(result, pb::finish::Outcome::Succeeded(pb::Succeeded {}))
            .await
    }
}

fn status(finished: &pb::AttemptFinished) -> pb::attempt_finished::Status {
    pb::attempt_finished::Status::try_from(finished.status).unwrap()
}

async fn collect(
    mut stream: impl futures::Stream<Item = Result<ExecutionEvent, anyserve_core::error::RuntimeError>>
    + Unpin,
) -> Vec<EventKind> {
    let mut kinds = Vec::new();
    while let Some(event) = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("event in time")
    {
        kinds.push(event.expect("event").kind);
    }
    kinds
}

async fn wait_state(runtime: &Runtime, job_id: &str, state: JobState) -> api::JobView {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let job = runtime.get_job(job_id, true).await.unwrap();
        if job.state == state {
            return job;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job stayed {:?}",
            job.state
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn invoke_streams_output_and_returns_result() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"hi"),
            ..InvokeRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    assert!(!assign.inputs_via_attempt);
    assert_eq!(assign.inline_inputs.len(), 1);
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    assert_eq!(attempt.frame(1, b"a").await, 1);
    assert_eq!(attempt.frame(2, b"b").await, 2);
    // A resent frame is acknowledged without being logged twice.
    assert_eq!(attempt.frame(2, b"b").await, 2);
    let finished = attempt.succeed(b"ab").await;
    assert_eq!(status(&finished), pb::attempt_finished::Status::Committed);

    let kinds = collect(execution).await;
    let outputs: Vec<&[u8]> = kinds
        .iter()
        .filter_map(|kind| match kind {
            EventKind::Output { data, .. } => Some(data.as_ref()),
            _ => None,
        })
        .collect();
    assert_eq!(outputs, vec![&b"a"[..], &b"b"[..]]);
    match kinds.last().unwrap() {
        EventKind::Finished { state, outputs, .. } => {
            assert_eq!(*state, JobState::Succeeded);
            assert_eq!(outputs, &vec![Object::inline(&b"ab"[..])]);
        }
        other => panic!("unexpected last event {other:?}"),
    }
}

#[tokio::test]
async fn large_inputs_and_params_travel_on_the_attempt_stream() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let big = vec![7u8; 200 * 1024];
    let mut job = spec(&big);
    job.inputs.push(Object::Uri {
        uri: "s3://bucket/key".to_string(),
        metadata: Default::default(),
    });
    job.params = Bytes::from(vec![3u8; 100 * 1024]);
    let outcome = server
        .runtime
        .submit(SubmitRequest {
            spec: JobSpec {
                policy: api::ExecutionPolicy {
                    durability: Some(Durability::Ephemeral),
                    ..Default::default()
                },
                ..job
            },
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    assert!(assign.inputs_via_attempt);
    assert!(assign.inline_inputs.is_empty() && assign.params.is_empty());
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    let (objects, params) = attempt.inputs().await;
    assert_eq!(objects.len(), 2);
    assert_eq!(objects[0], big);
    assert!(objects[1].is_empty());
    assert_eq!(params.len(), 100 * 1024);
    attempt.succeed(b"ok").await;
    wait_state(&server.runtime, &outcome.job.job_id, JobState::Succeeded).await;
}

#[tokio::test]
async fn submit_is_idempotent_by_fingerprint() {
    let server = serve(config()).await;
    let request = |input: &[u8]| SubmitRequest {
        job_id: Some("job-1".to_string()),
        conflict: ConflictPolicy::UseExisting,
        spec: JobSpec {
            policy: api::ExecutionPolicy {
                durability: Some(Durability::Ephemeral),
                ..Default::default()
            },
            ..spec(input)
        },
    };
    assert!(server.runtime.submit(request(b"a")).await.unwrap().created);
    assert!(!server.runtime.submit(request(b"a")).await.unwrap().created);
    let mut traced = request(b"a");
    traced
        .spec
        .metadata
        .insert("traceparent".to_string(), "00-1".to_string());
    assert!(!server.runtime.submit(traced).await.unwrap().created);
    let conflict = server.runtime.submit(request(b"b")).await.unwrap_err();
    assert_eq!(conflict.kind, anyserve_core::error::ErrorKind::Conflict);
    let mut strict = request(b"a");
    strict.conflict = ConflictPolicy::Fail;
    assert!(server.runtime.submit(strict).await.is_err());
}

#[tokio::test]
async fn lost_workers_consume_the_attempt_budget() {
    let mut cfg = config();
    cfg.heartbeat_timeout = Duration::from_millis(300);
    cfg.default_max_attempts = 2;
    let server = serve(cfg).await;
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: JobSpec {
                policy: api::ExecutionPolicy {
                    durability: Some(Durability::Ephemeral),
                    ..Default::default()
                },
                ..spec(b"x")
            },
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    for _ in 0..2 {
        let mut worker = TestWorker::connect(&server.url, "w-lost", &[]).await;
        let assign = worker.next_assign().await;
        worker.reply(&assign.lease_id, true).await;
        drop(worker);
    }
    let failed = wait_state(&server.runtime, &job.job_id, JobState::Failed).await;
    assert_eq!(failed.attempt_count, 2);
    assert_eq!(failed.error.unwrap().code, codes::ATTEMPTS_EXHAUSTED);
}

#[tokio::test]
async fn streamed_output_is_never_spliced_with_a_retry() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"x"),
            ..InvokeRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    attempt.frame(1, b"partial").await;
    let finished = attempt
        .finish(
            b"",
            pb::finish::Outcome::Failed(pb::ErrorInfo {
                code: "upstream".to_string(),
                message: "broke".to_string(),
                retryable: true,
            }),
        )
        .await;
    assert_eq!(status(&finished), pb::attempt_finished::Status::Committed);
    // Only now does the caller read.
    let kinds = collect(execution).await;
    assert!(
        !kinds
            .iter()
            .any(|k| matches!(k, EventKind::AttemptRestarted { .. }))
    );
    match kinds.last().unwrap() {
        EventKind::Finished { state, error, .. } => {
            assert_eq!(*state, JobState::Failed);
            assert_eq!(error.as_ref().unwrap().code, codes::OUTPUT_INTERRUPTED);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn unexposed_failures_retry_before_output() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"x"),
            ..InvokeRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    attempt
        .finish(
            b"",
            pb::finish::Outcome::Failed(pb::ErrorInfo {
                code: "upstream".to_string(),
                message: "broke".to_string(),
                retryable: true,
            }),
        )
        .await;
    let retry = worker.next_assign().await;
    assert_eq!(retry.attempt_number, 2);
    worker.reply(&retry.lease_id, true).await;
    let mut attempt = worker.attempt(&retry.lease_id, retry.generation).await;
    attempt.succeed(b"done").await;
    // Read only after the end: the execution's own reader keeps every event.
    let kinds = collect(execution).await;
    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, EventKind::AttemptRestarted { .. }))
    );
    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, EventKind::Assigned { worker_id, .. } if worker_id == "w1"))
    );
    assert!(matches!(
        kinds.last().unwrap(),
        EventKind::Finished {
            state: JobState::Succeeded,
            ..
        }
    ));
}

#[tokio::test]
async fn dropping_an_execution_cancels_the_worker() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"x"),
            ..InvokeRequest::default()
        })
        .await
        .unwrap();
    let job_id = execution.job_id().to_string();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    drop(execution);
    loop {
        if let pb::server_message::Msg::Cancel(cancel) = worker.next().await {
            assert_eq!(cancel.lease_id, assign.lease_id);
            break;
        }
    }
    let job = server.runtime.get_job(&job_id, false).await.unwrap();
    assert_eq!(job.state, JobState::Cancelled);
    // The worker's late completion is fenced, not recorded.
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    if attempt.fenced.is_none() {
        let finished = attempt.succeed(b"late").await;
        assert_eq!(status(&finished), pb::attempt_finished::Status::Fenced);
    }
}

#[tokio::test]
async fn full_queues_reject_new_work() {
    let mut cfg = config();
    cfg.default_max_queue = 1;
    let server = serve(cfg).await;
    let submit = || {
        server.runtime.submit(SubmitRequest {
            spec: JobSpec {
                policy: api::ExecutionPolicy {
                    durability: Some(Durability::Ephemeral),
                    ..Default::default()
                },
                ..spec(b"x")
            },
            ..SubmitRequest::default()
        })
    };
    submit().await.unwrap();
    let error = submit().await.unwrap_err();
    assert_eq!(
        error.kind,
        anyserve_core::error::ErrorKind::ResourceExhausted
    );
}

#[tokio::test]
async fn jobs_behind_an_unmatchable_head_are_dispatched() {
    let server = serve(config()).await;
    for _ in 0..600 {
        let mut job = spec(b"x");
        job.demand
            .required_attributes
            .insert("zone".to_string(), "nowhere".to_string());
        job.policy.durability = Some(Durability::Ephemeral);
        server
            .runtime
            .submit(SubmitRequest {
                spec: job,
                ..SubmitRequest::default()
            })
            .await
            .unwrap();
    }
    let mut ok = spec(b"reachable");
    ok.policy.durability = Some(Durability::Ephemeral);
    let reachable = server
        .runtime
        .submit(SubmitRequest {
            spec: ok,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let assign = worker.next_assign().await;
    assert_eq!(assign.job_id, reachable.job_id);
}

#[tokio::test]
async fn durable_jobs_survive_restart_and_completion_is_idempotent() {
    let path = journal_path("restart");
    let durable_config = || RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        ..config()
    };

    // Accepted before any worker exists, then the control plane restarts.
    let server = serve(durable_config()).await;
    let first_generation = server.runtime.generation();
    let job = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("durable-1".to_string()),
            spec: spec(b"payload"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    assert_eq!(job.durability, Durability::Durable);
    server.runtime.shutdown().await;
    drop(server);

    let server = serve(durable_config()).await;
    assert!(server.runtime.generation() > first_generation);
    let recovered = server.runtime.get_job("durable-1", false).await.unwrap();
    assert_eq!(recovered.state, JobState::Queued);
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let assign = worker.next_assign().await;
    assert_eq!(assign.inline_inputs.len(), 1);
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    assert_eq!(
        status(&attempt.succeed(b"result").await),
        pb::attempt_finished::Status::Committed
    );
    let done = wait_state(&server.runtime, "durable-1", JobState::Succeeded).await;
    assert_eq!(done.outputs.unwrap(), vec![Object::inline(&b"result"[..])]);
    let generation = server.runtime.generation();
    server.runtime.shutdown().await;
    drop(worker);
    drop(server);

    // After another restart the old lease can only replay its result.
    let server = serve(durable_config()).await;
    let view = server.runtime.get_job("durable-1", true).await.unwrap();
    assert_eq!(view.state, JobState::Succeeded);
    assert_eq!(view.outputs.unwrap(), vec![Object::inline(&b"result"[..])]);
    let worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut replay = worker.attempt(&assign.lease_id, generation).await;
    assert!(replay.opened.as_ref().unwrap().terminal_replay);
    assert_eq!(
        status(&replay.succeed(b"result").await),
        pb::attempt_finished::Status::Committed
    );
    let mut different = worker.attempt(&assign.lease_id, generation).await;
    assert_eq!(
        status(&different.succeed(b"other").await),
        pb::attempt_finished::Status::Rejected
    );
    let unknown = worker.attempt("no-such-lease", generation).await;
    assert_eq!(
        status(unknown.fenced.as_ref().unwrap()),
        pb::attempt_finished::Status::Fenced
    );
    // Resubmitting the same job returns the recorded one.
    let again = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("durable-1".to_string()),
            spec: spec(b"payload"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    assert!(!again.created);
    assert_eq!(again.job.state, JobState::Succeeded);
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn durable_attempt_budget_survives_restart() {
    let path = journal_path("budget");
    let durable_config = || RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        default_max_attempts: 1,
        ..config()
    };
    let server = serve(durable_config()).await;
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("budget-1".to_string()),
            spec: spec(b"x"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    // The control plane dies while the only allowed attempt runs.
    server.runtime.shutdown().await;
    drop(worker);
    drop(server);

    let server = serve(durable_config()).await;
    let job = wait_state(&server.runtime, "budget-1", JobState::Failed).await;
    assert_eq!(job.error.unwrap().code, codes::ATTEMPTS_EXHAUSTED);
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn streaming_input_reaches_the_worker() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"head"),
            input: InputMode::Streaming,
        })
        .await
        .unwrap();
    let input = execution.take_input().unwrap();
    input
        .send(Bytes::from_static(b"one "), Default::default())
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    assert!(assign.streaming_input && assign.inputs_via_attempt);
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    let sender = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        input
            .send(Bytes::from_static(b"two"), Default::default())
            .await
            .unwrap();
        input.finish().await.unwrap();
    });
    let (objects, _) = attempt.inputs().await;
    sender.await.unwrap();
    assert_eq!(
        objects,
        vec![b"head".to_vec(), b"one ".to_vec(), b"two".to_vec()]
    );
    attempt.succeed(b"ok").await;
    let kinds = collect(execution).await;
    assert!(matches!(
        kinds.last().unwrap(),
        EventKind::Finished {
            state: JobState::Succeeded,
            ..
        }
    ));
}

#[tokio::test]
async fn reconnecting_worker_keeps_its_leases() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"x"),
            ..InvokeRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let session = worker.welcome.session_id.clone();
    drop(worker);
    let worker = TestWorker::connect_with(
        &server.url,
        "w1",
        &[],
        vec![assign.lease_id.clone(), "stale-lease".to_string()],
        session.clone(),
    )
    .await;
    assert_eq!(worker.welcome.session_id, session);
    assert_eq!(
        worker.welcome.resumed_lease_ids,
        vec![assign.lease_id.clone()]
    );
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    attempt.succeed(b"kept").await;
    let kinds = collect(execution).await;
    assert!(matches!(
        kinds.last().unwrap(),
        EventKind::Finished {
            state: JobState::Succeeded,
            ..
        }
    ));
    let _ = HashMap::<(), ()>::new();
}

#[tokio::test]
async fn a_full_journal_rejects_durable_work_but_not_ephemeral() {
    let path = journal_path("full");
    let mut journal = JournalConfig::new(&path);
    journal.max_bytes = Some(256 * 1024);
    let server = serve(RuntimeConfig {
        journal: Some(journal),
        ..config()
    })
    .await;
    let big = vec![1u8; 512 * 1024];
    let error = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("too-big".to_string()),
            spec: spec(&big),
            ..SubmitRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.kind,
        anyserve_core::error::ErrorKind::PersistenceUnavailable
    );
    // The rolled-back id is free again and nothing half-written remains.
    assert!(server.runtime.get_job("too-big", false).await.is_err());
    assert!(!server.runtime.health().ready);

    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"still served"),
            ..InvokeRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    worker
        .attempt(&assign.lease_id, assign.generation)
        .await
        .succeed(b"ok")
        .await;
    let kinds = collect(execution).await;
    assert!(matches!(
        kinds.last().unwrap(),
        EventKind::Finished {
            state: JobState::Succeeded,
            ..
        }
    ));
    // Durable admission recovers once small writes fit again.
    let deadline = tokio::time::Instant::now() + WAIT;
    while !server.runtime.health().ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "journal stayed unavailable"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    server
        .runtime
        .submit(SubmitRequest {
            spec: spec(b"small"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn queue_and_execution_timeouts_end_attempts() {
    let server = serve(config()).await;
    let mut queued = spec(b"x");
    queued.policy.durability = Some(Durability::Ephemeral);
    queued.policy.queue_timeout = Some(Duration::from_millis(100));
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: queued,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let failed = wait_state(&server.runtime, &job.job_id, JobState::Failed).await;
    assert_eq!(failed.error.unwrap().code, codes::QUEUE_TIMEOUT);

    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut slow = spec(b"y");
    slow.policy.durability = Some(Durability::Ephemeral);
    slow.policy.execution_timeout = Some(Duration::from_millis(100));
    slow.policy.max_attempts = Some(1);
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: slow,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    loop {
        if let pb::server_message::Msg::Cancel(cancel) = worker.next().await {
            assert_eq!(cancel.lease_id, assign.lease_id);
            break;
        }
    }
    let failed = wait_state(&server.runtime, &job.job_id, JobState::Failed).await;
    assert_eq!(failed.error.unwrap().code, codes::ATTEMPTS_EXHAUSTED);
    assert_eq!(server.runtime.stats().failed, 2);
}

#[tokio::test]
async fn durable_cancel_returns_the_committed_state() {
    let server = serve(RuntimeConfig {
        journal: Some(JournalConfig::new(journal_path("cancel"))),
        ..config()
    })
    .await;
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: spec(b"x"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let cancelled = server
        .runtime
        .cancel(&job.job_id, "no longer needed".to_string())
        .await
        .unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert_eq!(cancelled.error.unwrap().code, codes::CANCELLED);
    let stats = server.runtime.stats();
    assert_eq!((stats.succeeded, stats.failed, stats.cancelled), (0, 0, 1));
    server.runtime.shutdown().await;
}

impl Attempt {
    /// Sends result chunks and a successful Finish; returns the final reply,
    /// or the stream's error status.
    #[allow(
        clippy::result_large_err,
        reason = "test helpers preserve gRPC status codes"
    )]
    async fn upload(
        &mut self,
        chunks: Vec<pb::ResultChunk>,
    ) -> Result<pb::AttemptFinished, tonic::Status> {
        for chunk in chunks {
            if self
                .tx
                .send(up(pb::attempt_up::Msg::Result(chunk)))
                .await
                .is_err()
            {
                break;
            }
        }
        let _ = self
            .tx
            .send(up(pb::attempt_up::Msg::Finish(pb::Finish {
                outcome: Some(pb::finish::Outcome::Succeeded(pb::Succeeded {})),
            })))
            .await;
        loop {
            match tokio::time::timeout(WAIT, self.rx.message()).await.unwrap() {
                Ok(Some(pb::AttemptDown {
                    msg: Some(pb::attempt_down::Msg::Finished(finished)),
                })) => return Ok(finished),
                Ok(Some(_)) => continue,
                Ok(None) => return Err(tonic::Status::aborted("stream ended")),
                Err(status) => return Err(status),
            }
        }
    }
}

fn empty_object(index: u32) -> pb::ResultChunk {
    pb::ResultChunk {
        index,
        last: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn result_object_count_is_bounded() {
    let mut cfg = config();
    cfg.limits.max_objects = 4;
    let server = serve(cfg).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut job = spec(b"x");
    job.policy.durability = Some(Durability::Ephemeral);
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    let finished = attempt
        .upload((0..10_000).map(empty_object).collect())
        .await
        .unwrap();
    assert_eq!(status(&finished), pb::attempt_finished::Status::Committed);
    let failed = wait_state(&server.runtime, &job.job_id, JobState::Failed).await;
    assert_eq!(failed.error.unwrap().code, codes::OUTPUT_TOO_LARGE);
}

#[tokio::test]
async fn result_uploads_share_a_global_budget() {
    let mut cfg = config();
    cfg.max_result_bytes = 128 * 1024;
    let server = serve(cfg).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut job = spec(b"x");
    job.policy.durability = Some(Durability::Ephemeral);
    server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    let chunks = (0..4)
        .map(|i| pb::ResultChunk {
            index: 0,
            data: Bytes::from(vec![0u8; 64 * 1024]),
            last: i == 3,
            ..Default::default()
        })
        .collect();
    let refused = attempt.upload(chunks).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
    // The partial upload released its reservation with the stream.
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.runtime.stats().result_bytes != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "result budget leaked"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // A result that fits is still accepted on the same lease.
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    assert_eq!(
        status(&attempt.succeed(b"small").await),
        pb::attempt_finished::Status::Committed
    );
    assert!(
        server.runtime.stats().result_bytes > 0,
        "retained result keeps its bytes"
    );
}

#[tokio::test]
async fn evicted_durable_jobs_are_read_back_from_the_journal() {
    let server = serve(RuntimeConfig {
        journal: Some(JournalConfig::new(journal_path("evict"))),
        terminal_retention: Duration::ZERO,
        ..config()
    })
    .await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("cold-1".to_string()),
            spec: spec(b"x"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    worker
        .attempt(&assign.lease_id, assign.generation)
        .await
        .succeed(b"cold result")
        .await;
    // Maintenance evicts it from memory within about a second.
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.runtime.stats().retained_jobs != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "job was not evicted"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let view = server.runtime.get_job("cold-1", true).await.unwrap();
    assert_eq!(
        view.outputs.unwrap(),
        vec![Object::inline(&b"cold result"[..])]
    );
    let again = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("cold-1".to_string()),
            spec: spec(b"x"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    assert!(!again.created);
    let conflict = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("cold-1".to_string()),
            spec: spec(b"other"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(conflict.kind, anyserve_core::error::ErrorKind::Conflict);
    let kinds = collect(server.runtime.watch("cold-1", 0).unwrap()).await;
    assert!(matches!(
        kinds.last().unwrap(),
        EventKind::Finished {
            state: JobState::Succeeded,
            ..
        }
    ));
    let mut replay = worker.attempt(&assign.lease_id, assign.generation).await;
    assert!(replay.opened.as_ref().unwrap().terminal_replay);
    assert_eq!(
        status(&replay.succeed(b"cold result").await),
        pb::attempt_finished::Status::Committed
    );
    let mut different = worker.attempt(&assign.lease_id, assign.generation).await;
    assert_eq!(
        status(&different.succeed(b"changed").await),
        pb::attempt_finished::Status::Rejected
    );
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn cold_job_reads_and_watch_share_the_upload_result_budget() {
    let server = serve(RuntimeConfig {
        journal: Some(JournalConfig::new(journal_path("cold-budget"))),
        max_terminal_jobs: 0,
        max_result_bytes: 128,
        ..config()
    })
    .await;
    let mut worker = TestWorker::connect(&server.url, "w", &[]).await;
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: spec(b"input"),
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    worker
        .attempt(&assign.lease_id, assign.generation)
        .await
        .succeed(&[3; 128])
        .await;
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.runtime.stats().result_bytes != 0 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(server.runtime.stats().retained_jobs, 0);
    let view = server.runtime.get_job(&job.job_id, true).await.unwrap();
    assert_eq!(server.runtime.stats().result_bytes, 128);
    let clone = view.outputs.as_ref().unwrap()[0].clone();
    drop(view);
    let refused = server.runtime.get_job(&job.job_id, true).await.unwrap_err();
    assert_eq!(
        refused.kind,
        anyserve_core::error::ErrorKind::ResourceExhausted
    );
    // Management reads have their own bounded representation and remain
    // available while a business client holds all result-byte capacity.
    let preview = server
        .runtime
        .inspect_job_preview(&job.job_id)
        .await
        .unwrap();
    assert!(preview.job.outputs.is_none());
    assert!(preview.spec.is_none());
    assert_eq!(preview.outputs[0].size_bytes, Some(128));
    assert_eq!(preview.outputs[0].prefix, Object::inline(vec![3; 128]));
    assert_eq!(server.runtime.stats().result_bytes, 128);
    assert!(matches!(preview.events.last().unwrap().kind,
        EventKind::Finished { ref outputs, .. } if outputs.is_empty()));
    drop(preview);
    assert_eq!(
        server
            .runtime
            .get_job(&job.job_id, false)
            .await
            .unwrap()
            .state,
        JobState::Succeeded
    );
    let mut watch = server.runtime.watch(&job.job_id, 0).unwrap();
    assert_eq!(
        watch.next().await.unwrap().unwrap_err().kind,
        anyserve_core::error::ErrorKind::ResourceExhausted
    );
    drop(watch);
    drop(clone);
    assert_eq!(server.runtime.stats().result_bytes, 0);
    let replay = collect(server.runtime.watch(&job.job_id, 0).unwrap()).await;
    assert_eq!(server.runtime.stats().result_bytes, 128);
    assert!(matches!(
        replay.last(),
        Some(EventKind::Finished {
            state: JobState::Succeeded,
            ..
        })
    ));
    drop(replay);
    assert_eq!(server.runtime.stats().result_bytes, 0);
    // The console needs both the result and its replay. They share one read,
    // even when the budget fits exactly one result.
    let detail = server.runtime.inspect_job(&job.job_id).await.unwrap();
    assert!(detail.spec.is_none());
    assert_eq!(
        detail.job.outputs.as_ref().unwrap()[0],
        Object::inline(vec![3; 128])
    );
    assert_eq!(server.runtime.stats().result_bytes, 128);
    drop(detail.job);
    assert_eq!(server.runtime.stats().result_bytes, 128);
    drop(detail.events);
    assert_eq!(server.runtime.stats().result_bytes, 0);
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn identifiers_are_bounded() {
    let server = serve(config()).await;
    let mut job = spec(b"x");
    job.demand.interface = "i".repeat(300);
    let error = server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, anyserve_core::error::ErrorKind::InvalidArgument);
    let error = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("j".repeat(300)),
            spec: spec(b"x"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, anyserve_core::error::ErrorKind::InvalidArgument);
    let mut client = WorkerClient::connect(server.url.clone()).await.unwrap();
    let (tx, outbound) = mpsc::channel(1);
    tx.send(msg(pb::worker_message::Msg::Hello(pb::Hello {
        worker_id: "w".to_string(),
        interfaces: vec!["i".repeat(300)],
        ..Default::default()
    })))
    .await
    .unwrap();
    let refused = client
        .session(ReceiverStream::new(outbound))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn events_and_spec_read_live_and_evicted_jobs() {
    let server = serve(RuntimeConfig {
        journal: Some(JournalConfig::new(journal_path("events"))),
        terminal_retention: Duration::ZERO,
        ..config()
    })
    .await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("ev-1".to_string()),
            spec: spec(b"x"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(
        server.runtime.spec("ev-1").unwrap().demand.interface,
        "echo"
    );
    let (events, finished) = server.runtime.events("ev-1", 0).await.unwrap();
    assert!(!finished);
    assert!(matches!(events[0].kind, EventKind::Accepted));
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    attempt.frame(1, b"partial").await;
    attempt.succeed(b"done").await;
    let (events, finished) = server.runtime.events("ev-1", 0).await.unwrap();
    assert!(finished);
    assert!(
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::Output { .. }))
    );
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.runtime.stats().retained_jobs != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "job was not evicted"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Evicted: acceptance and outcome come back from the journal.
    let (events, finished) = server.runtime.events("ev-1", 0).await.unwrap();
    assert!(finished);
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[1].kind,
        EventKind::Finished {
            state: JobState::Succeeded,
            ..
        }
    ));
    assert!(server.runtime.spec("ev-1").is_err());
    let missing = server.runtime.events("nope", 0).await.unwrap_err();
    assert_eq!(missing.kind, anyserve_core::error::ErrorKind::NotFound);
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn dropped_events_report_the_oldest_readable_sequence() {
    let mut cfg = config();
    cfg.event_log_max_events = 4;
    let server = serve(cfg).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut job = spec(b"x");
    job.policy.durability = Some(Durability::Ephemeral);
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    for sequence in 1..=8 {
        attempt.frame(sequence, b"f").await;
    }
    // A cursor that saw the first event is now behind what is held.
    let first_seen = (server.runtime.generation() << 32) + 1;
    let error = server
        .runtime
        .events(&job.job_id, first_seen)
        .await
        .unwrap_err();
    let anyserve_core::error::ErrorKind::CursorExpired { oldest } = error.kind else {
        panic!("expected CursorExpired, got {error:?}");
    };
    let (events, _) = server
        .runtime
        .events(&job.job_id, oldest - 1)
        .await
        .unwrap();
    assert_eq!(events.first().unwrap().sequence, oldest);
    // 0 means "from the oldest event still held" and never fails.
    let (events, _) = server.runtime.events(&job.job_id, 0).await.unwrap();
    assert_eq!(events.first().unwrap().sequence, oldest);
}

#[tokio::test]
async fn only_idempotent_submissions_are_fingerprinted() {
    let server = serve(config()).await;
    let mut ephemeral = spec(b"x");
    ephemeral.policy.durability = Some(Durability::Ephemeral);
    let auto = server
        .runtime
        .submit(SubmitRequest {
            spec: ephemeral.clone(),
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    assert!(auto.spec_hash.is_empty());
    let execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"x"),
            ..InvokeRequest::default()
        })
        .await
        .unwrap();
    for id in [auto.job_id.clone(), execution.job_id().to_string()] {
        let error = server
            .runtime
            .submit(SubmitRequest {
                job_id: Some(id),
                spec: ephemeral.clone(),
                ..SubmitRequest::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, anyserve_core::error::ErrorKind::Conflict);
    }
    let named = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("named".to_string()),
            spec: ephemeral,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    assert!(!named.spec_hash.is_empty());
}

#[tokio::test]
async fn upload_pressure_does_not_evict_finished_jobs() {
    let mut cfg = config();
    cfg.max_result_bytes = 1024 * 1024;
    let server = serve(cfg).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let submit = || {
        let mut job = spec(b"x");
        job.policy.durability = Some(Durability::Ephemeral);
        server.runtime.submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
    };
    let finished = submit().await.unwrap().job;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    worker
        .attempt(&assign.lease_id, assign.generation)
        .await
        .succeed(b"kept")
        .await;
    wait_state(&server.runtime, &finished.job_id, JobState::Succeeded).await;

    submit().await.unwrap();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    // Most of the budget is in flight, across several maintenance passes.
    attempt
        .tx
        .send(up(pb::attempt_up::Msg::Result(pb::ResultChunk {
            index: 0,
            data: Bytes::from(vec![0u8; 900 * 1024]),
            last: false,
            ..Default::default()
        })))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let job = server
        .runtime
        .get_job(&finished.job_id, true)
        .await
        .unwrap();
    assert_eq!(job.outputs.unwrap(), vec![Object::inline(&b"kept"[..])]);
}

#[tokio::test]
async fn an_upload_that_does_not_fit_evicts_old_results_once() {
    let mut cfg = config();
    cfg.max_result_bytes = 1024 * 1024;
    let server = serve(cfg).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut ids = Vec::new();
    for _ in 0..2 {
        let mut job = spec(b"x");
        job.policy.durability = Some(Durability::Ephemeral);
        ids.push(
            server
                .runtime
                .submit(SubmitRequest {
                    spec: job,
                    ..SubmitRequest::default()
                })
                .await
                .unwrap()
                .job
                .job_id,
        );
        let assign = worker.next_assign().await;
        worker.reply(&assign.lease_id, true).await;
        let finished = worker
            .attempt(&assign.lease_id, assign.generation)
            .await
            .succeed(&vec![7u8; 600 * 1024])
            .await;
        assert_eq!(status(&finished), pb::attempt_finished::Status::Committed);
    }
    // The second upload only fit after the first job's result left memory.
    assert!(server.runtime.get_job(&ids[0], false).await.is_err());
    let second = server.runtime.get_job(&ids[1], true).await.unwrap();
    assert_eq!(second.state, JobState::Succeeded);
}

impl Attempt {
    /// Sends one frame and returns its acknowledgement, or the stream's error.
    #[allow(
        clippy::result_large_err,
        reason = "test helpers preserve gRPC status codes"
    )]
    async fn try_frame(&mut self, sequence: u64, data: Vec<u8>) -> Result<u64, tonic::Status> {
        self.tx
            .send(up(pb::attempt_up::Msg::Frame(pb::Frame {
                sequence,
                data: Bytes::from(data),
                ..Default::default()
            })))
            .await
            .unwrap();
        loop {
            match tokio::time::timeout(WAIT, self.rx.message()).await.unwrap() {
                Ok(Some(pb::AttemptDown {
                    msg: Some(pb::attempt_down::Msg::Ack(ack)),
                })) => return Ok(ack.up_to_sequence),
                Ok(Some(_)) => continue,
                Ok(None) => return Err(tonic::Status::aborted("stream ended")),
                Err(status) => return Err(status),
            }
        }
    }
}

const EVENT_LIMIT: u64 = 64 * 1024;

fn event_budget_config() -> RuntimeConfig {
    RuntimeConfig {
        max_event_bytes: EVENT_LIMIT,
        consumer_stall_timeout: Duration::from_millis(600),
        ..config()
    }
}

/// Starts a streamed invocation whose events a background task keeps reading.
async fn invoke_read(server: &Server) -> (String, tokio::task::JoinHandle<Vec<EventKind>>) {
    let execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"x"),
            ..InvokeRequest::default()
        })
        .await
        .unwrap();
    let job_id = execution.job_id().to_string();
    (job_id, tokio::spawn(collect(execution)))
}

#[tokio::test]
async fn global_event_budget_evicts_terminal_jobs_before_waiting() {
    let server = serve(RuntimeConfig {
        journal: Some(JournalConfig::new(journal_path("event-reclaim"))),
        ..event_budget_config()
    })
    .await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    // A finished durable job keeps most of the event budget in its log
    // (ephemeral ones keep only their outcome).
    let job = spec(b"z");
    let finished = server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    attempt.try_frame(1, vec![1; 28 * 1024]).await.unwrap();
    attempt.try_frame(2, vec![1; 28 * 1024]).await.unwrap();
    attempt.succeed(b"z").await;
    wait_state(&server.runtime, &finished.job_id, JobState::Succeeded).await;

    let (_reader_job, _reader) = invoke_read(&server).await;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    let started = tokio::time::Instant::now();
    assert_eq!(attempt.try_frame(1, vec![2; 24 * 1024]).await.unwrap(), 1);
    assert!(
        started.elapsed() < Duration::from_millis(600),
        "did not wait"
    );
    // Evicted from memory; the journal still answers for it.
    assert_eq!(server.runtime.stats().retained_jobs, 0);
    assert_eq!(
        server
            .runtime
            .get_job(&finished.job_id, false)
            .await
            .unwrap()
            .state,
        JobState::Succeeded
    );
    assert!(server.runtime.stats().event_bytes <= EVENT_LIMIT);
}

#[tokio::test]
async fn a_full_global_event_budget_is_never_forced() {
    let server = serve(event_budget_config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    // A streamed job whose caller never reads holds almost all of the budget.
    let _unread = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"x"),
            ..InvokeRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut holder = worker.attempt(&assign.lease_id, assign.generation).await;
    holder.try_frame(1, vec![1; 60 * 1024]).await.unwrap();

    // This job's caller is caught up; only the shared budget is in the way and
    // there is no terminal job to evict.
    let (job_id, _reader) = invoke_read(&server).await;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    let started = tokio::time::Instant::now();
    let refused = attempt.try_frame(1, vec![2; 24 * 1024]).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "gave up without waiting"
    );
    assert!(
        server.runtime.stats().event_bytes <= EVENT_LIMIT,
        "frame was forced in"
    );
    // The server does not fail the job; the worker's Finish decides.
    let job = server.runtime.get_job(&job_id, false).await.unwrap();
    assert_eq!(job.state, JobState::Running);
}

/// Holds SQLite's write lock on a journal from a second connection, so the
/// runtime's commits wait (and fail after the journal's busy timeout).
async fn lock_journal(path: &std::path::Path) -> sqlx::SqliteConnection {
    use sqlx::Connection;
    let mut conn = sqlx::SqliteConnection::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut conn)
        .await
        .unwrap();
    conn
}

async fn unlock_journal(mut conn: sqlx::SqliteConnection) {
    sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
}

async fn wait_attempts(runtime: &Runtime, job_id: &str, attempts: u32) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while runtime.get_job(job_id, false).await.unwrap().attempt_count != attempts {
        assert!(
            tokio::time::Instant::now() < deadline,
            "attempt count never reached {attempts}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn replayed_outcomes_follow_every_live_sequence() {
    let path = journal_path("replay-seq");
    let durable = || RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        terminal_retention: Duration::ZERO,
        ..config()
    };
    let server = serve(durable()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("seq-1".to_string()),
            spec: spec(b"x"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    let (events, _) = server.runtime.events("seq-1", 0).await.unwrap();
    let assigned = events
        .iter()
        .find(|e| matches!(e.kind, EventKind::Assigned { .. }))
        .unwrap()
        .sequence;
    worker.reply(&assign.lease_id, true).await;
    worker
        .attempt(&assign.lease_id, assign.generation)
        .await
        .succeed(b"done")
        .await;
    let deadline = tokio::time::Instant::now() + WAIT;
    while server.runtime.stats().retained_jobs != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "job was not evicted"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // A watcher that saw Assigned before the job left memory still gets the outcome.
    let kinds = collect(server.runtime.watch("seq-1", assigned).unwrap()).await;
    assert!(matches!(
        kinds.last(),
        Some(EventKind::Finished {
            state: JobState::Succeeded,
            ..
        })
    ));
    server.runtime.shutdown().await;
    drop(worker);
    drop(server);

    // Across a restart, a cursor from the previous generation reports the gap.
    let server = serve(durable()).await;
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("seq-2".to_string()),
            spec: spec(b"y"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    let (events, _) = server.runtime.events("seq-2", 0).await.unwrap();
    let old_cursor = events[0].sequence;
    server.runtime.shutdown().await;
    drop(server);
    let server = serve(durable()).await;
    let error = server
        .runtime
        .events("seq-2", old_cursor)
        .await
        .unwrap_err();
    let anyserve_core::error::ErrorKind::CursorExpired { oldest } = error.kind else {
        panic!("expected CursorExpired, got {error:?}");
    };
    assert_eq!(oldest, (server.runtime.generation() << 32) + 1);
    // A fresh watch from 0 follows the new generation through to the outcome.
    let watch = server.runtime.watch("seq-2", 0).unwrap();
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    worker
        .attempt(&assign.lease_id, assign.generation)
        .await
        .succeed(b"after restart")
        .await;
    let kinds = collect(watch).await;
    assert!(matches!(kinds.first(), Some(EventKind::Accepted)));
    assert!(matches!(
        kinds.last(),
        Some(EventKind::Finished {
            state: JobState::Succeeded,
            ..
        })
    ));
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn a_rolled_back_terminal_commit_is_retried_until_it_lands() {
    let path = journal_path("terminal-retry");
    let server = serve(RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        ..config()
    })
    .await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: spec(b"x"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    let lock = lock_journal(&path).await;
    // The commit waits out the journal's busy timeout and rolls back.
    let refused = attempt.upload(vec![pb::ResultChunk {
        index: 0,
        data: Bytes::from_static(b"result"),
        last: true,
        ..Default::default()
    }]);
    let refused = tokio::time::timeout(Duration::from_secs(20), refused)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable);
    unlock_journal(lock).await;
    // Nobody resends Finish: the runtime commits the same outcome itself.
    let done = wait_state(&server.runtime, &job.job_id, JobState::Succeeded).await;
    assert_eq!(done.outputs.unwrap(), vec![Object::inline(&b"result"[..])]);
    assert_eq!(server.runtime.list_workers()[0].active_leases, 0);
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn a_lease_being_started_survives_a_worker_reconnect() {
    let path = journal_path("starting-reconnect");
    let server = serve(RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        ..config()
    })
    .await;
    let mut once = spec(b"x");
    once.policy.max_attempts = Some(1);
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: once,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let lock = lock_journal(&path).await;
    let worker = TestWorker::connect(&server.url, "w1", &[]).await;
    // The attempt start is now waiting on the journal.
    wait_attempts(&server.runtime, &job.job_id, 1).await;
    let session = worker.welcome.session_id.clone();
    drop(worker);
    let mut worker = TestWorker::connect_with(&server.url, "w1", &[], Vec::new(), session).await;
    unlock_journal(lock).await;
    let assign = worker.next_assign().await;
    assert_eq!(assign.attempt_number, 1);
    worker.reply(&assign.lease_id, true).await;
    worker
        .attempt(&assign.lease_id, assign.generation)
        .await
        .succeed(b"ok")
        .await;
    let done = wait_state(&server.runtime, &job.job_id, JobState::Succeeded).await;
    assert_eq!(done.attempt_count, 1);
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn a_start_committed_while_the_worker_is_away_is_retried_at_once() {
    let path = journal_path("start-away");
    let server = serve(RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        ..config()
    })
    .await;
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: spec(b"x"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let lock = lock_journal(&path).await;
    let worker = TestWorker::connect(&server.url, "w-away", &[]).await;
    wait_attempts(&server.runtime, &job.job_id, 1).await;
    drop(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;
    unlock_journal(lock).await;
    // The first worker is in its 30 s grace; the job does not wait for it.
    let mut other = TestWorker::connect(&server.url, "w-other", &[]).await;
    let assign = other.next_assign().await;
    assert_eq!(assign.attempt_number, 2);
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn a_rolled_back_start_returns_the_workers_slot() {
    let path = journal_path("start-rollback");
    let server = serve(RuntimeConfig {
        journal: Some(JournalConfig::new(&path)),
        ..config()
    })
    .await;
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: spec(b"x"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let lock = lock_journal(&path).await;
    let _worker = TestWorker::connect(&server.url, "w1", &[]).await;
    wait_attempts(&server.runtime, &job.job_id, 1).await;
    // The start rolls back after the journal's busy timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while server
        .runtime
        .get_job(&job.job_id, false)
        .await
        .unwrap()
        .attempt_count
        != 0
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "start never rolled back"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let worker = &server.runtime.list_workers()[0];
    assert_eq!(
        (worker.active_leases, worker.free_slots),
        (0, worker.max_concurrency)
    );
    unlock_journal(lock).await;
    server.runtime.shutdown().await;
}

#[tokio::test]
async fn huge_timeouts_do_not_overflow() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[("zone", "a")]).await;
    let mut job = spec(b"x");
    job.policy.durability = Some(Durability::Ephemeral);
    job.policy.queue_timeout = Some(Duration::from_millis(u64::MAX));
    job.policy.execution_timeout = Some(Duration::MAX);
    job.policy.total_timeout = Some(Duration::from_millis(u64::MAX));
    job.policy.spillover_after = Some(Duration::from_millis(u64::MAX));
    job.demand
        .preferred_attributes
        .insert("zone".to_string(), "a".to_string());
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let assign = worker.next_assign().await;
    assert_eq!(assign.total_deadline_ms, u64::MAX);
    worker.reply(&assign.lease_id, true).await;
    worker
        .attempt(&assign.lease_id, assign.generation)
        .await
        .succeed(b"ok")
        .await;
    wait_state(&server.runtime, &job.job_id, JobState::Succeeded).await;
    // The runtime is still healthy for the next request.
    let mut next = spec(b"y");
    next.policy.durability = Some(Durability::Ephemeral);
    server
        .runtime
        .submit(SubmitRequest {
            spec: next,
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn a_rejected_assignment_frees_its_slot_at_once() {
    // With a long retry backoff only the reply itself can hand the freed slot
    // to the job waiting behind it.
    let server = serve(RuntimeConfig {
        default_initial_backoff: Duration::from_secs(60),
        default_max_backoff: Duration::from_secs(60),
        ..config()
    })
    .await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut jobs = Vec::new();
    for _ in 0..5 {
        let mut job = spec(b"x");
        job.policy.durability = Some(Durability::Ephemeral);
        let job = server
            .runtime
            .submit(SubmitRequest {
                spec: job,
                ..SubmitRequest::default()
            })
            .await
            .unwrap()
            .job;
        jobs.push(job.job_id);
    }
    let mut assigns = Vec::new();
    for _ in 0..4 {
        assigns.push(worker.next_assign().await);
    }
    // The worker has room, so only the server's lease count holds the fifth
    // job back.
    worker
        .tx
        .send(msg(pb::worker_message::Msg::Credit(pb::Credit {
            free_slots: 4,
        })))
        .await
        .unwrap();
    worker.reply(&assigns[0].lease_id, false).await;
    assert_eq!(worker.next_assign().await.job_id, jobs[4]);
}

#[tokio::test]
async fn a_rejected_assignment_is_dispatched_again() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut job = spec(b"x");
    job.policy.durability = Some(Durability::Ephemeral);
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let first = worker.next_assign().await;
    worker.reply(&first.lease_id, false).await;
    // The only worker gets it again after the retry backoff.
    let second = worker.next_assign().await;
    assert_eq!(second.attempt_number, 2);
    worker.reply(&second.lease_id, true).await;
    worker
        .attempt(&second.lease_id, second.generation)
        .await
        .succeed(b"ok")
        .await;
    wait_state(&server.runtime, &job.job_id, JobState::Succeeded).await;
}

#[tokio::test]
async fn the_terminal_job_bound_holds_as_jobs_finish() {
    let mut cfg = config();
    cfg.max_terminal_jobs = 3;
    let server = serve(cfg).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut ids = Vec::new();
    for _ in 0..10 {
        let mut job = spec(b"x");
        job.policy.durability = Some(Durability::Ephemeral);
        let job = server
            .runtime
            .submit(SubmitRequest {
                spec: job,
                ..SubmitRequest::default()
            })
            .await
            .unwrap()
            .job;
        let assign = worker.next_assign().await;
        worker.reply(&assign.lease_id, true).await;
        worker
            .attempt(&assign.lease_id, assign.generation)
            .await
            .succeed(b"ok")
            .await;
        wait_state(&server.runtime, &job.job_id, JobState::Succeeded).await;
        worker
            .tx
            .send(msg(pb::worker_message::Msg::Credit(pb::Credit {
                free_slots: 4,
            })))
            .await
            .unwrap();
        ids.push(job.job_id);
        // No maintenance pass is needed for the bound to hold.
        assert!(server.runtime.stats().retained_jobs <= 3);
    }
    assert!(server.runtime.get_job(&ids[0], false).await.is_err());
    assert!(server.runtime.get_job(&ids[9], false).await.is_ok());
}

#[tokio::test]
async fn finished_ephemeral_jobs_keep_only_their_outcome() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut job = spec(b"x");
    job.policy.durability = Some(Durability::Ephemeral);
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    for sequence in 1..=8 {
        attempt.try_frame(sequence, vec![1; 1024]).await.unwrap();
    }
    let (live, _) = server.runtime.events(&job.job_id, 0).await.unwrap();
    let first_seen = live[0].sequence;
    attempt.succeed(b"done").await;
    wait_state(&server.runtime, &job.job_id, JobState::Succeeded).await;
    let (events, finished) = server.runtime.events(&job.job_id, 0).await.unwrap();
    assert!(finished);
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0].kind,
        EventKind::Finished { state: JobState::Succeeded, outputs, .. }
            if outputs == &vec![Object::inline(&b"done"[..])]
    ));
    let error = server
        .runtime
        .events(&job.job_id, first_seen)
        .await
        .unwrap_err();
    assert_eq!(
        error.kind,
        anyserve_core::error::ErrorKind::CursorExpired {
            oldest: events[0].sequence
        }
    );
    assert!(
        server.runtime.stats().event_bytes < 1024,
        "frames were released"
    );
}

#[tokio::test]
async fn a_streamed_caller_still_reads_everything_after_completion() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let execution = server
        .runtime
        .invoke(InvokeRequest {
            spec: spec(b"x"),
            ..InvokeRequest::default()
        })
        .await
        .unwrap();
    let job_id = execution.job_id().to_string();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    for sequence in 1..=64 {
        attempt.try_frame(sequence, vec![1; 1024]).await.unwrap();
    }
    attempt.succeed(b"").await;
    wait_state(&server.runtime, &job_id, JobState::Succeeded).await;
    // The job finished before the caller read anything: nothing is lost.
    let kinds = collect(execution).await;
    let frames = kinds
        .iter()
        .filter(|k| matches!(k, EventKind::Output { .. }))
        .count();
    assert_eq!(frames, 64);
    // Once the caller is done, only the outcome stays.
    let (events, _) = server.runtime.events(&job_id, 0).await.unwrap();
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn a_live_watcher_keeps_every_event_through_completion() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut job = spec(b"x");
    job.policy.durability = Some(Durability::Ephemeral);
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    // The watcher has read only Accepted when the job runs and finishes.
    let mut watch = server.runtime.watch(&job.job_id, 0).unwrap();
    let first = tokio::time::timeout(WAIT, watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(first.kind, EventKind::Accepted));
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    for sequence in 1..=4 {
        attempt.try_frame(sequence, vec![1; 64]).await.unwrap();
    }
    attempt.succeed(b"done").await;
    wait_state(&server.runtime, &job.job_id, JobState::Succeeded).await;
    // It still gets everything, in order, with no CursorExpired.
    let rest = collect(watch).await;
    assert!(matches!(rest.first(), Some(EventKind::Assigned { .. })));
    assert_eq!(
        rest.iter()
            .filter(|k| matches!(k, EventKind::Output { .. }))
            .count(),
        4
    );
    assert!(matches!(
        rest.last(),
        Some(EventKind::Finished {
            state: JobState::Succeeded,
            ..
        })
    ));
    // Its departure compacted the log: only the outcome is left, and a new
    // watch from an older nonzero cursor is refused.
    let (events, _) = server.runtime.events(&job.job_id, 0).await.unwrap();
    assert_eq!(events.len(), 1);
    let mut stale = server.runtime.watch(&job.job_id, first.sequence).unwrap();
    let refused = tokio::time::timeout(WAIT, stale.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        refused.kind,
        anyserve_core::error::ErrorKind::CursorExpired { .. }
    ));
}

#[tokio::test]
async fn an_unread_job_reclaims_room_before_dropping_output() {
    let server = serve(RuntimeConfig {
        journal: Some(JournalConfig::new(journal_path("reclaim-unread"))),
        ..event_budget_config()
    })
    .await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    // A finished durable job keeps most of the event budget in its log.
    let finished = server
        .runtime
        .submit(SubmitRequest {
            spec: spec(b"z"),
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    attempt.try_frame(1, vec![1; 28 * 1024]).await.unwrap();
    attempt.try_frame(2, vec![1; 28 * 1024]).await.unwrap();
    attempt.succeed(b"z").await;
    wait_state(&server.runtime, &finished.job_id, JobState::Succeeded).await;

    // Nobody reads this job, so output that still does not fit after one
    // eviction is dropped: the evicted job's bytes must be free by then.
    let mut job = spec(b"x");
    job.policy.durability = Some(Durability::Ephemeral);
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap()
        .job;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    assert_eq!(attempt.try_frame(1, vec![2; 24 * 1024]).await.unwrap(), 1);
    let (events, _) = server.runtime.events(&job.job_id, 0).await.unwrap();
    assert!(
        events.iter().any(
            |event| matches!(&event.kind, EventKind::Output { data, .. } if data.len() == 24 * 1024)
        ),
        "the frame was dropped"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_back_pressured_writer_is_woken_by_every_read() {
    // The caller's log holds four events, so most frames find it behind. A
    // missed wake-up would leave a frame waiting out the stall timeout,
    // which fails the job.
    let mut cfg = config();
    cfg.event_log_max_events = 4;
    cfg.consumer_stall_timeout = Duration::from_secs(2);
    let server = serve(cfg).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let (_job_id, reader) = invoke_read(&server).await;
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    // Frames go out without waiting for acknowledgements, so the writer keeps
    // running ahead of the caller.
    let sender = attempt.tx.clone();
    let send = tokio::spawn(async move {
        for sequence in 1..=1000u64 {
            let frame = pb::Frame {
                sequence,
                data: Bytes::copy_from_slice(&sequence.to_le_bytes()),
                ..Default::default()
            };
            sender
                .send(up(pb::attempt_up::Msg::Frame(frame)))
                .await
                .unwrap();
        }
    });
    let mut acknowledged = 0;
    while acknowledged < 1000 {
        let message = tokio::time::timeout(WAIT, attempt.rx.message())
            .await
            .expect("acknowledgement in time")
            .unwrap()
            .expect("attempt stream open");
        match message.msg {
            Some(pb::attempt_down::Msg::Ack(ack)) => acknowledged = ack.up_to_sequence,
            Some(pb::attempt_down::Msg::Input(_)) => {}
            other => panic!("expected ack, got {other:?}"),
        }
    }
    send.await.unwrap();
    attempt.succeed(b"done").await;
    let kinds = reader.await.unwrap();
    let frames: Vec<u64> = kinds
        .iter()
        .filter_map(|kind| match kind {
            EventKind::Output { data, .. } => {
                Some(u64::from_le_bytes(data[..].try_into().unwrap()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(frames, (1..=1000).collect::<Vec<_>>());
    assert!(matches!(
        kinds.last(),
        Some(EventKind::Finished {
            state: JobState::Succeeded,
            ..
        })
    ));
}

#[tokio::test]
async fn an_answered_attempt_stream_is_read_until_the_worker_closes_it() {
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    let mut job = spec(b"x");
    job.policy.durability = Some(Durability::Ephemeral);
    server
        .runtime
        .submit(SubmitRequest {
            spec: job,
            ..SubmitRequest::default()
        })
        .await
        .unwrap();
    let assign = worker.next_assign().await;
    worker.reply(&assign.lease_id, true).await;
    let late = || {
        up(pb::attempt_up::Msg::Frame(pb::Frame {
            sequence: 1,
            ..Default::default()
        }))
    };

    // After its answer the server ends the response but keeps reading: the
    // stream is not reset under a worker still writing.
    let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
    let finished = attempt.succeed(b"ok").await;
    assert_eq!(status(&finished), pb::attempt_finished::Status::Committed);
    assert!(attempt.rx.message().await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(100)).await;
    attempt.tx.send(late()).await.expect("request stream open");
    drop(attempt);

    // So does an attempt fenced as it opens.
    let mut fenced = worker.attempt("no-such-lease", assign.generation).await;
    assert!(fenced.fenced.is_some());
    assert!(fenced.rx.message().await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(100)).await;
    fenced.tx.send(late()).await.expect("request stream open");
}

/// Manual stress check, timing dependent, not in the default suite:
/// `cargo test -p anyserve-core --test runtime_v2 one_worker_connection -- --ignored`.
/// Before the attempt streams were read to their end it reproduced the
/// GOAWAY (1025 frames on forgotten streams, then "locally-reset streams
/// reached limit (1024)") only while h2 debug logging slowed the connection;
/// without logging it passed either way.
#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_worker_connection_carries_thousands_of_attempts() {
    // An attempt stream must close cleanly even while the worker is still
    // writing when the server answers its Finish. Every stream the server
    // resets with the worker's data in flight counts against h2's limit of
    // 1024 locally reset streams per connection; past it the server sends
    // GOAWAY and breaks every stream on the worker's connection.
    let server = serve(config()).await;
    let mut worker = TestWorker::connect(&server.url, "w1", &[]).await;
    for round in 0..3000 {
        let mut job = spec(b"x");
        job.policy.durability = Some(Durability::Ephemeral);
        server
            .runtime
            .submit(SubmitRequest {
                spec: job,
                ..SubmitRequest::default()
            })
            .await
            .unwrap();
        let assign = worker.next_assign().await;
        worker.reply(&assign.lease_id, true).await;
        let mut attempt = worker.attempt(&assign.lease_id, assign.generation).await;
        for message in [
            pb::attempt_up::Msg::Result(pb::ResultChunk {
                index: 0,
                data: Bytes::from_static(b"ok"),
                last: true,
                ..Default::default()
            }),
            pb::attempt_up::Msg::Finish(pb::Finish {
                outcome: Some(pb::finish::Outcome::Succeeded(pb::Succeeded {})),
            }),
        ] {
            attempt.tx.send(up(message)).await.unwrap();
        }
        // The worker keeps writing after its Finish, a message every few
        // microseconds: some is always in flight when the server answers.
        let writer = attempt.tx.clone();
        let filler = tokio::spawn(async move {
            while writer.send(pb::AttemptUp { msg: None }).await.is_ok() {
                let next = std::time::Instant::now() + Duration::from_micros(20);
                while std::time::Instant::now() < next {
                    tokio::task::yield_now().await;
                }
            }
        });
        let finished = loop {
            match tokio::time::timeout(WAIT, attempt.rx.message())
                .await
                .unwrap()
            {
                Ok(Some(pb::AttemptDown {
                    msg: Some(pb::attempt_down::Msg::Finished(finished)),
                })) => break finished,
                Ok(Some(_)) => continue,
                other => panic!("attempt {round}: expected AttemptFinished, got {other:?}"),
            }
        };
        assert_eq!(
            status(&finished),
            pb::attempt_finished::Status::Committed,
            "attempt {round}"
        );
        // The response ends (or the stream is reset) before the worker
        // stops writing and closes its side.
        while let Ok(Some(_)) = attempt.rx.message().await {}
        filler.abort();
        drop(attempt);
        worker
            .tx
            .send(msg(pb::worker_message::Msg::Credit(pb::Credit {
                free_slots: 4,
            })))
            .await
            .unwrap();
    }
}

// Run these explicitly in release mode on the same host for before/after data.
fn report_scale(name: &str, mut samples: Vec<std::time::Duration>) {
    samples.sort();
    println!(
        "{}",
        serde_json::json!({
            "case": name, "samples": samples.len(),
            "p50_us": samples[samples.len() / 2].as_secs_f64() * 1e6,
            "p95_us": samples[samples.len() * 95 / 100].as_secs_f64() * 1e6,
        })
    );
}

#[tokio::test]
async fn paginated_queries_match_individual_jobs_after_transitions() {
    let runtime = Runtime::start(config()).await.unwrap();
    let mut views = Vec::new();
    for i in 0..23 {
        let mut spec = spec(b"x");
        spec.demand.interface = format!("interface-{}", i % 3);
        spec.policy.durability = Some(Durability::Ephemeral);
        let job = runtime
            .submit(SubmitRequest {
                job_id: Some(format!("job-{:03}", 23 - i)),
                spec,
                ..Default::default()
            })
            .await
            .unwrap()
            .job;
        if i % 2 == 0 {
            runtime.cancel(&job.job_id, "test".into()).await.unwrap();
        }
        views.push(runtime.get_job(&job.job_id, false).await.unwrap());
    }
    for states in [
        Vec::new(),
        vec![JobState::Queued],
        vec![JobState::Cancelled, JobState::Queued, JobState::Cancelled],
    ] {
        for interface in [
            None,
            Some("interface-1".to_string()),
            Some("absent".to_string()),
        ] {
            let mut expected: Vec<_> = views
                .iter()
                .filter(|job| {
                    (states.is_empty() || states.contains(&job.state))
                        && interface.as_ref().is_none_or(|name| name == &job.interface)
                })
                .cloned()
                .collect();
            expected.sort_by(|a, b| {
                b.updated_at_ms
                    .cmp(&a.updated_at_ms)
                    .then_with(|| a.job_id.cmp(&b.job_id))
            });
            let mut actual = Vec::new();
            let mut token = None;
            loop {
                let page = runtime
                    .list_jobs(api::JobFilter {
                        states: states.clone(),
                        interface: interface.clone(),
                        limit: 3,
                        page_token: token,
                    })
                    .await
                    .unwrap();
                actual.extend(page.jobs);
                token = page.next_page_token;
                if token.is_none() {
                    break;
                }
                assert!(actual.len() < expected.len());
                assert_eq!(token.as_deref(), Some(actual.len().to_string().as_str()));
            }
            assert_eq!(actual, expected);
        }
    }
    let stats = runtime.stats();
    assert_eq!((stats.queued, stats.cancelled, stats.running), (11, 12, 0));
    assert_eq!(
        stats
            .interfaces
            .values()
            .map(|entry| entry.queued)
            .sum::<usize>(),
        11
    );
}

#[tokio::test]
#[ignore = "local scalability measurement"]
async fn scale_management_queries() {
    let runtime = Runtime::start(RuntimeConfig {
        default_max_queue: 20_000,
        ..config()
    })
    .await
    .unwrap();
    let start = std::time::Instant::now();
    for i in 0..10_000 {
        let mut spec = spec(b"x");
        spec.policy.durability = Some(Durability::Ephemeral);
        spec.policy.queue_timeout = Some(Duration::from_secs(3600));
        runtime
            .submit(SubmitRequest {
                job_id: Some(format!("job-{i:06}")),
                spec,
                ..Default::default()
            })
            .await
            .unwrap();
    }
    println!(
        "{}",
        serde_json::json!({"case": "admit_10000", "elapsed_ms": start.elapsed().as_secs_f64() * 1000.0})
    );
    for i in 0..5_000 {
        runtime
            .cancel(&format!("job-{i:06}"), "scale".into())
            .await
            .unwrap();
    }
    let mut lists = Vec::new();
    let mut stats = Vec::new();
    for _ in 0..500 {
        let start = std::time::Instant::now();
        let page = runtime
            .list_jobs(api::JobFilter {
                states: vec![JobState::Queued],
                limit: 100,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.jobs.len(), 100);
        std::hint::black_box(page);
        lists.push(start.elapsed());
        let start = std::time::Instant::now();
        let snapshot = runtime.stats();
        assert_eq!((snapshot.queued, snapshot.cancelled), (5000, 5000));
        std::hint::black_box(snapshot);
        stats.push(start.elapsed());
    }
    report_scale("list_100_of_10000", lists);
    report_scale("stats_10000", stats);
    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local scalability measurement"]
async fn scale_dispatch_interfaces() {
    let server = serve(RuntimeConfig {
        heartbeat_timeout: Duration::from_secs(600),
        ..config()
    })
    .await;
    for _ in 0..256 {
        let mut spec = spec(b"x");
        spec.policy.durability = Some(Durability::Ephemeral);
        spec.demand
            .required_attributes
            .insert("unavailable".into(), "true".into());
        server
            .runtime
            .submit(SubmitRequest {
                spec,
                ..Default::default()
            })
            .await
            .unwrap();
    }
    let mut unrelated = Vec::new();
    for i in 0..128 {
        let worker = TestWorker::connect_hello(
            &server.url,
            pb::Hello {
                worker_id: format!("unrelated-{i}"),
                interfaces: vec![format!("other-{i}")],
                max_concurrency: 1,
                ..Default::default()
            },
        )
        .await;
        worker.heartbeat.abort();
        unrelated.push(worker);
    }
    let mut worker = TestWorker::connect_hello(
        &server.url,
        pb::Hello {
            worker_id: "matching".into(),
            interfaces: vec!["echo".into()],
            max_concurrency: 1024,
            ..Default::default()
        },
    )
    .await;
    worker.heartbeat.abort();
    let mut samples = Vec::new();
    for _ in 0..100 {
        let mut spec = spec(b"x");
        spec.policy.durability = Some(Durability::Ephemeral);
        spec.policy.priority = 3;
        let start = std::time::Instant::now();
        let job = server
            .runtime
            .submit(SubmitRequest {
                spec,
                ..Default::default()
            })
            .await
            .unwrap()
            .job;
        samples.push(start.elapsed());
        assert_eq!(worker.next_assign().await.job_id, job.job_id);
    }
    report_scale("dispatch_129_workers_256_blocked", samples);
    server.runtime.shutdown().await;
}

#[tokio::test]
#[ignore = "local scalability measurement"]
async fn scale_journal_recovery() {
    let journal = JournalConfig::new(journal_path("scale-recovery"));
    let cfg = RuntimeConfig {
        journal: Some(journal.clone()),
        ..config()
    };
    let runtime = Runtime::start(cfg.clone()).await.unwrap();
    for chunk in 0..32 {
        let accepts = (0..64).map(|i| {
            let mut spec = spec(&[i; 64]);
            spec.params = Bytes::from(vec![1; 32]);
            spec.policy.queue_timeout = Some(Duration::from_secs(3600));
            runtime.submit(SubmitRequest {
                job_id: Some(format!("job-{chunk}-{i}")),
                spec,
                ..Default::default()
            })
        });
        for result in futures::future::join_all(accepts).await {
            result.unwrap();
        }
    }
    runtime.shutdown().await;
    drop(runtime);
    let mut samples = Vec::new();
    for _ in 0..5 {
        let start = std::time::Instant::now();
        let runtime = Runtime::start(cfg.clone()).await.unwrap();
        samples.push(start.elapsed());
        assert_eq!(runtime.stats().queued, 2048);
        runtime.shutdown().await;
    }
    report_scale("recover_2048_jobs", samples);
    std::fs::remove_dir_all(journal.path.parent().unwrap()).unwrap();
}
