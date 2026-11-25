//! Shared dual-backend (`SqliteJournal` + `PgJournal`) scheduling/recovery tests
//! and real PostgreSQL 18 single-leader failover & fencing drills.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use anyserve_client::ExecutionClient;
use anyserve_client::execution::{Attempt as ClientAttempt, WorkerSession};
use anyserve_proto::v2 as pb;
use anyserve_proto::v2::client_server::ClientServer;
use bytes::Bytes;
use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::Server;

use super::PgJournal;
use crate::client_service::ClientGrpcService;
use crate::error::ErrorKind;
use crate::journal::JournalBackend;
use crate::runtime::Runtime;
use crate::runtime::api::*;
use crate::worker_service::WorkerGrpcService;

const TIMEOUT: Duration = Duration::from_secs(15);

struct GrpcHarness {
    runtime: Runtime,
    endpoint: String,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn start_grpc(runtime: Runtime) -> GrpcHarness {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let client = ClientServer::new(ClientGrpcService::new(runtime.clone()))
        .max_decoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE)
        .max_encoding_message_size(anyserve_proto::MAX_MESSAGE_SIZE);
    let worker = WorkerGrpcService::server(runtime.clone());
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(client)
            .add_service(worker)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = rx.await;
            })
            .await;
    });
    GrpcHarness {
        runtime,
        endpoint: format!("http://{addr}"),
        _stop: tx,
    }
}

struct TestSchema {
    url: String,
    schema: String,
}

impl TestSchema {
    async fn create(prefix: &str) -> Self {
        let url = std::env::var("ANYSERVE_TEST_POSTGRES_URL")
            .expect("ANYSERVE_TEST_POSTGRES_URL must be set for ignored postgres_ tests");
        let schema = format!(
            "as_{}_{}",
            prefix,
            uuid::Uuid::new_v4().simple().to_string()[..12].to_lowercase()
        );
        Self { url, schema }
    }

    fn journal_config(&self, lease_ms: u64, poll_ms: u64) -> JournalConfig {
        let mut cfg = JournalConfig::postgres(self.url.clone());
        let pg = cfg.postgres.as_mut().unwrap();
        pg.schema = self.schema.clone();
        pg.lease_duration = Duration::from_millis(lease_ms);
        pg.poll_interval = Duration::from_millis(poll_ms);
        pg.max_connections = 4;
        cfg
    }

    async fn drop_schema(&self) {
        if let Ok(pool) = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.url)
            .await
        {
            let _ = sqlx::query(&format!(
                "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                self.schema
            ))
            .execute(&pool)
            .await;
            pool.close().await;
        }
    }
}

fn durable_spec(interface: &str, payload: &[u8]) -> JobSpec {
    let mut spec = JobSpec::default();
    spec.demand.interface = interface.to_string();
    spec.policy.durability = Some(Durability::Durable);
    spec.inputs
        .push(Object::inline(Bytes::copy_from_slice(payload)));
    spec.params = Bytes::from_static(b"params-v1");
    spec
}

async fn spawn_echo_worker(
    endpoint: &str,
    worker_id: &str,
    pool: &str,
    interfaces: &[&str],
    max_concurrency: u32,
    delay: Duration,
    in_flight_tracker: Option<(Arc<AtomicUsize>, Arc<AtomicUsize>)>,
) -> tokio::task::JoinHandle<()> {
    let mut session = WorkerSession::connect(
        endpoint,
        None,
        pb::Hello {
            worker_id: worker_id.to_string(),
            pool: pool.to_string(),
            interfaces: interfaces.iter().map(|s| (*s).to_string()).collect(),
            max_concurrency,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let welcome = match session.messages.message().await.unwrap().unwrap().msg {
        Some(pb::server_message::Msg::Welcome(w)) => w,
        other => panic!("expected Welcome, got {other:?}"),
    };
    let limits = welcome.limits.unwrap();
    let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
    tokio::spawn(async move {
        while let Ok(Some(msg)) = session.messages.message().await {
            let Some(pb::server_message::Msg::Assign(assign)) = msg.msg else {
                continue;
            };
            let _ = session
                .control
                .send(pb::WorkerMessage {
                    msg: Some(pb::worker_message::Msg::AssignReply(pb::AssignReply {
                        lease_id: assign.lease_id.clone(),
                        accepted: true,
                        reject_reason: String::new(),
                    })),
                })
                .await;
            if let Some((cur, peak)) = &in_flight_tracker {
                let prev = cur.fetch_add(1, Ordering::SeqCst);
                peak.fetch_max(prev + 1, Ordering::SeqCst);
            }
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let mut attempt = ClientAttempt::open(&mut session.data, &assign, &limits, hb_timeout)
                .await
                .unwrap();
            let mut body = assign.params.to_vec();
            if let Some(first) = assign.inline_inputs.first()
                && let Some(pb::object::Body::Inline(data)) = &first.body
            {
                body = data.to_vec();
            }
            if let Some((cur, _)) = &in_flight_tracker {
                cur.fetch_sub(1, Ordering::SeqCst);
            }
            let _ = attempt
                .finish(
                    &[pb::Object {
                        body: Some(pb::object::Body::Inline(Bytes::from(body))),
                        ..Default::default()
                    }],
                    pb::finish::Outcome::Succeeded(pb::Succeeded {}),
                )
                .await;
            let _ = session
                .control
                .send(pb::WorkerMessage {
                    msg: Some(pb::worker_message::Msg::Credit(pb::Credit {
                        free_slots: max_concurrency,
                    })),
                })
                .await;
        }
    })
}

async fn wait_succeeded(runtime: &Runtime, job_id: &str) -> JobView {
    tokio::time::timeout(TIMEOUT, async {
        let mut stream = runtime.watch(job_id, 0).unwrap();
        while let Some(ev) = stream.next().await {
            if let EventKind::Finished { state, error, .. } = ev.unwrap().kind {
                assert_eq!(state, JobState::Succeeded, "job {job_id} failed: {error:?}");
                break;
            }
        }
        runtime.get_job(job_id, true).await.unwrap()
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for job {job_id}"))
}

/// Exact same scheduling, durability, cold-read, receipt-replay, purge, and 50/50
/// recovery suite executed against both `SqliteJournal` and `PgJournal`.
async fn run_unified_backend_suite(make_journal: impl Fn() -> JournalConfig) {
    let mut interfaces = BTreeMap::new();
    interfaces.insert(
        "serial.v1".to_string(),
        InterfacePolicy {
            max_in_flight_per_key: Some(1),
            ..Default::default()
        },
    );
    interfaces.insert(
        "tiered.v1".to_string(),
        InterfacePolicy {
            pool_tiers: vec![
                PoolTier {
                    pool: "edge".into(),
                    spillover_after: Duration::ZERO,
                    reconnect_grace: None,
                },
                PoolTier {
                    pool: "cloud".into(),
                    spillover_after: Duration::from_millis(60),
                    reconnect_grace: None,
                },
            ],
            ..Default::default()
        },
    );
    let base_config = || RuntimeConfig {
        journal: Some(make_journal()),
        default_initial_backoff: Duration::from_millis(5),
        default_max_backoff: Duration::from_millis(20),
        default_max_attempts: 3,
        max_terminal_jobs: 2,
        interfaces: interfaces.clone(),
        ..Default::default()
    };

    let server = start_grpc(Runtime::start(base_config()).await.unwrap()).await;
    assert!(server.runtime.is_leader_active());

    // 1. Priority + round-robin fairness_key on durable jobs.
    for i in 0..8 {
        let mut s = durable_spec("fair.v1", b"bulk");
        s.policy.fairness_key = "tenant-bulk".into();
        server
            .runtime
            .submit(SubmitRequest {
                job_id: Some(format!("bulk-{i}")),
                spec: s,
                ..Default::default()
            })
            .await
            .unwrap();
    }
    let mut interactive = durable_spec("fair.v1", b"interactive");
    interactive.policy.fairness_key = "tenant-interactive".into();
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("interactive-0".into()),
            spec: interactive,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut urgent = durable_spec("fair.v1", b"urgent");
    urgent.policy.priority = 3;
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("urgent-0".into()),
            spec: urgent,
            ..Default::default()
        })
        .await
        .unwrap();

    let mut fair_session = WorkerSession::connect(
        &server.endpoint,
        None,
        pb::Hello {
            worker_id: "fair-w".into(),
            interfaces: vec!["fair.v1".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let welcome = match fair_session.messages.message().await.unwrap().unwrap().msg {
        Some(pb::server_message::Msg::Welcome(w)) => w,
        other => panic!("expected Welcome: {other:?}"),
    };
    let limits = welcome.limits.unwrap();
    let hb = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
    let mut order = Vec::new();
    for _ in 0..10 {
        let assign = loop {
            if let Some(pb::server_message::Msg::Assign(a)) =
                fair_session.messages.message().await.unwrap().unwrap().msg
            {
                break a;
            }
        };
        order.push(assign.job_id.clone());
        let _ = fair_session
            .control
            .send(pb::WorkerMessage {
                msg: Some(pb::worker_message::Msg::AssignReply(pb::AssignReply {
                    lease_id: assign.lease_id.clone(),
                    accepted: true,
                    reject_reason: String::new(),
                })),
            })
            .await;
        let mut att = ClientAttempt::open(&mut fair_session.data, &assign, &limits, hb)
            .await
            .unwrap();
        att.finish(
            &[pb::Object {
                body: Some(pb::object::Body::Inline(Bytes::from(vec![b'z'; 16 * 1024]))),
                ..Default::default()
            }],
            pb::finish::Outcome::Succeeded(pb::Succeeded {}),
        )
        .await
        .unwrap();
        let _ = fair_session
            .control
            .send(pb::WorkerMessage {
                msg: Some(pb::worker_message::Msg::Credit(pb::Credit {
                    free_slots: 1,
                })),
            })
            .await;
    }
    assert_eq!(order[0], "urgent-0", "strict priority must run first");
    assert!(
        order[1] == "interactive-0" || order[2] == "interactive-0",
        "round-robin fairness_key must schedule interactive job in first pass: {order:?}"
    );
    drop(fair_session);

    // 2. Cold idempotency, 4 KiB bounded preview without touching result_budget, and scan_job_metadata.
    // Because max_terminal_jobs == 2, "urgent-0" was evicted from memory to cold journal!
    assert!(server.runtime.spec("urgent-0").is_err());
    let before_budget = server.runtime.stats().result_bytes;
    let preview = server
        .runtime
        .inspect_job_preview("urgent-0")
        .await
        .unwrap();
    assert_eq!(preview.job.state, JobState::Succeeded);
    assert_eq!(preview.outputs.len(), 1);
    assert_eq!(preview.outputs[0].size_bytes, Some(16 * 1024));
    assert_eq!(
        preview.outputs[0].prefix.inline_len(),
        INSPECTION_PREVIEW_BYTES
    );
    assert_eq!(server.runtime.stats().result_bytes, before_budget);

    let dup = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("urgent-0".into()),
            spec: {
                let mut u = durable_spec("fair.v1", b"urgent");
                u.policy.priority = 3;
                u
            },
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!dup.created);
    assert_eq!(dup.job.state, JobState::Succeeded);

    let conflict_err = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("urgent-0".into()),
            spec: durable_spec("fair.v1", b"different-input"),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(conflict_err.kind, ErrorKind::Conflict);

    let now_scan = crate::now_ms();
    let archived = server
        .runtime
        .scan_job_metadata(JobScan {
            archived: true,
            from_ms: now_scan.saturating_sub(3_600_000),
            to_ms: now_scan + 10_000,
            limit: 50,
            cursor: None,
            namespace: None,
        })
        .await
        .unwrap();
    assert!(archived.jobs.len() >= 10);

    // 3. Concurrency-key serialization across 3 concurrent workers.
    let cur = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut serial_workers = Vec::new();
    for idx in 0..3 {
        serial_workers.push(
            spawn_echo_worker(
                &server.endpoint,
                &format!("serial-{idx}"),
                "",
                &["serial.v1"],
                2,
                Duration::from_millis(20),
                Some((Arc::clone(&cur), Arc::clone(&peak))),
            )
            .await,
        );
    }
    for i in 0..4 {
        let mut s = durable_spec("serial.v1", b"step");
        s.policy.concurrency_key = "arm-left".into();
        server
            .runtime
            .submit(SubmitRequest {
                job_id: Some(format!("serial-{i}")),
                spec: s,
                ..Default::default()
            })
            .await
            .unwrap();
    }
    for i in 0..4 {
        wait_succeeded(&server.runtime, &format!("serial-{i}")).await;
    }
    assert_eq!(peak.load(Ordering::SeqCst), 1);

    // 4. Pool-tier spillover onto cloud worker.
    let cloud_worker = spawn_echo_worker(
        &server.endpoint,
        "cloud-tier-w",
        "cloud",
        &["tiered.v1"],
        2,
        Duration::ZERO,
        None,
    )
    .await;
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("tiered-job-1".into()),
            spec: durable_spec("tiered.v1", b"spill-payload"),
            ..Default::default()
        })
        .await
        .unwrap();
    let tiered_done = wait_succeeded(&server.runtime, "tiered-job-1").await;
    assert_eq!(
        tiered_done.current_worker_id.as_deref(),
        Some("cloud-tier-w")
    );

    // 5. Failed-worker avoidance on durable jobs (15/15 succeed despite 1 broken worker).
    let mut bad_session = WorkerSession::connect(
        &server.endpoint,
        None,
        pb::Hello {
            worker_id: "flaky-w".into(),
            interfaces: vec!["avoid.v1".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bad_welcome = match bad_session.messages.message().await.unwrap().unwrap().msg {
        Some(pb::server_message::Msg::Welcome(w)) => w,
        other => panic!("expected Welcome: {other:?}"),
    };
    let bad_limits = bad_welcome.limits.unwrap();
    let bad_hb = Duration::from_millis(u64::from(bad_welcome.heartbeat_timeout_ms));
    let bad_task = tokio::spawn(async move {
        while let Ok(Some(msg)) = bad_session.messages.message().await {
            if let Some(pb::server_message::Msg::Assign(assign)) = msg.msg {
                let _ = bad_session
                    .control
                    .send(pb::WorkerMessage {
                        msg: Some(pb::worker_message::Msg::AssignReply(pb::AssignReply {
                            lease_id: assign.lease_id.clone(),
                            accepted: true,
                            reject_reason: String::new(),
                        })),
                    })
                    .await;
                let mut att =
                    ClientAttempt::open(&mut bad_session.data, &assign, &bad_limits, bad_hb)
                        .await
                        .unwrap();
                let _ = att
                    .finish(
                        &[],
                        pb::finish::Outcome::Failed(pb::ErrorInfo {
                            code: "worker_failed".into(),
                            message: "transient gpu fault".into(),
                            retryable: true,
                        }),
                    )
                    .await;
                let _ = bad_session
                    .control
                    .send(pb::WorkerMessage {
                        msg: Some(pb::worker_message::Msg::Credit(pb::Credit {
                            free_slots: 1,
                        })),
                    })
                    .await;
            }
        }
    });
    let good_a = spawn_echo_worker(
        &server.endpoint,
        "good-a",
        "",
        &["avoid.v1"],
        2,
        Duration::ZERO,
        None,
    )
    .await;
    let good_b = spawn_echo_worker(
        &server.endpoint,
        "good-b",
        "",
        &["avoid.v1"],
        2,
        Duration::ZERO,
        None,
    )
    .await;
    for i in 0..15 {
        server
            .runtime
            .submit(SubmitRequest {
                job_id: Some(format!("avoid-{i}")),
                spec: durable_spec("avoid.v1", b"ok"),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    for i in 0..15 {
        wait_succeeded(&server.runtime, &format!("avoid-{i}")).await;
    }

    for h in serial_workers {
        h.abort();
    }
    cloud_worker.abort();
    bad_task.abort();
    good_a.abort();
    good_b.abort();

    // 6. 50/50 durable recovery across restart (including mid-flight started attempts) + receipt replay + purge.
    for i in 0..50 {
        server
            .runtime
            .submit(SubmitRequest {
                job_id: Some(format!("rec-{i:02}")),
                spec: durable_spec("recover.v1", format!("payload-{i:02}").as_bytes()),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    // Start 2 attempts without finishing before restart.
    let mut stalled_worker = WorkerSession::connect(
        &server.endpoint,
        None,
        pb::Hello {
            worker_id: "crashed-before-restart".into(),
            interfaces: vec!["recover.v1".into()],
            max_concurrency: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let _ = stalled_worker.messages.message().await.unwrap();
    let mut started_count = 0;
    while started_count < 2 {
        if let Some(pb::server_message::Msg::Assign(_)) = stalled_worker
            .messages
            .message()
            .await
            .unwrap()
            .unwrap()
            .msg
        {
            started_count += 1;
        }
    }
    drop(stalled_worker);
    let term1 = server.runtime.generation();
    server.runtime.shutdown().await;
    drop(server);

    // Restart on the same journal -> generation advances, 50/50 durable jobs recover and finish!
    let server2 = start_grpc(Runtime::start(base_config()).await.unwrap()).await;
    assert!(server2.runtime.generation() > term1);
    let rec_w1 = spawn_echo_worker(
        &server2.endpoint,
        "rec-w1",
        "",
        &["recover.v1"],
        4,
        Duration::ZERO,
        None,
    )
    .await;
    let rec_w2 = spawn_echo_worker(
        &server2.endpoint,
        "rec-w2",
        "",
        &["recover.v1"],
        4,
        Duration::ZERO,
        None,
    )
    .await;
    for i in 0..50 {
        let view = wait_succeeded(&server2.runtime, &format!("rec-{i:02}")).await;
        let expected = format!("payload-{i:02}");
        match &view.outputs.unwrap()[0] {
            Object::Inline { data, .. } => assert_eq!(data.as_ref(), expected.as_bytes()),
            other => panic!("expected inline output, got {other:?}"),
        }
    }
    // Verify purge cleans up terminal rows cleanly.
    let purged = server2
        .runtime
        .inner
        .journal
        .as_ref()
        .unwrap()
        .purge(server2.runtime.generation(), crate::now_ms() + 60_000)
        .await
        .unwrap();
    assert!(purged >= 50, "expected >= 50 purged jobs, got {purged}");
    rec_w1.abort();
    rec_w2.abort();
    server2.runtime.shutdown().await;
}

#[tokio::test]
async fn sqlite_unified_backend_scheduling_and_recovery_suite() {
    let dir = PathBuf::from(format!(
        "/tmp/anyserve-unified-sqlite-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("journal.sqlite");
    run_unified_backend_suite(|| {
        let mut cfg = JournalConfig::new(&db_path);
        cfg.full_sync = false;
        cfg
    })
    .await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_unified_backend_scheduling_and_recovery_suite() {
    let schema = TestSchema::create("pg_unified").await;
    run_unified_backend_suite(|| schema.journal_config(5_000, 80)).await;
    schema.drop_schema().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_standby_gate_and_graceful_relinquish_takeover() {
    let schema = TestSchema::create("pg_standby").await;
    let make_rt = || {
        let j = schema.journal_config(4_000, 50);
        RuntimeConfig {
            journal: Some(j),
            ..Default::default()
        }
    };

    let cp1 = start_grpc(Runtime::start(make_rt()).await.unwrap()).await;
    let cp2 = start_grpc(Runtime::start(make_rt()).await.unwrap()).await;

    let (leader, standby) = if cp1.runtime.is_leader_active() {
        (&cp1, &cp2)
    } else {
        (&cp2, &cp1)
    };
    assert!(leader.runtime.health().ready);
    assert!(standby.runtime.health().live);
    assert!(!standby.runtime.health().ready);

    // Direct call to standby must return Unavailable on both Client and Worker services.
    let mut direct_standby = ExecutionClient::connect(&standby.endpoint).await.unwrap();
    let standby_err = direct_standby
        .list(pb::ListJobsRequest::default())
        .await
        .unwrap_err();
    assert_eq!(standby_err.code(), Code::Unavailable);
    let worker_standby_err = WorkerSession::connect(
        &standby.endpoint,
        None,
        pb::Hello {
            worker_id: "probe".into(),
            interfaces: vec!["ha.v1".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .err()
    .expect("standby must reject worker session");
    assert!(
        format!("{worker_standby_err:#}").contains("standby")
            || format!("{worker_standby_err:#}").contains("Unavailable"),
        "unexpected standby worker error: {worker_standby_err:#}"
    );

    // Multi-endpoint client & worker skip standby and stick to active leader!
    let multi_endpoint = format!("{},{}", standby.endpoint, leader.endpoint);
    let mut client = ExecutionClient::connect(&multi_endpoint).await.unwrap();
    let sub = client
        .submit(
            Some("ha-graceful-1".into()),
            pb::JobSpec {
                demand: Some(pb::Demand {
                    interface: "ha.v1".into(),
                    ..Default::default()
                }),
                policy: Some(pb::ExecutionPolicy {
                    durability: pb::Durability::Durable as i32,
                    ..Default::default()
                }),
                inputs: vec![pb::Object {
                    body: Some(pb::object::Body::Inline(Bytes::from_static(
                        b"before-stepdown",
                    ))),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(sub.created);

    // Graceful shutdown of leader relinquishes scheduler_lease immediately -> standby takes over!
    let mut standby_leader_rx = standby.runtime.subscribe_leadership();
    let term1 = leader.runtime.generation();
    leader.runtime.shutdown().await;

    tokio::time::timeout(TIMEOUT, async {
        while !*standby_leader_rx.borrow_and_update() {
            standby_leader_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("standby must take over promptly after graceful relinquish");

    assert!(standby.runtime.is_leader_active());
    assert_eq!(standby.runtime.generation(), term1 + 1);
    assert!(standby.runtime.health().ready);

    // Multi-endpoint worker & existing ExecutionClient automatically switch to the new leader!
    let worker_handle = spawn_echo_worker(
        &multi_endpoint,
        "ha-worker-after-switch",
        "",
        &["ha.v1"],
        2,
        Duration::ZERO,
        None,
    )
    .await;
    let done = wait_succeeded(&standby.runtime, "ha-graceful-1").await;
    assert_eq!(done.state, JobState::Succeeded);

    let via_client = client.get("ha-graceful-1", true).await.unwrap();
    assert_eq!(via_client.state, pb::JobState::Succeeded as i32);

    worker_handle.abort();
    standby.runtime.shutdown().await;
    schema.drop_schema().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_abrupt_crash_takeover_preserves_every_acknowledged_job() {
    let schema = TestSchema::create("pg_crash").await;
    let j1_cfg = schema.journal_config(800, 50);
    let pg1 = PgJournal::open_concrete(&j1_cfg).await.unwrap();
    let cp1 = start_grpc(
        Runtime::start_with_backend(
            RuntimeConfig {
                journal: Some(j1_cfg),
                default_max_attempts: 3,
                ..Default::default()
            },
            Some(Arc::clone(&pg1) as Arc<dyn JournalBackend>),
        )
        .await
        .unwrap(),
    )
    .await;
    assert!(cp1.runtime.is_leader_active());

    let cp2 = start_grpc(
        Runtime::start(RuntimeConfig {
            journal: Some(schema.journal_config(800, 50)),
            default_max_attempts: 3,
            ..Default::default()
        })
        .await
        .unwrap(),
    )
    .await;
    assert!(!cp2.runtime.is_leader_active());

    for i in 0..20 {
        cp1.runtime
            .submit(SubmitRequest {
                job_id: Some(format!("crash-job-{i:02}")),
                spec: durable_spec("crash.v1", format!("item-{i:02}").as_bytes()),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    // Start 3 attempts on CP1 and hold them unfinished when CP1 abruptly crashes (no relinquish).
    let mut half_worker = WorkerSession::connect(
        &cp1.endpoint,
        None,
        pb::Hello {
            worker_id: "doomed-worker".into(),
            interfaces: vec!["crash.v1".into()],
            max_concurrency: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let _ = half_worker.messages.message().await.unwrap();
    let mut assigned_before_crash = Vec::new();
    while assigned_before_crash.len() < 3 {
        if let Some(pb::server_message::Msg::Assign(a)) =
            half_worker.messages.message().await.unwrap().unwrap().msg
        {
            assigned_before_crash.push(a.job_id);
        }
    }

    let mut cp2_leader_rx = cp2.runtime.subscribe_leadership();
    // Simulate kill -9: close CP1's pools and pause renewal WITHOUT calling relinquish()!
    pg1.abort_without_relinquish().await;
    drop(half_worker);

    tokio::time::timeout(TIMEOUT, async {
        while !*cp2_leader_rx.borrow_and_update() {
            cp2_leader_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("standby must acquire lease after crashed leader's lease expires");

    assert_eq!(cp2.runtime.generation(), 2);
    let rec_worker = spawn_echo_worker(
        &cp2.endpoint,
        "survivor-worker",
        "",
        &["crash.v1"],
        4,
        Duration::ZERO,
        None,
    )
    .await;

    for i in 0..20 {
        let id = format!("crash-job-{i:02}");
        let view = wait_succeeded(&cp2.runtime, &id).await;
        assert_eq!(view.state, JobState::Succeeded);
        if assigned_before_crash.contains(&id) {
            assert_eq!(
                view.attempt_count, 2,
                "mid-flight job {id} must record attempt 2"
            );
        } else {
            assert_eq!(view.attempt_count, 1);
        }
    }

    rec_worker.abort();
    cp2.runtime.shutdown().await;
    schema.drop_schema().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_stalled_leader_fenced_on_single_statement_write_and_exits() {
    let schema = TestSchema::create("pg_fenced").await;
    let j1_cfg = schema.journal_config(700, 50);
    let pg1 = PgJournal::open_concrete(&j1_cfg).await.unwrap();
    let cp1 = Runtime::start_with_backend(
        RuntimeConfig {
            journal: Some(j1_cfg),
            ..Default::default()
        },
        Some(Arc::clone(&pg1) as Arc<dyn JournalBackend>),
    )
    .await
    .unwrap();
    assert_eq!(cp1.generation(), 1);

    let cp2 = Runtime::start(RuntimeConfig {
        journal: Some(schema.journal_config(700, 50)),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut cp2_leader_rx = cp2.subscribe_leadership();

    // Freeze CP1 renewal (simulating SIGSTOP past lease TTL).
    pg1.set_pause_renewal(true);

    tokio::time::timeout(TIMEOUT, async {
        while !*cp2_leader_rx.borrow_and_update() {
            cp2_leader_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("CP2 must take over once CP1 stalls past lease_duration");
    assert_eq!(cp2.generation(), 2);

    // CP2 writes a legitimate durable job under generation 2.
    cp2.submit(SubmitRequest {
        job_id: Some("new-leader-job".into()),
        spec: durable_spec("fence.v1", b"valid"),
        ..Default::default()
    })
    .await
    .unwrap();

    // Now CP1 wakes up ("SIGCONT") and tries to commit a durable job under stale generation 1.
    // Single-statement CTE FOR SHARE check must reject it, write ZERO rows, and trigger lease_loss!
    let stale_err = cp1
        .submit(SubmitRequest {
            job_id: Some("stale-leader-job".into()),
            spec: durable_spec("fence.v1", b"must-not-persist"),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(stale_err.kind, ErrorKind::PersistenceUnavailable);

    tokio::time::timeout(Duration::from_secs(2), cp1.wait_for_lease_loss())
        .await
        .expect("fenced batch write must trigger wait_for_lease_loss immediately");
    assert!(!cp1.health().live);
    assert!(!cp1.health().ready);

    // Verify stale-leader-job was NEVER written to PostgreSQL.
    let not_found = cp2.get_job("stale-leader-job", false).await.unwrap_err();
    assert_eq!(not_found.kind, ErrorKind::NotFound);
    assert_eq!(
        cp2.get_job("new-leader-job", false).await.unwrap().state,
        JobState::Queued
    );

    cp2.shutdown().await;
    schema.drop_schema().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_severed_database_connection_triggers_leader_self_exit_and_standby_takeover() {
    let schema = TestSchema::create("pg_netcut").await;
    let parsed: sqlx::postgres::PgConnectOptions = schema.url.parse().unwrap();
    let pg_host = parsed.get_host().to_string();
    let pg_port = parsed.get_port();

    // Local TCP proxy for CP1 so we can sever all its PostgreSQL TCP connections on demand.
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    let cut = Arc::new(AtomicBool::new(false));
    let (cut_tx, cut_rx) = watch::channel(false);
    let cut_accept = Arc::clone(&cut);
    let proxy_task = tokio::spawn(async move {
        loop {
            let Ok((mut client_sock, _)) = proxy_listener.accept().await else {
                break;
            };
            if cut_accept.load(Ordering::Acquire) {
                drop(client_sock);
                continue;
            }
            let target = format!("{pg_host}:{pg_port}");
            let mut kill_rx = cut_rx.clone();
            tokio::spawn(async move {
                let Ok(mut upstream) = TcpStream::connect(&target).await else {
                    return;
                };
                tokio::select! {
                    _ = copy_bidirectional(&mut client_sock, &mut upstream) => {}
                    _ = kill_rx.changed() => {}
                }
            });
        }
    });

    let proxied_url = {
        // Replace host:port in URL with proxy_addr while preserving credentials and db name.
        let rest = schema
            .url
            .strip_prefix("postgresql://")
            .or_else(|| schema.url.strip_prefix("postgres://"))
            .unwrap();
        let (userinfo, host_and_db) = rest.rsplit_once('@').unwrap();
        let (_, db_part) = host_and_db.split_once('/').unwrap();
        format!("postgresql://{userinfo}@{proxy_addr}/{db_part}")
    };

    let mut cp1_journal = JournalConfig::postgres(proxied_url);
    {
        let pg = cp1_journal.postgres.as_mut().unwrap();
        pg.schema = schema.schema.clone();
        pg.lease_duration = Duration::from_millis(800);
        pg.poll_interval = Duration::from_millis(50);
        pg.max_connections = 4;
    }

    let cp1 = Runtime::start(RuntimeConfig {
        journal: Some(cp1_journal),
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(cp1.is_leader_active());
    cp1.submit(SubmitRequest {
        job_id: Some("before-netcut".into()),
        spec: durable_spec("net.v1", b"survives-netcut"),
        ..Default::default()
    })
    .await
    .unwrap();

    let cp2 = start_grpc(
        Runtime::start(RuntimeConfig {
            journal: Some(schema.journal_config(800, 50)),
            ..Default::default()
        })
        .await
        .unwrap(),
    )
    .await;
    let mut cp2_leader_rx = cp2.runtime.subscribe_leadership();

    // Sever CP1 -> PostgreSQL TCP connections completely!
    cut.store(true, Ordering::Release);
    cut_tx.send_replace(true);
    proxy_task.abort();

    // CP1 must detect lost lease renewal and self-fence (`wait_for_lease_loss`), while CP2 takes over!
    tokio::time::timeout(TIMEOUT, cp1.wait_for_lease_loss())
        .await
        .expect("CP1 must detect severed DB connection and signal lease loss");
    assert!(!cp1.health().live);

    tokio::time::timeout(TIMEOUT, async {
        while !*cp2_leader_rx.borrow_and_update() {
            cp2_leader_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("CP2 must acquire leadership after CP1's severed lease expires");

    let w = spawn_echo_worker(
        &cp2.endpoint,
        "netcut-worker",
        "",
        &["net.v1"],
        2,
        Duration::ZERO,
        None,
    )
    .await;
    let view = wait_succeeded(&cp2.runtime, "before-netcut").await;
    assert_eq!(view.state, JobState::Succeeded);

    w.abort();
    cp2.runtime.shutdown().await;
    schema.drop_schema().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_legacy_schemas_are_rejected_cleanly() {
    let schema_v2 = TestSchema::create("pg_legacy_v2").await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&schema_v2.url)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA \"{}\"", schema_v2.schema))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TABLE \"{}\".cluster_schema (version BIGINT PRIMARY KEY)",
        schema_v2.schema
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO \"{}\".cluster_schema VALUES (2)",
        schema_v2.schema
    ))
    .execute(&pool)
    .await
    .unwrap();

    let err_v2 = match Runtime::start(RuntimeConfig {
        journal: Some(schema_v2.journal_config(5_000, 100)),
        ..Default::default()
    })
    .await
    {
        Ok(_) => panic!("v2 schema must be rejected"),
        Err(err) => format!("{err:#}"),
    };
    assert!(
        err_v2.contains("incompatible PostgreSQL schema version 2"),
        "unexpected error: {err_v2}"
    );

    let schema_nodes = TestSchema::create("pg_legacy_nodes").await;
    sqlx::query(&format!("CREATE SCHEMA \"{}\"", schema_nodes.schema))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TABLE \"{}\".nodes (node_id TEXT PRIMARY KEY)",
        schema_nodes.schema
    ))
    .execute(&pool)
    .await
    .unwrap();
    let err_nodes = match Runtime::start(RuntimeConfig {
        journal: Some(schema_nodes.journal_config(5_000, 100)),
        ..Default::default()
    })
    .await
    {
        Ok(_) => panic!("legacy nodes table must be rejected"),
        Err(err) => format!("{err:#}"),
    };
    assert!(
        err_nodes.contains("incompatible legacy tables"),
        "unexpected error: {err_nodes}"
    );

    pool.close().await;
    schema_v2.drop_schema().await;
    schema_nodes.drop_schema().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_scan_jobs_paginates_past_250_rows() {
    let schema = TestSchema::create("pg_scan250").await;
    let runtime = Runtime::start(RuntimeConfig {
        journal: Some(schema.journal_config(5_000, 80)),
        ..Default::default()
    })
    .await
    .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&schema.url)
        .await
        .unwrap();
    let spec_json = serde_json::to_string(&crate::journal::StoredSpec {
        demand: Demand {
            interface: "echo".into(),
            ..Default::default()
        },
        policy: ExecutionPolicy::default(),
        metadata: Metadata::new(),
    })
    .unwrap();
    for i in 0..260i64 {
        sqlx::query(&format!(
            "INSERT INTO \"{}\".jobs (job_id, namespace, state, spec_hash, spec_json, max_attempts, created_at_ms, updated_at_ms)
             VALUES ($1, 'default', 'succeeded', 'h', $2, 1, $3, $4)",
            schema.schema
        ))
        .bind(format!("pg-j-{i:03}"))
        .bind(&spec_json)
        .bind(1_000 + i)
        .bind(2_000 + i)
        .execute(&pool)
        .await
        .unwrap();
    }
    pool.close().await;

    let page1 = runtime
        .scan_job_metadata(JobScan {
            archived: true,
            from_ms: 500,
            to_ms: 5_000,
            limit: 250,
            cursor: None,
            namespace: None,
        })
        .await
        .unwrap();
    assert_eq!(page1.jobs.len(), 250);
    assert!(page1.next_page_token.is_some());

    let page2 = runtime
        .scan_job_metadata(JobScan {
            archived: true,
            from_ms: 500,
            to_ms: 5_000,
            limit: 250,
            cursor: page1.next_page_token,
            namespace: None,
        })
        .await
        .unwrap();
    assert_eq!(page2.jobs.len(), 10);
    assert!(page2.next_page_token.is_none());

    runtime.shutdown().await;
    schema.drop_schema().await;
}
