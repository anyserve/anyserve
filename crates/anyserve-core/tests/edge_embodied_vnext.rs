//! Integration tests for Anyserve vNext Edge / Embodied reliability, multi-tenancy,
//! hierarchical pool scheduling, DRR fairness, concurrency limits, affinity routing, and autoscaling.

use std::collections::{BTreeMap, HashMap};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use anyserve_core::client_service::ClientGrpcService;
use anyserve_core::runtime::Runtime;
use anyserve_core::runtime::api::*;
use anyserve_core::worker_service::WorkerGrpcService;
use anyserve_proto::v2::client_server::ClientServer;
use anyserve_proto::v2::{self as pb};
use bytes::Bytes;
use futures::StreamExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;

struct TestServer {
    runtime: Runtime,
    url: String,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn serve(config: RuntimeConfig) -> TestServer {
    let runtime = Runtime::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let client = ClientServer::new(ClientGrpcService::new(runtime.clone()));
    let worker = WorkerGrpcService::server(runtime.clone());
    tokio::spawn(async move {
        Server::builder()
            .add_service(client)
            .add_service(worker)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = rx.await;
            })
            .await
            .unwrap();
    });
    TestServer {
        runtime,
        url: format!("http://{addr}"),
        _shutdown: tx,
    }
}

struct TestWorker {
    control: tokio::sync::mpsc::Sender<pb::WorkerMessage>,
    messages: tonic::Streaming<pb::ServerMessage>,
    client: pb::worker_client::WorkerClient<tonic::transport::Channel>,
}

impl TestWorker {
    async fn connect(
        url: &str,
        worker_id: &str,
        pool: &str,
        interfaces: &[&str],
        attributes: &[(&str, &str)],
        max_concurrency: u32,
        namespace: Option<&str>,
    ) -> Self {
        let channel = tonic::transport::Endpoint::from_shared(url.to_string())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut control_client = pb::worker_client::WorkerClient::new(channel.clone());
        let (control, requests) = tokio::sync::mpsc::channel(64);
        control
            .send(pb::WorkerMessage {
                msg: Some(pb::worker_message::Msg::Hello(pb::Hello {
                    worker_id: worker_id.to_string(),
                    pool: pool.to_string(),
                    interfaces: interfaces.iter().map(|s| (*s).to_string()).collect(),
                    attributes: attributes
                        .iter()
                        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                        .collect(),
                    max_concurrency,
                    ..Default::default()
                })),
            })
            .await
            .unwrap();
        let mut req = tonic::Request::new(ReceiverStream::new(requests));
        if let Some(ns) = namespace {
            req.metadata_mut()
                .insert("x-anyserve-namespace", ns.parse().unwrap());
        }
        let mut messages = control_client.session(req).await.unwrap().into_inner();
        let first = messages.message().await.unwrap().unwrap();
        assert!(matches!(
            first.msg,
            Some(pb::server_message::Msg::Welcome(_))
        ));
        Self {
            control,
            messages,
            client: pb::worker_client::WorkerClient::new(channel),
        }
    }

    async fn next_assign(&mut self) -> pb::Assign {
        loop {
            let msg = self.messages.message().await.unwrap().unwrap();
            if let Some(pb::server_message::Msg::Assign(assign)) = msg.msg {
                return assign;
            }
        }
    }

    async fn next_server_msg(&mut self) -> pb::server_message::Msg {
        self.messages.message().await.unwrap().unwrap().msg.unwrap()
    }

    async fn reply(&mut self, lease_id: &str, accepted: bool, reason: &str) {
        self.control
            .send(pb::WorkerMessage {
                msg: Some(pb::worker_message::Msg::AssignReply(pb::AssignReply {
                    lease_id: lease_id.to_string(),
                    accepted,
                    reject_reason: reason.to_string(),
                })),
            })
            .await
            .unwrap();
    }

    async fn send_credit(&mut self, free_slots: u32) {
        self.control
            .send(pb::WorkerMessage {
                msg: Some(pb::worker_message::Msg::Credit(pb::Credit { free_slots })),
            })
            .await
            .unwrap();
    }

    async fn update_attributes(
        &mut self,
        attributes: Vec<(&str, &str)>,
        capacity: Vec<(&str, i64)>,
        pool: &str,
    ) {
        self.control
            .send(pb::WorkerMessage {
                msg: Some(pb::worker_message::Msg::UpdateAttributes(
                    pb::UpdateAttributes {
                        attributes: attributes
                            .into_iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect(),
                        total_capacity: capacity
                            .into_iter()
                            .map(|(k, v)| (k.to_string(), v))
                            .collect(),
                        pool: pool.to_string(),
                    },
                )),
            })
            .await
            .unwrap();
    }

    async fn finish_inline(&mut self, assign: &pb::Assign, payload: &[u8]) {
        self.reply(&assign.lease_id, true, "").await;
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tx.send(pb::AttemptUp {
            msg: Some(pb::attempt_up::Msg::Open(pb::AttemptOpen {
                lease_id: assign.lease_id.clone(),
                generation: assign.generation,
                resume_after_sequence: 0,
            })),
        })
        .await
        .unwrap();
        let mut down = self
            .client
            .attempt(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let opened = down.message().await.unwrap().unwrap();
        assert!(matches!(opened.msg, Some(pb::attempt_down::Msg::Opened(_))));
        tx.send(pb::AttemptUp {
            msg: Some(pb::attempt_up::Msg::Result(pb::ResultChunk {
                index: 0,
                data: Bytes::copy_from_slice(payload),
                last: true,
                ..Default::default()
            })),
        })
        .await
        .unwrap();
        tx.send(pb::AttemptUp {
            msg: Some(pb::attempt_up::Msg::Finish(pb::Finish {
                outcome: Some(pb::finish::Outcome::Succeeded(pb::Succeeded {})),
            })),
        })
        .await
        .unwrap();
        let finished = loop {
            let msg = down.message().await.unwrap().unwrap().msg;
            if let Some(pb::attempt_down::Msg::Finished(f)) = msg {
                break f;
            }
        };
        assert_eq!(
            finished.status,
            pb::attempt_finished::Status::Committed as i32
        );
        self.send_credit(1).await;
    }

    async fn fail_retryable(&mut self, assign: &pb::Assign, msg: &str) {
        self.reply(&assign.lease_id, true, "").await;
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tx.send(pb::AttemptUp {
            msg: Some(pb::attempt_up::Msg::Open(pb::AttemptOpen {
                lease_id: assign.lease_id.clone(),
                generation: assign.generation,
                resume_after_sequence: 0,
            })),
        })
        .await
        .unwrap();
        let mut down = self
            .client
            .attempt(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let _ = down.message().await.unwrap().unwrap();
        tx.send(pb::AttemptUp {
            msg: Some(pb::attempt_up::Msg::Finish(pb::Finish {
                outcome: Some(pb::finish::Outcome::Failed(pb::ErrorInfo {
                    code: "worker_failed".into(),
                    message: msg.into(),
                    retryable: true,
                })),
            })),
        })
        .await
        .unwrap();
        loop {
            let msg = down.message().await.unwrap().unwrap().msg;
            if matches!(msg, Some(pb::attempt_down::Msg::Finished(_))) {
                break;
            }
        }
        self.send_credit(1).await;
    }
}

fn edge_spec(interface: &str) -> JobSpec {
    let mut spec = JobSpec::default();
    spec.demand.interface = interface.to_string();
    spec.policy.durability = Some(Durability::Ephemeral);
    spec.inputs.push(Object::inline(Bytes::from_static(b"req")));
    spec
}

#[tokio::test]
async fn phase0_flaky_worker_avoidance_achieves_40_of_40() {
    let config = RuntimeConfig {
        default_initial_backoff: Duration::from_millis(2),
        default_max_backoff: Duration::from_millis(10),
        default_max_attempts: 3,
        ..Default::default()
    };
    let server = serve(config).await;

    let mut bad = TestWorker::connect(&server.url, "bad-worker", "", &["vla"], &[], 1, None).await;
    let good1 = TestWorker::connect(&server.url, "good-worker-1", "", &["vla"], &[], 1, None).await;
    let good2 = TestWorker::connect(&server.url, "good-worker-2", "", &["vla"], &[], 1, None).await;

    tokio::spawn(async move {
        loop {
            let assign = tokio::select! {
                msg = bad.messages.message() => match msg {
                    Ok(Some(m)) => match m.msg {
                        Some(pb::server_message::Msg::Assign(a)) => a,
                        _ => continue,
                    },
                    _ => break,
                }
            };
            bad.fail_retryable(&assign, "cuda oom").await;
        }
    });
    for mut good in [good1, good2] {
        tokio::spawn(async move {
            while let Ok(Some(m)) = good.messages.message().await {
                if let Some(pb::server_message::Msg::Assign(a)) = m.msg {
                    good.finish_inline(&a, b"ok").await;
                }
            }
        });
    }

    let mut ids = Vec::new();
    for _ in 0..40 {
        let res = server
            .runtime
            .submit(SubmitRequest {
                spec: edge_spec("vla"),
                ..Default::default()
            })
            .await
            .unwrap();
        ids.push(res.job.job_id);
    }

    for id in ids {
        let mut events = server.runtime.watch(&id, 0).unwrap();
        while let Some(ev) = events.next().await {
            let ev = ev.unwrap();
            if let EventKind::Finished { state, error, .. } = ev.kind {
                assert_eq!(state, JobState::Succeeded, "job {id} failed: {error:?}");
                break;
            }
        }
    }
}

#[tokio::test]
async fn phase0_fast_reconnect_grace_and_preemptive_drain() {
    let mut interfaces = BTreeMap::new();
    interfaces.insert(
        "llm".to_string(),
        InterfacePolicy {
            pool_tiers: vec![
                PoolTier {
                    pool: "spot".into(),
                    spillover_after: Duration::ZERO,
                    reconnect_grace: Some(Duration::from_millis(40)),
                    idle_drain_after: None,
                    min_sessions: 0,
                },
                PoolTier {
                    pool: "cloud".into(),
                    spillover_after: Duration::from_millis(80),
                    reconnect_grace: Some(Duration::from_millis(40)),
                    idle_drain_after: None,
                    min_sessions: 0,
                },
            ],
            ..Default::default()
        },
    );
    let server = serve(RuntimeConfig {
        heartbeat_timeout: Duration::from_secs(30),
        default_initial_backoff: Duration::from_millis(5),
        default_max_backoff: Duration::from_millis(20),
        interfaces,
        ..Default::default()
    })
    .await;

    // 1. Preemptive drain triggers immediate zero-backoff reschedule onto healthy cloud worker.
    let mut spot = TestWorker::connect(&server.url, "spot-1", "spot", &["llm"], &[], 1, None).await;
    let mut cloud =
        TestWorker::connect(&server.url, "cloud-1", "cloud", &["llm"], &[], 1, None).await;
    let mut spec = edge_spec("llm");
    spec.policy.priority = 1;
    let job = server
        .runtime
        .submit(SubmitRequest {
            spec,
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
    let assign1 = spot.next_assign().await;
    assert_eq!(assign1.job_id, job.job_id);
    spot.reply(&assign1.lease_id, true, "").await;
    spot.control
        .send(pb::WorkerMessage {
            msg: Some(pb::worker_message::Msg::Draining(pb::Draining {
                preempting: true,
            })),
        })
        .await
        .unwrap();
    let cancel_msg = spot.next_server_msg().await;
    assert!(matches!(cancel_msg, pb::server_message::Msg::Cancel(_)));

    let assign2 = tokio::time::timeout(Duration::from_millis(500), cloud.next_assign())
        .await
        .expect("preemptive reschedule should happen immediately");
    assert_eq!(assign2.job_id, job.job_id);
    cloud.finish_inline(&assign2, b"done").await;
    let done_view = server.runtime.get_job(&job.job_id, true).await.unwrap();
    assert_eq!(done_view.state, JobState::Succeeded);

    // 2. Pool-level 40ms reconnect_grace triggers failover in <400ms instead of 30s heartbeat_timeout.
    let mut spot2 =
        TestWorker::connect(&server.url, "spot-2", "spot", &["llm"], &[], 1, None).await;
    let job2 = server
        .runtime
        .submit(SubmitRequest {
            spec: edge_spec("llm"),
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
    let a = spot2.next_assign().await;
    spot2.reply(&a.lease_id, true, "").await;
    drop(spot2);

    let assign3 = tokio::time::timeout(Duration::from_millis(600), cloud.next_assign())
        .await
        .expect("pool reconnect_grace 40ms should expire crash rapidly");
    assert_eq!(assign3.job_id, job2.job_id);
    cloud.finish_inline(&assign3, b"recovered").await;
}

#[tokio::test]
async fn phase1a_namespace_isolation_quotas_telemetry_and_uri_digest() {
    struct TestQuota;
    impl QuotaPolicy for TestQuota {
        fn check_admission(
            &self,
            namespace: &str,
            _spec: &JobSpec,
        ) -> Result<QuotaDecision, anyserve_core::error::RuntimeError> {
            if namespace == "alpha" {
                Ok(QuotaDecision {
                    max_queued_jobs: Some(1),
                    max_in_flight_jobs: Some(1),
                    ..Default::default()
                })
            } else {
                Ok(QuotaDecision::default())
            }
        }
    }

    #[derive(Default)]
    struct TestSink(Mutex<Vec<BillingEvent>>);
    impl TelemetrySink for TestSink {
        fn emit(&self, event: BillingEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    let sink = Arc::new(TestSink::default());
    let server = serve(RuntimeConfig {
        hooks: RuntimeHooks {
            quota: Some(Arc::new(TestQuota)),
            telemetry: Some(sink.clone()),
        },
        ..Default::default()
    })
    .await;

    // Content digest deduplicates ephemeral-style transient URLs, excluding scheduling hints.
    let mut s1 = edge_spec("vision");
    let mut meta1 = BTreeMap::new();
    meta1.insert(OBJECT_SHA256_METADATA_KEY.into(), "deadbeef".into());
    meta1.insert(OBJECT_SIZE_METADATA_KEY.into(), "1234".into());
    s1.inputs = vec![Object::Uri {
        uri: "https://cdn.example/frame.png?sig=first".into(),
        metadata: meta1,
    }];
    s1.demand.affinity_key = "robot-1".into();
    s1.policy.fairness_key = "high".into();
    s1.policy.concurrency_key = "arm-1".into();

    let mut s2 = edge_spec("vision");
    let mut meta2 = BTreeMap::new();
    meta2.insert(OBJECT_SHA256_METADATA_KEY.into(), "deadbeef".into());
    meta2.insert(OBJECT_SIZE_METADATA_KEY.into(), "1234".into());
    s2.inputs = vec![Object::Uri {
        uri: "https://cdn.example/frame.png?sig=second".into(),
        metadata: meta2,
    }];
    s2.demand.affinity_key = "robot-2".into();
    s2.policy.fairness_key = "low".into();
    s2.policy.concurrency_key = "arm-2".into();

    let r1 = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("mission-1".into()),
            namespace: Some("alpha".into()),
            conflict: ConflictPolicy::UseExisting,
            spec: s1.clone(),
        })
        .await
        .unwrap();
    assert!(r1.created);
    assert_eq!(r1.job.namespace, "alpha");
    assert_eq!(r1.job.job_id, "mission-1");

    // Same job_id in alpha with different URL but identical SHA256 digest & different hints -> idempotent!
    let r1_dup = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("mission-1".into()),
            namespace: Some("alpha".into()),
            conflict: ConflictPolicy::UseExisting,
            spec: s2,
        })
        .await
        .unwrap();
    assert!(!r1_dup.created);

    // Alpha quota allows max 1 queued job -> second distinct job fails with ResourceExhausted.
    let err = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("mission-2".into()),
            namespace: Some("alpha".into()),
            conflict: ConflictPolicy::Fail,
            spec: s1.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.kind, anyserve_core::error::ErrorKind::ResourceExhausted);

    // Beta uses the exact same caller job_id "mission-1" independently with zero collision!
    let r2 = server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("mission-1".into()),
            namespace: Some("beta".into()),
            conflict: ConflictPolicy::Fail,
            spec: s1,
        })
        .await
        .unwrap();
    assert!(r2.created);
    assert_eq!(r2.job.namespace, "beta");
    assert_eq!(r2.job.job_id, "mission-1");

    // Namespace isolation on list_jobs and workers.
    let alpha_jobs = server
        .runtime
        .list_jobs(JobFilter {
            namespace: Some("alpha".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(alpha_jobs.jobs.len(), 1);
    let mut w_alpha =
        TestWorker::connect(&server.url, "w-a", "", &["vision"], &[], 1, Some("alpha")).await;
    let assign_a = w_alpha.next_assign().await;
    assert_eq!(assign_a.job_id, "mission-1");
    w_alpha.finish_inline(&assign_a, b"alpha-out").await;

    // Beta job is still queued, untouched by alpha's worker.
    let beta_view = server
        .runtime
        .get_job_in("beta", "mission-1", false)
        .await
        .unwrap();
    assert_eq!(beta_view.state, JobState::Queued);

    // Verify telemetry sink observed JobAccepted and AttemptFinished outside lock.
    let events = sink.0.lock().unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        BillingEvent::JobAccepted { namespace, job_id, .. }
        if namespace == "alpha" && job_id == "mission-1"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        BillingEvent::AttemptFinished { namespace, job_id, state: JobState::Succeeded, .. }
        if namespace == "alpha" && job_id == "mission-1"
    )));
}

#[tokio::test]
async fn phase1b_three_tier_pool_waterfall_and_update_attributes() {
    let mut interfaces = BTreeMap::new();
    interfaces.insert(
        "vla".to_string(),
        InterfacePolicy {
            pool_tiers: vec![
                PoolTier {
                    pool: "jetson-local".into(),
                    spillover_after: Duration::ZERO,
                    ..Default::default()
                },
                PoolTier {
                    pool: "site-rack".into(),
                    spillover_after: Duration::from_millis(80),
                    ..Default::default()
                },
                PoolTier {
                    pool: "cloud-burst".into(),
                    spillover_after: Duration::from_millis(160),
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
    );
    let server = serve(RuntimeConfig {
        interfaces,
        ..Default::default()
    })
    .await;

    // Connect rack & cloud workers first; jobs stay waiting for Tier 0 until spillover elapses.
    let mut cloud = TestWorker::connect(
        &server.url,
        "w-cloud",
        "cloud-burst",
        &["vla"],
        &[("model", "vla-v2")],
        2,
        None,
    )
    .await;
    let mut rack = TestWorker::connect(
        &server.url,
        "w-rack",
        "site-rack",
        &["vla"],
        &[("model", "vla-v1")],
        2,
        None,
    )
    .await;

    let mut s = edge_spec("vla");
    s.demand
        .required_attributes
        .insert("model".into(), "vla-v2".into());
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("tier-job".into()),
            spec: s.clone(),
            ..Default::default()
        })
        .await
        .unwrap();

    // UpdateAttributes upgrades rack worker model to vla-v2 and moves into jetson-local tier 0 -> immediate match!
    rack.update_attributes(vec![("model", "vla-v2")], vec![], "jetson-local")
        .await;
    let a = tokio::time::timeout(Duration::from_millis(50), rack.next_assign())
        .await
        .expect("tier 0 after UpdateAttributes should match immediately");
    assert_eq!(a.job_id, "tier-job");
    rack.finish_inline(&a, b"ok").await;

    // Now test natural spillover from Tier 0 (absent) -> Tier 1 (site-rack @80ms) -> Tier 2 (cloud-burst @160ms).
    let mut rack_only = TestWorker::connect(
        &server.url,
        "w-rack-only",
        "site-rack",
        &["vla"],
        &[("model", "vla-v2")],
        1,
        None,
    )
    .await;
    // Occupy rack slots; move original rack away from model=vla-v2 so Tier 1 handles one and Tier 2 handles next.
    rack.update_attributes(vec![("model", "other")], vec![], "site-rack")
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("j-tier1".into()),
            spec: s.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let a1 = tokio::time::timeout(Duration::from_millis(250), rack_only.next_assign())
        .await
        .expect("should spill to Tier 1 at 80ms before Tier 2 at 160ms");
    assert_eq!(a1.job_id, "j-tier1");
    // Hold a1 busy on rack_only, submit j-tier2 which spills all the way to cloud-burst at 160ms.
    server
        .runtime
        .submit(SubmitRequest {
            job_id: Some("j-tier2".into()),
            spec: s,
            ..Default::default()
        })
        .await
        .unwrap();
    let a2 = tokio::time::timeout(Duration::from_millis(400), cloud.next_assign())
        .await
        .expect("should spill to Tier 2 at 160ms");
    assert_eq!(a2.job_id, "j-tier2");
    cloud.finish_inline(&a2, b"ok").await;
    rack_only.finish_inline(&a1, b"ok").await;
}

#[tokio::test]
async fn phase1c_affinity_drr_concurrency_key_and_idle_drain() {
    let mut interfaces = BTreeMap::new();
    interfaces.insert(
        "control".to_string(),
        InterfacePolicy {
            max_in_flight_per_key: Some(1),
            pool_tiers: vec![PoolTier {
                pool: "cloud-burst".into(),
                spillover_after: Duration::ZERO,
                reconnect_grace: None,
                idle_drain_after: Some(Duration::from_millis(80)),
                min_sessions: 1,
            }],
            ..Default::default()
        },
    );
    let server = serve(RuntimeConfig {
        interfaces,
        ..Default::default()
    })
    .await;

    // 1. Concurrency-key serialization across 3 free workers + PoolStats desired slots check.
    let w1 = TestWorker::connect(
        &server.url,
        "ctrl-1",
        "cloud-burst",
        &["control"],
        &[],
        4,
        None,
    )
    .await;
    let w2 = TestWorker::connect(
        &server.url,
        "ctrl-2",
        "cloud-burst",
        &["control"],
        &[],
        4,
        None,
    )
    .await;
    let w3 = TestWorker::connect(
        &server.url,
        "ctrl-3",
        "cloud-burst",
        &["control"],
        &[],
        4,
        None,
    )
    .await;

    let mut arm_spec = edge_spec("control");
    arm_spec.policy.concurrency_key = "arm-left".into();
    for i in 0..3 {
        server
            .runtime
            .submit(SubmitRequest {
                job_id: Some(format!("arm-{i}")),
                spec: arm_spec.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    // Check that ONLY 1 arm job is dispatched concurrently while 2 stay queued.
    tokio::time::sleep(Duration::from_millis(40)).await;
    let stats = server.runtime.stats();
    assert_eq!(stats.running + stats.queued, 3);
    assert_eq!(stats.queued, 2);

    // Finish the active arm jobs sequentially across whichever worker received them.
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    for mut w in [w1, w2, w3] {
        let active = Arc::clone(&active);
        let max_active = Arc::clone(&max_active);
        tokio::spawn(async move {
            while let Ok(Some(m)) = w.messages.message().await {
                if let Some(pb::server_message::Msg::Assign(a)) = m.msg {
                    let prev = active.fetch_add(1, Ordering::SeqCst);
                    max_active.fetch_max(prev + 1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    w.finish_inline(&a, b"ok").await;
                }
            }
        });
    }
    for i in 0..3 {
        let mut evs = server.runtime.watch(&format!("arm-{i}"), 0).unwrap();
        while let Some(ev) = evs.next().await {
            if matches!(ev.unwrap().kind, EventKind::Finished { .. }) {
                break;
            }
        }
    }
    assert_eq!(max_active.load(Ordering::SeqCst), 1);

    // 2. Idle scale-down sends Drain to excess sessions while keeping min_sessions = 1 undrained.
    tokio::time::sleep(Duration::from_millis(1250)).await;
    let workers = server.runtime.list_workers();
    let drained_count = workers.iter().filter(|w| w.draining).count();
    let undrained_count = workers.iter().filter(|w| !w.draining).count();
    assert_eq!(drained_count, 2, "expected 2 excess idle workers drained");
    assert_eq!(undrained_count, 1, "expected min_sessions=1 kept undrained");

    // 3. Affinity routing + overload spillover + DRR fairness_key anti-starvation on a fresh server.
    let server2 = serve(RuntimeConfig::default()).await;
    let mut a1 = TestWorker::connect(&server2.url, "sticky-a", "", &["chat"], &[], 2, None).await;
    let mut a2 = TestWorker::connect(&server2.url, "sticky-b", "", &["chat"], &[], 2, None).await;
    let mut sticky = edge_spec("chat");
    sticky.demand.affinity_key = "kv-session-7".into();
    let j1 = server2
        .runtime
        .submit(SubmitRequest {
            spec: sticky.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
    let j2 = server2
        .runtime
        .submit(SubmitRequest {
            spec: sticky.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .job;
    let j3 = server2
        .runtime
        .submit(SubmitRequest {
            spec: sticky,
            ..Default::default()
        })
        .await
        .unwrap()
        .job;

    tokio::time::sleep(Duration::from_millis(40)).await;
    let workers2 = server2.runtime.list_workers();
    let counts: HashMap<&str, u32> = workers2
        .iter()
        .map(|w| (w.worker_id.as_str(), w.active_leases))
        .collect();
    // Primary receives 2 (<0.85 threshold -> 2/2 reaches >=0.85 -> 3rd spills to secondary!).
    assert_eq!(counts.values().copied().sum::<u32>(), 3);
    assert!(counts.values().any(|&c| c == 2));
    assert!(counts.values().any(|&c| c == 1));
    let _ = (j1, j2, j3, &mut a1, &mut a2);

    // 4. DRR fairness_key: 20 bulk jobs queued + 1 interactive job on concurrency-1 worker.
    let server3 = serve(RuntimeConfig::default()).await;
    let mut bulk_spec = edge_spec("infer");
    bulk_spec.policy.fairness_key = "bulk".into();
    for i in 0..20 {
        server3
            .runtime
            .submit(SubmitRequest {
                job_id: Some(format!("bulk-{i:02}")),
                spec: bulk_spec.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    let mut fast_spec = edge_spec("infer");
    fast_spec.policy.fairness_key = "robot-interactive".into();
    server3
        .runtime
        .submit(SubmitRequest {
            job_id: Some("robot-1".into()),
            spec: fast_spec,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut slow_w =
        TestWorker::connect(&server3.url, "infer-1", "", &["infer"], &[], 1, None).await;
    let first = slow_w.next_assign().await;
    slow_w.finish_inline(&first, b"1").await;
    let second = slow_w.next_assign().await;
    slow_w.finish_inline(&second, b"2").await;
    assert!(
        first.job_id == "robot-1" || second.job_id == "robot-1",
        "DRR must serve interactive fairness_key within first round, got {}, {}",
        first.job_id,
        second.job_id
    );
}
