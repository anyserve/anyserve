#![cfg(feature = "direct")]
//! Hermetic integration tests exercising the optional `iroh` direct data plane
//! against the REAL `anyserve-core` `Runtime` scheduler and gRPC services
//! (zero mock relay, zero internet / n0 relay access: `disable_relays = true`
//! + loopback `127.0.0.1` + injected `PathClassifier`).

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use anyserve_client::direct::{
    DirectCancel, DirectConfig, DirectEndpoint, DirectError, DirectPath, IrohConnection,
    ObservedPath, PathClassifier,
};
use anyserve_client::execution::{
    Attempt, DirectAttemptError, DirectExecutionEvents, DirectWorkerSession, ExecutionClient,
    WorkerSession, v2,
};
use anyserve_client::{
    DIRECT_CAPABILITY_KEY, DIRECT_CAPABILITY_VALUE, DIRECT_PATH_METADATA_KEY,
    ERROR_CODE_DIRECT_PUBLIC_RELAY_THROTTLED, ERROR_CODE_DIRECT_UNSUPPORTED, is_direct_uri,
};
use anyserve_core::client_service::ClientGrpcService;
use anyserve_core::runtime::Runtime;
use anyserve_core::runtime::api::RuntimeConfig;
use anyserve_core::worker_service::WorkerGrpcService;
use anyserve_proto::v2::client_server::ClientServer;
use bytes::Bytes;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Status};

struct TestRuntimeServer {
    _runtime: Runtime,
    url: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for TestRuntimeServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn start_real_runtime(config: RuntimeConfig) -> TestRuntimeServer {
    let runtime = Runtime::start(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let client = ClientServer::new(ClientGrpcService::new(runtime.clone()));
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
    TestRuntimeServer {
        _runtime: runtime,
        url: format!("http://{addr}"),
        shutdown: Some(tx),
    }
}

fn hermetic_direct_config() -> DirectConfig {
    DirectConfig {
        disable_relays: true,
        bind_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
        threshold_bytes: 1024,
        inline_fallback_limit_bytes: 64 * 1024,
        upgrade_window: Duration::from_millis(50),
        idle_timeout: Duration::from_secs(5),
        ..DirectConfig::default()
    }
}

#[derive(Debug)]
struct AlwaysPublicRelayClassifier;

impl PathClassifier for AlwaysPublicRelayClassifier {
    fn classify(&self, _conn: &IrohConnection, _has_custom_relay: bool) -> ObservedPath {
        ObservedPath::PublicRelayOnly
    }
}

async fn send_credit(control: &tokio::sync::mpsc::Sender<v2::WorkerMessage>, free_slots: u32) {
    control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Credit(v2::Credit { free_slots })),
        })
        .await
        .unwrap();
}

async fn accept_assign(control: &tokio::sync::mpsc::Sender<v2::WorkerMessage>, lease_id: &str) {
    control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                lease_id: lease_id.to_owned(),
                accepted: true,
                reject_reason: String::new(),
            })),
        })
        .await
        .unwrap();
}

async fn consume_direct_events(
    events: &mut DirectExecutionEvents,
) -> Result<(String, v2::ExecutionFinished, Vec<String>), Box<Status>> {
    let mut seen_job_ids = Vec::new();
    while let Some(ev) = events.message().await.map_err(Box::new)? {
        if seen_job_ids.last().map(String::as_str) != Some(ev.job_id.as_str()) {
            seen_job_ids.push(ev.job_id.clone());
        }
        if let Some(v2::execution_event::Event::Finished(fin)) = ev.event {
            return Ok((ev.job_id, fin, seen_job_ids));
        }
    }
    Err(Box::new(Status::unavailable(
        "stream ended without Finished",
    )))
}

#[tokio::test]
async fn real_runtime_direct_routing_metadata_failover_and_capability_negotiation() {
    let server = start_real_runtime(RuntimeConfig::default()).await;

    // 1. Register a NON-direct worker FIRST on "direct.echo" (NO anyserve.direct attribute).
    let mut nd_session = WorkerSession::connect(
        &server.url,
        None,
        v2::Hello {
            worker_id: "real-w-non-direct".into(),
            interfaces: vec!["direct.echo".into()],
            max_concurrency: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nd_limits = match nd_session.messages.message().await.unwrap().unwrap().msg {
        Some(v2::server_message::Msg::Welcome(w)) => w.limits.unwrap(),
        other => panic!("expected welcome, got {other:?}"),
    };
    send_credit(&nd_session.control, 2).await;

    let non_direct_assigns = Arc::new(AtomicUsize::new(0));
    let nd_assigns_task = Arc::clone(&non_direct_assigns);
    let nd_task = tokio::spawn(async move {
        while let Ok(Some(v2::ServerMessage {
            msg: Some(v2::server_message::Msg::Assign(assign)),
        })) = nd_session.messages.message().await
        {
            nd_assigns_task.fetch_add(1, Ordering::SeqCst);
            accept_assign(&nd_session.control, &assign.lease_id).await;
            let _ = Attempt::open(
                &mut nd_session.data,
                &assign,
                &nd_limits,
                Duration::from_secs(5),
            )
            .await;
            send_credit(&nd_session.control, 2).await;
        }
    });

    // 2. Register direct-capable worker #1 via DirectWorkerSession::connect
    //    (automatically advertises Hello.attributes["anyserve.direct"] = "1").
    //    Worker #2 connects only after worker #1 fetches Attempt #1, drains, and
    //    fails retryably — guaranteeing 100% deterministic Attempt #1 -> #2 handoff
    //    on the real anyserve-core scheduler.
    let w1_ep = DirectEndpoint::bind(hermetic_direct_config())
        .await
        .unwrap();
    let mut s1 = DirectWorkerSession::connect(
        &server.url,
        None,
        v2::Hello {
            worker_id: "real-w-direct-1".into(),
            interfaces: vec!["direct.echo".into()],
            max_concurrency: 1,
            ..Default::default()
        },
        Arc::clone(&w1_ep),
    )
    .await
    .unwrap();
    let limits1 = match s1.session.messages.message().await.unwrap().unwrap().msg {
        Some(v2::server_message::Msg::Welcome(w)) => w.limits.unwrap(),
        other => panic!("expected welcome, got {other:?}"),
    };
    send_credit(&s1.session.control, 1).await;

    let w2_ep = DirectEndpoint::bind(hermetic_direct_config())
        .await
        .unwrap();
    let w2_ep_task = Arc::clone(&w2_ep);
    let w2_url = server.url.clone();
    let w2_start_notify = Arc::new(tokio::sync::Notify::new());
    let w2_start_wait = Arc::clone(&w2_start_notify);
    let w2_ready_notify = Arc::new(tokio::sync::Notify::new());
    let w2_ready_signal = Arc::clone(&w2_ready_notify);

    let saw_assign_direct_metadata = Arc::new(AtomicBool::new(false));
    let saw_meta_w1 = Arc::clone(&saw_assign_direct_metadata);
    let w1_fetched = Arc::new(AtomicUsize::new(0));
    let w1_fetched_task = Arc::clone(&w1_fetched);
    let w1_task = tokio::spawn(async move {
        if let Ok(Some(v2::ServerMessage {
            msg: Some(v2::server_message::Msg::Assign(assign)),
        })) = s1.session.messages.message().await
        {
            // Prove metadata["anyserve.direct"] == "1" arrived in real Assign.metadata!
            if assign
                .metadata
                .get(DIRECT_CAPABILITY_KEY)
                .map(String::as_str)
                == Some(DIRECT_CAPABILITY_VALUE)
            {
                saw_meta_w1.store(true, Ordering::SeqCst);
            }
            accept_assign(&s1.session.control, &assign.lease_id).await;
            let mut att = s1
                .open_attempt(
                    &assign,
                    &limits1,
                    Duration::from_secs(5),
                    &DirectCancel::new(),
                )
                .await
                .unwrap();
            assert!(att.client_supports_direct());
            w1_fetched_task.store(att.input_reports()[0].bytes as usize, Ordering::SeqCst);
            // Drain w1 and bring w2 online so Attempt #2 deterministically lands on w2.
            s1.drain(false).await.unwrap();
            w2_start_notify.notify_one();
            w2_ready_notify.notified().await;
            att.finish_failed(v2::ErrorInfo {
                code: "worker_lost".into(),
                message: "simulated crash after fetching direct input".into(),
                retryable: true,
            })
            .await
            .unwrap();
        }
    });

    let w2_task = tokio::spawn(async move {
        w2_start_wait.notified().await;
        let mut s2 = DirectWorkerSession::connect(
            &w2_url,
            None,
            v2::Hello {
                worker_id: "real-w-direct-2".into(),
                interfaces: vec!["direct.echo".into()],
                max_concurrency: 1,
                ..Default::default()
            },
            w2_ep_task,
        )
        .await
        .unwrap();
        let limits2 = match s2.session.messages.message().await.unwrap().unwrap().msg {
            Some(v2::server_message::Msg::Welcome(w)) => w.limits.unwrap(),
            other => panic!("expected welcome, got {other:?}"),
        };
        send_credit(&s2.session.control, 1).await;
        w2_ready_signal.notify_one();

        // Handle 2 assignments: (1) attempt #2 of direct job, (2) legacy non-direct client job.
        for _ in 0..2 {
            let Some(Ok(v2::ServerMessage {
                msg: Some(v2::server_message::Msg::Assign(assign)),
            })) = s2.session.messages.message().await.transpose()
            else {
                break;
            };
            accept_assign(&s2.session.control, &assign.lease_id).await;
            let mut att = s2
                .open_attempt(
                    &assign,
                    &limits2,
                    Duration::from_secs(5),
                    &DirectCancel::new(),
                )
                .await
                .unwrap();
            let input_bytes = match &att.inputs()[0].body {
                Some(v2::object::Body::Inline(b)) => b.clone(),
                other => panic!("expected resolved inline input, got {other:?}"),
            };
            let mut reversed = input_bytes.to_vec();
            reversed.reverse();
            att.finish_succeeded(vec![(Bytes::from(reversed), HashMap::new())])
                .await
                .unwrap();
            send_credit(&s2.session.control, 1).await;
        }
    });

    // Give workers a brief moment to register credits on real Runtime.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Direct client invokes 128 KiB payload through REAL Runtime.
    let mut direct_client = ExecutionClient::connect(&server.url)
        .await
        .unwrap()
        .with_direct(hermetic_direct_config());
    let payload: Vec<u8> = (0..(128 * 1024)).map(|i| (i % 251) as u8).collect();
    let expected_out: Vec<u8> = payload.iter().rev().copied().collect();
    let mut events = direct_client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "direct.echo".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(Bytes::from(payload.clone()))),
                ..Default::default()
            }],
            policy: Some(v2::ExecutionPolicy {
                max_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    let (direct_job_id, fin, _) = consume_direct_events(&mut events).await.unwrap();
    assert_eq!(fin.state, v2::JobState::Succeeded as i32);
    assert_eq!(w1_fetched.load(Ordering::SeqCst), payload.len());
    assert!(
        saw_assign_direct_metadata.load(Ordering::SeqCst),
        "Assign.metadata must contain anyserve.direct=1 from real Runtime"
    );
    assert_eq!(
        non_direct_assigns.load(Ordering::SeqCst),
        0,
        "non-direct worker must NEVER be assigned a direct-input job"
    );
    assert_eq!(events.output_reports().len(), 1);
    assert_eq!(events.output_reports()[0].path, DirectPath::Direct);
    match &fin.outputs[0].body {
        Some(v2::object::Body::Inline(b)) => assert_eq!(b.as_ref(), expected_out.as_slice()),
        other => panic!("expected resolved inline output, got {other:?}"),
    }
    assert_eq!(
        fin.outputs[0]
            .metadata
            .get(DIRECT_PATH_METADATA_KEY)
            .map(String::as_str),
        Some("direct")
    );

    // Inspect the REAL job stored in anyserve-core Runtime:
    // attempt_count == 2 (w1 retryable fail + w2 success), and control plane holds URI (not 128 KiB inline).
    let stored_job = direct_client.get(&direct_job_id, true).await.unwrap();
    assert_eq!(stored_job.attempt_count, 2);
    assert_eq!(stored_job.state, v2::JobState::Succeeded as i32);
    match &stored_job.outputs[0].body {
        Some(v2::object::Body::Uri(u)) => assert!(is_direct_uri(u), "expected direct URI: {u}"),
        other => panic!("expected control plane to hold direct URI, got {other:?}"),
    }

    // 4. Legacy client (no anyserve.direct metadata) calling direct worker w2 -> receives inline output!
    let mut legacy_client = ExecutionClient::connect(&server.url).await.unwrap();
    let legacy_payload = Bytes::from(vec![0x5Au8; 4096]);
    let mut legacy_stream = legacy_client
        .invoke(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "direct.echo".into(),
                required_attributes: [(
                    DIRECT_CAPABILITY_KEY.to_owned(),
                    DIRECT_CAPABILITY_VALUE.to_owned(),
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(legacy_payload.clone())),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let mut legacy_fin = None;
    while let Some(ev) = legacy_stream.message().await.unwrap() {
        if let Some(v2::execution_event::Event::Finished(f)) = ev.event {
            legacy_fin = Some(f);
        }
    }
    let legacy_fin = legacy_fin.unwrap();
    assert_eq!(legacy_fin.state, v2::JobState::Succeeded as i32);
    assert!(
        matches!(&legacy_fin.outputs[0].body, Some(v2::object::Body::Inline(b)) if b.len() == 4096),
        "legacy client must receive inline output from direct worker"
    );

    // 5. Defense-in-depth on a NON-direct-only interface:
    //    (a) Manual request bypassing required_attributes with anyserve+iroh:// URI ->
    //        plain Attempt::open auto-commits non-retryable direct_unsupported,
    //        real Runtime records attempt_count == 1 (even with max_attempts = 3!), handler_calls == 0.
    //    (b) invoke_direct on an interface with NO direct worker online queues until queue_timeout
    //        and NEVER dispatches to the non-direct worker.
    let mut nd_only = WorkerSession::connect(
        &server.url,
        None,
        v2::Hello {
            worker_id: "real-w-nd-only".into(),
            interfaces: vec!["nd.only".into()],
            max_concurrency: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nd_only_limits = match nd_only.messages.message().await.unwrap().unwrap().msg {
        Some(v2::server_message::Msg::Welcome(w)) => w.limits.unwrap(),
        other => panic!("expected welcome, got {other:?}"),
    };
    send_credit(&nd_only.control, 2).await;

    let nd_only_assigns = Arc::new(AtomicUsize::new(0));
    let nd_only_assigns_t = Arc::clone(&nd_only_assigns);
    let nd_only_handlers = Arc::new(AtomicUsize::new(0));
    let nd_only_handlers_t = Arc::clone(&nd_only_handlers);
    let nd_only_task = tokio::spawn(async move {
        while let Ok(Some(v2::ServerMessage {
            msg: Some(v2::server_message::Msg::Assign(assign)),
        })) = nd_only.messages.message().await
        {
            nd_only_assigns_t.fetch_add(1, Ordering::SeqCst);
            accept_assign(&nd_only.control, &assign.lease_id).await;
            // Standard non-direct worker calling plain Attempt::open — zero manual direct checks!
            let Ok(mut att) = Attempt::open(
                &mut nd_only.data,
                &assign,
                &nd_only_limits,
                Duration::from_secs(5),
            )
            .await
            else {
                send_credit(&nd_only.control, 2).await;
                continue;
            };
            nd_only_handlers_t.fetch_add(1, Ordering::SeqCst);
            let _ = att
                .finish(&[], v2::finish::Outcome::Succeeded(v2::Succeeded {}))
                .await;
            send_credit(&nd_only.control, 2).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(40)).await;
    let client_ep = direct_client.direct_endpoint().await.unwrap().unwrap();
    let mut manual_stream = legacy_client
        .invoke(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "nd.only".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Uri(format!(
                    "anyserve+iroh://{}/0123456789abcdef0123456789abcdef",
                    client_ep.id()
                ))),
                ..Default::default()
            }],
            policy: Some(v2::ExecutionPolicy {
                max_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut manual_job_id = String::new();
    let mut manual_fin = None;
    while let Some(ev) = manual_stream.message().await.unwrap() {
        manual_job_id = ev.job_id;
        if let Some(v2::execution_event::Event::Finished(f)) = ev.event {
            manual_fin = Some(f);
        }
    }
    let manual_fin = manual_fin.unwrap();
    assert_eq!(manual_fin.state, v2::JobState::Failed as i32);
    let manual_err = manual_fin.error.unwrap();
    assert_eq!(manual_err.code, ERROR_CODE_DIRECT_UNSUPPORTED);
    assert!(!manual_err.retryable);
    assert_eq!(nd_only_handlers.load(Ordering::SeqCst), 0);
    assert_eq!(nd_only_assigns.load(Ordering::SeqCst), 1);
    let manual_job = legacy_client.get(&manual_job_id, false).await.unwrap();
    assert_eq!(
        manual_job.attempt_count, 1,
        "non-retryable direct_unsupported must consume exactly 1 attempt on real Runtime"
    );

    // 5b. invoke_direct when ONLY non-direct worker is online -> hits queue_timeout (no silent downgrade!).
    let mut qto_events = direct_client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "nd.only".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(Bytes::from(vec![0x7Au8; 4096]))),
                ..Default::default()
            }],
            policy: Some(v2::ExecutionPolicy {
                queue_timeout_ms: 250,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    let (_, qto_fin, _) = consume_direct_events(&mut qto_events).await.unwrap();
    assert_eq!(qto_fin.state, v2::JobState::Failed as i32);
    assert_eq!(
        nd_only_assigns.load(Ordering::SeqCst),
        1,
        "non-direct worker must never receive direct-input job during queue_timeout"
    );
    assert_eq!(nd_only_handlers.load(Ordering::SeqCst), 0);

    nd_task.abort();
    nd_only_task.abort();
    w1_task.await.unwrap();
    w2_task.await.unwrap();
    w1_ep.close().await;
    w2_ep.close().await;
    client_ep.close().await;
}

#[tokio::test]
async fn real_runtime_public_relay_throttled_attempts_fallback_and_output_fail_fast() {
    // Configure real Runtime with 16 KiB max_inline_object_bytes to test verbatim server rejection.
    let mut cfg = RuntimeConfig::default();
    cfg.limits.max_inline_object_bytes = 16 * 1024;
    let server = start_real_runtime(cfg).await;

    // Worker #1 with injected AlwaysPublicRelayClassifier (hermetic, zero internet).
    let w1_ep = DirectEndpoint::bind(DirectConfig {
        path_classifier: Arc::new(AlwaysPublicRelayClassifier),
        ..hermetic_direct_config()
    })
    .await
    .unwrap();
    let mut s1 = DirectWorkerSession::connect(
        &server.url,
        None,
        v2::Hello {
            worker_id: "real-w-throttled".into(),
            interfaces: vec!["throttled.echo".into()],
            max_concurrency: 2,
            ..Default::default()
        },
        Arc::clone(&w1_ep),
    )
    .await
    .unwrap();
    let limits1 = match s1.session.messages.message().await.unwrap().unwrap().msg {
        Some(v2::server_message::Msg::Welcome(w)) => w.limits.unwrap(),
        other => panic!("expected welcome, got {other:?}"),
    };
    send_credit(&s1.session.control, 2).await;

    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls_w = Arc::clone(&handler_calls);
    let saw_direct_on_fallback = Arc::new(AtomicBool::new(false));
    let saw_direct_w = Arc::clone(&saw_direct_on_fallback);
    let w1_task = tokio::spawn(async move {
        for _ in 0..4 {
            let Some(Ok(v2::ServerMessage {
                msg: Some(v2::server_message::Msg::Assign(assign)),
            })) = s1.session.messages.message().await.transpose()
            else {
                break;
            };
            accept_assign(&s1.session.control, &assign.lease_id).await;
            let mut att = match s1
                .open_attempt(
                    &assign,
                    &limits1,
                    Duration::from_secs(5),
                    &DirectCancel::new(),
                )
                .await
            {
                Ok(att) => att,
                Err(DirectAttemptError::Direct(DirectError::PublicRelayThrottled { .. })) => {
                    // open_attempt already committed non-retryable direct_public_relay_throttled!
                    send_credit(&s1.session.control, 2).await;
                    continue;
                }
                Err(other) => panic!("unexpected open_attempt error: {other:?}"),
            };
            handler_calls_w.fetch_add(1, Ordering::SeqCst);
            if att.client_supports_direct() {
                saw_direct_w.store(true, Ordering::SeqCst);
            }
            let data = match &att.inputs()[0].body {
                Some(v2::object::Body::Inline(b)) => b.clone(),
                _ => Bytes::new(),
            };
            att.finish_succeeded(vec![(data, HashMap::new())])
                .await
                .unwrap();
            send_credit(&s1.session.control, 2).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(40)).await;

    let mut client = ExecutionClient::connect(&server.url)
        .await
        .unwrap()
        .with_direct(hermetic_direct_config());

    // Case 1: 4 KiB <= 64 KiB fallback limit & <= 16 KiB server limit, max_attempts = 3
    // -> throttled job records real attempt_count == 1, client falls back once inline and succeeds!
    let small = Bytes::from(vec![0x11u8; 4 * 1024]);
    let mut ev1 = client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "throttled.echo".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(small.clone())),
                ..Default::default()
            }],
            policy: Some(v2::ExecutionPolicy {
                max_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    let (fb_job_id, fin1, _) = consume_direct_events(&mut ev1).await.unwrap();
    assert_eq!(fin1.state, v2::JobState::Succeeded as i32);
    assert!(ev1.fell_back_inline());
    let orig_job_id = ev1
        .fallback_from_job_id()
        .expect("fallback_from_job_id must be populated")
        .to_owned();
    assert_ne!(orig_job_id, fb_job_id);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    assert!(!saw_direct_on_fallback.load(Ordering::SeqCst));
    assert!(matches!(&fin1.outputs[0].body, Some(v2::object::Body::Inline(b)) if b == &small));

    let orig_job = client.get(&orig_job_id, false).await.unwrap();
    assert_eq!(orig_job.state, v2::JobState::Failed as i32);
    assert_eq!(
        orig_job.attempt_count, 1,
        "real Runtime must NOT retry non-retryable direct_public_relay_throttled"
    );
    assert_eq!(
        orig_job.error.unwrap().code,
        ERROR_CODE_DIRECT_PUBLIC_RELAY_THROTTLED
    );

    // Case 2: 20 KiB <= 64 KiB fallback limit BUT > 16 KiB real Runtime max_inline_object_bytes
    // -> server InvalidArgument propagated verbatim, handler NOT called, real attempt_count == 1!
    let medium = Bytes::from(vec![0x22u8; 20 * 1024]);
    let mut ev2 = client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "throttled.echo".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(medium)),
                ..Default::default()
            }],
            policy: Some(v2::ExecutionPolicy {
                max_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    let err2 = consume_direct_events(&mut ev2).await.unwrap_err();
    assert_eq!(err2.code(), Code::InvalidArgument);
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    let case2_orig_id = ev2.fallback_from_job_id().unwrap().to_owned();
    let case2_job = client.get(&case2_orig_id, false).await.unwrap();
    assert_eq!(case2_job.attempt_count, 1);

    // Case 3: 80 KiB > 64 KiB fallback limit with max_attempts = 3
    // -> fails fast (FailedPrecondition), no inline resend, handler NOT called, real attempt_count == 1!
    let large = Bytes::from(vec![0x33u8; 80 * 1024]);
    let mut ev3 = client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "throttled.echo".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(large)),
                ..Default::default()
            }],
            policy: Some(v2::ExecutionPolicy {
                max_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    // Capture job_id from Accepted before terminal error so we can query real Runtime::get!
    let mut case3_job_id = String::new();
    let err3 = loop {
        match ev3.message().await {
            Ok(Some(ev)) => {
                if case3_job_id.is_empty() {
                    case3_job_id = ev.job_id;
                }
            }
            Ok(None) => panic!("expected PublicRelayThrottled error"),
            Err(status) => break status,
        }
    };
    assert_eq!(err3.code(), Code::FailedPrecondition);
    assert!(err3.message().contains("rate-limited public n0 relay"));
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    assert!(!ev3.fell_back_inline());
    let case3_job = client.get(&case3_job_id, false).await.unwrap();
    assert_eq!(case3_job.state, v2::JobState::Failed as i32);
    assert_eq!(
        case3_job.attempt_count, 1,
        "real Runtime must record attempt_count == 1 when max_attempts == 3"
    );

    // Case 4: Output side public-relay-only through REAL Runtime -> fails fast, NO retroactive fallback!
    let w_out_ep = DirectEndpoint::bind(hermetic_direct_config())
        .await
        .unwrap();
    let mut s_out = DirectWorkerSession::connect(
        &server.url,
        None,
        v2::Hello {
            worker_id: "real-w-big-out".into(),
            interfaces: vec!["big.output".into()],
            max_concurrency: 1,
            ..Default::default()
        },
        Arc::clone(&w_out_ep),
    )
    .await
    .unwrap();
    let limits_out = match s_out.session.messages.message().await.unwrap().unwrap().msg {
        Some(v2::server_message::Msg::Welcome(w)) => w.limits.unwrap(),
        other => panic!("expected welcome, got {other:?}"),
    };
    send_credit(&s_out.session.control, 1).await;

    let out_worker_task = tokio::spawn(async move {
        if let Ok(Some(v2::ServerMessage {
            msg: Some(v2::server_message::Msg::Assign(assign)),
        })) = s_out.session.messages.message().await
        {
            accept_assign(&s_out.session.control, &assign.lease_id).await;
            let mut att = s_out
                .open_attempt(
                    &assign,
                    &limits_out,
                    Duration::from_secs(5),
                    &DirectCancel::new(),
                )
                .await
                .unwrap();
            // Worker returns a 32 KiB direct output (>= 1 KiB threshold) to direct client.
            att.finish_succeeded(vec![(Bytes::from(vec![0xEEu8; 32 * 1024]), HashMap::new())])
                .await
                .unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(40)).await;
    let mut throttled_client = ExecutionClient::connect(&server.url)
        .await
        .unwrap()
        .with_direct(DirectConfig {
            path_classifier: Arc::new(AlwaysPublicRelayClassifier),
            ..hermetic_direct_config()
        });
    let mut ev4 = throttled_client
        .invoke_direct(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "big.output".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Inline(Bytes::from_static(b"tiny"))),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let err4 = consume_direct_events(&mut ev4).await.unwrap_err();
    assert_eq!(err4.code(), Code::FailedPrecondition);
    assert!(
        err4.message().contains("rate-limited public n0 relay"),
        "expected output-side public relay fast-fail, got: {}",
        err4.message()
    );
    assert!(!ev4.fell_back_inline());

    w1_task.await.unwrap();
    out_worker_task.await.unwrap();
    w1_ep.close().await;
    w_out_ep.close().await;
    client
        .direct_endpoint()
        .await
        .unwrap()
        .unwrap()
        .close()
        .await;
    throttled_client
        .direct_endpoint()
        .await
        .unwrap()
        .unwrap()
        .close()
        .await;
}

#[tokio::test]
async fn real_runtime_open_stream_and_input_chunk_reject_direct_uris_as_direct_unsupported() {
    // Part A: default assign_inline_limit -> anyserve+iroh:// arrives in assignment.inline_inputs;
    //         Attempt::open_stream must commit non-retryable direct_unsupported pre-handler!
    let server_inline = start_real_runtime(RuntimeConfig::default()).await;
    let mut s_inline = WorkerSession::connect(
        &server_inline.url,
        None,
        v2::Hello {
            worker_id: "stream-w-inline".into(),
            interfaces: vec!["stream.direct.inline".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let limits_inline = match s_inline.messages.message().await.unwrap().unwrap().msg {
        Some(v2::server_message::Msg::Welcome(w)) => w.limits.unwrap(),
        other => panic!("expected welcome, got {other:?}"),
    };
    send_credit(&s_inline.control, 1).await;

    let inline_handler_calls = Arc::new(AtomicUsize::new(0));
    let inline_handlers_t = Arc::clone(&inline_handler_calls);
    let w_inline_task = tokio::spawn(async move {
        if let Ok(Some(v2::ServerMessage {
            msg: Some(v2::server_message::Msg::Assign(assign)),
        })) = s_inline.messages.message().await
        {
            assert!(
                !assign.inputs_via_attempt,
                "expected URI object in assignment.inline_inputs"
            );
            accept_assign(&s_inline.control, &assign.lease_id).await;
            let _att = match Attempt::open_stream(
                &mut s_inline.data,
                &assign,
                &limits_inline,
                Duration::from_secs(5),
            )
            .await
            {
                Ok(att) => att,
                Err(status) => {
                    assert_eq!(status.code(), Code::FailedPrecondition);
                    assert!(status.message().contains("direct_unsupported"), "{status}");
                    return;
                }
            };
            inline_handlers_t.fetch_add(1, Ordering::SeqCst);
        }
    });

    tokio::time::sleep(Duration::from_millis(40)).await;
    let mut c_inline = ExecutionClient::connect(&server_inline.url).await.unwrap();
    let mut st_inline = c_inline
        .invoke(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "stream.direct.inline".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Uri(
                    "anyserve+iroh://0000000000000000000000000000000000000000000000000000000000000001/0123456789abcdef0123456789abcdef".into(),
                )),
                ..Default::default()
            }],
            policy: Some(v2::ExecutionPolicy {
                max_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut inline_job_id = String::new();
    let mut inline_fin = None;
    while let Some(ev) = st_inline.message().await.unwrap() {
        inline_job_id = ev.job_id;
        if let Some(v2::execution_event::Event::Finished(f)) = ev.event {
            inline_fin = Some(f);
        }
    }
    let inline_fin = inline_fin.unwrap();
    assert_eq!(inline_fin.state, v2::JobState::Failed as i32);
    let inline_err = inline_fin.error.unwrap();
    assert_eq!(inline_err.code, ERROR_CODE_DIRECT_UNSUPPORTED);
    assert!(!inline_err.retryable);
    assert_eq!(inline_handler_calls.load(Ordering::SeqCst), 0);
    let inline_job = c_inline.get(&inline_job_id, false).await.unwrap();
    assert_eq!(
        inline_job.attempt_count, 1,
        "open_stream direct_unsupported must consume exactly 1 attempt even with max_attempts=3"
    );
    w_inline_task.await.unwrap();

    // Part B: assign_inline_limit = 1 -> real Runtime sets inputs_via_attempt = true and delivers
    //         anyserve+iroh:// via InputData.uri; Attempt::input_chunk must reject it as non-retryable
    //         direct_unsupported!
    let mut cfg_stream = RuntimeConfig::default();
    cfg_stream.limits.assign_inline_limit = 1;
    let server_chunk = start_real_runtime(cfg_stream).await;
    let mut s_chunk = WorkerSession::connect(
        &server_chunk.url,
        None,
        v2::Hello {
            worker_id: "stream-w-chunk".into(),
            interfaces: vec!["stream.direct.chunk".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let limits_chunk = match s_chunk.messages.message().await.unwrap().unwrap().msg {
        Some(v2::server_message::Msg::Welcome(w)) => w.limits.unwrap(),
        other => panic!("expected welcome, got {other:?}"),
    };
    send_credit(&s_chunk.control, 1).await;

    let chunk_handler_calls = Arc::new(AtomicUsize::new(0));
    let chunk_handlers_t = Arc::clone(&chunk_handler_calls);
    let w_chunk_task = tokio::spawn(async move {
        if let Ok(Some(v2::ServerMessage {
            msg: Some(v2::server_message::Msg::Assign(assign)),
        })) = s_chunk.messages.message().await
        {
            assert!(
                assign.inputs_via_attempt,
                "assign_inline_limit=1 must force URI delivery over Attempt stream (InputData.uri)"
            );
            accept_assign(&s_chunk.control, &assign.lease_id).await;
            let mut att = Attempt::open_stream(
                &mut s_chunk.data,
                &assign,
                &limits_chunk,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            match att.input_chunk().await {
                Ok(_) => {
                    chunk_handlers_t.fetch_add(1, Ordering::SeqCst);
                }
                Err(status) => {
                    assert_eq!(status.code(), Code::FailedPrecondition);
                    assert!(status.message().contains("direct_unsupported"), "{status}");
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(40)).await;
    let mut c_chunk = ExecutionClient::connect(&server_chunk.url).await.unwrap();
    let mut st_chunk = c_chunk
        .invoke(v2::JobSpec {
            demand: Some(v2::Demand {
                interface: "stream.direct.chunk".into(),
                ..Default::default()
            }),
            inputs: vec![v2::Object {
                body: Some(v2::object::Body::Uri(
                    "anyserve+iroh://0000000000000000000000000000000000000000000000000000000000000001/fedcba9876543210fedcba9876543210".into(),
                )),
                ..Default::default()
            }],
            policy: Some(v2::ExecutionPolicy {
                max_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut chunk_job_id = String::new();
    let mut chunk_fin = None;
    while let Some(ev) = st_chunk.message().await.unwrap() {
        chunk_job_id = ev.job_id;
        if let Some(v2::execution_event::Event::Finished(f)) = ev.event {
            chunk_fin = Some(f);
        }
    }
    let chunk_fin = chunk_fin.unwrap();
    assert_eq!(chunk_fin.state, v2::JobState::Failed as i32);
    let chunk_err = chunk_fin.error.unwrap();
    assert_eq!(chunk_err.code, ERROR_CODE_DIRECT_UNSUPPORTED);
    assert!(!chunk_err.retryable);
    assert_eq!(chunk_handler_calls.load(Ordering::SeqCst), 0);
    let chunk_job = c_chunk.get(&chunk_job_id, false).await.unwrap();
    assert_eq!(
        chunk_job.attempt_count, 1,
        "input_chunk direct_unsupported must consume exactly 1 attempt even with max_attempts=3"
    );
    w_chunk_task.await.unwrap();
}
