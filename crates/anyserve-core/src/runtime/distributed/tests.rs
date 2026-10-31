use std::collections::HashSet;
use std::time::Duration;

use anyserve_proto::v2 as pb;
use bytes::Bytes;
use futures::{StreamExt, future::join_all};
use sqlx::PgPool;

use crate::error::ErrorKind;
use crate::runtime::Runtime;
use crate::runtime::api::*;
use crate::runtime::state::FinishStatus;
use crate::runtime::worker::{AttemptLink, AttemptOutcome, SessionHandle};

struct Cluster {
    a: Runtime,
    b: Runtime,
    admin: PgPool,
    schema: String,
}

impl Cluster {
    async fn start() -> Self {
        let url = std::env::var("ANYSERVE_TEST_POSTGRES_URL")
            .expect("set ANYSERVE_TEST_POSTGRES_URL to a disposable PostgreSQL database");
        let schema = format!("test_{}", uuid::Uuid::new_v4().simple());
        let mut journal = JournalConfig::postgres(&url);
        let pg = journal.postgres.as_mut().unwrap();
        pg.schema = schema.clone();
        pg.lease_duration = Duration::from_secs(3);
        pg.poll_interval = Duration::from_millis(20);
        let config = RuntimeConfig {
            journal: Some(journal),
            default_initial_backoff: Duration::from_millis(20),
            heartbeat_timeout: Duration::from_secs(30),
            event_log_max_events: 4,
            event_log_max_bytes: 16 * 1024,
            ..Default::default()
        };
        // Concurrent startup exercises schema bootstrap as well as membership.
        let (a, b) = tokio::join!(Runtime::start(config.clone()), Runtime::start(config));
        Self {
            a: a.unwrap(),
            b: b.unwrap(),
            admin: PgPool::connect(&url).await.unwrap(),
            schema,
        }
    }

    async fn close(self) {
        self.a.shutdown().await;
        self.b.shutdown().await;
        sqlx::query(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }

    async fn expire_a(&self) {
        // Simulate an owner that cannot renew its lease, without expiring B or
        // granting A an opportunity to voluntarily hand over its work.
        let node = &self.a.inner.postgres.as_ref().unwrap().node_id;
        sqlx::query(&format!("UPDATE \"{}\".nodes SET expires_at=clock_timestamp()-interval '1 second' WHERE node_id=$1", self.schema))
            .bind(node).execute(&self.admin).await.unwrap();
    }
}

fn request(id: &str) -> SubmitRequest {
    let mut spec = JobSpec::default();
    spec.demand.interface = "echo".into();
    spec.inputs = vec![
        Object::inline("input"),
        Object::Uri {
            uri: "s3://large-media/input".into(),
            metadata: Metadata::new(),
        },
    ];
    spec.params = Bytes::from_static(b"params");
    SubmitRequest {
        job_id: Some(id.into()),
        spec,
        ..Default::default()
    }
}

fn worker(runtime: &Runtime, id: &str, concurrency: u32) -> SessionHandle {
    runtime
        .open_session(pb::Hello {
            worker_id: id.into(),
            interfaces: vec!["echo".into()],
            max_concurrency: concurrency,
            ..Default::default()
        })
        .unwrap()
}

async fn assigned(session: &mut SessionHandle) -> pb::Assign {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(pb::server_message::Msg::Assign(assign)) =
                session.messages.recv().await.expect("session open").msg
            {
                return assign;
            }
        }
    })
    .await
    .expect("assignment in time")
}

async fn open(runtime: &Runtime, assign: &pb::Assign) -> AttemptLink {
    runtime
        .open_attempt(&pb::AttemptOpen {
            lease_id: assign.lease_id.clone(),
            generation: runtime.generation(),
            resume_after_sequence: 0,
        })
        .await
        .unwrap()
        .0
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_idempotency_is_global_and_atomic() {
    let c = Cluster::start().await;
    assert_ne!(c.a.generation(), c.b.generation());
    let requests = (0..64).map(|i| {
        let r = if i % 2 == 0 { &c.a } else { &c.b };
        async move { r.submit(request("same")).await.unwrap() }
    });
    let outcomes = join_all(requests).await;
    assert_eq!(outcomes.iter().filter(|o| o.created).count(), 1);
    let mut conflict = request("same");
    conflict.spec.params = Bytes::from_static(b"different");
    assert_eq!(
        c.b.submit(conflict).await.unwrap_err().kind,
        ErrorKind::Conflict
    );
    let mut fail = request("same");
    fail.conflict = ConflictPolicy::Fail;
    assert_eq!(
        c.a.submit(fail).await.unwrap_err().kind,
        ErrorKind::Conflict
    );
    let mut occupied = super::super::events::Permit::new(Arc::clone(&c.a.inner.input_budget));
    assert!(occupied.grow(c.a.config().max_pending_bytes.try_into().unwrap()));
    assert!(!c.a.submit(request("same")).await.unwrap().created);
    assert_eq!(
        c.a.submit(request("new")).await.unwrap_err().kind,
        ErrorKind::ResourceExhausted
    );
    let mut conflict = request("same");
    conflict.spec.params = Bytes::from_static(b"different");
    assert_eq!(
        c.a.submit(conflict).await.unwrap_err().kind,
        ErrorKind::Conflict
    );
    drop(occupied);
    assert_eq!(
        c.b.list_jobs(JobFilter::default())
            .await
            .unwrap()
            .jobs
            .len(),
        1
    );
    assert_eq!(
        c.b.inspect_job_preview("same").await.unwrap().job.state,
        JobState::Queued
    );
    assert_eq!(
        c.b.list_jobs(JobFilter {
            page_token: Some("0".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .jobs
        .len(),
        1
    );
    let mut ephemeral = request("same");
    ephemeral.spec.policy.durability = Some(Durability::Ephemeral);
    assert_eq!(
        c.b.submit(ephemeral.clone()).await.unwrap_err().kind,
        ErrorKind::InvalidArgument
    );
    ephemeral.job_id = None;
    let local = c.b.submit(ephemeral).await.unwrap().job;
    assert!(local.job_id.starts_with("ephemeral:"));
    assert_eq!(
        c.a.get_job(&local.job_id, false).await.unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        c.a.get_job("same", false).await.unwrap().durability,
        Durability::Durable
    );
    c.close().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_two_replicas_execute_without_duplicate_claims() {
    let c = Cluster::start().await;
    let mut wa = worker(&c.a, "a", 16);
    let mut wb = worker(&c.b, "b", 16);
    for i in 0..32 {
        c.a.submit(request(&format!("job-{i}"))).await.unwrap();
    }
    let mut seen = HashSet::new();
    for _ in 0..16 {
        for (runtime, session) in [(&c.a, &mut wa), (&c.b, &mut wb)] {
            let assign = assigned(session).await;
            assert!(
                seen.insert(assign.job_id.clone()),
                "job assigned on two live replicas"
            );
            assert_eq!(assign.inline_inputs.len(), 2);
            assert_eq!(assign.params, Bytes::from_static(b"params"));
            let link = open(runtime, &assign).await;
            assert_eq!(
                runtime
                    .finish_attempt(&link, AttemptOutcome::Succeeded, vec![Object::inline("ok")])
                    .await
                    .unwrap(),
                FinishStatus::Committed
            );
        }
    }
    assert_eq!(seen.len(), 32);
    assert_eq!(
        c.b.list_jobs(JobFilter {
            states: vec![JobState::Succeeded],
            ..Default::default()
        })
        .await
        .unwrap()
        .jobs
        .len(),
        32
    );
    for r in [&c.a, &c.b] {
        crate::runtime::state::assert_reservations(&r.inner.lock());
    }
    c.close().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_takeover_fences_old_result_and_preserves_watch_cursor() {
    let c = Cluster::start().await;
    let mut wa = worker(&c.a, "a", 1);
    c.b.submit(request("failover")).await.unwrap();
    let first = assigned(&mut wa).await;
    let old_link = open(&c.a, &first).await;
    let (before, done) = c.b.events("failover", 0).await.unwrap();
    assert!(!done);
    let after = before.last().unwrap().sequence;
    let mut wb = worker(&c.b, "b", 1);
    c.expire_a().await;
    let second = assigned(&mut wb).await;
    assert_eq!(second.job_id, first.job_id);
    assert_ne!(second.lease_id, first.lease_id);
    let late =
        c.a.finish_attempt(
            &old_link,
            AttemptOutcome::Succeeded,
            vec![Object::inline("stale")],
        )
        .await;
    assert!(!matches!(late, Ok(FinishStatus::Committed)));
    let link = open(&c.b, &second).await;
    assert_eq!(
        c.b.finish_attempt(
            &link,
            AttemptOutcome::Succeeded,
            vec![Object::inline("winner")]
        )
        .await
        .unwrap(),
        FinishStatus::Committed
    );
    let job = c.a.get_job("failover", true).await.unwrap();
    assert_eq!(job.attempt_count, 2);
    assert_eq!(job.outputs.unwrap(), vec![Object::inline("winner")]);
    let rest: Vec<_> = c.a.watch("failover", after).unwrap().collect().await;
    assert!(rest.iter().all(|e| e.as_ref().unwrap().sequence > after));
    assert!(matches!(
        rest.last().unwrap().as_ref().unwrap().kind,
        EventKind::Finished {
            state: JobState::Succeeded,
            ..
        }
    ));
    // An expired replica cannot renew itself and become a second owner again.
    assert!(
        c.a.inner
            .postgres
            .as_ref()
            .unwrap()
            .heartbeat()
            .await
            .is_err()
    );
    assert!(!c.a.health().ready);
    assert!(!c.a.health().live);
    c.close().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_cancel_on_another_replica_wins_over_late_finish() {
    let c = Cluster::start().await;
    let mut wa = worker(&c.a, "a", 1);
    c.b.submit(request("cancel")).await.unwrap();
    let assign = assigned(&mut wa).await;
    let link = open(&c.a, &assign).await;
    assert_eq!(
        c.b.cancel("cancel", "operator".into()).await.unwrap().state,
        JobState::Cancelled
    );
    let late =
        c.a.finish_attempt(
            &link,
            AttemptOutcome::Succeeded,
            vec![Object::inline("late")],
        )
        .await;
    assert!(!matches!(late, Ok(FinishStatus::Committed)));
    assert_eq!(
        c.a.get_job("cancel", true).await.unwrap().state,
        JobState::Cancelled
    );
    assert!(c.a.health().ready && c.b.health().ready);
    c.close().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_retry_receipt_and_result_budget_survive_owner_change() {
    let c = Cluster::start().await;
    let mut wa = worker(&c.a, "a", 1);
    c.b.submit(request("retry")).await.unwrap();
    let first = assigned(&mut wa).await;
    let link = open(&c.a, &first).await;
    let failure = ErrorInfo::new("retry_me", "retry on another replica", true);
    assert_eq!(
        c.a.finish_attempt(&link, AttemptOutcome::Failed(failure.clone()), vec![])
            .await
            .unwrap(),
        FinishStatus::Committed
    );
    // The retry receipt is visible on B even before the next attempt finishes.
    let replay =
        c.b.open_attempt(&pb::AttemptOpen {
            lease_id: first.lease_id.clone(),
            generation: c.a.generation(),
            resume_after_sequence: 0,
        })
        .await
        .unwrap()
        .0;
    assert_eq!(
        c.b.finish_attempt(&replay, AttemptOutcome::Failed(failure), vec![])
            .await
            .unwrap(),
        FinishStatus::Committed
    );
    c.a.session_closed(&wa.session_id, wa.connection);
    let mut wb = worker(&c.b, "b", 1);
    let second = assigned(&mut wb).await;
    let link = open(&c.b, &second).await;
    let output = Object::inline(vec![7; 2 * 1024 * 1024]);
    c.b.finish_attempt(&link, AttemptOutcome::Succeeded, vec![output.clone()])
        .await
        .unwrap();
    let preview = c.a.inspect_job_preview("retry").await.unwrap();
    assert_eq!(preview.outputs[0].size_bytes, Some(2 * 1024 * 1024));
    assert_eq!(
        preview.outputs[0].prefix.inline_len(),
        INSPECTION_PREVIEW_BYTES
    );
    let budget = Arc::new(crate::runtime::events::ByteBudget::new(1));
    let pg = c.a.inner.postgres.as_ref().unwrap();
    let error = pg
        .snapshot(
            "retry",
            crate::postgres::ReadMode::Full,
            None,
            budget.clone(),
            budget.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<crate::error::RuntimeError>()
            .unwrap()
            .kind,
        ErrorKind::ResourceExhausted
    );
    assert_eq!(budget.used(), 0);
    assert_eq!(
        c.a.get_job("retry", true).await.unwrap().outputs.unwrap(),
        vec![output]
    );
    c.close().await;
}

use std::sync::Arc;

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_sdk_keeps_worker_streams_on_one_tcp_backend() {
    use anyserve_client::execution::{Attempt, WorkerSession};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_stream::wrappers::TcpListenerStream;
    let c = Cluster::start().await;
    let mut servers = Vec::new();
    let mut addresses = Vec::new();
    for runtime in [&c.a, &c.b] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap());
        let service = crate::worker_service::WorkerGrpcService::server(runtime.clone());
        servers.push(tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        }));
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let connections = Arc::new(AtomicUsize::new(0));
    let count = connections.clone();
    let proxy = tokio::spawn(async move {
        while let Ok((mut incoming, _)) = listener.accept().await {
            let address = addresses[count.fetch_add(1, Ordering::SeqCst) % addresses.len()];
            tokio::spawn(async move {
                let mut upstream = TcpStream::connect(address).await.unwrap();
                let _ = tokio::io::copy_bidirectional(&mut incoming, &mut upstream).await;
            });
        }
    });
    let mut session = WorkerSession::connect(
        &endpoint,
        None,
        pb::Hello {
            worker_id: "balanced".into(),
            interfaces: vec!["echo".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let welcome = match session
        .messages
        .message()
        .await
        .unwrap()
        .unwrap()
        .msg
        .unwrap()
    {
        pb::server_message::Msg::Welcome(w) => w,
        _ => panic!("welcome"),
    };
    c.b.submit(request("tcp-affinity")).await.unwrap();
    let assign = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(pb::server_message::Msg::Assign(a)) =
                session.messages.message().await.unwrap().unwrap().msg
            {
                break a;
            }
        }
    })
    .await
    .unwrap();
    let mut attempt = Attempt::open(
        &mut session.data,
        &assign,
        &welcome.limits.unwrap(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(!attempt.terminal_replay);
    assert_eq!(
        connections.load(Ordering::SeqCst),
        1,
        "control and data must reach the same replica"
    );
    attempt
        .finish(&[], pb::finish::Outcome::Succeeded(pb::Succeeded {}))
        .await
        .unwrap();
    assert_eq!(
        c.b.get_job("tcp-affinity", false).await.unwrap().state,
        JobState::Succeeded
    );
    drop(attempt);
    drop(session);
    proxy.abort();
    for server in servers {
        server.abort();
    }
    c.close().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_progress_replays_on_other_nodes_with_bounded_cursors() {
    let c = Cluster::start().await;
    let mut wa = worker(&c.a, "a", 1);
    c.b.submit(request("frames")).await.unwrap();
    let assign = assigned(&mut wa).await;
    let link = open(&c.a, &assign).await;
    let frame = |sequence| pb::Frame {
        sequence,
        data: Bytes::from(vec![3; 1024]),
        progress: true,
        ..Default::default()
    };
    c.a.push_frame(&assign.lease_id, frame(1)).await.unwrap();
    c.a.push_frame(&assign.lease_id, frame(1)).await.unwrap();
    let (events, done) = c.b.events("frames", 0).await.unwrap();
    assert!(!done);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::Output { .. }))
            .count(),
        1
    );
    let cursor = events.last().unwrap().sequence;
    assert!(
        matches!(&events.last().unwrap().kind, EventKind::Output { data, .. } if data.len()==1024)
    );
    for sequence in 2..=8 {
        c.a.push_frame(&assign.lease_id, frame(sequence))
            .await
            .unwrap();
    }
    assert!(matches!(
        c.b.events("frames", cursor).await.unwrap_err().kind,
        ErrorKind::CursorExpired { .. }
    ));
    let (events, _) = c.b.events("frames", 0).await.unwrap();
    assert!(events.len() <= 4);
    c.a.finish_attempt(
        &link,
        AttemptOutcome::Succeeded,
        vec![Object::inline("done")],
    )
    .await
    .unwrap();
    let (_, done) =
        c.b.events("frames", events.last().unwrap().sequence)
            .await
            .unwrap();
    assert!(done);
    c.close().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_failure_respects_attempt_limit_and_worker_placement() {
    let c = Cluster::start().await;
    let mut wrong = worker(&c.a, "wrong", 1);
    let mut right =
        c.b.open_session(pb::Hello {
            worker_id: "gpu".into(),
            interfaces: vec!["echo".into()],
            attributes: [("region".into(), "west".into())].into(),
            total_capacity: [("gpu".into(), 1)].into(),
            max_concurrency: 1,
            ..Default::default()
        })
        .unwrap();
    let mut r = request("placement");
    r.spec
        .demand
        .required_attributes
        .insert("region".into(), "west".into());
    r.spec.demand.required_capacity.insert("gpu".into(), 1);
    r.spec.policy.max_attempts = Some(1);
    c.a.submit(r).await.unwrap();
    let assign = assigned(&mut right).await;
    assert_eq!(assign.job_id, "placement");
    while let Ok(message) = wrong.messages.try_recv() {
        assert!(!matches!(
            message.msg,
            Some(pb::server_message::Msg::Assign(_))
        ));
    }
    let node = &c.b.inner.postgres.as_ref().unwrap().node_id;
    sqlx::query(&format!(
        "UPDATE \"{}\".nodes SET expires_at=clock_timestamp()-interval '1 second' WHERE node_id=$1",
        c.schema
    ))
    .bind(node)
    .execute(&c.admin)
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let job = c.a.get_job("placement", false).await.unwrap();
            if job.state == JobState::Failed {
                assert_eq!(job.attempt_count, 1);
                assert_eq!(job.error.unwrap().code, codes::ATTEMPTS_EXHAUSTED);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    c.close().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_unassigned_timeouts_and_keyset_history() {
    let c = Cluster::start().await;
    for i in 0..7 {
        let mut r = request(&format!("timeout-{i}"));
        r.spec.policy.queue_timeout = Some(Duration::from_millis(50));
        c.a.submit(r).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if c.b
                .list_jobs(JobFilter {
                    states: vec![JobState::Failed],
                    ..Default::default()
                })
                .await
                .unwrap()
                .jobs
                .len()
                == 7
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let mut seen = HashSet::new();
    let mut cursor = None;
    loop {
        let page =
            c.b.scan_job_metadata(JobScan {
                archived: true,
                from_ms: crate::now_ms() - 60_000,
                to_ms: crate::now_ms() + 1000,
                limit: 2,
                cursor,
            })
            .await
            .unwrap();
        for job in page.jobs {
            assert!(seen.insert(job.job_id));
            assert!(job.outputs.is_none());
        }
        cursor = page.next_page_token;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), 7);
    c.close().await;
}
