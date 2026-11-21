use super::*;
use crate::journal::{AcceptRecord, AttemptStartRecord, TerminalRecord};
use crate::runtime::api::{
    ConflictPolicy, ErrorInfo, EventKind, JobSpec, JobState, Object, RuntimeConfig,
};

async fn node() -> (Arc<Postgres>, PgPool) {
    let url = std::env::var("ANYSERVE_TEST_POSTGRES_URL")
        .expect("set ANYSERVE_TEST_POSTGRES_URL to a disposable PostgreSQL database");
    let mut config = JournalConfig::postgres(&url);
    let pg = config.postgres.as_mut().unwrap();
    pg.schema = format!("test_{}", uuid::Uuid::new_v4().simple());
    pg.lease_duration = Duration::from_secs(3);
    (
        Postgres::open(&config).await.unwrap(),
        PgPool::connect(&url).await.unwrap(),
    )
}

async fn close(pg: Arc<Postgres>, admin: PgPool) {
    pg.close().await;
    sqlx::query(&format!("DROP SCHEMA \"{}\" CASCADE", pg.config.schema))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

fn acceptance(id: &str, total_timeout: Option<Duration>) -> AcceptRecord {
    let mut spec = JobSpec::default();
    spec.policy.total_timeout = total_timeout;
    AcceptRecord {
        job_id: id.into(),
        spec_hash: "same-spec".into(),
        spec: Arc::new(spec),
        max_attempts: 2,
        created_at_ms: 1,
    }
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_scan_preserves_state_subsets_and_mixed_filters() {
    use JobState::{Cancelled, Failed, Queued, Running, Succeeded};

    let (pg, admin) = node().await;
    for (index, id) in ["queued", "running", "succeeded", "failed", "cancelled"]
        .into_iter()
        .enumerate()
    {
        let mut record = acceptance(id, None);
        record.spec = Arc::new(JobSpec {
            demand: crate::runtime::api::Demand {
                interface: if index % 2 == 0 { "alpha" } else { "beta" }.into(),
                ..Default::default()
            },
            ..Default::default()
        });
        pg.accept(record, ConflictPolicy::UseExisting, None)
            .await
            .unwrap();
        sqlx::query("UPDATE jobs SET state=$1,created_at_ms=$2 WHERE job_id=$1")
            .bind(id)
            .bind(1000 + index as i64)
            .execute(&pg.pool)
            .await
            .unwrap();
    }
    for (states, expected) in [
        (
            vec![],
            vec!["cancelled", "failed", "succeeded", "running", "queued"],
        ),
        (vec![Queued], vec!["queued"]),
        (vec![Running], vec!["running"]),
        (vec![Succeeded], vec!["succeeded"]),
        (vec![Failed], vec!["failed"]),
        (vec![Cancelled], vec!["cancelled"]),
        (vec![Queued, Running], vec!["running", "queued"]),
        (
            vec![Succeeded, Failed, Cancelled],
            vec!["cancelled", "failed", "succeeded"],
        ),
        (vec![Queued, Succeeded], vec!["succeeded", "queued"]),
        (vec![Running, Cancelled], vec!["cancelled", "running"]),
        (vec![Queued, Queued], vec!["queued"]),
    ] {
        let jobs = pg
            .scan_jobs(0, u64::MAX, None, &states, None, 10)
            .await
            .unwrap();
        assert_eq!(
            jobs.iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            expected,
            "{states:?}"
        );
    }
    for (states, expected) in [
        (vec![Queued, Running], vec!["queued"]),
        (
            vec![Succeeded, Failed, Cancelled],
            vec!["cancelled", "succeeded"],
        ),
        (vec![Queued, Failed], vec!["queued"]),
    ] {
        let jobs = pg
            .scan_jobs(0, u64::MAX, None, &states, Some("alpha"), 10)
            .await
            .unwrap();
        assert_eq!(
            jobs.iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            expected,
            "{states:?}"
        );
    }
    let jobs = pg
        .scan_jobs(
            1001,
            1004,
            Some((1003, "failed".into())),
            &[Succeeded, Failed, Cancelled],
            None,
            1,
        )
        .await
        .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, "succeeded");
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_accept_uses_database_time_and_saturates_deadlines() {
    let (pg, admin) = node().await;
    for (id, timeout) in [
        ("unlimited", None),
        ("immediate", Some(Duration::ZERO)),
        ("bounded", Some(Duration::from_millis(17))),
        ("overflow", Some(Duration::MAX)),
    ] {
        let (accepted, created) = pg
            .accept(acceptance(id, timeout), ConflictPolicy::UseExisting, None)
            .await
            .unwrap();
        assert!(created);
        assert_eq!(accepted.job_id, id);
        assert_eq!(accepted.state, JobState::Queued);
        assert_eq!(accepted.attempt_count, 0);
        assert_eq!(accepted.spec_hash, "same-spec");
        assert_eq!(accepted.spec.policy.total_timeout, timeout);
        let (replayed, created) = pg
            .accept(acceptance(id, timeout), ConflictPolicy::UseExisting, None)
            .await
            .unwrap();
        assert!(!created);
        assert_eq!(replayed.created_at_ms, accepted.created_at_ms);
        let row = sqlx::query(
            "SELECT created_at_ms,updated_at_ms,queued_at_ms,total_deadline_ms,event_sequence,
             (SELECT at_ms FROM events WHERE job_id=jobs.job_id AND sequence=1) AS event_at,
             (extract(epoch FROM clock_timestamp())*1000)::bigint AS db_now
             FROM jobs WHERE job_id=$1",
        )
        .bind(id)
        .fetch_one(&pg.pool)
        .await
        .unwrap();
        let created: i64 = row.get("created_at_ms");
        assert_eq!(accepted.created_at_ms, created as u64);
        assert!(created > 1 && created <= row.get::<i64, _>("db_now"));
        assert_eq!(created, row.get::<i64, _>("updated_at_ms"));
        assert_eq!(created, row.get::<i64, _>("queued_at_ms"));
        assert_eq!(created, row.get::<i64, _>("event_at"));
        assert_eq!(row.get::<i64, _>("event_sequence"), 1);
        let expected = match id {
            "unlimited" => None,
            "immediate" => Some(created),
            "bounded" => Some(created + 17),
            _ => Some(i64::MAX),
        };
        assert_eq!(row.get::<Option<i64>, _>("total_deadline_ms"), expected);
    }
    let mut different = acceptance("unlimited", None);
    different.spec_hash = "different-spec".into();
    assert!(
        pg.accept(different, ConflictPolicy::UseExisting, None)
            .await
            .is_err()
    );
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_attempt_and_terminal_share_database_time_with_events() {
    let (pg, admin) = node().await;
    for (id, retry_at) in [
        ("finished", None),
        ("past-retry", Some(1)),
        ("future-retry", Some(117)),
        ("overflow-retry", Some(u64::MAX)),
    ] {
        pg.accept(acceptance(id, None), ConflictPolicy::Fail, None)
            .await
            .unwrap();
        sqlx::query("UPDATE jobs SET owner_id=$2,ownership_epoch=7 WHERE job_id=$1")
            .bind(id)
            .bind(&pg.node_id)
            .execute(&pg.pool)
            .await
            .unwrap();
        let start = |epoch| {
            JournalOp::AttemptStart(AttemptStartRecord {
                job_id: id.into(),
                attempt_number: 1,
                at_ms: 1,
                ownership_epoch: Some(epoch),
                attempt_id: Some("attempt".into()),
                worker_id: Some("worker".into()),
            })
        };
        assert!(matches!(pg.apply(start(6)).await, CommitOutcome::Fenced(_)));
        assert_eq!(pg.apply(start(7)).await, CommitOutcome::Committed);
        let (started, assigned): (i64, i64) = sqlx::query_as(
            "SELECT updated_at_ms,at_ms FROM jobs JOIN events USING(job_id)
             WHERE job_id=$1 AND sequence=2",
        )
        .bind(id)
        .fetch_one(&pg.pool)
        .await
        .unwrap();
        assert!(started > 1);
        assert_eq!(started, assigned);

        let terminal = |epoch| {
            JournalOp::Terminal(Box::new(TerminalRecord {
                job_id: id.into(),
                state: if retry_at.is_some() {
                    JobState::Queued
                } else {
                    JobState::Succeeded
                },
                outputs: vec![Object::inline("output")],
                error: retry_at.map(|_| ErrorInfo::new("RETRY", "retry", true)),
                attempt_id: Some("attempt".into()),
                worker_id: Some("worker".into()),
                winning_lease_id: retry_at.is_none().then(|| "lease".into()),
                outcome_digest: retry_at.is_none().then(|| "digest".into()),
                at_ms: 100,
                ownership_epoch: Some(epoch),
                retry_at_ms: retry_at,
            }))
        };
        assert!(matches!(
            pg.apply(terminal(6)).await,
            CommitOutcome::Fenced(_)
        ));
        assert_eq!(pg.apply(terminal(7)).await, CommitOutcome::Committed);
        let row = sqlx::query(
            "SELECT updated_at_ms,queued_at_ms,not_before_ms,event_sequence,owner_id,
             (SELECT at_ms FROM events WHERE job_id=jobs.job_id AND sequence=3) AS event_at,
             (extract(epoch FROM clock_timestamp())*1000)::bigint AS db_now
             FROM jobs WHERE job_id=$1",
        )
        .bind(id)
        .fetch_one(&pg.pool)
        .await
        .unwrap();
        let updated: i64 = row.get("updated_at_ms");
        assert!(updated >= started && updated <= row.get::<i64, _>("db_now"));
        assert_eq!(updated, row.get::<i64, _>("queued_at_ms"));
        assert_eq!(updated, row.get::<i64, _>("event_at"));
        assert_eq!(row.get::<i64, _>("event_sequence"), 3);
        assert_eq!(row.get::<Option<String>, _>("owner_id"), None);
        let expected = match id {
            "finished" => 0,
            "past-retry" => updated,
            "future-retry" => updated + 17,
            _ => i64::MAX,
        };
        assert_eq!(row.get::<i64, _>("not_before_ms"), expected);
    }
    let (output, digest): (Vec<u8>, String) = sqlx::query_as(
        "SELECT content,digest FROM objects JOIN receipts USING(job_id)
         WHERE job_id='finished' AND role='output'",
    )
    .fetch_one(&pg.pool)
    .await
    .unwrap();
    assert_eq!(output, b"output");
    assert_eq!(digest, "digest");
    let (replayed, created) = pg
        .accept(
            acceptance("finished", None),
            ConflictPolicy::UseExisting,
            None,
        )
        .await
        .unwrap();
    assert!(!created);
    assert_eq!(replayed.state, JobState::Succeeded);
    assert_eq!(replayed.attempt_count, 1);
    assert_eq!(replayed.last_attempt_id.as_deref(), Some("attempt"));
    assert_eq!(replayed.last_worker_id.as_deref(), Some("worker"));
    assert!(replayed.outputs.is_empty());
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_attempt_start_is_fenced_and_atomic_with_assigned_event() {
    let (pg, admin) = node().await;
    pg.accept(acceptance("atomic-start", None), ConflictPolicy::Fail, None)
        .await
        .unwrap();
    let start = |epoch| {
        JournalOp::AttemptStart(AttemptStartRecord {
            job_id: "atomic-start".into(),
            attempt_number: 1,
            at_ms: 1,
            ownership_epoch: Some(epoch),
            attempt_id: Some("attempt".into()),
            worker_id: Some("worker".into()),
        })
    };
    let snapshot = "SELECT jsonb_build_object('job',to_jsonb(jobs),'events',
        (SELECT jsonb_agg(to_jsonb(events) ORDER BY sequence) FROM events
         WHERE events.job_id=jobs.job_id))::text FROM jobs WHERE job_id='atomic-start'";
    for (owner, epoch) in [("another-owner", 7), (pg.node_id.as_str(), 6)] {
        sqlx::query("UPDATE jobs SET owner_id=$1,ownership_epoch=7 WHERE job_id='atomic-start'")
            .bind(owner)
            .execute(&pg.pool)
            .await
            .unwrap();
        let before: String = sqlx::query_scalar(snapshot)
            .fetch_one(&pg.pool)
            .await
            .unwrap();
        assert!(matches!(
            pg.apply(start(epoch)).await,
            CommitOutcome::Fenced(_)
        ));
        let after: String = sqlx::query_scalar(snapshot)
            .fetch_one(&pg.pool)
            .await
            .unwrap();
        assert_eq!(after, before, "fenced start changed the job or events");
    }
    let before: String = sqlx::query_scalar(snapshot)
        .fetch_one(&pg.pool)
        .await
        .unwrap();
    // Fail Assigned insertion after the guarded UPDATE has matched the job.
    sqlx::query("ALTER TABLE events ADD CONSTRAINT reject_assigned CHECK (sequence<>2)")
        .execute(&pg.pool)
        .await
        .unwrap();
    assert!(matches!(
        pg.apply(start(7)).await,
        CommitOutcome::RolledBack(reason) if reason.contains("reject_assigned")
    ));
    let after: String = sqlx::query_scalar(snapshot)
        .fetch_one(&pg.pool)
        .await
        .unwrap();
    assert_eq!(
        after, before,
        "failed event insertion did not roll back the job"
    );
    sqlx::query("ALTER TABLE events DROP CONSTRAINT reject_assigned")
        .execute(&pg.pool)
        .await
        .unwrap();
    assert_eq!(pg.apply(start(7)).await, CommitOutcome::Committed);
    assert!(matches!(pg.apply(start(7)).await, CommitOutcome::Fenced(_)));
    let (count, kind): (i64, String) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM events),kind_json FROM events
         WHERE job_id='atomic-start' AND sequence=2",
    )
    .fetch_one(&pg.pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    assert_eq!(
        serde_json::from_str::<EventKind>(&kind).unwrap(),
        EventKind::Assigned {
            worker_id: "worker".into(),
            attempt_number: 1,
        }
    );
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_terminal_rechecks_state_after_row_lock_wait() {
    let (pg, admin) = node().await;
    pg.accept(acceptance("terminal-epq", None), ConflictPolicy::Fail, None)
        .await
        .unwrap();
    sqlx::query("UPDATE jobs SET owner_id=$1,ownership_epoch=7 WHERE job_id='terminal-epq'")
        .bind(&pg.node_id)
        .execute(&pg.pool)
        .await
        .unwrap();
    assert_eq!(
        pg.apply(JournalOp::AttemptStart(AttemptStartRecord {
            job_id: "terminal-epq".into(),
            attempt_number: 1,
            at_ms: 1,
            ownership_epoch: Some(7),
            attempt_id: Some("attempt".into()),
            worker_id: Some("worker".into()),
        }))
        .await,
        CommitOutcome::Committed
    );
    let snapshot = "SELECT jsonb_build_object(
        'jobs',(SELECT jsonb_agg(to_jsonb(jobs) ORDER BY job_id) FROM jobs),
        'events',(SELECT jsonb_agg(to_jsonb(events) ORDER BY job_id,sequence) FROM events),
        'objects',(SELECT jsonb_agg(to_jsonb(objects) ORDER BY job_id,role,idx) FROM objects),
        'receipts',(SELECT jsonb_agg(to_jsonb(receipts) ORDER BY lease_id) FROM receipts))::text";
    // Give the short test lease a fresh confirmed deadline before blocking a write.
    pg.heartbeat().await.unwrap();
    let mut blocker = pg.pool.begin().await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    // Change only state: owner, epoch and attempt still match the pending finish.
    assert_eq!(
        sqlx::query(
            "UPDATE jobs SET state='cancelled' WHERE job_id='terminal-epq'
            AND owner_id=$1 AND ownership_epoch=7 AND state='running'"
        )
        .bind(&pg.node_id)
        .execute(&mut *blocker)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    let expected: String = sqlx::query_scalar(snapshot)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let terminal = {
        let pg = Arc::clone(&pg);
        tokio::spawn(async move {
            pg.apply(JournalOp::Terminal(Box::new(TerminalRecord {
                job_id: "terminal-epq".into(),
                state: JobState::Succeeded,
                outputs: vec![Object::inline("late output")],
                error: None,
                attempt_id: Some("attempt".into()),
                worker_id: Some("worker".into()),
                winning_lease_id: Some("late-lease".into()),
                outcome_digest: Some("late-digest".into()),
                at_ms: 1,
                ownership_epoch: Some(7),
                retry_at_ms: None,
            })))
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                 WHERE application_name=$1 AND wait_event_type='Lock'
                   AND $2=ANY(pg_blocking_pids(pid)))",
            )
            .bind(format!("anyserve:{}", pg.node_id))
            .bind(blocker_pid)
            .fetch_one(&admin)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("terminal write must wait for the state-changing transaction");
    blocker.commit().await.unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(1), terminal)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(outcome, CommitOutcome::Fenced(_)));
    assert!(pg.healthy(), "local lease expired during the guard test");
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT expires_at>clock_timestamp() FROM nodes WHERE node_id=$1",
        )
        .bind(&pg.node_id)
        .fetch_one(&pg.pool)
        .await
        .unwrap(),
        "database lease expired during the guard test"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(snapshot)
            .fetch_one(&pg.pool)
            .await
            .unwrap(),
        expected,
        "fenced terminal write changed the job, events, objects or receipts"
    );
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_heartbeat_recovers_within_confirmed_lease() {
    let (pg, admin) = node().await;
    let deadline = *pg.valid_until.lock().unwrap();
    // Occupying the dedicated connection forces the first renewal to time out.
    let connection = pg.heartbeat_pool.acquire().await.unwrap();
    let heartbeat = {
        let pg = Arc::clone(&pg);
        tokio::spawn(async move { pg.heartbeat().await })
    };
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(!heartbeat.is_finished());
    assert!(pg.healthy());
    assert_eq!(*pg.valid_until.lock().unwrap(), deadline);
    drop(connection);
    tokio::time::timeout(Duration::from_secs(1), heartbeat)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(pg.healthy());
    assert!(*pg.valid_until.lock().unwrap() > deadline);
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_heartbeat_cannot_extend_unconfirmed_lease() {
    let (pg, admin) = node().await;
    let deadline = Instant::now() + Duration::from_millis(400);
    *pg.valid_until.lock().unwrap() = deadline;
    let connection = pg.heartbeat_pool.acquire().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), pg.heartbeat())
            .await
            .unwrap()
            .is_err()
    );
    assert!(!pg.healthy());
    assert_eq!(*pg.valid_until.lock().unwrap(), deadline);
    drop(connection);
    // Even a still-valid database row cannot revive a locally fenced identity.
    assert!(pg.heartbeat().await.is_err());
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_heartbeat_fences_expired_database_membership() {
    let (pg, admin) = node().await;
    sqlx::query(
        "UPDATE nodes SET expires_at=clock_timestamp()-interval '1 second' WHERE node_id=$1",
    )
    .bind(&pg.node_id)
    .execute(&pg.pool)
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), pg.heartbeat())
            .await
            .unwrap()
            .is_err()
    );
    assert!(!pg.healthy());
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_heartbeat_lock_wait_cannot_revive_expired_membership() {
    let (pg, admin) = node().await;
    let deadline = *pg.valid_until.lock().unwrap();
    let mut lock = pg.pool.begin().await.unwrap();
    sqlx::query("SELECT node_id FROM nodes WHERE node_id=$1 FOR UPDATE")
        .bind(&pg.node_id)
        .execute(&mut *lock)
        .await
        .unwrap();
    tokio::time::sleep_until((deadline - Duration::from_secs(1)).into()).await;
    let heartbeat = {
        let pg = Arc::clone(&pg);
        tokio::spawn(async move { pg.heartbeat().await })
    };
    tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')",
            )
            .bind(format!("anyserve:{}", pg.node_id))
            .fetch_one(&admin)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep_until((deadline + Duration::from_millis(100)).into()).await;
    assert!(heartbeat.await.unwrap().is_err());
    assert!(!pg.healthy());
    lock.rollback().await.unwrap();
    // Wait for the cancelled query and transaction rollback to drain.
    drop(pg.heartbeat_pool.acquire().await.unwrap());
    let expired: bool =
        sqlx::query_scalar("SELECT expires_at<=clock_timestamp() FROM nodes WHERE node_id=$1")
            .bind(&pg.node_id)
            .fetch_one(&pg.pool)
            .await
            .unwrap();
    assert!(expired);
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_whale_100k_backlog_does_not_starve_small_tenant() {
    let (pg, admin) = node().await;
    let empty_json = serde_json::json!({});
    let empty_str = empty_json.to_string();
    sqlx::query(
        "INSERT INTO jobs(
            job_id, state, spec_hash, spec_json, namespace, interface, fairness_key,
            concurrency_key, failed_workers, required_attributes, preferred_attributes,
            required_capacity, priority, input_bytes, attempt_count, max_attempts,
            created_at_ms, updated_at_ms, queued_at_ms, not_before_ms
        )
        SELECT
            'whale-' || lpad(i::text, 6, '0'),
            'queued',
            'h',
            '{\"demand\":{\"interface\":\"echo\",\"required_attributes\":{},\"preferred_attributes\":{},\"required_capacity\":{},\"affinity_key\":\"\"},\"policy\":{\"durability\":\"Durable\",\"priority\":0,\"queue_timeout\":null,\"execution_timeout\":null,\"total_deadline\":null,\"total_timeout\":null,\"spillover_after\":null,\"max_attempts\":3,\"fairness_key\":\"whale\",\"concurrency_key\":\"\"},\"metadata\":{}}',
            'default',
            'echo',
            'whale',
            NULL,
            '{}'::text[],
            $1::jsonb,
            $1::jsonb,
            $1::jsonb,
            0,
            4,
            0,
            3,
            i,
            i,
            i,
            0
        FROM generate_series(1, 100000) AS i",
    )
    .bind(&empty_str)
    .execute(&pg.pool)
    .await
    .unwrap();

    let mut small_rec = acceptance("small-1", None);
    let mut small_spec = JobSpec::default();
    small_spec.demand.interface = "echo".into();
    small_spec.policy.fairness_key = "small".into();
    small_rec.spec = Arc::new(small_spec);
    let (_, created) = pg
        .accept(small_rec, ConflictPolicy::Fail, Some(60_000))
        .await
        .unwrap();
    assert!(created);

    let budget = Arc::new(crate::runtime::events::ByteBudget::new(1024 * 1024));
    let workers = serde_json::json!([{
        "worker_id": "w1",
        "namespace": "default",
        "pool": "",
        "interfaces": ["echo"],
        "attributes": {},
        "capacity": {"slot": 4}
    }]);
    let t0 = std::time::Instant::now();
    let explain_sql = format!("EXPLAIN (ANALYZE, BUFFERS) {}", write::SKIP_SCAN_SQL);
    for (cur_p, cur_o, cur_f) in [
        (Vec::<i64>::new(), Vec::<i64>::new(), Vec::<String>::new()),
        (vec![0i64], vec![1i64], vec!["whale".to_string()]),
    ] {
        let explain_rows: Vec<(String,)> = sqlx::query_as(&explain_sql)
            .bind(vec!["default"])
            .bind(vec!["echo"])
            .bind(i64::MAX)
            .bind(cur_p)
            .bind(cur_o)
            .bind(cur_f)
            .fetch_all(&pg.pool)
            .await
            .unwrap();
        let explain_text: String = explain_rows
            .iter()
            .map(|(line,)| line.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            explain_text.contains("jobs_claim_fair_groups"),
            "expected skip-scan to use jobs_claim_fair_groups:\n{explain_text}"
        );
        assert!(
            !explain_text.contains("Seq Scan on jobs"),
            "skip-scan must never Seq Scan jobs:\n{explain_text}"
        );
        let root_buf_line = explain_rows
            .iter()
            .find(|(line,)| line.trim_start().starts_with("Buffers: shared"))
            .map(|(line,)| line.as_str())
            .unwrap_or_else(|| panic!("missing root Buffers line in EXPLAIN:\n{explain_text}"));
        let parse_buf = |prefix: &str| -> usize {
            root_buf_line
                .split_whitespace()
                .find_map(|tok| tok.strip_prefix(prefix)?.parse::<usize>().ok())
                .unwrap_or(0)
        };
        let total_buffers = parse_buf("hit=") + parse_buf("read=");
        assert!(
            total_buffers > 0 && total_buffers < 50,
            "expected O(buckets) constant buffer access (<50) over 100k whale bucket, got {total_buffers} ({root_buf_line}):\n{explain_text}"
        );
    }

    let claimed = pg.claim(workers, 4, budget).await.unwrap();
    let elapsed = t0.elapsed();
    assert_eq!(claimed.len(), 4);
    assert!(
        claimed.iter().any(|c| c.job.job_id == "small-1"),
        "expected small-1 in first claim despite 100k whale backlog (got {:?})",
        claimed.iter().map(|c| &c.job.job_id).collect::<Vec<_>>()
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "fair claim over 100k backlog took too long: {elapsed:?}"
    );
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_200_unserved_buckets_do_not_starve_serviceable_interface() {
    let (pg, admin) = node().await;
    let empty_str = "{}";
    sqlx::query(
        "INSERT INTO jobs(
            job_id, state, spec_hash, spec_json, namespace, interface, fairness_key, concurrency_key,
            failed_workers, required_attributes, preferred_attributes, required_capacity,
            priority, input_bytes, attempt_count, max_attempts, created_at_ms, updated_at_ms, queued_at_ms, not_before_ms
        )
        SELECT
            'unserved-' || i,
            'queued',
            'h-' || i,
            '{\"demand\":{\"interface\":\"a.noworker.v1\",\"required_attributes\":{},\"preferred_attributes\":{},\"required_capacity\":{},\"affinity_key\":\"\"},\"policy\":{\"durability\":\"Durable\",\"priority\":3,\"queue_timeout\":null,\"execution_timeout\":null,\"total_deadline\":null,\"total_timeout\":null,\"spillover_after\":null,\"max_attempts\":3,\"fairness_key\":\"u-' || i || '\",\"concurrency_key\":\"\"},\"metadata\":{}}',
            'default',
            'a.noworker.v1',
            'u-' || lpad(i::text, 3, '0'),
            NULL,
            '{}'::text[],
            $1::jsonb,
            $1::jsonb,
            $1::jsonb,
            3,
            4,
            0,
            3,
            i,
            i,
            i,
            0
        FROM generate_series(1, 200) AS i",
    )
    .bind(empty_str)
    .execute(&pg.pool)
    .await
    .unwrap();

    for i in 0..5 {
        let mut spec = JobSpec::default();
        spec.demand.interface = "z.serve.v1".into();
        let rec = AcceptRecord {
            job_id: format!("servable-{i}"),
            spec_hash: format!("sh-{i}"),
            spec: Arc::new(spec),
            max_attempts: 2,
            created_at_ms: 10_000 + i as u64,
        };
        assert!(
            pg.accept(rec, ConflictPolicy::Fail, Some(60_000))
                .await
                .unwrap()
                .1
        );
    }

    let budget = Arc::new(crate::runtime::events::ByteBudget::new(1024 * 1024));
    let workers = serde_json::json!([{
        "worker_id": "w-z",
        "namespace": "default",
        "pool": "",
        "interfaces": ["z.serve.v1"],
        "attributes": {},
        "capacity": {"slot": 8}
    }]);
    let t0 = std::time::Instant::now();
    let claimed = pg.claim(workers, 8, budget).await.unwrap();
    let elapsed = t0.elapsed();
    let ids: Vec<&str> = claimed.iter().map(|c| c.job.job_id.as_str()).collect();
    assert_eq!(ids.len(), 5, "expected all 5 servable jobs, got {ids:?}");
    for i in 0..5 {
        assert!(ids.contains(&format!("servable-{i}").as_str()));
    }
    assert!(elapsed < Duration::from_millis(200), "took {elapsed:?}");
    close(pg, admin).await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_200_fairness_keys_round_robin_beyond_64_window() {
    use crate::runtime::Runtime;
    use crate::runtime::worker::{AttemptLink, AttemptOutcome};
    use anyserve_proto::v2 as pb;

    let url = std::env::var("ANYSERVE_TEST_POSTGRES_URL").unwrap();
    let schema = format!("test_{}", uuid::Uuid::new_v4().simple());
    let mut journal = JournalConfig::postgres(&url);
    let pg_cfg = journal.postgres.as_mut().unwrap();
    pg_cfg.schema = schema.clone();
    pg_cfg.lease_duration = Duration::from_secs(10);
    pg_cfg.poll_interval = Duration::from_millis(10);
    let rt = Runtime::start(RuntimeConfig {
        journal: Some(journal),
        ..Default::default()
    })
    .await
    .unwrap();
    let pg = rt.inner.postgres.as_ref().unwrap().clone();

    sqlx::query(
        "INSERT INTO jobs(
            job_id, state, spec_hash, spec_json, namespace, interface, fairness_key, concurrency_key,
            failed_workers, required_attributes, preferred_attributes, required_capacity,
            priority, input_bytes, attempt_count, max_attempts, created_at_ms, updated_at_ms, queued_at_ms, not_before_ms
        )
        SELECT
            'k-' || lpad(k::text, 3, '0') || '-j-' || j_idx,
            'queued',
            'h-' || k || '-' || j_idx,
            '{\"demand\":{\"interface\":\"z.serve.v1\",\"required_attributes\":{},\"preferred_attributes\":{},\"required_capacity\":{},\"affinity_key\":\"\"},\"policy\":{\"durability\":\"Durable\",\"priority\":0,\"queue_timeout\":null,\"execution_timeout\":null,\"total_deadline\":null,\"total_timeout\":null,\"spillover_after\":null,\"max_attempts\":3,\"fairness_key\":\"k-' || lpad(k::text, 3, '0') || '\",\"concurrency_key\":\"\"},\"metadata\":{}}',
            'default',
            'z.serve.v1',
            'k-' || lpad(k::text, 3, '0'),
            NULL,
            '{}'::text[],
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            0,
            4,
            0,
            3,
            (k * 10 + j_idx),
            (k * 10 + j_idx),
            (k * 10 + j_idx),
            0
        FROM generate_series(0, 199) AS k
        CROSS JOIN generate_series(0, 2) AS j_idx",
    )
    .execute(&pg.pool)
    .await
    .unwrap();

    let mut session = rt
        .open_session(pb::Hello {
            worker_id: "w-rr".into(),
            interfaces: vec!["z.serve.v1".into()],
            max_concurrency: 8,
            ..Default::default()
        })
        .unwrap();

    let mut completion_order: Vec<(usize, usize)> = Vec::with_capacity(600);
    tokio::time::timeout(Duration::from_secs(20), async {
        while completion_order.len() < 600 {
            let msg = session.messages.recv().await.expect("session open").msg;
            if let Some(pb::server_message::Msg::Assign(assign)) = msg {
                let parts: Vec<&str> = assign.job_id.split('-').collect();
                let k_idx: usize = parts[1].parse().unwrap();
                let j_idx: usize = parts[3].parse().unwrap();
                let rt_ref = rt.clone();
                let lease_id = assign.lease_id.clone();
                let link = rt_ref
                    .open_attempt(&pb::AttemptOpen {
                        lease_id: lease_id.clone(),
                        generation: rt_ref.generation(),
                        resume_after_sequence: 0,
                    })
                    .await
                    .unwrap()
                    .0;
                assert!(matches!(link, AttemptLink::Live { .. }));
                rt_ref
                    .finish_attempt(&link, AttemptOutcome::Succeeded, vec![])
                    .await
                    .unwrap();
                completion_order.push((k_idx, j_idx));
                let _ = rt_ref.session_message(
                    &session.session_id,
                    session.connection,
                    pb::worker_message::Msg::Credit(pb::Credit { free_slots: 8 }),
                );
            }
        }
    })
    .await
    .expect("drain 600 multi-key jobs within 20s");

    let max_first_wave = completion_order
        .iter()
        .enumerate()
        .filter(|(_, (_, j_idx))| *j_idx == 0)
        .map(|(pos, _)| pos)
        .max()
        .unwrap();
    let min_third_wave = completion_order
        .iter()
        .enumerate()
        .filter(|(_, (_, j_idx))| *j_idx == 2)
        .map(|(pos, _)| pos)
        .min()
        .unwrap();
    assert!(
        max_first_wave < min_third_wave,
        "expected every fairness_key's 1st job to finish before any key's 3rd job (max j=0 at {max_first_wave}, min j=2 at {min_third_wave})"
    );

    drop(session);
    rt.shutdown().await;
    let admin = PgPool::connect(&url).await.unwrap();
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
#[ignore = "requires ANYSERVE_TEST_POSTGRES_URL"]
async fn postgres_cluster_v1_migrates_to_v2_and_v2_skips_alter_lock() {
    let url = std::env::var("ANYSERVE_TEST_POSTGRES_URL").unwrap();
    let schema = format!("test_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPool::connect(&url).await.unwrap();
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA \"{schema}\";
         SET search_path TO \"{schema}\",pg_catalog;
         CREATE TABLE cluster_schema (version BIGINT PRIMARY KEY);
         INSERT INTO cluster_schema VALUES (1);
         CREATE SEQUENCE node_generation;
         CREATE TABLE nodes (
             node_id TEXT PRIMARY KEY,
             generation BIGINT NOT NULL,
             expires_at TIMESTAMPTZ NOT NULL
         );
         CREATE TABLE jobs (
             job_id TEXT COLLATE \"C\" PRIMARY KEY,
             state TEXT NOT NULL,
             spec_hash TEXT NOT NULL,
             spec_json TEXT NOT NULL,
             interface TEXT NOT NULL,
             required_attributes JSONB NOT NULL,
             preferred_attributes JSONB NOT NULL,
             required_capacity JSONB NOT NULL,
             priority BIGINT NOT NULL,
             input_bytes BIGINT NOT NULL,
             attempt_count BIGINT NOT NULL DEFAULT 0,
             max_attempts BIGINT NOT NULL,
             created_at_ms BIGINT NOT NULL,
             updated_at_ms BIGINT NOT NULL,
             queued_at_ms BIGINT NOT NULL,
             not_before_ms BIGINT NOT NULL DEFAULT 0,
             queue_timeout_ms BIGINT,
             total_deadline_ms BIGINT,
             spillover_ms BIGINT,
             owner_id TEXT,
             ownership_epoch BIGINT NOT NULL DEFAULT 0,
             event_sequence BIGINT NOT NULL DEFAULT 0,
             events_floor BIGINT NOT NULL DEFAULT 0,
             last_frame_seq BIGINT NOT NULL DEFAULT 0,
             error_json TEXT,
             last_attempt_id TEXT,
             last_worker_id TEXT
         );
         CREATE INDEX jobs_claim ON jobs(interface, priority DESC, created_at_ms, job_id) WHERE state IN ('queued','running');
         CREATE INDEX jobs_claim_order ON jobs(priority DESC, created_at_ms, job_id) WHERE state IN ('queued','running');
         CREATE INDEX jobs_owner ON jobs(owner_id) WHERE state IN ('queued','running');
         CREATE TABLE objects (job_id TEXT NOT NULL, role TEXT NOT NULL, idx BIGINT NOT NULL, uri TEXT, content BYTEA, metadata_json TEXT NOT NULL, PRIMARY KEY(job_id,role,idx));
         CREATE TABLE events (job_id TEXT NOT NULL, sequence BIGINT NOT NULL, attempt_id TEXT, at_ms BIGINT NOT NULL, kind_json TEXT NOT NULL, frame_data BYTEA, PRIMARY KEY(job_id,sequence));
         CREATE TABLE receipts (lease_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, digest TEXT NOT NULL);
         CREATE TABLE concurrency_slots (namespace TEXT NOT NULL, concurrency_key TEXT NOT NULL, in_flight INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(namespace, concurrency_key));"
    ))
    .execute(&admin)
    .await
    .unwrap();

    let mut cfg = JournalConfig::postgres(&url);
    let pg_cfg = cfg.postgres.as_mut().unwrap();
    pg_cfg.schema = schema.clone();
    pg_cfg.lease_duration = Duration::from_secs(3);

    // First open upgrades v1 -> v2 cleanly and drops obsolete v1 indexes.
    let pg1 = Postgres::open(&cfg).await.unwrap();
    let max_v: i64 = sqlx::query_scalar(&format!(
        "SELECT MAX(version) FROM \"{schema}\".cluster_schema"
    ))
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(max_v, 2);
    let idx_names: Vec<String> = sqlx::query_scalar(
        "SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = 'jobs'",
    )
    .bind(&schema)
    .fetch_all(&admin)
    .await
    .unwrap();
    for obsolete in ["jobs_claim", "jobs_claim_order", "jobs_owner"] {
        assert!(
            !idx_names.iter().any(|n| n == obsolete),
            "obsolete index {obsolete} still present: {idx_names:?}"
        );
    }
    for required in ["jobs_claim_fair_groups", "jobs_queued_expiry"] {
        assert!(
            idx_names.iter().any(|n| n == required),
            "missing v2 index {required}: {idx_names:?}"
        );
    }

    // Hold a ROW SHARE lock on jobs, which blocks any ALTER TABLE, then open a second replica.
    let mut blocker = admin.begin().await.unwrap();
    sqlx::query(&format!("SELECT COUNT(*) FROM \"{schema}\".jobs"))
        .execute(&mut *blocker)
        .await
        .unwrap();
    let pg2 = tokio::time::timeout(Duration::from_secs(2), Postgres::open(&cfg))
        .await
        .expect("v2 open must skip ALTER TABLE DDL locks")
        .unwrap();

    blocker.rollback().await.unwrap();
    pg1.close().await;
    pg2.close().await;
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
