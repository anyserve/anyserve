use super::*;
use crate::journal::{AcceptRecord, AttemptStartRecord, TerminalRecord};
use crate::runtime::api::{ConflictPolicy, ErrorInfo, JobSpec, JobState, Object};

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
