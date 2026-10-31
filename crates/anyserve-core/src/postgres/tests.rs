use super::*;

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
