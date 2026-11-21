//! Shared durable execution. Every replica can submit and claim jobs. Node
//! leases use the database clock; job epochs fence writes after ownership moves.
//! Local memory holds only work claimed for this replica's worker connections.
mod read;
#[cfg(test)]
mod tests;
mod write;
pub(crate) use read::{JobSnapshot, ReadMode};

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::error::RuntimeError;
use crate::journal::{CommitOutcome, JournalOp, StoredJob};
use crate::runtime::api::{JournalConfig, JournalStats, PostgresConfig};
use crate::runtime::events::Permit;

pub(crate) struct ClaimedJob {
    pub job: StoredJob,
    pub epoch: i64,
    pub queued_at_ms: u64,
    pub input_permit: Permit,
    pub failure: Option<crate::runtime::api::ErrorInfo>,
}

pub(crate) struct Postgres {
    pub(super) pool: PgPool,
    heartbeat_pool: PgPool,
    pub(crate) config: PostgresConfig,
    pub(super) node_id: String,
    pub(crate) generation: u64,
    fenced: AtomicBool,
    valid_until: Mutex<Instant>,
    operations: Arc<Semaphore>,
    frame_batch_tx: mpsc::Sender<write::FrameWrite>,
    pending: AtomicUsize,
    stats: Mutex<JournalStats>,
    /// Per-priority fairness rotation cursor `(namespace, interface, fairness_key)`.
    fair_cursors: Mutex<BTreeMap<i64, (String, String, String)>>,
    last_expiry_sweep_ms: AtomicU64,
    initial_analyze_done: AtomicBool,
}

impl Postgres {
    pub(crate) async fn open(journal: &JournalConfig) -> Result<Arc<Self>> {
        let config = journal
            .postgres
            .as_ref()
            .context("PostgreSQL config missing")?;
        let valid_schema = !config.schema.is_empty()
            && config.schema.len() <= 63
            && config.schema.bytes().enumerate().all(|(i, b)| {
                b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit())
            });
        if !valid_schema || config.schema.starts_with("pg_") {
            bail!("PostgreSQL schema must be a non-reserved SQL identifier of 1..63 bytes");
        }
        if config.lease_duration < Duration::from_secs(3)
            || config.lease_duration > Duration::from_secs(300)
            || config.poll_interval < Duration::from_millis(10)
            || config.poll_interval > config.lease_duration / 4
            || config.max_connections < 2
            || journal.queue_capacity == 0
            || !journal.full_sync
            || journal.max_bytes.is_some()
        {
            bail!(
                "invalid PostgreSQL lease/poll/pool configuration; full_sync must be true and max_bytes is SQLite-only"
            );
        }
        // Never attach the raw connection URL (which can contain credentials)
        // to an error or Debug output.
        let options: PgConnectOptions = config
            .url
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid PostgreSQL connection URL"))?;
        let node_id = uuid::Uuid::new_v4().to_string();
        let options = options
            .application_name(&format!("anyserve:{node_id}"))
            .options([
                ("search_path", format!("\"{}\",pg_catalog", config.schema)),
                ("synchronous_commit", "on".to_string()),
                ("statement_timeout", "5000".to_string()),
                ("lock_timeout", "3000".to_string()),
                ("idle_in_transaction_session_timeout", "10000".to_string()),
            ]);
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(options.clone())
            .await
            .context("connect PostgreSQL")?;
        let heartbeat_pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(config.lease_duration / 3)
            .connect_with(options)
            .await
            .context("connect PostgreSQL heartbeat")?;
        // Steady-state restarts inspect schema version read-only so they never
        // contend for ACCESS EXCLUSIVE locks behind active claim/submit work.
        let current_version = schema_version(&pool).await?;
        match current_version {
            Some(2) => {}
            Some(v) if v > 2 => bail!("unsupported Anyserve PostgreSQL schema version {v}"),
            _ => {
                let mut tx = pool.begin().await?;
                sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 42517))")
                    .bind(format!("anyserve-schema:{}", config.schema))
                    .execute(&mut *tx)
                    .await?;
                sqlx::query(&format!(
                    "CREATE SCHEMA IF NOT EXISTS \"{}\"",
                    config.schema
                ))
                .execute(&mut *tx)
                .await?;
                let locked_version = schema_version_tx(&mut tx).await?;
                match locked_version {
                    None => {
                        sqlx::raw_sql(include_str!("schema.sql"))
                            .execute(&mut *tx)
                            .await?;
                    }
                    Some(1) => {
                        migrate_v1_to_v2(&mut tx).await?;
                    }
                    Some(2) => {}
                    Some(v) => bail!("unsupported Anyserve PostgreSQL schema version {v}"),
                }
                tx.commit().await?;
            }
        }
        let started = Instant::now();
        let generation: i64 = sqlx::query_scalar(
            "INSERT INTO nodes(node_id,generation,expires_at,internal_grpc_addr)
             VALUES($1,nextval('node_generation'),clock_timestamp()+$2*interval '1 millisecond',$3)
             RETURNING generation",
        )
        .bind(&node_id)
        .bind(config.lease_duration.as_millis() as i64)
        .bind(config.internal_grpc_addr.as_deref())
        .fetch_one(&heartbeat_pool)
        .await?;
        let (frame_batch_tx, frame_batch_rx) = mpsc::channel(journal.queue_capacity.max(256));
        let pg = Arc::new(Self {
            pool,
            heartbeat_pool,
            config: config.clone(),
            node_id,
            generation: generation as u64,
            fenced: AtomicBool::new(false),
            valid_until: Mutex::new(started + config.lease_duration),
            operations: Arc::new(Semaphore::new(journal.queue_capacity)),
            frame_batch_tx,
            pending: AtomicUsize::new(0),
            stats: Mutex::new(JournalStats::default()),
            fair_cursors: Mutex::new(BTreeMap::new()),
            last_expiry_sweep_ms: AtomicU64::new(0),
            initial_analyze_done: AtomicBool::new(false),
        });
        tokio::spawn(write::run_frame_batcher(
            Arc::downgrade(&pg),
            frame_batch_rx,
        ));
        Ok(pg)
    }

    pub(crate) async fn list_live_peers(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT node_id, internal_grpc_addr FROM nodes
             WHERE node_id <> $1
               AND expires_at > clock_timestamp()
               AND internal_grpc_addr IS NOT NULL
               AND internal_grpc_addr <> ''",
        )
        .bind(&self.node_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get("node_id"), r.get("internal_grpc_addr")))
            .collect())
    }

    pub(crate) fn healthy(&self) -> bool {
        if Instant::now() >= *self.valid_until.lock().expect("node deadline") {
            self.fence();
        }
        !self.fenced.load(Ordering::Acquire)
    }

    pub(crate) fn fence(&self) {
        self.fenced.store(true, Ordering::Release);
    }

    pub(crate) fn ensure_healthy(&self) -> Result<(), RuntimeError> {
        if self.healthy() {
            Ok(())
        } else {
            Err(RuntimeError::unavailable(
                "PostgreSQL node lease lost; restart this replica to rejoin",
            ))
        }
    }

    pub(crate) async fn heartbeat(&self) -> Result<()> {
        loop {
            self.ensure_healthy()?;
            let deadline = *self.valid_until.lock().expect("node deadline");
            let started = Instant::now();
            let renewed = tokio::time::timeout_at(
                deadline
                    .min(started + self.config.lease_duration / 3)
                    .into(),
                async {
                    let mut tx = self.heartbeat_pool.begin().await?;
                    // Check expiry after any lock wait, not against a pre-wait row.
                    sqlx::query("SELECT node_id FROM nodes WHERE node_id=$1 FOR UPDATE")
                        .bind(&self.node_id)
                        .execute(&mut *tx)
                        .await?;
                    let result = sqlx::query(
                        "UPDATE nodes SET expires_at=clock_timestamp()+$2*interval '1 millisecond'
                        WHERE node_id=$1 AND expires_at>clock_timestamp()",
                    )
                    .bind(&self.node_id)
                    .bind(self.config.lease_duration.as_millis() as i64)
                    .execute(&mut *tx)
                    .await?;
                    tx.commit().await?;
                    Ok::<_, sqlx::Error>(result)
                },
            )
            .await;
            if let Ok(Ok(result)) = renewed {
                let mut valid_until = self.valid_until.lock().expect("node deadline");
                if result.rows_affected() == 1
                    && Instant::now() < *valid_until
                    && !self.fenced.load(Ordering::Acquire)
                {
                    *valid_until = started + self.config.lease_duration;
                    return Ok(());
                }
                self.fence();
                bail!("PostgreSQL node lease renewal failed; replica fenced");
            }
            // An uncertain renewal never extends the last confirmed local deadline.
            self.ensure_healthy()?;
            tracing::warn!("PostgreSQL lease renewal failed; retrying within confirmed lease");
            tokio::time::sleep_until(
                deadline
                    .min(Instant::now() + Duration::from_millis(100))
                    .into(),
            )
            .await;
        }
    }

    pub(crate) fn enqueue(
        self: &Arc<Self>,
        op: JournalOp,
    ) -> Result<oneshot::Receiver<CommitOutcome>, RuntimeError> {
        self.ensure_healthy()?;
        let permit = self
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| RuntimeError::exhausted("PostgreSQL commit queue is full"))?;
        let (reply, receiver) = oneshot::channel();
        let store = Arc::clone(self);
        self.pending.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            let started = Instant::now();
            let outcome = match tokio::time::timeout(Duration::from_secs(10), store.apply(op)).await
            {
                Ok(outcome) => outcome,
                Err(_) => CommitOutcome::Unknown("PostgreSQL operation timed out".into()),
            };
            if matches!(outcome, CommitOutcome::Unknown(_)) {
                store.fence();
            }
            if outcome == CommitOutcome::Committed {
                let mut stats = store.stats.lock().expect("PG stats");
                stats.committed_ops += 1;
                stats.committed_batches += 1;
                stats.last_batch_size = 1;
                stats.last_commit_micros = started.elapsed().as_micros() as u64;
            }
            store.pending.fetch_sub(1, Ordering::Relaxed);
            let _ = reply.send(outcome);
            drop(permit);
        });
        Ok(receiver)
    }

    pub(crate) fn stats(&self) -> JournalStats {
        let mut stats = self.stats.lock().expect("PG stats").clone();
        stats.pending_ops = self.pending.load(Ordering::Relaxed);
        stats
    }

    pub(crate) async fn close(&self) {
        self.fence();
        // Expire immediately on graceful shutdown. Owned job writes check
        // this row and the epoch, so an old write cannot overwrite a takeover.
        let _ = sqlx::query("UPDATE nodes SET expires_at=clock_timestamp() WHERE node_id=$1")
            .bind(&self.node_id)
            .execute(&self.heartbeat_pool)
            .await;
        self.pool.close().await;
        self.heartbeat_pool.close().await;
    }

    pub(crate) async fn lost_jobs(&self, ids: &[String]) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT job_id FROM jobs WHERE job_id=ANY($1) AND owner_id=$2
            AND state IN ('queued','running')",
        )
        .bind(ids)
        .bind(&self.node_id)
        .fetch_all(&self.pool)
        .await?;
        let owned: std::collections::HashSet<String> =
            rows.iter().map(|r| r.get("job_id")).collect();
        Ok(ids
            .iter()
            .filter(|id| !owned.contains(*id))
            .cloned()
            .collect())
    }
}

async fn schema_version(pool: &PgPool) -> Result<Option<i64>> {
    let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass('cluster_schema')::text")
        .fetch_one(pool)
        .await?;
    if exists.is_none() {
        return Ok(None);
    }
    Ok(
        sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(version) FROM cluster_schema")
            .fetch_one(pool)
            .await?,
    )
}

async fn schema_version_tx(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<Option<i64>> {
    let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass('cluster_schema')::text")
        .fetch_one(&mut **tx)
        .await?;
    if exists.is_none() {
        return Ok(None);
    }
    Ok(
        sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(version) FROM cluster_schema")
            .fetch_one(&mut **tx)
            .await?,
    )
}

async fn migrate_v1_to_v2(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<()> {
    sqlx::raw_sql(
        "ALTER TABLE nodes ADD COLUMN IF NOT EXISTS internal_grpc_addr TEXT;
         ALTER TABLE jobs ADD COLUMN IF NOT EXISTS namespace TEXT NOT NULL DEFAULT 'default';
         ALTER TABLE jobs ADD COLUMN IF NOT EXISTS fairness_key TEXT NOT NULL DEFAULT '_default';
         ALTER TABLE jobs ADD COLUMN IF NOT EXISTS concurrency_key TEXT;
         ALTER TABLE jobs ADD COLUMN IF NOT EXISTS failed_workers TEXT[] NOT NULL DEFAULT '{}';
         DROP TABLE IF EXISTS concurrency_slots;
         DROP INDEX IF EXISTS jobs_claim;
         DROP INDEX IF EXISTS jobs_claim_order;
         DROP INDEX IF EXISTS jobs_owner;
         DROP INDEX IF EXISTS jobs_claim_fair_groups;
         CREATE INDEX IF NOT EXISTS jobs_claim_fair_groups
             ON jobs(namespace, interface, priority DESC, fairness_key ASC, queued_at_ms ASC, job_id ASC)
             WHERE state = 'queued' AND owner_id IS NULL;
         CREATE INDEX IF NOT EXISTS jobs_queued_expiry ON jobs (
             LEAST(
                 COALESCE(total_deadline_ms, 9223372036854775807),
                 CASE WHEN queue_timeout_ms IS NULL THEN 9223372036854775807
                      WHEN queued_at_ms > 9223372036854775807 - queue_timeout_ms THEN 9223372036854775807
                      ELSE queued_at_ms + queue_timeout_ms END
             )
         ) WHERE state = 'queued' AND owner_id IS NULL
           AND (total_deadline_ms IS NOT NULL OR queue_timeout_ms IS NOT NULL);
         CREATE INDEX IF NOT EXISTS jobs_orphan_recovery
             ON jobs(owner_id, updated_at_ms)
             WHERE state IN ('queued','running') AND owner_id IS NOT NULL;
         CREATE INDEX IF NOT EXISTS jobs_concurrency_active
             ON jobs(namespace, concurrency_key)
             WHERE concurrency_key IS NOT NULL AND concurrency_key <> '' AND owner_id IS NOT NULL AND state IN ('queued','running');
         CREATE INDEX IF NOT EXISTS jobs_ns_created ON jobs(namespace, created_at_ms DESC, job_id DESC);
         INSERT INTO cluster_schema VALUES (2) ON CONFLICT DO NOTHING;",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}
