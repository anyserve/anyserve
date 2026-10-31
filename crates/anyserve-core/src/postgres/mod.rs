//! Shared durable execution. Every replica can submit and claim jobs. Node
//! leases use the database clock; job epochs fence writes after ownership moves.
//! Local memory holds only work claimed for this replica's worker connections.
mod read;
#[cfg(test)]
mod tests;
mod write;
pub(crate) use read::{JobSnapshot, ReadMode};

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use tokio::sync::{Semaphore, oneshot};

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
    pending: AtomicUsize,
    stats: Mutex<JournalStats>,
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
        let mut tx = pool.begin().await?;
        // Only DDL needs serialization. This transaction lock is released
        // before this node joins; there is no cluster leader or lifetime lock.
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
        sqlx::raw_sql(include_str!("schema.sql"))
            .execute(&mut *tx)
            .await?;
        let versions: Vec<i64> = sqlx::query_scalar("SELECT version FROM cluster_schema")
            .fetch_all(&mut *tx)
            .await?;
        if versions != [1] {
            bail!("unsupported Anyserve PostgreSQL schema version");
        }
        tx.commit().await?;
        let started = Instant::now();
        let generation: i64 = sqlx::query_scalar(
            "INSERT INTO nodes(node_id,generation,expires_at)
             VALUES($1,nextval('node_generation'),clock_timestamp()+$2*interval '1 millisecond')
             RETURNING generation",
        )
        .bind(&node_id)
        .bind(config.lease_duration.as_millis() as i64)
        .fetch_one(&heartbeat_pool)
        .await?;
        Ok(Arc::new(Self {
            pool,
            heartbeat_pool,
            config: config.clone(),
            node_id,
            generation: generation as u64,
            fenced: AtomicBool::new(false),
            valid_until: Mutex::new(started + config.lease_duration),
            operations: Arc::new(Semaphore::new(journal.queue_capacity)),
            pending: AtomicUsize::new(0),
            stats: Mutex::new(JournalStats::default()),
        }))
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
