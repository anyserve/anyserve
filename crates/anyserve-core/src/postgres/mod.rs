//! Single-leader PostgreSQL journal backend.
//!
//! Only the active leader holding `scheduler_lease` mutates durable state.
//! Every write batch executes as a single implicit-transaction SQL statement
//! (`WITH lease AS (... FOR SHARE), ... UNNEST(...)`) in one round-trip so a
//! stalled or crashed leader never holds an open interactive transaction across
//! network waits.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use futures::TryStreamExt;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Postgres as Pg, QueryBuilder, Row};
use tokio::sync::{mpsc, oneshot, watch};

use crate::error::RuntimeError;
use crate::journal::{
    BoxFuture, CommitOutcome, JournalBackend, JournalOp, Leadership, LeaseHandle, Recovery,
    StoredJob, StoredJobPreview, StoredSpecRef, TerminalRecovery, job_from_row, metadata_to_json,
    object_from_row, state_text,
};
use crate::runtime::api::{
    INSPECTION_PREVIEW_BYTES, JobState, JournalConfig, JournalStats, Object, ObjectPreview,
    PostgresConfig,
};
use crate::runtime::events::{ByteBudget, Permit, budgeted_bytes};
use crate::runtime::identity::{decode_scoped_id, encode_scoped_id};

const SCHEMA_VERSION: i64 = 3;
const RECOVERY_BATCH: i64 = 256;
const PURGE_BATCH: i64 = 500;

const BATCH_WRITE_SQL: &str = r#"
WITH lease AS (
    SELECT 1 AS ok
    FROM scheduler_lease
    WHERE id = 1
      AND holder_id = $1
      AND generation = $2
      AND expires_at > clock_timestamp()
    FOR SHARE
),
ins_jobs AS (
    INSERT INTO jobs (
        job_id, namespace, state, spec_hash, spec_json,
        attempt_count, max_attempts, created_at_ms, updated_at_ms
    )
    SELECT u.job_id, u.namespace, 'queued', u.spec_hash, u.spec_json,
           0, u.max_attempts, u.created_at_ms, u.created_at_ms
    FROM UNNEST(
        $3::text[], $4::text[], $5::text[], $6::text[], $7::bigint[], $8::bigint[]
    ) AS u(job_id, namespace, spec_hash, spec_json, max_attempts, created_at_ms)
    WHERE EXISTS (SELECT 1 FROM lease)
    RETURNING job_id
),
upd_starts AS (
    UPDATE jobs AS j
    SET attempt_count = j.attempt_count + 1,
        updated_at_ms = u.at_ms,
        last_attempt_id = COALESCE(u.attempt_id, j.last_attempt_id),
        last_worker_id = COALESCE(u.worker_id, j.last_worker_id)
    FROM UNNEST(
        $9::text[], $10::bigint[], $11::text[], $12::text[]
    ) AS u(job_id, at_ms, attempt_id, worker_id)
    WHERE EXISTS (SELECT 1 FROM lease)
      AND j.job_id = u.job_id
      AND j.state = 'queued'
    RETURNING j.job_id
),
upd_terms AS (
    UPDATE jobs AS j
    SET state = u.state,
        updated_at_ms = u.at_ms,
        error_json = u.error_json,
        winning_lease_id = u.winning_lease_id,
        result_digest = u.result_digest,
        last_attempt_id = u.attempt_id,
        last_worker_id = u.worker_id
    FROM UNNEST(
        $13::text[], $14::text[], $15::bigint[], $16::text[],
        $17::text[], $18::text[], $19::text[], $20::text[]
    ) AS u(job_id, state, at_ms, error_json, winning_lease_id, result_digest, attempt_id, worker_id)
    WHERE EXISTS (SELECT 1 FROM lease)
      AND j.job_id = u.job_id
      AND j.state = 'queued'
    RETURNING j.job_id
),
ins_objs AS (
    INSERT INTO objects (job_id, role, idx, uri, content, metadata_json)
    SELECT o.job_id, o.role, o.idx, o.uri, o.content, o.metadata_json
    FROM UNNEST(
        $21::text[], $22::text[], $23::bigint[], $24::text[], $25::bytea[], $26::text[]
    ) AS o(job_id, role, idx, uri, content, metadata_json)
    WHERE EXISTS (SELECT 1 FROM lease)
      AND (
          o.role IN ('input', 'params')
          OR EXISTS (SELECT 1 FROM upd_terms ut WHERE ut.job_id = o.job_id)
      )
    RETURNING 1
)
SELECT
    EXISTS (SELECT 1 FROM lease) AS lease_ok,
    ARRAY(SELECT job_id FROM ins_jobs) AS accepted_ids,
    ARRAY(SELECT job_id FROM upd_starts) AS started_ids,
    ARRAY(SELECT job_id FROM upd_terms) AS terminal_ids
"#;

const PURGE_BATCH_SQL: &str = r#"
WITH lease AS (
    SELECT 1
    FROM scheduler_lease
    WHERE id = 1
      AND holder_id = $1
      AND generation = $2
      AND expires_at > clock_timestamp()
    FOR SHARE
),
victim AS (
    SELECT job_id
    FROM jobs
    WHERE EXISTS (SELECT 1 FROM lease)
      AND state IN ('succeeded', 'failed', 'cancelled')
      AND updated_at_ms < $3
    LIMIT $4
),
deleted AS (
    DELETE FROM jobs
    WHERE job_id IN (SELECT job_id FROM victim)
    RETURNING 1
)
SELECT
    EXISTS (SELECT 1 FROM lease) AS lease_ok,
    (SELECT COUNT(*) FROM deleted)::bigint AS removed
"#;

const JOB_COLUMNS: &str = "SELECT job_id, state, spec_hash, spec_json, attempt_count,
    max_attempts, created_at_ms, updated_at_ms, error_json, winning_lease_id,
    result_digest, last_attempt_id, last_worker_id FROM jobs";
const JOB_BY_ID_SQL: &str = "SELECT job_id, state, spec_hash, spec_json, attempt_count,
    max_attempts, created_at_ms, updated_at_ms, error_json, winning_lease_id,
    result_digest, last_attempt_id, last_worker_id FROM jobs WHERE job_id = $1";
const RECOVERY_FIRST_SQL: &str = "SELECT job_id, state, spec_hash, spec_json, attempt_count,
    max_attempts, created_at_ms, updated_at_ms, error_json, winning_lease_id,
    result_digest, last_attempt_id, last_worker_id FROM jobs
    WHERE state = 'queued' ORDER BY created_at_ms, job_id LIMIT $1";
const RECOVERY_NEXT_SQL: &str = "SELECT job_id, state, spec_hash, spec_json, attempt_count,
    max_attempts, created_at_ms, updated_at_ms, error_json, winning_lease_id,
    result_digest, last_attempt_id, last_worker_id FROM jobs
    WHERE state = 'queued' AND (created_at_ms, job_id) > ($1, $2)
    ORDER BY created_at_ms, job_id LIMIT $3";
const RECOVERY_TERMINAL_SQL: &str = "SELECT job_id, state, spec_hash, spec_json, attempt_count,
    max_attempts, created_at_ms, updated_at_ms, error_json, winning_lease_id,
    result_digest, last_attempt_id, last_worker_id FROM jobs
    WHERE state IN ('succeeded', 'failed', 'cancelled') AND updated_at_ms >= $1
    ORDER BY updated_at_ms DESC, job_id DESC LIMIT $2";

enum WriteRequest {
    Op {
        term: u64,
        op: JournalOp,
        reply: oneshot::Sender<CommitOutcome>,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
}

type ActiveTermSender = Mutex<Option<(u64, watch::Sender<bool>)>>;

pub(crate) struct PgJournal {
    read_pool: PgPool,
    lease_pool: PgPool,
    config: PostgresConfig,
    terminal_recovery: TerminalRecovery,
    holder_id: String,
    tx: mpsc::Sender<WriteRequest>,
    pending: Arc<AtomicUsize>,
    stats: Arc<Mutex<JournalStats>>,
    active_lost_tx: Arc<ActiveTermSender>,
    #[cfg(test)]
    pause_renewal: Arc<std::sync::atomic::AtomicBool>,
}

impl PgJournal {
    pub(crate) async fn open_with_recovery(
        journal: &JournalConfig,
        terminal_recovery: TerminalRecovery,
    ) -> Result<Arc<dyn JournalBackend>> {
        Ok(Self::open_concrete_with_recovery(journal, terminal_recovery).await?)
    }

    #[cfg(test)]
    pub(crate) async fn open_concrete(journal: &JournalConfig) -> Result<Arc<Self>> {
        Self::open_concrete_with_recovery(journal, TerminalRecovery::default()).await
    }

    pub(crate) async fn open_concrete_with_recovery(
        journal: &JournalConfig,
        terminal_recovery: TerminalRecovery,
    ) -> Result<Arc<Self>> {
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
        let lease_ms = u64::try_from(config.lease_duration.as_millis()).unwrap_or(u64::MAX);
        let poll_ms = u64::try_from(config.poll_interval.as_millis()).unwrap_or(u64::MAX);
        if !(500..=300_000).contains(&lease_ms)
            || poll_ms < 10
            || poll_ms > lease_ms / 2
            || config.max_connections < 2
            || journal.queue_capacity == 0
            || journal.max_batch == 0
            || !journal.full_sync
            || journal.max_bytes.is_some()
        {
            bail!(
                "invalid PostgreSQL lease/poll/pool configuration; full_sync must be true and max_bytes is SQLite-only"
            );
        }

        let base_options: PgConnectOptions = config
            .url
            .parse()
            .map_err(|_| anyhow!("invalid PostgreSQL connection URL"))?;
        let holder_id = uuid::Uuid::new_v4().to_string();
        let search_path = format!("\"{}\",pg_catalog", config.schema);

        let stmt_timeout_ms = lease_ms.clamp(500, 10_000);
        let idle_tx_ms = (lease_ms * 3 / 10).clamp(250, 5_000);
        let write_lock_ms = (lease_ms / 2).clamp(250, 5_000);
        let acquire_lock_ms = (lease_ms / 4).clamp(100, 500);

        let read_options = base_options
            .clone()
            .application_name(&format!("anyserve-read:{holder_id}"))
            .options([
                ("search_path", search_path.clone()),
                ("statement_timeout", format!("{stmt_timeout_ms}")),
                ("lock_timeout", format!("{write_lock_ms}")),
                (
                    "idle_in_transaction_session_timeout",
                    format!("{idle_tx_ms}"),
                ),
            ]);
        let write_options = base_options
            .clone()
            .application_name(&format!("anyserve-write:{holder_id}"))
            .options([
                ("search_path", search_path.clone()),
                ("synchronous_commit", "on".to_string()),
                ("statement_timeout", format!("{stmt_timeout_ms}")),
                ("lock_timeout", format!("{write_lock_ms}")),
                (
                    "idle_in_transaction_session_timeout",
                    format!("{idle_tx_ms}"),
                ),
            ]);
        let lease_options = base_options
            .application_name(&format!("anyserve-lease:{holder_id}"))
            .options([
                ("search_path", search_path),
                ("synchronous_commit", "on".to_string()),
                (
                    "statement_timeout",
                    format!("{}", stmt_timeout_ms.min(2_000)),
                ),
                ("lock_timeout", format!("{acquire_lock_ms}")),
                (
                    "idle_in_transaction_session_timeout",
                    format!("{idle_tx_ms}"),
                ),
            ]);

        let read_conns = config.max_connections.saturating_sub(1).max(1);
        let read_pool = PgPoolOptions::new()
            .max_connections(read_conns)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(read_options)
            .await
            .context("connect PostgreSQL read pool")?;

        ensure_schema_v3(&read_pool, &config.schema).await?;

        let tx_timeout_sql = format!("SET transaction_timeout = '{stmt_timeout_ms}ms'");
        let write_pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(stmt_timeout_ms))
            .after_connect(move |conn, _meta| {
                let sql = tx_timeout_sql.clone();
                Box::pin(async move {
                    // Best-effort on PG 17+; older versions enforce statement + idle_in_transaction timeouts.
                    let _ = sqlx::query(&sql).execute(conn).await;
                    Ok(())
                })
            })
            .connect_with(write_options)
            .await
            .context("connect PostgreSQL write pool")?;
        let lease_pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis((lease_ms / 3).max(200)))
            .connect_with(lease_options)
            .await
            .context("connect PostgreSQL lease pool")?;

        let (tx, rx) = mpsc::channel(journal.queue_capacity.max(1));
        let pending = Arc::new(AtomicUsize::new(0));
        let stats = Arc::new(Mutex::new(JournalStats::default()));
        let active_lost_tx = Arc::new(Mutex::new(None));
        #[cfg(test)]
        let pause_renewal = Arc::new(std::sync::atomic::AtomicBool::new(false));

        tokio::spawn(writer_loop(
            write_pool,
            read_pool.clone(),
            holder_id.clone(),
            rx,
            journal.max_batch.max(1),
            Arc::clone(&pending),
            Arc::clone(&stats),
            Arc::clone(&active_lost_tx),
        ));

        Ok(Arc::new(Self {
            read_pool,
            lease_pool,
            config: config.clone(),
            terminal_recovery,
            holder_id,
            tx,
            pending,
            stats,
            active_lost_tx,
            #[cfg(test)]
            pause_renewal,
        }))
    }

    #[cfg(test)]
    pub(crate) fn set_pause_renewal(&self, paused: bool) {
        self.pause_renewal.store(paused, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) async fn abort_without_relinquish(&self) {
        self.pause_renewal.store(true, Ordering::SeqCst);
        self.lease_pool.close().await;
        self.read_pool.close().await;
    }
}

impl JournalBackend for PgJournal {
    fn try_acquire_leadership(&self) -> BoxFuture<'_, Result<Option<Leadership>>> {
        Box::pin(async move {
            let lease_ms = self.config.lease_duration.as_millis() as i64;
            let started = Instant::now();
            let acquired = sqlx::query_scalar::<_, i64>(
                "INSERT INTO scheduler_lease (id, holder_id, generation, expires_at)
                 VALUES (1, $1, 1, clock_timestamp() + $2 * interval '1 millisecond')
                 ON CONFLICT (id) DO UPDATE
                 SET holder_id = EXCLUDED.holder_id,
                     generation = scheduler_lease.generation + 1,
                     expires_at = EXCLUDED.expires_at
                 WHERE scheduler_lease.expires_at <= clock_timestamp()
                 RETURNING generation",
            )
            .bind(&self.holder_id)
            .bind(lease_ms)
            .fetch_optional(&self.lease_pool)
            .await;

            let term = match acquired {
                Ok(Some(term_i64)) => term_i64 as u64,
                Ok(None) => return Ok(None),
                Err(err) => {
                    tracing::debug!(%err, "scheduler_lease acquire contended or timed out");
                    return Ok(None);
                }
            };

            let recovery = load_recovery(&self.read_pool, term, self.terminal_recovery).await?;
            // Confirm lease is still held after loading the recovery snapshot.
            let still_valid: Option<i32> = sqlx::query_scalar(
                "SELECT 1 FROM scheduler_lease
                 WHERE id = 1 AND holder_id = $1 AND generation = $2 AND expires_at > clock_timestamp()",
            )
            .bind(&self.holder_id)
            .bind(term as i64)
            .fetch_optional(&self.lease_pool)
            .await?;
            if still_valid.is_none() || started.elapsed() >= self.config.lease_duration {
                return Ok(None);
            }

            let (lost_tx, lost_rx) = watch::channel(false);
            *self
                .active_lost_tx
                .lock()
                .expect("active lost sender poisoned") = Some((term, lost_tx.clone()));

            let (stop_renew_tx, stop_renew_rx) = watch::channel(false);
            tokio::spawn(renew_lease_loop(
                self.lease_pool.clone(),
                self.holder_id.clone(),
                term,
                self.config.lease_duration,
                started + self.config.lease_duration,
                lost_tx,
                stop_renew_rx,
                #[cfg(test)]
                Arc::clone(&self.pause_renewal),
            ));

            let lease_pool = self.lease_pool.clone();
            let holder_id = self.holder_id.clone();
            let lease = LeaseHandle::new(lost_rx, move || {
                Box::pin(async move {
                    stop_renew_tx.send_replace(true);
                    let _ = sqlx::query(
                        "UPDATE scheduler_lease
                         SET expires_at = clock_timestamp()
                         WHERE id = 1 AND holder_id = $1 AND generation = $2",
                    )
                    .bind(&holder_id)
                    .bind(term as i64)
                    .execute(&lease_pool)
                    .await;
                })
            });

            Ok(Some(Leadership {
                term,
                recovery,
                lease,
            }))
        })
    }

    fn standby_poll_interval(&self) -> Duration {
        self.config.poll_interval
    }

    fn enqueue(
        &self,
        term: u64,
        op: JournalOp,
    ) -> Result<oneshot::Receiver<CommitOutcome>, RuntimeError> {
        let (reply, receiver) = oneshot::channel();
        self.pending.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(WriteRequest::Op { term, op, reply }) {
            Ok(()) => Ok(receiver),
            Err(err) => {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                Err(match err {
                    mpsc::error::TrySendError::Full(_) => {
                        RuntimeError::exhausted("PostgreSQL journal commit queue is full")
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        RuntimeError::unavailable("PostgreSQL journal is closed")
                    }
                })
            }
        }
    }

    fn spec_hash<'a>(&'a self, job_id: &'a str) -> BoxFuture<'a, Result<Option<String>>> {
        Box::pin(async move {
            sqlx::query_scalar("SELECT spec_hash FROM jobs WHERE job_id = $1")
                .bind(job_id)
                .fetch_optional(&self.read_pool)
                .await
                .context("look up PostgreSQL job spec_hash")
        })
    }

    fn receipt<'a>(&'a self, lease_id: &'a str) -> BoxFuture<'a, Result<Option<(String, String)>>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT job_id, result_digest FROM jobs
                 WHERE winning_lease_id = $1 AND result_digest IS NOT NULL",
            )
            .bind(lease_id)
            .fetch_optional(&self.read_pool)
            .await
            .context("look up PostgreSQL completion receipt")?;
            row.map(|r| Ok((r.try_get("job_id")?, r.try_get("result_digest")?)))
                .transpose()
        })
    }

    fn load_job<'a>(
        &'a self,
        job_id: &'a str,
        include_outputs: bool,
        result_budget: Arc<ByteBudget>,
    ) -> BoxFuture<'a, Result<Option<StoredJob>>> {
        Box::pin(async move {
            if !include_outputs {
                let row = sqlx::query(JOB_BY_ID_SQL)
                    .bind(job_id)
                    .fetch_optional(&self.read_pool)
                    .await
                    .context("load PostgreSQL job metadata")?;
                return row.as_ref().map(job_from_row).transpose();
            }
            let mut tx = self
                .read_pool
                .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .await
                .context("begin PostgreSQL job read")?;
            let Some(row) = sqlx::query(
                "SELECT job_id, state, spec_hash, spec_json, attempt_count,
                        max_attempts, created_at_ms, updated_at_ms, error_json, winning_lease_id,
                        result_digest, last_attempt_id, last_worker_id,
                        (SELECT COALESCE(SUM(octet_length(content)), 0)::bigint
                         FROM objects WHERE objects.job_id = jobs.job_id AND role = 'output' AND uri IS NULL) AS result_bytes
                 FROM jobs
                 WHERE job_id = $1",
            )
            .bind(job_id)
            .fetch_optional(&mut *tx)
            .await?
            else {
                return Ok(None);
            };
            let mut job = job_from_row(&row)?;
            if job.state == JobState::Succeeded {
                let bytes: i64 = row.try_get("result_bytes")?;
                let mut permit = Permit::new(result_budget);
                if !permit.grow(usize::try_from(bytes).context("PostgreSQL result size")?) {
                    return Err(RuntimeError::exhausted(
                        "result budget is exhausted; retry the read later",
                    )
                    .into());
                }
                let mut rows = sqlx::query(
                    "SELECT uri, content, metadata_json FROM objects
                     WHERE job_id = $1 AND role = 'output' ORDER BY idx",
                )
                .bind(job_id)
                .fetch(&mut *tx);
                while let Some(obj_row) = rows.try_next().await? {
                    let mut object = object_from_row(&obj_row)?;
                    if let Object::Inline { data, .. } = &mut object {
                        let reserved = permit.split(data.len());
                        *data = budgeted_bytes(std::mem::take(data), reserved);
                    }
                    job.outputs.push(object);
                }
            }
            tx.commit().await?;
            Ok(Some(job))
        })
    }

    fn load_job_preview<'a>(
        &'a self,
        job_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<StoredJobPreview>>> {
        Box::pin(async move {
            let mut tx = self
                .read_pool
                .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .await
                .context("begin PostgreSQL preview read")?;
            let Some(row) = sqlx::query(JOB_BY_ID_SQL)
                .bind(job_id)
                .fetch_optional(&mut *tx)
                .await?
            else {
                return Ok(None);
            };
            let job = job_from_row(&row)?;
            let rows = sqlx::query(
                "SELECT uri, substring(content FROM 1 FOR $2) AS content,
                        octet_length(content)::bigint AS size_bytes, metadata_json
                 FROM objects
                 WHERE job_id = $1 AND role = 'output'
                 ORDER BY idx",
            )
            .bind(job_id)
            .bind(INSPECTION_PREVIEW_BYTES as i32)
            .fetch_all(&mut *tx)
            .await?;
            let mut previews = Vec::with_capacity(rows.len());
            for r in rows {
                let prefix = object_from_row(&r)?;
                let size_bytes = if matches!(prefix, Object::Inline { .. }) {
                    r.try_get::<Option<i64>, _>("size_bytes")?
                        .map(|n| usize::try_from(n).unwrap_or(0))
                } else {
                    None
                };
                previews.push(ObjectPreview { prefix, size_bytes });
            }
            tx.commit().await?;
            Ok(Some((job, previews)))
        })
    }

    fn scan_jobs<'a>(
        &'a self,
        from_ms: u64,
        to_ms: u64,
        before: Option<(u64, String)>,
        namespace: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<StoredJob>>> {
        Box::pin(async move {
            let before = before.map(|(at, id)| match namespace {
                Some(ns) => (at, encode_scoped_id(ns, &id)),
                None => (at, id),
            });
            let mut query = QueryBuilder::<Pg>::new(JOB_COLUMNS);
            query
                .push(" WHERE state IN ('succeeded', 'failed', 'cancelled') AND created_at_ms >= ")
                .push_bind(from_ms.min(i64::MAX as u64) as i64)
                .push(" AND created_at_ms < ")
                .push_bind(to_ms.min(i64::MAX as u64) as i64);
            if let Some(ns) = namespace {
                query.push(" AND namespace = ").push_bind(ns);
            }
            if let Some((at, id)) = before {
                query
                    .push(" AND (created_at_ms, job_id) < (")
                    .push_bind(at.min(i64::MAX as u64) as i64)
                    .push(", ")
                    .push_bind(id)
                    .push(")");
            }
            query
                .push(" ORDER BY created_at_ms DESC, job_id DESC LIMIT ")
                .push_bind(limit.clamp(1, 251) as i64);
            query
                .build()
                .fetch_all(&self.read_pool)
                .await?
                .iter()
                .map(job_from_row)
                .collect()
        })
    }

    fn purge<'a>(&'a self, term: u64, before_ms: u64) -> BoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let mut total = 0usize;
            loop {
                let row = sqlx::query(PURGE_BATCH_SQL)
                    .bind(&self.holder_id)
                    .bind(term as i64)
                    .bind(before_ms.min(i64::MAX as u64) as i64)
                    .bind(PURGE_BATCH)
                    .fetch_one(&self.read_pool)
                    .await?;
                let lease_ok: bool = row.try_get("lease_ok")?;
                if !lease_ok {
                    signal_term_lost(&self.active_lost_tx, term);
                    bail!("PostgreSQL scheduler lease lost during purge");
                }
                let removed = row.try_get::<i64, _>("removed")? as usize;
                total += removed;
                if (removed as i64) < PURGE_BATCH {
                    return Ok(total);
                }
            }
        })
    }

    fn stats(&self) -> JournalStats {
        let mut stats = self
            .stats
            .lock()
            .expect("PG journal stats poisoned")
            .clone();
        stats.pending_ops = self.pending.load(Ordering::Relaxed);
        stats
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let (reply, receiver) = oneshot::channel();
            if self.tx.send(WriteRequest::Close { reply }).await.is_ok() {
                let _ = tokio::time::timeout(Duration::from_secs(5), receiver).await;
            }
            self.read_pool.close().await;
        })
    }
}

fn signal_term_lost(slot: &ActiveTermSender, term: u64) {
    if let Some((active_term, tx)) = slot.lock().expect("lost slot poisoned").as_ref()
        && *active_term == term
    {
        tx.send_replace(true);
    }
}

#[allow(clippy::too_many_arguments)]
async fn renew_lease_loop(
    lease_pool: PgPool,
    holder_id: String,
    term: u64,
    lease_duration: Duration,
    mut confirmed_until: Instant,
    lost_tx: watch::Sender<bool>,
    mut stop: watch::Receiver<bool>,
    #[cfg(test)] pause_renewal: Arc<std::sync::atomic::AtomicBool>,
) {
    let renew_every = Duration::from_millis((lease_duration.as_millis() as u64 * 3 / 10).max(100));
    let lease_ms = lease_duration.as_millis() as i64;
    loop {
        #[cfg(test)]
        if pause_renewal.load(Ordering::Acquire) {
            tokio::select! {
                _ = stop.changed() => if *stop.borrow() { return; },
                _ = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
            continue;
        }
        let sleep_until = confirmed_until.min(Instant::now() + renew_every);
        tokio::select! {
            _ = stop.changed() => {
                if *stop.borrow() {
                    return;
                }
            }
            _ = tokio::time::sleep_until(sleep_until.into()) => {}
        }
        if *stop.borrow() {
            return;
        }
        if Instant::now() >= confirmed_until {
            lost_tx.send_replace(true);
            return;
        }
        loop {
            if *stop.borrow() {
                return;
            }
            if Instant::now() >= confirmed_until {
                lost_tx.send_replace(true);
                return;
            }
            let started = Instant::now();
            let deadline = confirmed_until.min(started + renew_every);
            let res = tokio::time::timeout_at(
                deadline.into(),
                sqlx::query(
                    "UPDATE scheduler_lease
                     SET expires_at = clock_timestamp() + $3 * interval '1 millisecond'
                     WHERE id = 1
                       AND holder_id = $1
                       AND generation = $2
                       AND expires_at > clock_timestamp()",
                )
                .bind(&holder_id)
                .bind(term as i64)
                .bind(lease_ms)
                .execute(&lease_pool),
            )
            .await;

            match res {
                Ok(Ok(done)) if done.rows_affected() == 1 && Instant::now() < confirmed_until => {
                    confirmed_until = started + lease_duration;
                    break;
                }
                Ok(Ok(_)) => {
                    // Definitely lost or superseded in database time.
                    lost_tx.send_replace(true);
                    return;
                }
                _ => {
                    if Instant::now() >= confirmed_until {
                        lost_tx.send_replace(true);
                        return;
                    }
                    let retry_at = confirmed_until.min(Instant::now() + Duration::from_millis(80));
                    tokio::time::sleep_until(retry_at.into()).await;
                }
            }
        }
    }
}

async fn ensure_schema_v3(pool: &PgPool, schema: &str) -> Result<()> {
    if let Some(v) = read_schema_version(pool).await? {
        if v == SCHEMA_VERSION {
            return Ok(());
        }
        bail!(
            "incompatible PostgreSQL schema version {v} in schema '{schema}'; single-leader Anyserve requires schema v{SCHEMA_VERSION} (drain existing deployment and recreate the schema)"
        );
    }
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 42517))")
        .bind(format!("anyserve-schema:{schema}"))
        .execute(&mut *tx)
        .await?;
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
        .execute(&mut *tx)
        .await?;
    sqlx::query(&format!("SET LOCAL search_path = \"{schema}\", pg_catalog"))
        .execute(&mut *tx)
        .await?;
    let existing_legacy: Option<String> = sqlx::query_scalar(
        "SELECT COALESCE(to_regclass('nodes')::text, to_regclass('events')::text)",
    )
    .fetch_one(&mut *tx)
    .await?;
    if existing_legacy.is_some() {
        bail!(
            "incompatible legacy tables found in PostgreSQL schema '{schema}'; recreate the schema for v{SCHEMA_VERSION}"
        );
    }
    let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass('cluster_schema')::text")
        .fetch_one(&mut *tx)
        .await?;
    if exists.is_some() {
        let v: Option<i64> = sqlx::query_scalar("SELECT MAX(version) FROM cluster_schema")
            .fetch_one(&mut *tx)
            .await?;
        if let Some(v) = v
            && v != SCHEMA_VERSION
        {
            bail!(
                "incompatible PostgreSQL schema version {v} in schema '{schema}'; recreate the schema for v{SCHEMA_VERSION}"
            );
        }
    } else {
        sqlx::raw_sql(include_str!("schema.sql"))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn read_schema_version(pool: &PgPool) -> Result<Option<i64>> {
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

async fn load_recovery(
    pool: &PgPool,
    generation: u64,
    terminal_recovery: TerminalRecovery,
) -> Result<Recovery> {
    let mut active = Vec::new();
    let mut cursor: Option<(i64, String)> = None;
    loop {
        let rows = match &cursor {
            None => {
                sqlx::query(RECOVERY_FIRST_SQL)
                    .bind(RECOVERY_BATCH)
                    .fetch_all(pool)
                    .await?
            }
            Some((created, id)) => {
                sqlx::query(RECOVERY_NEXT_SQL)
                    .bind(*created)
                    .bind(id)
                    .bind(RECOVERY_BATCH)
                    .fetch_all(pool)
                    .await?
            }
        };
        if rows.is_empty() {
            break;
        }
        let mut batch: Vec<StoredJob> = rows.iter().map(job_from_row).collect::<Result<_>>()?;
        let positions: std::collections::HashMap<String, usize> = batch
            .iter()
            .enumerate()
            .map(|(i, j)| (j.job_id.clone(), i))
            .collect();
        let ids: Vec<String> = batch.iter().map(|j| j.job_id.clone()).collect();
        let mut obj_rows = sqlx::query(
            "SELECT job_id, role, idx, uri, content, metadata_json
             FROM objects
             WHERE job_id = ANY($1) AND role IN ('input', 'params')
             ORDER BY job_id, role, idx",
        )
        .bind(&ids)
        .fetch(pool);
        while let Some(r) = obj_rows.try_next().await? {
            let id: &str = r.try_get("job_id")?;
            let job = &mut batch[positions[id]];
            let object = object_from_row(&r)?;
            match r.try_get::<&str, _>("role")? {
                "input" => job.spec.inputs.push(object),
                "params" if r.try_get::<i64, _>("idx")? == 0 => {
                    if let Object::Inline { data, .. } = object {
                        job.spec.params = data;
                    }
                }
                _ => {}
            }
        }
        let last = batch.last().expect("non-empty batch");
        cursor = Some((last.created_at_ms as i64, last.job_id.clone()));
        let done = (batch.len() as i64) < RECOVERY_BATCH;
        active.extend(batch);
        if done {
            break;
        }
    }
    let mut terminal = Vec::new();
    if terminal_recovery.limit > 0 && !terminal_recovery.retention.is_zero() {
        let retention_ms =
            u64::try_from(terminal_recovery.retention.as_millis()).unwrap_or(u64::MAX);
        let cutoff_ms = crate::now_ms().saturating_sub(retention_ms);
        let limit = i64::try_from(terminal_recovery.limit).unwrap_or(i64::MAX);
        let rows = sqlx::query(RECOVERY_TERMINAL_SQL)
            .bind(cutoff_ms as i64)
            .bind(limit)
            .fetch_all(pool)
            .await?;
        terminal = rows.iter().map(job_from_row).collect::<Result<_>>()?;
        terminal.reverse();
    }
    Ok(Recovery {
        generation,
        active,
        terminal,
    })
}

fn op_job_id(op: &JournalOp) -> Option<&str> {
    match op {
        JournalOp::Accept(r) => Some(&r.job_id),
        JournalOp::AttemptStart(r) => Some(&r.job_id),
        JournalOp::Terminal(r) => Some(&r.job_id),
        JournalOp::Probe => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn writer_loop(
    write_pool: PgPool,
    read_pool: PgPool,
    holder_id: String,
    mut rx: mpsc::Receiver<WriteRequest>,
    max_batch: usize,
    pending: Arc<AtomicUsize>,
    stats: Arc<Mutex<JournalStats>>,
    active_lost_tx: Arc<ActiveTermSender>,
) {
    let mut deferred: Option<WriteRequest> = None;
    loop {
        let first = match deferred.take() {
            Some(req) => req,
            None => match rx.recv().await {
                Some(req) => req,
                None => break,
            },
        };
        match first {
            WriteRequest::Close { reply } => {
                while let Ok(req) = rx.try_recv() {
                    if let WriteRequest::Op { term, op, reply } = req {
                        let outcomes = commit_batch(
                            &write_pool,
                            &read_pool,
                            &holder_id,
                            term,
                            &[&op],
                            &active_lost_tx,
                        )
                        .await;
                        pending.fetch_sub(1, Ordering::Relaxed);
                        let outcome = outcomes
                            .and_then(|v| v.into_iter().next())
                            .unwrap_or(CommitOutcome::Committed);
                        let _ = reply.send(outcome);
                    }
                }
                rx.close();
                write_pool.close().await;
                let _ = reply.send(());
                return;
            }
            WriteRequest::Op { term, op, reply } => {
                let mut batch = vec![(op, reply)];
                let mut seen_ids: Option<HashSet<String>> = None;
                while batch.len() < max_batch {
                    match rx.try_recv() {
                        Ok(WriteRequest::Op {
                            term: next_term,
                            op: next_op,
                            reply: next_reply,
                        }) => {
                            let seen = seen_ids.get_or_insert_with(|| {
                                op_job_id(&batch[0].0)
                                    .map(str::to_string)
                                    .into_iter()
                                    .collect()
                            });
                            let duplicate = op_job_id(&next_op).is_some_and(|id| seen.contains(id));
                            if next_term != term || duplicate {
                                deferred = Some(WriteRequest::Op {
                                    term: next_term,
                                    op: next_op,
                                    reply: next_reply,
                                });
                                break;
                            }
                            if let Some(id) = op_job_id(&next_op) {
                                seen.insert(id.to_string());
                            }
                            batch.push((next_op, next_reply));
                        }
                        Ok(other) => {
                            deferred = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                let started = Instant::now();
                let ops: Vec<&JournalOp> = batch.iter().map(|(op, _)| op).collect();
                let outcomes = commit_batch(
                    &write_pool,
                    &read_pool,
                    &holder_id,
                    term,
                    &ops,
                    &active_lost_tx,
                )
                .await;
                let elapsed = started.elapsed();
                let batch_len = batch.len();
                let committed = match &outcomes {
                    None => batch_len as u64,
                    Some(v) => v.iter().filter(|o| **o == CommitOutcome::Committed).count() as u64,
                };
                {
                    let mut st = stats.lock().expect("PG stats poisoned");
                    st.committed_batches += 1;
                    st.committed_ops += committed;
                    st.last_batch_size = batch_len;
                    st.last_commit_micros = elapsed.as_micros() as u64;
                }
                pending.fetch_sub(batch_len, Ordering::Relaxed);
                match outcomes {
                    None => {
                        for (_, reply) in batch {
                            let _ = reply.send(CommitOutcome::Committed);
                        }
                    }
                    Some(outcomes) => {
                        for ((_, reply), outcome) in batch.into_iter().zip(outcomes) {
                            let _ = reply.send(outcome);
                        }
                    }
                }
            }
        }
    }
    write_pool.close().await;
}

enum SingleStmtError {
    Sql(String),
    Transport(String),
}

async fn execute_single_statement(
    write_pool: &PgPool,
    holder_id: &str,
    term: u64,
    ops: &[&JournalOp],
) -> std::result::Result<(bool, HashSet<String>, HashSet<String>, HashSet<String>), SingleStmtError>
{
    let mut acc_job_id: Vec<&str> = Vec::new();
    let mut acc_ns: Vec<&str> = Vec::new();
    let mut acc_hash: Vec<&str> = Vec::new();
    let mut acc_spec: Vec<String> = Vec::new();
    let mut acc_max_att: Vec<i64> = Vec::new();
    let mut acc_at: Vec<i64> = Vec::new();

    let mut st_job_id: Vec<&str> = Vec::new();
    let mut st_at: Vec<i64> = Vec::new();
    let mut st_attempt_id: Vec<Option<&str>> = Vec::new();
    let mut st_worker_id: Vec<Option<&str>> = Vec::new();

    let mut tm_job_id: Vec<&str> = Vec::new();
    let mut tm_state: Vec<&str> = Vec::new();
    let mut tm_at: Vec<i64> = Vec::new();
    let mut tm_err: Vec<Option<String>> = Vec::new();
    let mut tm_lease: Vec<Option<&str>> = Vec::new();
    let mut tm_digest: Vec<Option<&str>> = Vec::new();
    let mut tm_attempt_id: Vec<Option<&str>> = Vec::new();
    let mut tm_worker_id: Vec<Option<&str>> = Vec::new();

    let mut objs = PgObjects::default();

    for op in ops {
        match op {
            JournalOp::Accept(r) => {
                let (ns, _) = decode_scoped_id(&r.job_id);
                let stored = StoredSpecRef {
                    demand: &r.spec.demand,
                    policy: &r.spec.policy,
                    metadata: &r.spec.metadata,
                };
                let spec_json = serde_json::to_string(&stored)
                    .map_err(|e| SingleStmtError::Sql(e.to_string()))?;
                acc_job_id.push(r.job_id.as_str());
                acc_ns.push(ns);
                acc_hash.push(r.spec_hash.as_str());
                acc_spec.push(spec_json);
                acc_max_att.push(i64::from(r.max_attempts));
                acc_at.push(r.created_at_ms.min(i64::MAX as u64) as i64);
                objs.push_slice(&r.job_id, "input", &r.spec.inputs)?;
                if !r.spec.params.is_empty() {
                    objs.push_row(
                        &r.job_id,
                        "params",
                        0,
                        None,
                        Some(&r.spec.params),
                        "{}".into(),
                    );
                }
            }
            JournalOp::AttemptStart(r) => {
                st_job_id.push(r.job_id.as_str());
                st_at.push(r.at_ms.min(i64::MAX as u64) as i64);
                st_attempt_id.push(r.attempt_id.as_deref());
                st_worker_id.push(r.worker_id.as_deref());
            }
            JournalOp::Terminal(r) => {
                let err_json = r
                    .error
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|e| SingleStmtError::Sql(e.to_string()))?;
                tm_job_id.push(r.job_id.as_str());
                tm_state.push(state_text(r.state));
                tm_at.push(r.at_ms.min(i64::MAX as u64) as i64);
                tm_err.push(err_json);
                tm_lease.push(r.winning_lease_id.as_deref());
                tm_digest.push(r.outcome_digest.as_deref());
                tm_attempt_id.push(r.attempt_id.as_deref());
                tm_worker_id.push(r.worker_id.as_deref());
                objs.push_slice(&r.job_id, "output", &r.outputs)?;
            }
            JournalOp::Probe => {}
        }
    }

    let row = sqlx::query(BATCH_WRITE_SQL)
        .bind(holder_id)
        .bind(term as i64)
        .bind(&acc_job_id)
        .bind(&acc_ns)
        .bind(&acc_hash)
        .bind(&acc_spec)
        .bind(&acc_max_att)
        .bind(&acc_at)
        .bind(&st_job_id)
        .bind(&st_at)
        .bind(&st_attempt_id)
        .bind(&st_worker_id)
        .bind(&tm_job_id)
        .bind(&tm_state)
        .bind(&tm_at)
        .bind(&tm_err)
        .bind(&tm_lease)
        .bind(&tm_digest)
        .bind(&tm_attempt_id)
        .bind(&tm_worker_id)
        .bind(&objs.job_id)
        .bind(&objs.role)
        .bind(&objs.idx)
        .bind(&objs.uri)
        .bind(&objs.content)
        .bind(&objs.meta)
        .fetch_one(write_pool)
        .await
        .map_err(|err| {
            if err.as_database_error().is_some() {
                SingleStmtError::Sql(err.to_string())
            } else {
                SingleStmtError::Transport(err.to_string())
            }
        })?;

    let lease_ok: bool = row
        .try_get("lease_ok")
        .map_err(|e| SingleStmtError::Sql(e.to_string()))?;
    let accepted: Vec<String> = row
        .try_get("accepted_ids")
        .map_err(|e| SingleStmtError::Sql(e.to_string()))?;
    let started: Vec<String> = row
        .try_get("started_ids")
        .map_err(|e| SingleStmtError::Sql(e.to_string()))?;
    let terminal: Vec<String> = row
        .try_get("terminal_ids")
        .map_err(|e| SingleStmtError::Sql(e.to_string()))?;
    Ok((
        lease_ok,
        accepted.into_iter().collect(),
        started.into_iter().collect(),
        terminal.into_iter().collect(),
    ))
}

async fn verify_op(pool: &PgPool, op: &JournalOp) -> Result<bool> {
    Ok(match op {
        JournalOp::Accept(r) => {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM jobs WHERE job_id = $1")
                .bind(&r.job_id)
                .fetch_one(pool)
                .await?
                > 0
        }
        JournalOp::AttemptStart(r) => {
            sqlx::query_scalar::<_, i64>("SELECT attempt_count FROM jobs WHERE job_id = $1")
                .bind(&r.job_id)
                .fetch_optional(pool)
                .await?
                .is_some_and(|c| c >= i64::from(r.attempt_number))
        }
        JournalOp::Terminal(r) => {
            sqlx::query_scalar::<_, String>("SELECT state FROM jobs WHERE job_id = $1")
                .bind(&r.job_id)
                .fetch_optional(pool)
                .await?
                .is_some_and(|s| s == state_text(r.state))
        }
        JournalOp::Probe => false,
    })
}

#[derive(Default)]
struct PgObjects<'a> {
    job_id: Vec<&'a str>,
    role: Vec<&'static str>,
    idx: Vec<i64>,
    uri: Vec<Option<&'a str>>,
    content: Vec<Option<&'a [u8]>>,
    meta: Vec<String>,
}

impl<'a> PgObjects<'a> {
    fn push_row(
        &mut self,
        job_id: &'a str,
        role: &'static str,
        idx: i64,
        uri: Option<&'a str>,
        content: Option<&'a [u8]>,
        meta: String,
    ) {
        self.job_id.push(job_id);
        self.role.push(role);
        self.idx.push(idx);
        self.uri.push(uri);
        self.content.push(content);
        self.meta.push(meta);
    }

    fn push_slice(
        &mut self,
        job_id: &'a str,
        role: &'static str,
        objects: &'a [Object],
    ) -> std::result::Result<(), SingleStmtError> {
        for (idx, obj) in objects.iter().enumerate() {
            let (uri, content, meta) = match obj {
                Object::Inline { data, metadata } => (None, Some(data.as_ref()), metadata),
                Object::Uri { uri, metadata } => (Some(uri.as_str()), None, metadata),
            };
            let meta_json = metadata_to_json(meta)
                .map_err(|e| SingleStmtError::Sql(e.to_string()))?
                .into_owned();
            self.push_row(job_id, role, idx as i64, uri, content, meta_json);
        }
        Ok(())
    }
}

fn op_committed(
    op: &JournalOp,
    accepted: &HashSet<String>,
    started: &HashSet<String>,
    terminal: &HashSet<String>,
) -> bool {
    match op {
        JournalOp::Accept(r) => accepted.contains(&r.job_id),
        JournalOp::AttemptStart(r) => started.contains(&r.job_id),
        JournalOp::Terminal(r) => terminal.contains(&r.job_id),
        JournalOp::Probe => true,
    }
}

async fn commit_batch(
    write_pool: &PgPool,
    read_pool: &PgPool,
    holder_id: &str,
    term: u64,
    ops: &[&JournalOp],
    active_lost_tx: &ActiveTermSender,
) -> Option<Vec<CommitOutcome>> {
    commit_batch_inner(
        write_pool,
        read_pool,
        holder_id,
        term,
        ops,
        active_lost_tx,
        true,
    )
    .await
}

async fn commit_batch_inner(
    write_pool: &PgPool,
    read_pool: &PgPool,
    holder_id: &str,
    term: u64,
    ops: &[&JournalOp],
    active_lost_tx: &ActiveTermSender,
    allow_split: bool,
) -> Option<Vec<CommitOutcome>> {
    match execute_single_statement(write_pool, holder_id, term, ops).await {
        Ok((false, _, _, _)) => {
            signal_term_lost(active_lost_tx, term);
            Some(vec![
                CommitOutcome::Fenced(
                    "PostgreSQL scheduler lease expired or superseded".into()
                );
                ops.len()
            ])
        }
        Ok((true, accepted, started, terminal)) => {
            if ops
                .iter()
                .all(|op| op_committed(op, &accepted, &started, &terminal))
            {
                return None;
            }
            Some(
                ops.iter()
                    .map(|op| {
                        if op_committed(op, &accepted, &started, &terminal) {
                            CommitOutcome::Committed
                        } else {
                            CommitOutcome::RolledBack(
                                "job is not open in PostgreSQL journal".into(),
                            )
                        }
                    })
                    .collect(),
            )
        }
        Err(SingleStmtError::Sql(err)) if !allow_split || ops.len() == 1 => {
            Some(vec![CommitOutcome::RolledBack(err); ops.len()])
        }
        Err(SingleStmtError::Sql(_)) => {
            let mut out = Vec::with_capacity(ops.len());
            for op in ops {
                let outcome = Box::pin(commit_batch_inner(
                    write_pool,
                    read_pool,
                    holder_id,
                    term,
                    &[op],
                    active_lost_tx,
                    false,
                ))
                .await
                .and_then(|mut v| v.pop())
                .unwrap_or(CommitOutcome::Committed);
                out.push(outcome);
            }
            Some(out)
        }
        Err(SingleStmtError::Transport(err)) => {
            let mut out = Vec::with_capacity(ops.len());
            let mut any_unknown = false;
            for op in ops {
                match verify_op(read_pool, op).await {
                    Ok(true) => out.push(CommitOutcome::Committed),
                    Ok(false) => out.push(CommitOutcome::RolledBack(err.clone())),
                    Err(verify_err) => {
                        any_unknown = true;
                        out.push(CommitOutcome::Unknown(format!(
                            "PostgreSQL write failed ({err}) and could not be verified: {verify_err:#}"
                        )));
                    }
                }
            }
            if any_unknown {
                // Database connectivity lost; exit immediately so standby takes over cleanly.
                signal_term_lost(active_lost_tx, term);
            }
            Some(out)
        }
    }
}

#[cfg(test)]
mod tests;
