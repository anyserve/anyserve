//! SQLite journal for durable jobs.
//!
//! It records three commit boundaries — acceptance, attempt start and terminal
//! outcome — as a recovery snapshot per job, not as an event history. A single
//! writer task owns the write connection and groups queued operations into one
//! WAL transaction; callers never hold runtime locks while it waits on fsync.

mod preview;

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use fs2::FileExt;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqlitePool, SqlitePoolOptions,
    SqliteRow, SqliteSynchronous,
};
use sqlx::{Connection, QueryBuilder, Row, Sqlite, Transaction};
use tokio::sync::{mpsc, oneshot};

use crate::error::RuntimeError;
use crate::runtime::api::{
    Demand, ErrorInfo, ExecutionPolicy, JobSpec, JobState, JournalConfig, JournalStats, Metadata,
    Object, ObjectPreview,
};
use crate::runtime::events::{ByteBudget, Permit, budgeted_bytes};

#[derive(Debug)]
pub(crate) enum JournalOp {
    Accept(Box<AcceptRecord>),
    AttemptStart(AttemptStartRecord),
    Terminal(Box<TerminalRecord>),
    /// Verifies that writes succeed again after a rollback.
    Probe,
}

#[derive(Debug)]
pub(crate) struct AcceptRecord {
    pub job_id: String,
    pub spec_hash: String,
    pub spec: Arc<JobSpec>,
    pub max_attempts: u32,
    pub created_at_ms: u64,
}

#[derive(Debug)]
pub(crate) struct AttemptStartRecord {
    pub job_id: String,
    /// The job's attempt count once this start is committed.
    pub attempt_number: u32,
    pub at_ms: u64,
}

#[derive(Debug)]
pub(crate) struct TerminalRecord {
    pub job_id: String,
    pub state: JobState,
    pub outputs: Vec<Object>,
    pub error: Option<ErrorInfo>,
    pub attempt_id: Option<String>,
    pub worker_id: Option<String>,
    pub winning_lease_id: Option<String>,
    /// Server-computed digest of the winning outcome (`result_digest` column).
    pub outcome_digest: Option<String>,
    pub at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CommitOutcome {
    Committed,
    /// Definitely not written.
    RolledBack(String),
    /// The commit failed and its effect could not be verified.
    Unknown(String),
}

/// A job as stored in the journal.
#[derive(Clone, Debug)]
pub(crate) struct StoredJob {
    pub job_id: String,
    pub state: JobState,
    pub spec: JobSpec,
    pub spec_hash: String,
    pub attempt_count: u32,
    pub max_attempts: u32,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub error: Option<ErrorInfo>,
    pub outputs: Vec<Object>,
    pub last_attempt_id: Option<String>,
    pub last_worker_id: Option<String>,
}

pub(crate) struct Recovery {
    pub generation: u64,
    /// Unfinished jobs with their full input. Terminal jobs stay on disk and
    /// are looked up on demand.
    pub active: Vec<StoredJob>,
}

#[derive(Serialize, Deserialize)]
struct StoredSpec {
    demand: Demand,
    policy: ExecutionPolicy,
    metadata: Metadata,
}

enum Request {
    Op {
        op: JournalOp,
        reply: oneshot::Sender<CommitOutcome>,
    },
    Purge {
        before_ms: u64,
        reply: oneshot::Sender<Result<usize>>,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
}

pub(crate) struct Journal {
    tx: mpsc::Sender<Request>,
    read_pool: SqlitePool,
    pending: Arc<AtomicUsize>,
    stats: Arc<Mutex<JournalStats>>,
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS journal_meta (
        key TEXT PRIMARY KEY,
        value INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS journal_jobs (
        job_id TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        spec_hash TEXT NOT NULL,
        spec_json TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        error_json TEXT,
        winning_lease_id TEXT,
        result_digest TEXT,
        last_attempt_id TEXT,
        last_worker_id TEXT
    )",
    "CREATE INDEX IF NOT EXISTS journal_jobs_state_updated
        ON journal_jobs(state, updated_at_ms)",
    "CREATE INDEX IF NOT EXISTS journal_jobs_recovery
        ON journal_jobs(created_at_ms, job_id) WHERE state = 'queued'",
    "CREATE INDEX IF NOT EXISTS journal_jobs_winner ON journal_jobs(winning_lease_id)",
    "CREATE INDEX IF NOT EXISTS journal_jobs_history ON journal_jobs(created_at_ms, job_id)
        WHERE state IN ('succeeded', 'failed', 'cancelled')",
    "CREATE TABLE IF NOT EXISTS journal_objects (
        job_id TEXT NOT NULL,
        role TEXT NOT NULL,
        idx INTEGER NOT NULL,
        uri TEXT,
        content BLOB,
        metadata_json TEXT NOT NULL,
        PRIMARY KEY (job_id, role, idx)
    )",
    // Metadata follows the BLOB in the table record. Cover large outputs so
    // previews avoid its overflow pages, without extra index writes for small
    // messages or URIs. Small outputs can be read whole within the preview cap.
    "CREATE INDEX IF NOT EXISTS journal_large_outputs_preview
        ON journal_objects(job_id, idx, uri, metadata_json, length(content))
        WHERE role = 'output' AND uri IS NULL AND length(content) > 4096",
];

const ROLE_INPUT: &str = "input";
const ROLE_PARAMS: &str = "params";
const ROLE_OUTPUT: &str = "output";

impl Journal {
    pub(crate) async fn scan_jobs(
        &self,
        from_ms: u64,
        to_ms: u64,
        before: Option<(u64, String)>,
        limit: usize,
    ) -> Result<Vec<StoredJob>> {
        let mut query = QueryBuilder::<Sqlite>::new(JOB_COLUMNS);
        query
            .push(" INDEXED BY journal_jobs_history WHERE state IN ('succeeded', 'failed', 'cancelled') AND created_at_ms >= ")
            .push_bind(from_ms as i64)
            .push(" AND created_at_ms < ")
            .push_bind(to_ms as i64);
        if let Some((at, id)) = before {
            query
                .push(" AND (created_at_ms, job_id) < (")
                .push_bind(at as i64)
                .push(", ")
                .push_bind(id)
                .push(")");
        }
        let rows = query
            .push(" ORDER BY created_at_ms DESC, job_id DESC LIMIT ")
            .push_bind(limit as i64)
            .build()
            .fetch_all(&self.read_pool)
            .await?;
        rows.iter().map(job_from_row).collect()
    }

    /// Opens (creating if needed) the journal, starts a new generation and
    /// loads what recovery needs. Only one process may hold a journal.
    pub(crate) async fn open(config: &JournalConfig) -> Result<(Journal, Recovery)> {
        if let Some(parent) = config.path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create journal directory {}", parent.display()))?;
        }
        let lock = acquire_lock(&config.path)?;
        let options = SqliteConnectOptions::new()
            .filename(&config.path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(if config.full_sync {
                SqliteSynchronous::Full
            } else {
                SqliteSynchronous::Normal
            })
            .busy_timeout(Duration::from_secs(5));
        let mut writer_options = options.clone();
        if let Some(max_bytes) = config.max_bytes {
            // SQLite's default page size; the cap applies to this connection.
            writer_options =
                writer_options.pragma("max_page_count", (max_bytes / 4096).max(1).to_string());
        }
        let mut conn = SqliteConnection::connect_with(&writer_options)
            .await
            .with_context(|| format!("open journal {}", config.path.display()))?;
        for statement in SCHEMA {
            sqlx::query(statement)
                .execute(&mut conn)
                .await
                .with_context(|| format!("migrate journal: {statement}"))?;
        }
        let generation = next_generation(&mut conn).await?;
        let recovery = load_recovery(&mut conn, generation).await?;
        let read_pool = SqlitePoolOptions::new()
            .max_connections(4)
            .after_connect(|connection, _| {
                Box::pin(async move { preview::register(&mut connection.lock_handle().await?) })
            })
            .connect_with(options.clone().create_if_missing(false))
            .await
            .context("open journal read pool")?;

        let (tx, rx) = mpsc::channel(config.queue_capacity.max(1));
        let pending = Arc::new(AtomicUsize::new(0));
        let stats = Arc::new(Mutex::new(JournalStats::default()));
        tokio::spawn(writer(
            conn,
            rx,
            config.max_batch.max(1),
            Arc::clone(&pending),
            Arc::clone(&stats),
            lock,
        ));
        Ok((
            Journal {
                tx,
                read_pool,
                pending,
                stats,
            },
            recovery,
        ))
    }

    /// Queues an operation without waiting. Fails fast when the queue is full
    /// or the writer is gone.
    pub(crate) fn enqueue(
        &self,
        op: JournalOp,
    ) -> Result<oneshot::Receiver<CommitOutcome>, RuntimeError> {
        let (reply, receiver) = oneshot::channel();
        self.pending.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(Request::Op { op, reply }) {
            Ok(()) => Ok(receiver),
            Err(error) => {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                Err(match error {
                    mpsc::error::TrySendError::Full(_) => {
                        RuntimeError::exhausted("journal commit queue is full")
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        RuntimeError::unavailable("journal is closed")
                    }
                })
            }
        }
    }

    pub(crate) async fn load_job(
        &self,
        job_id: &str,
        include_outputs: bool,
        result_budget: Arc<ByteBudget>,
    ) -> Result<Option<StoredJob>> {
        let mut conn = self.read_pool.acquire().await.context("journal read")?;
        if !include_outputs {
            return load_job_record(&mut conn, job_id).await;
        }
        // Keep sizing and loading in the same snapshot, including across purge.
        let mut tx = conn.begin().await.context("begin journal read")?;
        let Some(mut job) = load_job_record(&mut tx, job_id).await? else {
            return Ok(None);
        };
        {
            let bytes: i64 = sqlx::query_scalar(
                "SELECT COALESCE(SUM(length(content)), 0) FROM journal_objects
                 WHERE job_id = ? AND role = 'output' AND uri IS NULL",
            )
            .bind(job_id)
            .fetch_one(&mut *tx)
            .await?;
            let mut permit = Permit::new(result_budget);
            if !permit.grow(usize::try_from(bytes).context("journal result size")?) {
                return Err(RuntimeError::exhausted(
                    "result budget is exhausted; retry the read later",
                )
                .into());
            }
            let mut rows = sqlx::query(
                "SELECT uri, content, metadata_json FROM journal_objects
                 WHERE job_id = ? AND role = 'output' ORDER BY idx",
            )
            .bind(job_id)
            .fetch(&mut *tx);
            while let Some(row) = rows.try_next().await? {
                let mut object = object_from_row(&row)?;
                if let Object::Inline { data, .. } = &mut object {
                    let reserved = permit.split(data.len());
                    *data = budgeted_bytes(std::mem::take(data), reserved);
                }
                job.outputs.push(object);
            }
        }
        tx.commit().await.context("finish journal read")?;
        Ok(Some(job))
    }

    /// Read lengths and prefixes in SQLite, so inspecting a large result never
    /// materializes its full blob or reserves the worker/client result budget.
    pub(crate) async fn load_job_preview(
        &self,
        job_id: &str,
    ) -> Result<Option<(StoredJob, Vec<ObjectPreview>)>> {
        let mut tx = self
            .read_pool
            .begin()
            .await
            .context("begin journal preview")?;
        let Some(job) = load_job_record(&mut tx, job_id).await? else {
            return Ok(None);
        };
        let mut outputs = Vec::new();
        {
            let mut rows = sqlx::query(
                "SELECT idx, uri, length(content) AS size_bytes,
                        anyserve_object_prefix(rowid) AS content, metadata_json
                 FROM journal_objects INDEXED BY journal_large_outputs_preview
                 WHERE job_id = ? AND role = 'output' AND uri IS NULL AND length(content) > 4096
                 UNION ALL
                 SELECT idx, uri, COALESCE(length(content), 0) AS size_bytes, content, metadata_json
                 FROM journal_objects
                 WHERE job_id = ? AND role = 'output'
                   AND (uri IS NOT NULL OR COALESCE(length(content), 0) <= 4096)
                 ORDER BY idx",
            )
            .bind(job_id)
            .bind(job_id)
            .fetch(&mut *tx);
            while let Some(row) = rows.try_next().await? {
                let prefix = object_from_row(&row)?;
                let size_bytes = if matches!(prefix, Object::Inline { .. }) {
                    Some(
                        usize::try_from(row.try_get::<i64, _>("size_bytes")?)
                            .context("journal preview size")?,
                    )
                } else {
                    None
                };
                outputs.push(ObjectPreview { prefix, size_bytes });
            }
        }
        tx.commit().await.context("finish journal preview")?;
        Ok(Some((job, outputs)))
    }

    /// Fingerprint of a stored job, if any.
    pub(crate) async fn spec_hash(&self, job_id: &str) -> Result<Option<String>> {
        sqlx::query_scalar("SELECT spec_hash FROM journal_jobs WHERE job_id = ?")
            .bind(job_id)
            .fetch_optional(&self.read_pool)
            .await
            .context("look up journal job")
    }

    /// The completion receipt recorded for `lease_id`: job id and digest.
    pub(crate) async fn receipt(&self, lease_id: &str) -> Result<Option<(String, String)>> {
        let row = sqlx::query(
            "SELECT job_id, result_digest FROM journal_jobs
             WHERE winning_lease_id = ? AND result_digest IS NOT NULL",
        )
        .bind(lease_id)
        .fetch_optional(&self.read_pool)
        .await
        .context("look up completion receipt")?;
        row.map(|row| Ok((row.try_get("job_id")?, row.try_get("result_digest")?)))
            .transpose()
    }

    /// Deletes terminal jobs last updated before `before_ms`; returns how many.
    /// Each batch is its own short transaction and queue entry, so commits
    /// queued meanwhile are not held up behind a large purge.
    pub(crate) async fn purge(&self, before_ms: u64) -> Result<usize> {
        let mut removed = 0;
        loop {
            let (reply, receiver) = oneshot::channel();
            self.tx
                .send(Request::Purge { before_ms, reply })
                .await
                .map_err(|_| anyhow!("journal is closed"))?;
            let batch = receiver.await.map_err(|_| anyhow!("journal is closed"))??;
            removed += batch;
            if batch < PURGE_BATCH {
                return Ok(removed);
            }
        }
    }

    /// Finishes queued operations, then closes the database and its lock.
    pub(crate) async fn close(&self) {
        let (reply, receiver) = oneshot::channel();
        if self.tx.send(Request::Close { reply }).await.is_ok() {
            let _ = receiver.await;
        }
        self.read_pool.close().await;
    }

    pub(crate) fn stats(&self) -> JournalStats {
        let mut stats = self.stats.lock().expect("journal stats poisoned").clone();
        stats.pending_ops = self.pending.load(Ordering::Relaxed);
        stats
    }
}

fn acquire_lock(path: &Path) -> Result<File> {
    let mut lock_path = PathBuf::from(path.as_os_str());
    lock_path.as_mut_os_string().push(".lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open journal lock {}", lock_path.display()))?;
    file.try_lock_exclusive().with_context(|| {
        format!(
            "journal {} is held by another control-plane process",
            path.display()
        )
    })?;
    Ok(file)
}

async fn next_generation(conn: &mut SqliteConnection) -> Result<u64> {
    let mut tx = conn.begin().await.context("begin generation tx")?;
    sqlx::query(
        "INSERT INTO journal_meta (key, value) VALUES ('generation', 1)
         ON CONFLICT(key) DO UPDATE SET value = value + 1",
    )
    .execute(&mut *tx)
    .await
    .context("advance generation")?;
    let generation: i64 =
        sqlx::query_scalar("SELECT value FROM journal_meta WHERE key = 'generation'")
            .fetch_one(&mut *tx)
            .await
            .context("read generation")?;
    tx.commit().await.context("commit generation")?;
    Ok(generation as u64)
}

const JOB_COLUMNS: &str = "SELECT job_id, state, spec_hash, spec_json, attempt_count,
        max_attempts, created_at_ms, updated_at_ms, error_json, winning_lease_id,
        result_digest, last_attempt_id, last_worker_id
     FROM journal_jobs";
const JOB_COLUMNS_BY_ID: &str = "SELECT job_id, state, spec_hash, spec_json, attempt_count,
        max_attempts, created_at_ms, updated_at_ms, error_json, winning_lease_id,
        result_digest, last_attempt_id, last_worker_id
     FROM journal_jobs WHERE job_id = ?";

const RECOVERY_BATCH: usize = 256;

async fn load_job_record(conn: &mut SqliteConnection, job_id: &str) -> Result<Option<StoredJob>> {
    let row = sqlx::query(JOB_COLUMNS_BY_ID)
        .bind(job_id)
        .fetch_optional(conn)
        .await
        .context("load journal job")?;
    row.as_ref().map(job_from_row).transpose()
}

async fn load_recovery(conn: &mut SqliteConnection, generation: u64) -> Result<Recovery> {
    let mut active = Vec::new();
    let mut cursor: Option<(u64, String)> = None;
    loop {
        let mut query = QueryBuilder::<Sqlite>::new(JOB_COLUMNS);
        query.push(" WHERE state = 'queued'");
        if let Some((created, id)) = &cursor {
            query
                .push(" AND (created_at_ms, job_id) > (")
                .push_bind(*created as i64)
                .push(", ")
                .push_bind(id)
                .push(")");
        }
        let rows = query
            .push(" ORDER BY created_at_ms, job_id LIMIT ")
            .push_bind(RECOVERY_BATCH as i64)
            .build()
            .fetch_all(&mut *conn)
            .await
            .context("load unfinished jobs")?;
        if rows.is_empty() {
            break;
        }
        let mut batch: Vec<StoredJob> = rows.iter().map(job_from_row).collect::<Result<_>>()?;
        let positions: std::collections::HashMap<String, usize> = batch
            .iter()
            .enumerate()
            .map(|(i, job)| (job.job_id.clone(), i))
            .collect();
        let mut objects = QueryBuilder::<Sqlite>::new(
            "SELECT job_id, role, idx, uri, content, metadata_json FROM journal_objects WHERE job_id IN (",
        );
        let mut ids = objects.separated(", ");
        for job in &batch {
            ids.push_bind(job.job_id.clone());
        }
        ids.push_unseparated(") AND role IN ('input', 'params') ORDER BY job_id, role, idx");
        // Bind owned ids so decoding can fill the batch while the query streams.
        let query = objects.build();
        let mut objects = query.fetch(&mut *conn);
        while let Some(row) = objects.try_next().await? {
            let id: String = row.try_get("job_id")?;
            let job = &mut batch[positions[&id]];
            let object = object_from_row(&row)?;
            match row.try_get::<&str, _>("role")? {
                ROLE_INPUT => job.spec.inputs.push(object),
                ROLE_PARAMS if row.try_get::<i64, _>("idx")? == 0 => {
                    if let Object::Inline { data, .. } = object {
                        job.spec.params = data;
                    }
                }
                _ => {}
            }
        }
        let last = batch.last().expect("nonempty recovery batch");
        cursor = Some((last.created_at_ms, last.job_id.clone()));
        let done = batch.len() < RECOVERY_BATCH;
        active.extend(batch);
        if done {
            break;
        }
    }
    Ok(Recovery { generation, active })
}

fn state_text(state: JobState) -> &'static str {
    match state {
        JobState::Queued | JobState::Running => "queued",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
    }
}

fn state_from_text(text: &str) -> Result<JobState> {
    Ok(match text {
        "queued" => JobState::Queued,
        "succeeded" => JobState::Succeeded,
        "failed" => JobState::Failed,
        "cancelled" => JobState::Cancelled,
        other => return Err(anyhow!("unknown journal job state '{other}'")),
    })
}

fn job_from_row(row: &SqliteRow) -> Result<StoredJob> {
    let spec_json: String = row.try_get("spec_json")?;
    let stored: StoredSpec = serde_json::from_str(&spec_json).context("decode journal spec")?;
    let error_json: Option<String> = row.try_get("error_json")?;
    Ok(StoredJob {
        job_id: row.try_get("job_id")?,
        state: state_from_text(&row.try_get::<String, _>("state")?)?,
        spec: JobSpec {
            demand: stored.demand,
            inputs: Vec::new(),
            params: Bytes::new(),
            policy: stored.policy,
            metadata: stored.metadata,
        },
        spec_hash: row.try_get("spec_hash")?,
        attempt_count: row.try_get::<i64, _>("attempt_count")? as u32,
        max_attempts: row.try_get::<i64, _>("max_attempts")? as u32,
        created_at_ms: row.try_get::<i64, _>("created_at_ms")? as u64,
        updated_at_ms: row.try_get::<i64, _>("updated_at_ms")? as u64,
        error: error_json
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .context("decode journal error")?,
        outputs: Vec::new(),
        last_attempt_id: row.try_get("last_attempt_id")?,
        last_worker_id: row.try_get("last_worker_id")?,
    })
}

fn object_from_row(row: &SqliteRow) -> Result<Object> {
    let metadata: Metadata = serde_json::from_str(&row.try_get::<String, _>("metadata_json")?)?;
    Ok(match row.try_get::<Option<String>, _>("uri")? {
        Some(uri) => Object::Uri { uri, metadata },
        None => Object::Inline {
            data: Bytes::from(
                row.try_get::<Option<Vec<u8>>, _>("content")?
                    .unwrap_or_default(),
            ),
            metadata,
        },
    })
}

async fn insert_objects(
    tx: &mut Transaction<'_, Sqlite>,
    job_id: &str,
    role: &str,
    objects: &[Object],
) -> Result<()> {
    for (index, object) in objects.iter().enumerate() {
        let (uri, content, metadata) = match object {
            Object::Inline { data, metadata } => (None, Some(&data[..]), metadata),
            Object::Uri { uri, metadata } => (Some(uri.as_str()), None, metadata),
        };
        sqlx::query(
            "INSERT INTO journal_objects (job_id, role, idx, uri, content, metadata_json)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(job_id)
        .bind(role)
        .bind(index as i64)
        .bind(uri)
        .bind(content)
        .bind(serde_json::to_string(metadata)?)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

async fn apply_op(tx: &mut Transaction<'_, Sqlite>, op: &JournalOp) -> Result<()> {
    match op {
        JournalOp::Accept(record) => {
            let spec = &record.spec;
            let stored = StoredSpec {
                demand: spec.demand.clone(),
                policy: spec.policy.clone(),
                metadata: spec.metadata.clone(),
            };
            sqlx::query(
                "INSERT INTO journal_jobs (job_id, state, spec_hash, spec_json, attempt_count,
                    max_attempts, created_at_ms, updated_at_ms)
                 VALUES (?, 'queued', ?, ?, 0, ?, ?, ?)",
            )
            .bind(&record.job_id)
            .bind(&record.spec_hash)
            .bind(serde_json::to_string(&stored)?)
            .bind(record.max_attempts as i64)
            .bind(record.created_at_ms as i64)
            .bind(record.created_at_ms as i64)
            .execute(&mut **tx)
            .await?;
            insert_objects(tx, &record.job_id, ROLE_INPUT, &spec.inputs).await?;
            if !spec.params.is_empty() {
                insert_objects(
                    tx,
                    &record.job_id,
                    ROLE_PARAMS,
                    &[Object::inline(spec.params.clone())],
                )
                .await?;
            }
        }
        JournalOp::AttemptStart(record) => {
            let updated = sqlx::query(
                "UPDATE journal_jobs SET attempt_count = attempt_count + 1, updated_at_ms = ?
                 WHERE job_id = ? AND state = 'queued'",
            )
            .bind(record.at_ms as i64)
            .bind(&record.job_id)
            .execute(&mut **tx)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(anyhow!(
                    "job '{}' is not open in the journal",
                    record.job_id
                ));
            }
        }
        JournalOp::Terminal(record) => {
            let updated = sqlx::query(
                "UPDATE journal_jobs SET state = ?, updated_at_ms = ?, error_json = ?,
                    winning_lease_id = ?, result_digest = ?, last_attempt_id = ?, last_worker_id = ?
                 WHERE job_id = ? AND state = 'queued'",
            )
            .bind(state_text(record.state))
            .bind(record.at_ms as i64)
            .bind(
                record
                    .error
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
            )
            .bind(&record.winning_lease_id)
            .bind(&record.outcome_digest)
            .bind(&record.attempt_id)
            .bind(&record.worker_id)
            .bind(&record.job_id)
            .execute(&mut **tx)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(anyhow!(
                    "job '{}' is not open in the journal",
                    record.job_id
                ));
            }
            insert_objects(tx, &record.job_id, ROLE_OUTPUT, &record.outputs).await?;
        }
        JournalOp::Probe => {
            sqlx::query("UPDATE journal_meta SET value = value WHERE key = 'generation'")
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(())
}

/// Whether `op` is visible in the database; used after an ambiguous commit.
async fn verify(conn: &mut SqliteConnection, op: &JournalOp) -> Result<bool> {
    Ok(match op {
        JournalOp::Accept(record) => {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM journal_jobs WHERE job_id = ?")
                .bind(&record.job_id)
                .fetch_one(&mut *conn)
                .await?
                > 0
        }
        // Only one start per job is ever in flight, and the count moves only
        // when a start commits.
        JournalOp::AttemptStart(record) => {
            sqlx::query_scalar::<_, i64>("SELECT attempt_count FROM journal_jobs WHERE job_id = ?")
                .bind(&record.job_id)
                .fetch_optional(&mut *conn)
                .await?
                .is_some_and(|count| count >= i64::from(record.attempt_number))
        }
        JournalOp::Terminal(record) => {
            sqlx::query_scalar::<_, String>("SELECT state FROM journal_jobs WHERE job_id = ?")
                .bind(&record.job_id)
                .fetch_optional(&mut *conn)
                .await?
                .is_some_and(|state| state == state_text(record.state))
        }
        JournalOp::Probe => false,
    })
}

enum Failure {
    /// A statement failed; the transaction never committed.
    Statement(String),
    /// COMMIT itself failed; the outcome must be verified.
    Commit(String),
}

async fn apply_batch(conn: &mut SqliteConnection, ops: &[&JournalOp]) -> Result<(), Failure> {
    let mut tx = conn
        .begin()
        .await
        .map_err(|error| Failure::Statement(error.to_string()))?;
    for op in ops {
        apply_op(&mut tx, op)
            .await
            .map_err(|error| Failure::Statement(format!("{error:#}")))?;
    }
    tx.commit()
        .await
        .map_err(|error| Failure::Commit(error.to_string()))
}

async fn resolve_commit_failure(
    conn: &mut SqliteConnection,
    ops: &[&JournalOp],
    error: &str,
) -> Vec<CommitOutcome> {
    let mut outcomes = Vec::with_capacity(ops.len());
    for op in ops {
        outcomes.push(match verify(conn, op).await {
            Ok(true) => CommitOutcome::Committed,
            Ok(false) => CommitOutcome::RolledBack(error.to_string()),
            Err(verify_error) => CommitOutcome::Unknown(format!(
                "commit failed ({error}) and could not be verified: {verify_error:#}"
            )),
        });
    }
    outcomes
}

async fn commit_batch(conn: &mut SqliteConnection, ops: &[&JournalOp]) -> Vec<CommitOutcome> {
    match apply_batch(conn, ops).await {
        Ok(()) => vec![CommitOutcome::Committed; ops.len()],
        Err(Failure::Commit(error)) => resolve_commit_failure(conn, ops, &error).await,
        Err(Failure::Statement(error)) if ops.len() == 1 => {
            vec![CommitOutcome::RolledBack(error)]
        }
        // Isolate the failing operation so it cannot sink the others.
        Err(Failure::Statement(_)) => {
            let mut outcomes = Vec::with_capacity(ops.len());
            for op in ops {
                outcomes.push(match apply_batch(conn, &[op]).await {
                    Ok(()) => CommitOutcome::Committed,
                    Err(Failure::Statement(error)) => CommitOutcome::RolledBack(error),
                    Err(Failure::Commit(error)) => {
                        resolve_commit_failure(conn, &[op], &error).await.remove(0)
                    }
                });
            }
            outcomes
        }
    }
}

/// Terminal jobs deleted per purge transaction.
const PURGE_BATCH: usize = 500;

async fn purge(conn: &mut SqliteConnection, before_ms: u64) -> Result<usize> {
    let mut tx = conn.begin().await?;
    let removed: Vec<String> = sqlx::query_scalar(
        "SELECT job_id FROM journal_jobs WHERE state != 'queued' AND updated_at_ms < ?
         LIMIT ?",
    )
    .bind(before_ms as i64)
    .bind(PURGE_BATCH as i64)
    .fetch_all(&mut *tx)
    .await?;
    for job_id in &removed {
        for table in ["journal_objects", "journal_jobs"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE job_id = ?"))
                .bind(job_id)
                .execute(&mut *tx)
                .await?;
        }
    }
    tx.commit().await?;
    Ok(removed.len())
}

async fn writer(
    mut conn: SqliteConnection,
    mut rx: mpsc::Receiver<Request>,
    max_batch: usize,
    pending: Arc<AtomicUsize>,
    stats: Arc<Mutex<JournalStats>>,
    _lock: File,
) {
    let mut deferred = None;
    loop {
        let request = match deferred.take() {
            Some(request) => request,
            None => match rx.recv().await {
                Some(request) => request,
                None => break,
            },
        };
        match request {
            Request::Close { reply } => {
                // Finish everything queued before the close request.
                while let Ok(request) = rx.try_recv() {
                    if let Request::Op { op, reply } = request {
                        let outcome = commit_batch(&mut conn, &[&op]).await;
                        pending.fetch_sub(1, Ordering::Relaxed);
                        let _ = reply.send(outcome.into_iter().next().expect("one outcome"));
                    }
                }
                rx.close();
                let _ = conn.close().await;
                let _ = reply.send(());
                return;
            }
            Request::Purge { before_ms, reply } => {
                let _ = reply.send(purge(&mut conn, before_ms).await);
            }
            Request::Op { op, reply } => {
                let mut batch = vec![(op, reply)];
                while batch.len() < max_batch {
                    match rx.try_recv() {
                        Ok(Request::Op { op, reply }) => batch.push((op, reply)),
                        Ok(other) => {
                            deferred = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                let started = Instant::now();
                let ops: Vec<&JournalOp> = batch.iter().map(|(op, _)| op).collect();
                let outcomes = commit_batch(&mut conn, &ops).await;
                let elapsed = started.elapsed();
                {
                    let mut stats = stats.lock().expect("journal stats poisoned");
                    stats.committed_batches += 1;
                    stats.committed_ops += outcomes
                        .iter()
                        .filter(|outcome| **outcome == CommitOutcome::Committed)
                        .count() as u64;
                    stats.last_batch_size = batch.len();
                    stats.last_commit_micros = elapsed.as_micros() as u64;
                }
                pending.fetch_sub(batch.len(), Ordering::Relaxed);
                for ((_, reply), outcome) in batch.into_iter().zip(outcomes) {
                    let _ = reply.send(outcome);
                }
            }
        }
    }
    let _ = conn.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::api::INSPECTION_PREVIEW_BYTES;

    fn config(name: &str) -> JournalConfig {
        JournalConfig::new(
            std::env::temp_dir()
                .join(format!("anyserve-journal-{}", uuid::Uuid::new_v4()))
                .join(format!("{name}.sqlite")),
        )
    }

    async fn connect(config: &JournalConfig) -> SqliteConnection {
        let options = SqliteConnectOptions::new()
            .filename(&config.path)
            .create_if_missing(true);
        SqliteConnection::connect_with(&options).await.unwrap()
    }

    fn start(job_id: &str, attempt_number: u32) -> JournalOp {
        JournalOp::AttemptStart(AttemptStartRecord {
            job_id: job_id.to_string(),
            attempt_number,
            at_ms: 2,
        })
    }

    async fn commit(journal: &Journal, op: JournalOp) -> CommitOutcome {
        journal.enqueue(op).unwrap().await.unwrap()
    }

    #[tokio::test]
    async fn an_attempt_start_is_verified_from_the_job_row() {
        let config = config("verify");
        let (journal, _) = Journal::open(&config).await.unwrap();
        let accept = JournalOp::Accept(Box::new(AcceptRecord {
            job_id: "j".to_string(),
            spec_hash: String::new(),
            spec: Arc::new(JobSpec::default()),
            max_attempts: 3,
            created_at_ms: 1,
        }));
        assert_eq!(commit(&journal, accept).await, CommitOutcome::Committed);
        let mut conn = connect(&config).await;
        assert!(!verify(&mut conn, &start("j", 1)).await.unwrap());
        assert_eq!(
            commit(&journal, start("j", 1)).await,
            CommitOutcome::Committed
        );
        assert!(verify(&mut conn, &start("j", 1)).await.unwrap());
        assert!(!verify(&mut conn, &start("j", 2)).await.unwrap());
        assert!(!verify(&mut conn, &start("missing", 1)).await.unwrap());
        conn.close().await.unwrap();
        journal.close().await;
    }

    async fn finished_fixture(config: &JournalConfig) -> Journal {
        let (journal, _) = Journal::open(config).await.unwrap();
        assert_eq!(
            commit(
                &journal,
                JournalOp::Accept(Box::new(AcceptRecord {
                    job_id: "result".into(),
                    spec_hash: "hash".into(),
                    spec: Arc::new(JobSpec::default()),
                    max_attempts: 3,
                    created_at_ms: 1,
                }))
            )
            .await,
            CommitOutcome::Committed
        );
        assert_eq!(
            commit(
                &journal,
                JournalOp::Terminal(Box::new(TerminalRecord {
                    job_id: "result".into(),
                    state: JobState::Succeeded,
                    outputs: vec![
                        Object::inline(vec![1; 40]),
                        Object::inline(vec![2; 60]),
                        Object::Uri {
                            uri: "s3://result".into(),
                            metadata: Metadata::new()
                        }
                    ],
                    error: None,
                    attempt_id: None,
                    worker_id: None,
                    winning_lease_id: None,
                    outcome_digest: None,
                    at_ms: 2,
                }))
            )
            .await,
            CommitOutcome::Committed
        );
        journal
    }

    #[tokio::test]
    async fn cold_results_hold_budget_until_each_objects_last_reference() {
        let config = config("read-budget");
        let journal = finished_fixture(&config).await;
        let budget = Arc::new(ByteBudget::new(100));
        let job = journal
            .load_job("result", true, Arc::clone(&budget))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.used(), 100);
        let held = job.outputs[0].clone();
        let refused = journal
            .load_job("result", true, Arc::clone(&budget))
            .await
            .unwrap_err();
        assert_eq!(
            refused.downcast_ref::<RuntimeError>().unwrap().kind,
            crate::error::ErrorKind::ResourceExhausted
        );
        // Reading just the state does not need payload capacity.
        assert!(
            journal
                .load_job("result", false, Arc::clone(&budget))
                .await
                .unwrap()
                .unwrap()
                .outputs
                .is_empty()
        );
        drop(job);
        assert_eq!(budget.used(), 40);
        drop(held);
        assert_eq!(budget.used(), 0);
        let again = journal
            .load_job("result", true, Arc::clone(&budget))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.outputs.len(), 3);
        drop(again);
        assert_eq!(budget.used(), 0);
        journal.close().await;
    }

    #[tokio::test]
    async fn concurrent_cold_reads_share_one_budget() {
        let journal = finished_fixture(&config("concurrent-read-budget")).await;
        let budget = Arc::new(ByteBudget::new(200));
        let reads = futures::future::join_all(
            (0..16).map(|_| journal.load_job("result", true, Arc::clone(&budget))),
        )
        .await;
        assert_eq!(reads.iter().filter(|read| read.is_ok()).count(), 2);
        assert!(
            reads
                .iter()
                .filter_map(|read| read.as_ref().err())
                .all(|error| error.downcast_ref::<RuntimeError>().unwrap().kind
                    == crate::error::ErrorKind::ResourceExhausted)
        );
        assert_eq!(budget.used(), 200);
        drop(reads);
        assert_eq!(budget.used(), 0);
        journal.close().await;
    }

    #[tokio::test]
    async fn cold_previews_match_live_prefixes_without_changing_full_results() {
        let config = config("read-preview");
        let journal = finished_fixture(&config).await;
        let mut conn = connect(&config).await;
        assert!(journal.load_job_preview("missing").await.unwrap().is_none());
        // A failed native BLOB open is a SQL error, and the connection remains usable.
        assert!(
            sqlx::query("SELECT anyserve_object_prefix(-1)")
                .execute(&journal.read_pool)
                .await
                .is_err()
        );
        for size in [
            0,
            INSPECTION_PREVIEW_BYTES,
            INSPECTION_PREVIEW_BYTES + 1,
            5 * 1024 * 1024,
        ] {
            sqlx::query("UPDATE journal_objects SET content = ? WHERE job_id = 'result' AND role = 'output' AND idx = 0")
                .bind(vec![b'x'; size]).execute(&mut conn).await.unwrap();
            let budget = Arc::new(ByteBudget::new((size + 60) as u64));
            let full = journal
                .load_job("result", true, Arc::clone(&budget))
                .await
                .unwrap()
                .unwrap();
            let (record, previews) = journal.load_job_preview("result").await.unwrap().unwrap();
            assert!(record.outputs.is_empty());
            assert_eq!(
                previews,
                full.outputs
                    .iter()
                    .map(ObjectPreview::from)
                    .collect::<Vec<_>>()
            );
            assert_eq!(previews[0].size_bytes, Some(size));
            assert_eq!(
                previews[0].prefix.inline_len(),
                size.min(INSPECTION_PREVIEW_BYTES)
            );
            assert_eq!(full.outputs[0].inline_len(), size);
            assert_eq!(budget.used(), (size + 60) as u64);
            drop(full);
            assert_eq!(budget.used(), 0);
        }
        conn.close().await.unwrap();
        journal.close().await;
    }

    #[tokio::test]
    async fn opening_an_existing_journal_adds_the_preview_index() {
        let config = config("preview-migration");
        let journal = finished_fixture(&config).await;
        journal.close().await;
        let mut conn = connect(&config).await;
        sqlx::query("UPDATE journal_objects SET content = ? WHERE role = 'output' AND idx = 0")
            .bind(vec![1u8; 8192])
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("DROP INDEX journal_large_outputs_preview")
            .execute(&mut conn)
            .await
            .unwrap();
        conn.close().await.unwrap();
        let (journal, _) = Journal::open(&config).await.unwrap();
        let (_, outputs) = journal.load_job_preview("result").await.unwrap().unwrap();
        assert_eq!(
            outputs[0].prefix,
            Object::inline(vec![1; INSPECTION_PREVIEW_BYTES])
        );
        assert_eq!(outputs[0].size_bytes, Some(8192));
        assert_eq!(outputs[1].size_bytes, Some(60));
        assert!(matches!(&outputs[2].prefix, Object::Uri { uri, .. } if uri == "s3://result"));
        journal.close().await;
    }

    #[tokio::test]
    async fn archive_scan_uses_creation_cursor_and_never_loads_objects() {
        let config = config("archive-scan");
        let journal = finished_fixture(&config).await;
        let mut conn = connect(&config).await;
        sqlx::query("UPDATE journal_objects SET content = ? WHERE job_id = 'result' AND role = 'output' AND idx = 0")
            .bind(vec![7u8; 5 * 1024 * 1024]).execute(&mut conn).await.unwrap();
        for (id, at, state) in [
            ("z", 2, "failed"),
            ("y", 2, "cancelled"),
            ("x", 2, "succeeded"),
            ("queued", 2, "queued"),
            ("future", 3, "succeeded"),
        ] {
            sqlx::query("INSERT INTO journal_jobs(job_id, state, spec_hash, spec_json, max_attempts, created_at_ms, updated_at_ms)
                SELECT ?, ?, spec_hash, spec_json, max_attempts, ?, 99 FROM journal_jobs WHERE job_id = 'result'")
                .bind(id).bind(state).bind(at).execute(&mut conn).await.unwrap();
        }
        let page = journal.scan_jobs(1, 3, None, 2).await.unwrap();
        assert_eq!(
            page.iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            ["z", "y"]
        );
        sqlx::query("DELETE FROM journal_jobs WHERE job_id = 'y'")
            .execute(&mut conn)
            .await
            .unwrap();
        let page = journal
            .scan_jobs(1, 3, Some((2, "y".into())), 2)
            .await
            .unwrap();
        assert_eq!(
            page.iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            ["x", "result"]
        );
        assert!(page.iter().all(|job| job.outputs.is_empty()
            && job.spec.inputs.is_empty()
            && job.spec.params.is_empty()));
        let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as("EXPLAIN QUERY PLAN SELECT job_id FROM journal_jobs INDEXED BY journal_jobs_history WHERE state IN ('succeeded','failed','cancelled') AND created_at_ms >= 1 AND created_at_ms < 3 ORDER BY created_at_ms DESC, job_id DESC LIMIT 2")
            .fetch_all(&mut conn).await.unwrap();
        assert!(
            plan.iter()
                .any(|row| row.3.contains("journal_jobs_history")),
            "{plan:?}"
        );
        assert!(
            !plan.iter().any(|row| row.3.contains("TEMP B-TREE")),
            "{plan:?}"
        );
        conn.close().await.unwrap();
        journal.close().await;
    }

    #[tokio::test]
    async fn failed_cold_reads_release_partial_results_and_unused_budget() {
        let config = config("failed-read-budget");
        let journal = finished_fixture(&config).await;
        let mut conn = connect(&config).await;
        sqlx::query("UPDATE journal_objects SET metadata_json = 'invalid' WHERE role = 'output' AND idx = 1")
            .execute(&mut conn).await.unwrap();
        let budget = Arc::new(ByteBudget::new(100));
        assert!(
            journal
                .load_job("result", true, Arc::clone(&budget))
                .await
                .is_err()
        );
        assert_eq!(budget.used(), 0);
        conn.close().await.unwrap();
        journal.close().await;
    }

    #[tokio::test]
    async fn recovery_batches_preserve_order_payloads_and_attempts() {
        let config = config("batched-recovery");
        let (journal, _) = Journal::open(&config).await.unwrap();
        journal.close().await;
        let mut conn = connect(&config).await;
        let mut tx = conn.begin().await.unwrap();
        let mut expected = Vec::new();
        for i in (0..RECOVERY_BATCH * 2 + 37).rev() {
            let id = format!("job-{i:04}");
            let created = (i % 2) as u64;
            let mut spec = JobSpec::default();
            spec.demand.interface = format!("interface-{}", i % 3);
            spec.metadata.insert("ordinal".into(), i.to_string());
            if i % 3 != 0 {
                spec.inputs = vec![
                    Object::inline(vec![i as u8; 3]),
                    Object::inline(vec![0; 17]),
                    Object::Uri {
                        uri: format!("s3://input/{i}"),
                        metadata: [("type".into(), "blob".into())].into(),
                    },
                ];
                spec.params = Bytes::from(format!("params-{i}"));
            }
            apply_op(
                &mut tx,
                &JournalOp::Accept(Box::new(AcceptRecord {
                    job_id: id.clone(),
                    spec_hash: format!("hash-{i}"),
                    spec: Arc::new(spec.clone()),
                    max_attempts: 5,
                    created_at_ms: created,
                })),
            )
            .await
            .unwrap();
            apply_op(&mut tx, &start(&id, 1)).await.unwrap();
            if i % 19 == 0 {
                apply_op(
                    &mut tx,
                    &JournalOp::Terminal(Box::new(TerminalRecord {
                        job_id: id,
                        state: JobState::Cancelled,
                        outputs: Vec::new(),
                        error: None,
                        attempt_id: None,
                        worker_id: None,
                        winning_lease_id: None,
                        outcome_digest: None,
                        at_ms: 3,
                    })),
                )
                .await
                .unwrap();
            } else {
                expected.push((created, id, spec));
            }
        }
        tx.commit().await.unwrap();
        expected.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        let recovery = load_recovery(&mut conn, 7).await.unwrap();
        assert_eq!(recovery.generation, 7);
        assert_eq!(recovery.active.len(), expected.len());
        for (job, (created, id, spec)) in recovery.active.iter().zip(&expected) {
            assert_eq!(
                (&job.created_at_ms, &job.job_id, &job.spec),
                (created, id, spec)
            );
            assert_eq!(job.attempt_count, 1);
            assert_eq!(job.max_attempts, 5);
            assert!(job.outputs.is_empty());
        }
        sqlx::query("DELETE FROM journal_jobs")
            .execute(&mut conn)
            .await
            .unwrap();
        assert!(load_recovery(&mut conn, 8).await.unwrap().active.is_empty());
        conn.close().await.unwrap();
    }
}
