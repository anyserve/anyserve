use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use fs2::FileExt;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::any::{AnyPoolOptions, install_default_drivers};
use sqlx::{AnyPool, Row, Transaction};
use tokio::sync::{Mutex, watch};
use uuid::Uuid;

use crate::model::{
    AttemptRecord, AttemptState, Attributes, EventKind, JobEvent, JobRecord, JobSpec, JobState,
    LeaseAssignment, LeaseRecord, ObjectRef, StreamDirection, StreamRecord, StreamScope,
    StreamState, WorkerRecord,
};
use crate::store::{
    JobListSummary, JobStateCounts, JobSummaryPage, LeaseDispatchMode, StateStore, StateTransition,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
}

#[derive(Default)]
struct SqlStoreWatchers {
    job_events: BTreeMap<String, watch::Sender<u64>>,
    job_streams: BTreeMap<String, watch::Sender<u64>>,
}

pub struct SqlStateStore {
    pool: AnyPool,
    dialect: SqlDialect,
    _sqlite_lock: Option<File>,
    watchers: Mutex<SqlStoreWatchers>,
}

const POSTGRES_DISPATCH_LOCK_ID: i64 = 0x4153_5256_4449_5350;

impl SqlStateStore {
    pub async fn connect(dsn: &str) -> Result<Self> {
        install_default_drivers();
        let sqlite_path = if dsn.starts_with("sqlite:") {
            prepare_sqlite_path(dsn)?
        } else {
            None
        };
        let pool = AnyPoolOptions::new()
            .max_connections(if dsn.starts_with("sqlite:") { 1 } else { 8 })
            .connect(dsn)
            .await
            .with_context(|| format!("connect durable state store {dsn}"))?;
        let dialect = if dsn.starts_with("sqlite:") {
            SqlDialect::Sqlite
        } else if dsn.starts_with("postgres:") || dsn.starts_with("postgresql:") {
            SqlDialect::Postgres
        } else {
            bail!("unsupported SQL dialect dsn: {dsn}");
        };

        let store = Self {
            pool,
            dialect,
            _sqlite_lock: sqlite_path
                .as_deref()
                .map(acquire_sqlite_lock)
                .transpose()?,
            watchers: Mutex::new(SqlStoreWatchers::default()),
        };
        store.prepare_connection().await?;
        store.migrate().await?;
        Ok(store)
    }

    pub fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    fn sql<'a>(&self, statement: &'a str) -> Cow<'a, str> {
        render_sql(self.dialect, statement)
    }

    async fn prepare_connection(&self) -> Result<()> {
        if self.dialect == SqlDialect::Sqlite {
            sqlx::query("PRAGMA journal_mode = WAL;")
                .execute(&self.pool)
                .await
                .context("enable sqlite WAL")?;
            sqlx::query("PRAGMA busy_timeout = 5000;")
                .execute(&self.pool)
                .await
                .context("set sqlite busy timeout")?;
        }
        Ok(())
    }

    async fn migrate(&self) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin durable state migration")?;
        if self.dialect == SqlDialect::Postgres {
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(POSTGRES_DISPATCH_LOCK_ID + 1)
                .execute(&mut *tx)
                .await
                .context("acquire postgres migration advisory xact lock")?;
        }
        for statement in [
            "CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                inputs_json TEXT NOT NULL,
                outputs_json TEXT NOT NULL,
                lease_id TEXT NULL,
                version BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                last_error TEXT NULL,
                current_attempt_id TEXT NULL
            )",
            "CREATE INDEX IF NOT EXISTS jobs_state_created_idx ON jobs(state, created_at_ms, job_id)",
            "CREATE TABLE IF NOT EXISTS job_events (
                job_id TEXT NOT NULL,
                sequence BIGINT NOT NULL,
                kind TEXT NOT NULL,
                payload BLOB NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                PRIMARY KEY (job_id, sequence)
            )",
            "CREATE TABLE IF NOT EXISTS attempts (
                attempt_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                lease_id TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                started_at_ms BIGINT NULL,
                finished_at_ms BIGINT NULL,
                last_error TEXT NULL,
                metadata_json TEXT NOT NULL
            )",
            "CREATE INDEX IF NOT EXISTS attempts_lease_idx ON attempts(lease_id)",
            "CREATE INDEX IF NOT EXISTS attempts_job_created_idx ON attempts(job_id, created_at_ms, attempt_id)",
            "CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                spec_json TEXT NOT NULL,
                status_json TEXT NOT NULL,
                registered_at_ms BIGINT NOT NULL,
                expires_at_ms BIGINT NOT NULL
            )",
            "CREATE TABLE IF NOT EXISTS leases (
                lease_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL UNIQUE,
                worker_id TEXT NOT NULL,
                issued_at_ms BIGINT NOT NULL,
                expires_at_ms BIGINT NOT NULL
            )",
            "CREATE INDEX IF NOT EXISTS leases_worker_idx ON leases(worker_id)",
            "CREATE INDEX IF NOT EXISTS leases_expires_idx ON leases(expires_at_ms)",
            "CREATE TABLE IF NOT EXISTS streams (
                stream_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                attempt_id TEXT NULL,
                lease_id TEXT NULL,
                stream_name TEXT NOT NULL,
                scope TEXT NOT NULL,
                direction TEXT NOT NULL,
                state TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                closed_at_ms BIGINT NULL,
                last_sequence BIGINT NOT NULL
            )",
            "CREATE INDEX IF NOT EXISTS streams_job_created_idx ON streams(job_id, created_at_ms, stream_id)",
        ] {
            let statement = self.sql(statement);
            sqlx::query(statement.as_ref())
                .execute(&mut *tx)
                .await
                .with_context(|| {
                    format!(
                        "migrate durable state store with statement: {}",
                        statement.as_ref()
                    )
                })?;
        }
        if self.dialect == SqlDialect::Postgres {
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS jobs_pending_dispatch_idx
                 ON jobs (
                    ((spec_json::jsonb ->> 'interface_name')),
                    ((COALESCE((spec_json::jsonb -> 'policy' ->> 'priority')::INTEGER, 0))),
                    created_at_ms,
                    job_id
                 )
                 WHERE state = 'pending'",
            )
            .execute(&mut *tx)
            .await
            .context("migrate durable state store with postgres dispatch index")?;
        }
        self.ensure_jobs_inputs_column(&mut tx).await?;

        tx.commit()
            .await
            .context("commit durable state migration")?;
        Ok(())
    }

    async fn ensure_jobs_inputs_column(&self, tx: &mut Transaction<'_, sqlx::Any>) -> Result<()> {
        let exists = match self.dialect {
            SqlDialect::Sqlite => {
                let rows = sqlx::query("PRAGMA table_info(jobs)")
                    .fetch_all(&mut **tx)
                    .await
                    .context("inspect sqlite jobs columns")?;
                rows.into_iter().any(|row| {
                    row.try_get::<String, _>("name")
                        .map(|name| name == "inputs_json")
                        .unwrap_or(false)
                })
            }
            SqlDialect::Postgres => {
                let exists: Option<i64> = sqlx::query_scalar(
                    "SELECT 1
                     FROM information_schema.columns
                     WHERE table_schema = 'public'
                       AND table_name = 'jobs'
                       AND column_name = 'inputs_json'
                     LIMIT 1",
                )
                .fetch_optional(&mut **tx)
                .await
                .context("inspect postgres jobs columns")?;
                exists.is_some()
            }
        };

        if exists {
            return Ok(());
        }

        let statement = self.sql(
            "ALTER TABLE jobs
             ADD COLUMN inputs_json TEXT NOT NULL DEFAULT '[]'",
        );
        sqlx::query(statement.as_ref())
            .execute(&mut **tx)
            .await
            .with_context(|| format!("add jobs.inputs_json column with {}", statement.as_ref()))?;
        Ok(())
    }

    async fn notify_job_events(&self, job_id: &str) {
        let mut watchers = self.watchers.lock().await;
        bump_version(
            watchers
                .job_events
                .entry(job_id.to_string())
                .or_insert_with(version_sender),
        );
    }

    async fn notify_job_streams(&self, job_id: &str) {
        let mut watchers = self.watchers.lock().await;
        bump_version(
            watchers
                .job_streams
                .entry(job_id.to_string())
                .or_insert_with(version_sender),
        );
    }

    async fn append_job_event_tx(
        dialect: SqlDialect,
        tx: &mut Transaction<'_, sqlx::Any>,
        job_id: &str,
        kind: EventKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<JobEvent> {
        let next_sequence_statement = render_sql(
            dialect,
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM job_events WHERE job_id = ?",
        );
        let next_sequence: i64 = sqlx::query_scalar(next_sequence_statement.as_ref())
            .bind(job_id)
            .fetch_one(&mut **tx)
            .await
            .context("query next job event sequence")?;
        let kind_text = enum_text(&kind)?;
        let metadata_json = json_text(&metadata)?;
        let insert_statement = render_sql(
            dialect,
            "INSERT INTO job_events (job_id, sequence, kind, payload, metadata_json, created_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)",
        );

        sqlx::query(insert_statement.as_ref())
            .bind(job_id)
            .bind(next_sequence)
            .bind(&kind_text)
            .bind(payload.clone())
            .bind(metadata_json)
            .bind(to_i64(now_ms)?)
            .execute(&mut **tx)
            .await
            .context("insert job event")?;

        Ok(JobEvent {
            job_id: job_id.to_string(),
            sequence: from_i64(next_sequence)?,
            kind,
            payload,
            metadata,
            created_at_ms: now_ms,
        })
    }

    async fn begin_transition_tx(&self) -> Result<Transaction<'_, sqlx::Any>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin state transition tx")?;
        if self.dialect == SqlDialect::Postgres {
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(POSTGRES_DISPATCH_LOCK_ID)
                .execute(&mut *tx)
                .await
                .context("acquire postgres advisory xact lock")?;
        }
        Ok(tx)
    }

    async fn get_job_tx(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        job_id: &str,
    ) -> Result<Option<JobRecord>> {
        let statement = self.sql(
            "SELECT job_id, state, spec_json, outputs_json, lease_id, version, created_at_ms,
                    updated_at_ms, last_error, current_attempt_id
             FROM jobs WHERE job_id = ?",
        );
        let row = sqlx::query(statement.as_ref())
            .bind(job_id)
            .fetch_optional(&mut **tx)
            .await
            .with_context(|| format!("get job '{job_id}' in transition"))?;
        row.map(job_from_row_without_inputs).transpose()
    }

    async fn get_worker_tx(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        worker_id: &str,
    ) -> Result<Option<WorkerRecord>> {
        let statement = self.sql(
            "SELECT worker_id, spec_json, status_json, registered_at_ms, expires_at_ms
             FROM workers WHERE worker_id = ?",
        );
        let row = sqlx::query(statement.as_ref())
            .bind(worker_id)
            .fetch_optional(&mut **tx)
            .await
            .with_context(|| format!("get worker '{worker_id}' in transition"))?;
        row.map(worker_from_row).transpose()
    }

    async fn get_worker_tx_for_update_postgres(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        worker_id: &str,
    ) -> Result<Option<WorkerRecord>> {
        debug_assert_eq!(self.dialect, SqlDialect::Postgres);
        let row = sqlx::query(
            "SELECT worker_id, spec_json, status_json, registered_at_ms, expires_at_ms
             FROM workers
             WHERE worker_id = $1
             FOR UPDATE",
        )
        .bind(worker_id)
        .fetch_optional(&mut **tx)
        .await
        .with_context(|| format!("get worker '{worker_id}' for update in transition"))?;
        row.map(worker_from_row).transpose()
    }

    async fn get_lease_tx(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        lease_id: &str,
    ) -> Result<Option<LeaseRecord>> {
        let statement = self.sql(
            "SELECT lease_id, job_id, worker_id, issued_at_ms, expires_at_ms
             FROM leases WHERE lease_id = ?",
        );
        let row = sqlx::query(statement.as_ref())
            .bind(lease_id)
            .fetch_optional(&mut **tx)
            .await
            .with_context(|| format!("get lease '{lease_id}' in transition"))?;
        row.map(lease_from_row).transpose()
    }

    async fn get_attempt_for_lease_tx(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        lease_id: &str,
    ) -> Result<Option<AttemptRecord>> {
        let statement = self.sql(
            "SELECT attempt_id, job_id, worker_id, lease_id, state, created_at_ms,
                    started_at_ms, finished_at_ms, last_error, metadata_json
             FROM attempts
             WHERE lease_id = ?
             ORDER BY created_at_ms DESC, attempt_id DESC
             LIMIT 1",
        );
        let row = sqlx::query(statement.as_ref())
            .bind(lease_id)
            .fetch_optional(&mut **tx)
            .await
            .with_context(|| format!("get attempt for lease '{lease_id}' in transition"))?;
        row.map(attempt_from_row).transpose()
    }

    async fn update_job_tx(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        job: &JobRecord,
        expected_version: u64,
    ) -> Result<()> {
        let statement = self.sql(
            "UPDATE jobs
             SET state = ?, outputs_json = ?, lease_id = ?, version = ?,
                 updated_at_ms = ?, last_error = ?, current_attempt_id = ?
             WHERE job_id = ? AND version = ?",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(enum_text(&job.state)?)
            .bind(json_text(&job.outputs)?)
            .bind(job.lease_id.clone())
            .bind(to_i64(job.version)?)
            .bind(to_i64(job.updated_at_ms)?)
            .bind(job.last_error.clone())
            .bind(job.current_attempt_id.clone())
            .bind(&job.job_id)
            .bind(to_i64(expected_version)?)
            .execute(&mut **tx)
            .await
            .with_context(|| format!("update job '{}' in transition", job.job_id))?;
        if rows.rows_affected() == 0 {
            bail!("job '{}' version conflict", job.job_id);
        }
        Ok(())
    }

    async fn update_attempt_tx(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        attempt: &AttemptRecord,
    ) -> Result<()> {
        let statement = self.sql(
            "UPDATE attempts
             SET job_id = ?, worker_id = ?, lease_id = ?, state = ?, created_at_ms = ?,
                 started_at_ms = ?, finished_at_ms = ?, last_error = ?, metadata_json = ?
             WHERE attempt_id = ?",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(&attempt.job_id)
            .bind(&attempt.worker_id)
            .bind(&attempt.lease_id)
            .bind(enum_text(&attempt.state)?)
            .bind(to_i64(attempt.created_at_ms)?)
            .bind(option_u64_to_i64(attempt.started_at_ms)?)
            .bind(option_u64_to_i64(attempt.finished_at_ms)?)
            .bind(attempt.last_error.clone())
            .bind(json_text(&attempt.metadata)?)
            .bind(&attempt.attempt_id)
            .execute(&mut **tx)
            .await
            .with_context(|| format!("update attempt '{}' in transition", attempt.attempt_id))?;
        if rows.rows_affected() == 0 {
            bail!("attempt '{}' does not exist", attempt.attempt_id);
        }
        Ok(())
    }

    async fn update_stream_tx(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        stream: &StreamRecord,
    ) -> Result<()> {
        let statement = self.sql(
            "UPDATE streams
             SET job_id = ?, attempt_id = ?, lease_id = ?, stream_name = ?, scope = ?,
                 direction = ?, state = ?, metadata_json = ?, created_at_ms = ?,
                 closed_at_ms = ?, last_sequence = ?
             WHERE stream_id = ?",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(&stream.job_id)
            .bind(stream.attempt_id.clone())
            .bind(stream.lease_id.clone())
            .bind(&stream.stream_name)
            .bind(enum_text(&stream.scope)?)
            .bind(enum_text(&stream.direction)?)
            .bind(enum_text(&stream.state)?)
            .bind(json_text(&stream.metadata)?)
            .bind(to_i64(stream.created_at_ms)?)
            .bind(option_u64_to_i64(stream.closed_at_ms)?)
            .bind(to_i64(stream.last_sequence)?)
            .bind(&stream.stream_id)
            .execute(&mut **tx)
            .await
            .with_context(|| format!("update stream '{}' in transition", stream.stream_id))?;
        if rows.rows_affected() == 0 {
            bail!("stream '{}' does not exist", stream.stream_id);
        }
        Ok(())
    }

    async fn delete_lease_tx(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        lease_id: &str,
    ) -> Result<bool> {
        let statement = self.sql("DELETE FROM leases WHERE lease_id = ?");
        let rows = sqlx::query(statement.as_ref())
            .bind(lease_id)
            .execute(&mut **tx)
            .await
            .with_context(|| format!("delete lease '{lease_id}' in transition"))?;
        Ok(rows.rows_affected() > 0)
    }

    async fn close_job_streams_tx(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        job_id: &str,
        now_ms: u64,
    ) -> Result<Vec<StreamRecord>> {
        let statement = self.sql(
            "SELECT stream_id, job_id, attempt_id, lease_id, stream_name, scope, direction,
                    state, metadata_json, created_at_ms, closed_at_ms, last_sequence
             FROM streams
             WHERE job_id = ?
             ORDER BY created_at_ms ASC, stream_id ASC",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(job_id)
            .fetch_all(&mut **tx)
            .await
            .with_context(|| format!("list streams for job '{job_id}' in transition"))?;
        let mut closed = Vec::new();
        for row in rows {
            let mut stream = stream_from_row(row)?;
            if stream.state.is_terminal() {
                continue;
            }
            stream.state = StreamState::Closed;
            stream.closed_at_ms = Some(now_ms);
            self.update_stream_tx(tx, &stream).await?;
            closed.push(stream);
        }
        Ok(closed)
    }

    async fn close_lease_streams_tx(
        &self,
        tx: &mut Transaction<'_, sqlx::Any>,
        job_id: &str,
        lease_id: &str,
        now_ms: u64,
    ) -> Result<Vec<StreamRecord>> {
        let statement = self.sql(
            "SELECT stream_id, job_id, attempt_id, lease_id, stream_name, scope, direction,
                    state, metadata_json, created_at_ms, closed_at_ms, last_sequence
             FROM streams
             WHERE job_id = ? AND lease_id = ?
             ORDER BY created_at_ms ASC, stream_id ASC",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(job_id)
            .bind(lease_id)
            .fetch_all(&mut **tx)
            .await
            .with_context(|| {
                format!(
                    "list lease streams for job '{job_id}' and lease '{lease_id}' in transition"
                )
            })?;
        let mut closed = Vec::new();
        for row in rows {
            let mut stream = stream_from_row(row)?;
            if stream.state.is_terminal() {
                continue;
            }
            stream.state = StreamState::Closed;
            stream.closed_at_ms = Some(now_ms);
            self.update_stream_tx(tx, &stream).await?;
            closed.push(stream);
        }
        Ok(closed)
    }

    fn postgres_dispatch_candidate_sql(interface_count: usize) -> String {
        let interface_placeholders = (0..interface_count)
            .map(|offset| format!("${}", offset + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let attrs_idx = interface_count + 2;
        let capacity_idx = interface_count + 3;
        format!(
            "SELECT job_id, state, spec_json, outputs_json, lease_id, version, created_at_ms,
                    updated_at_ms, last_error, current_attempt_id
             FROM jobs
             WHERE state = $1
               AND (spec_json::jsonb ->> 'interface_name') IN ({interface_placeholders})
               AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_each_text(
                        COALESCE(spec_json::jsonb -> 'demand' -> 'required_attributes', '{{}}'::jsonb)
                    ) AS req(key, value)
                    WHERE (${attrs_idx}::jsonb ->> req.key) IS DISTINCT FROM req.value
               )
               AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_each(
                        COALESCE(spec_json::jsonb -> 'demand' -> 'required_capacity', '{{}}'::jsonb)
                    ) AS cap(key, value)
                    WHERE COALESCE((${capacity_idx}::jsonb ->> cap.key)::bigint, 0)
                        < (cap.value::text)::bigint
               )
             ORDER BY
                COALESCE((spec_json::jsonb -> 'policy' ->> 'priority')::integer, 0) DESC,
                (
                    SELECT COUNT(*)
                    FROM jsonb_each_text(
                        COALESCE(spec_json::jsonb -> 'demand' -> 'preferred_attributes', '{{}}'::jsonb)
                    ) AS pref(key, value)
                    WHERE (${attrs_idx}::jsonb ->> pref.key) = pref.value
                ) DESC,
                created_at_ms ASC,
                job_id ASC
             FOR UPDATE SKIP LOCKED
             LIMIT 1"
        )
    }

    async fn try_assign_job_postgres(
        &self,
        worker_id: &str,
        default_lease_ttl_secs: u64,
        now_ms: u64,
    ) -> Result<Option<LeaseAssignment>> {
        debug_assert_eq!(self.dialect, SqlDialect::Postgres);

        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin postgres job assignment tx")?;
        let worker = self
            .get_worker_tx_for_update_postgres(&mut tx, worker_id)
            .await?
            .ok_or_else(|| anyhow!("worker '{}' does not exist", worker_id))?;
        if worker.expires_at_ms <= now_ms {
            bail!("worker '{}' heartbeat expired", worker_id);
        }
        if worker.spec.interfaces.is_empty() {
            tx.commit()
                .await
                .context("commit empty postgres job assignment tx for worker without interfaces")?;
            return Ok(None);
        }

        let active_leases: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM leases WHERE worker_id = $1")
                .bind(worker_id)
                .fetch_one(&mut *tx)
                .await
                .with_context(|| format!("count active leases for worker '{worker_id}'"))?;
        if from_i64(active_leases)? >= u64::from(worker.spec.max_active_leases) {
            tx.commit()
                .await
                .context("commit saturated postgres worker assignment tx")?;
            return Ok(None);
        }

        let candidate_sql = Self::postgres_dispatch_candidate_sql(worker.spec.interfaces.len());
        let pending_state = enum_text(&JobState::Pending)?;
        let worker_attributes = json_text(&worker.spec.attributes)?;
        let worker_capacity = json_text(&worker.status.available_capacity)?;

        let mut candidate_query = sqlx::query(candidate_sql.as_str()).bind(pending_state);
        for interface in &worker.spec.interfaces {
            candidate_query = candidate_query.bind(interface);
        }
        let candidate_row = candidate_query
            .bind(worker_attributes)
            .bind(worker_capacity)
            .fetch_optional(&mut *tx)
            .await
            .with_context(|| format!("select assignable job for worker '{worker_id}'"))?;

        let Some(candidate_row) = candidate_row else {
            tx.commit()
                .await
                .context("commit empty postgres job assignment tx")?;
            return Ok(None);
        };
        let current = job_from_row_without_inputs(candidate_row)?;

        let lease_id = Uuid::new_v4().to_string();
        let attempt_id = Uuid::new_v4().to_string();
        let lease_ttl_secs = if current.spec.policy.lease_ttl_secs == 0 {
            default_lease_ttl_secs
        } else {
            current.spec.policy.lease_ttl_secs
        };
        let mut job = current.clone();
        job.state = JobState::Leased;
        job.lease_id = Some(lease_id.clone());
        job.current_attempt_id = Some(attempt_id.clone());
        job.updated_at_ms = now_ms;
        job.version += 1;
        self.update_job_tx(&mut tx, &job, current.version).await?;

        let lease = LeaseRecord {
            lease_id: lease_id.clone(),
            job_id: job.job_id.clone(),
            worker_id: worker_id.to_string(),
            issued_at_ms: now_ms,
            expires_at_ms: now_ms + lease_ttl_secs * 1000,
        };
        let attempt = AttemptRecord {
            attempt_id: attempt_id.clone(),
            job_id: job.job_id.clone(),
            worker_id: worker_id.to_string(),
            lease_id: lease_id.clone(),
            state: AttemptState::Leased,
            created_at_ms: now_ms,
            started_at_ms: None,
            finished_at_ms: None,
            last_error: None,
            metadata: Attributes::new(),
        };

        sqlx::query(
            "INSERT INTO leases (lease_id, job_id, worker_id, issued_at_ms, expires_at_ms)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&lease.lease_id)
        .bind(&lease.job_id)
        .bind(&lease.worker_id)
        .bind(to_i64(lease.issued_at_ms)?)
        .bind(to_i64(lease.expires_at_ms)?)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("insert lease '{}'", lease.lease_id))?;

        sqlx::query(
            "INSERT INTO attempts (
                attempt_id, job_id, worker_id, lease_id, state, created_at_ms,
                started_at_ms, finished_at_ms, last_error, metadata_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(&attempt.attempt_id)
        .bind(&attempt.job_id)
        .bind(&attempt.worker_id)
        .bind(&attempt.lease_id)
        .bind(enum_text(&attempt.state)?)
        .bind(to_i64(attempt.created_at_ms)?)
        .bind(option_u64_to_i64(attempt.started_at_ms)?)
        .bind(option_u64_to_i64(attempt.finished_at_ms)?)
        .bind(attempt.last_error.clone())
        .bind(json_text(&attempt.metadata)?)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("insert attempt '{}'", attempt.attempt_id))?;

        Self::append_job_event_tx(
            self.dialect,
            &mut tx,
            &job.job_id,
            EventKind::LeaseGranted,
            Vec::new(),
            BTreeMap::from([
                ("lease_id".to_string(), lease_id),
                ("attempt_id".to_string(), attempt_id),
            ]),
            now_ms,
        )
        .await?;

        tx.commit()
            .await
            .context("commit postgres job assignment tx")?;
        self.notify_job_events(&job.job_id).await;
        Ok(Some(LeaseAssignment {
            lease,
            attempt,
            job,
        }))
    }
}

fn render_sql<'a>(dialect: SqlDialect, statement: &'a str) -> Cow<'a, str> {
    if dialect == SqlDialect::Sqlite {
        return Cow::Borrowed(statement);
    }

    let blob_rewritten = statement.replace("BLOB", "BYTEA");
    let mut output = String::with_capacity(blob_rewritten.len() + 16);
    let mut bind_index = 1;
    for ch in blob_rewritten.chars() {
        if ch == '?' {
            output.push('$');
            output.push_str(&bind_index.to_string());
            bind_index += 1;
        } else {
            output.push(ch);
        }
    }
    Cow::Owned(output)
}

fn prepare_sqlite_path(dsn: &str) -> Result<Option<PathBuf>> {
    let Some(path) = sqlite_database_path(dsn) else {
        return Ok(None);
    };
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create sqlite parent directory {}", parent.display()))?;
    }

    let _file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("create sqlite database file {}", path.display()))?;
    Ok(Some(path))
}

fn sqlite_database_path(dsn: &str) -> Option<PathBuf> {
    let raw_path = dsn
        .trim_start_matches("sqlite://")
        .trim_start_matches("sqlite:")
        .split('?')
        .next()
        .unwrap_or_default();
    if raw_path.is_empty() || raw_path == ":memory:" {
        return None;
    }
    Some(PathBuf::from(raw_path))
}

fn acquire_sqlite_lock(path: &Path) -> Result<File> {
    let file_name = path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "anyserve.sqlite".to_string());
    let lock_path = path.with_file_name(format!("{file_name}.control-plane.lock"));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open sqlite lock file {}", lock_path.display()))?;
    file.try_lock_exclusive().map_err(|source| {
        anyhow!(
            "SQLite backend only supports a single control-plane instance for '{}': {source}",
            path.display()
        )
    })?;
    Ok(file)
}

#[async_trait]
impl StateStore for SqlStateStore {
    fn lease_dispatch_mode(&self) -> LeaseDispatchMode {
        if self.dialect == SqlDialect::Postgres {
            LeaseDispatchMode::StoreNative
        } else {
            LeaseDispatchMode::KernelOrderedCandidates
        }
    }

    async fn create_job(&self, job: JobRecord) -> Result<()> {
        let statement = self.sql(
            "INSERT INTO jobs (
                job_id, state, spec_json, inputs_json, outputs_json, lease_id, version, created_at_ms,
                updated_at_ms, last_error, current_attempt_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        );
        sqlx::query(statement.as_ref())
            .bind(&job.job_id)
            .bind(enum_text(&job.state)?)
            .bind(json_text(&stored_job_spec(&job.spec))?)
            .bind(json_text(&stored_job_inputs(&job.spec))?)
            .bind(json_text(&job.outputs)?)
            .bind(job.lease_id.clone())
            .bind(to_i64(job.version)?)
            .bind(to_i64(job.created_at_ms)?)
            .bind(to_i64(job.updated_at_ms)?)
            .bind(job.last_error.clone())
            .bind(job.current_attempt_id.clone())
            .execute(&self.pool)
            .await
            .with_context(|| format!("create job '{}'", job.job_id))?;
        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>> {
        let statement = self.sql(
            "SELECT job_id, state, spec_json, inputs_json, outputs_json, lease_id, version, created_at_ms,
                    updated_at_ms, last_error, current_attempt_id
             FROM jobs WHERE job_id = ?",
        );
        let row = sqlx::query(statement.as_ref())
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("get job '{job_id}'"))?;
        row.map(job_from_row).transpose()
    }

    async fn list_jobs(&self) -> Result<Vec<JobRecord>> {
        let statement = self.sql(
            "SELECT job_id, state, spec_json, inputs_json, outputs_json, lease_id, version, created_at_ms,
                    updated_at_ms, last_error, current_attempt_id
             FROM jobs",
        );
        let rows = sqlx::query(statement.as_ref())
            .fetch_all(&self.pool)
            .await
            .context("list jobs")?;
        rows.into_iter().map(job_from_row).collect()
    }

    async fn job_state_counts(&self) -> Result<JobStateCounts> {
        let statement = self.sql(
            "SELECT state, COUNT(*) AS total
             FROM jobs
             GROUP BY state",
        );
        let rows = sqlx::query(statement.as_ref())
            .fetch_all(&self.pool)
            .await
            .context("count jobs by state")?;
        let mut counts = JobStateCounts::default();
        for row in rows {
            match enum_value::<JobState>(&row.try_get::<String, _>("state")?)? {
                JobState::Pending => counts.pending = from_i64(row.try_get("total")?)? as usize,
                JobState::Leased => counts.leased = from_i64(row.try_get("total")?)? as usize,
                JobState::Running => counts.running = from_i64(row.try_get("total")?)? as usize,
                JobState::Succeeded => counts.succeeded = from_i64(row.try_get("total")?)? as usize,
                JobState::Failed => counts.failed = from_i64(row.try_get("total")?)? as usize,
                JobState::Cancelled => counts.cancelled = from_i64(row.try_get("total")?)? as usize,
            }
        }
        Ok(counts)
    }

    async fn list_job_summary_page(
        &self,
        states: &[JobState],
        limit: usize,
        offset: usize,
    ) -> Result<JobSummaryPage> {
        let mut filter = String::new();
        if !states.is_empty() {
            let placeholders = vec!["?"; states.len()].join(", ");
            filter = format!(" WHERE j.state IN ({placeholders})");
        }

        let count_statement = format!("SELECT COUNT(*) AS total FROM jobs j{filter}");
        let count_sql = self.sql(&count_statement);
        let mut count_query = sqlx::query_scalar::<_, i64>(count_sql.as_ref());
        for state in states {
            count_query = count_query.bind(enum_text(state)?);
        }
        let total = from_i64(
            count_query
                .fetch_one(&self.pool)
                .await
                .context("count job summary page")?,
        )? as usize;

        let page_statement = format!(
            "SELECT
                 j.job_id,
                 j.state,
                 j.spec_json,
                 j.created_at_ms,
                 j.updated_at_ms,
                 j.last_error,
                 j.current_attempt_id,
                 (
                    SELECT COUNT(*)
                    FROM attempts a
                    WHERE a.job_id = j.job_id
                 ) AS attempt_count,
                 (
                    SELECT a.worker_id
                    FROM attempts a
                    WHERE a.job_id = j.job_id
                    ORDER BY a.created_at_ms DESC, a.attempt_id DESC
                    LIMIT 1
                 ) AS latest_worker_id
             FROM jobs j
             {filter}
             ORDER BY j.updated_at_ms DESC, j.created_at_ms DESC, j.job_id ASC
             LIMIT ? OFFSET ?"
        );
        let page_sql = self.sql(&page_statement);
        let mut page_query = sqlx::query(page_sql.as_ref());
        for state in states {
            page_query = page_query.bind(enum_text(state)?);
        }
        let rows = page_query
            .bind(to_i64(limit as u64)?)
            .bind(to_i64(offset as u64)?)
            .fetch_all(&self.pool)
            .await
            .context("list job summary page")?;

        Ok(JobSummaryPage {
            jobs: rows
                .into_iter()
                .map(job_list_summary_from_row)
                .collect::<Result<Vec<_>>>()?,
            total,
        })
    }

    async fn list_pending_jobs(&self) -> Result<Vec<JobRecord>> {
        let statement = self.sql(
            "SELECT job_id, state, spec_json, outputs_json, lease_id, version, created_at_ms,
                    updated_at_ms, last_error, current_attempt_id
             FROM jobs
             WHERE state = ?
             ORDER BY created_at_ms ASC, job_id ASC",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(enum_text(&JobState::Pending)?)
            .fetch_all(&self.pool)
            .await
            .context("list pending jobs")?;
        rows.into_iter().map(job_from_row_without_inputs).collect()
    }

    async fn update_job(&self, job: JobRecord) -> Result<()> {
        let expected_version = job
            .version
            .checked_sub(1)
            .ok_or_else(|| anyhow!("job '{}' version underflow", job.job_id))?;
        let statement = self.sql(
            "UPDATE jobs
             SET state = ?, spec_json = ?, inputs_json = ?, outputs_json = ?, lease_id = ?, version = ?,
                 updated_at_ms = ?, last_error = ?, current_attempt_id = ?
             WHERE job_id = ? AND version = ?",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(enum_text(&job.state)?)
            .bind(json_text(&stored_job_spec(&job.spec))?)
            .bind(json_text(&stored_job_inputs(&job.spec))?)
            .bind(json_text(&job.outputs)?)
            .bind(job.lease_id.clone())
            .bind(to_i64(job.version)?)
            .bind(to_i64(job.updated_at_ms)?)
            .bind(job.last_error.clone())
            .bind(job.current_attempt_id.clone())
            .bind(&job.job_id)
            .bind(to_i64(expected_version)?)
            .execute(&self.pool)
            .await
            .with_context(|| format!("update job '{}'", job.job_id))?;
        if rows.rows_affected() == 0 {
            bail!("job '{}' version conflict", job.job_id);
        }
        Ok(())
    }

    async fn persist_job_inputs(
        &self,
        job_id: &str,
        inputs: Vec<ObjectRef>,
        _now_ms: u64,
    ) -> Result<()> {
        let statement = self.sql(
            "UPDATE jobs
             SET inputs_json = ?
             WHERE job_id = ?",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(json_text(&inputs)?)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .with_context(|| format!("persist inputs for job '{job_id}'"))?;
        if rows.rows_affected() == 0 {
            bail!("job '{}' does not exist", job_id);
        }
        Ok(())
    }

    async fn try_assign_job(
        &self,
        worker_id: &str,
        ordered_candidates: &[JobRecord],
        default_lease_ttl_secs: u64,
        now_ms: u64,
    ) -> Result<Option<LeaseAssignment>> {
        if self.dialect == SqlDialect::Postgres {
            return self
                .try_assign_job_postgres(worker_id, default_lease_ttl_secs, now_ms)
                .await;
        }

        if ordered_candidates.is_empty() {
            return Ok(None);
        }

        let mut tx = self.begin_transition_tx().await?;
        let worker = self
            .get_worker_tx(&mut tx, worker_id)
            .await?
            .ok_or_else(|| anyhow!("worker '{}' does not exist", worker_id))?;
        if worker.expires_at_ms <= now_ms {
            bail!("worker '{}' heartbeat expired", worker_id);
        }

        let active_statement = self.sql("SELECT COUNT(*) FROM leases WHERE worker_id = ?");
        let active_leases: i64 = sqlx::query_scalar(active_statement.as_ref())
            .bind(worker_id)
            .fetch_one(&mut *tx)
            .await
            .with_context(|| format!("count active leases for worker '{worker_id}'"))?;
        if from_i64(active_leases)? >= u64::from(worker.spec.max_active_leases) {
            tx.commit()
                .await
                .context("commit saturated worker assignment tx")?;
            return Ok(None);
        }

        for candidate in ordered_candidates {
            let Some(current) = self.get_job_tx(&mut tx, &candidate.job_id).await? else {
                continue;
            };
            if current.state != JobState::Pending {
                continue;
            }

            let lease_id = Uuid::new_v4().to_string();
            let attempt_id = Uuid::new_v4().to_string();
            let lease_ttl_secs = if current.spec.policy.lease_ttl_secs == 0 {
                default_lease_ttl_secs
            } else {
                current.spec.policy.lease_ttl_secs
            };
            let mut job = current.clone();
            job.state = JobState::Leased;
            job.lease_id = Some(lease_id.clone());
            job.current_attempt_id = Some(attempt_id.clone());
            job.updated_at_ms = now_ms;
            job.version += 1;

            self.update_job_tx(&mut tx, &job, current.version).await?;

            let lease = LeaseRecord {
                lease_id: lease_id.clone(),
                job_id: job.job_id.clone(),
                worker_id: worker_id.to_string(),
                issued_at_ms: now_ms,
                expires_at_ms: now_ms + lease_ttl_secs * 1000,
            };
            let attempt = AttemptRecord {
                attempt_id: attempt_id.clone(),
                job_id: job.job_id.clone(),
                worker_id: worker_id.to_string(),
                lease_id: lease_id.clone(),
                state: AttemptState::Leased,
                created_at_ms: now_ms,
                started_at_ms: None,
                finished_at_ms: None,
                last_error: None,
                metadata: Attributes::new(),
            };

            let insert_lease_statement = self.sql(
                "INSERT INTO leases (lease_id, job_id, worker_id, issued_at_ms, expires_at_ms)
                 VALUES (?, ?, ?, ?, ?)",
            );
            sqlx::query(insert_lease_statement.as_ref())
                .bind(&lease.lease_id)
                .bind(&lease.job_id)
                .bind(&lease.worker_id)
                .bind(to_i64(lease.issued_at_ms)?)
                .bind(to_i64(lease.expires_at_ms)?)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("insert lease '{}'", lease.lease_id))?;

            let insert_attempt_statement = self.sql(
                "INSERT INTO attempts (
                    attempt_id, job_id, worker_id, lease_id, state, created_at_ms,
                    started_at_ms, finished_at_ms, last_error, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            );
            sqlx::query(insert_attempt_statement.as_ref())
                .bind(&attempt.attempt_id)
                .bind(&attempt.job_id)
                .bind(&attempt.worker_id)
                .bind(&attempt.lease_id)
                .bind(enum_text(&attempt.state)?)
                .bind(to_i64(attempt.created_at_ms)?)
                .bind(option_u64_to_i64(attempt.started_at_ms)?)
                .bind(option_u64_to_i64(attempt.finished_at_ms)?)
                .bind(attempt.last_error.clone())
                .bind(json_text(&attempt.metadata)?)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("insert attempt '{}'", attempt.attempt_id))?;

            Self::append_job_event_tx(
                self.dialect,
                &mut tx,
                &job.job_id,
                EventKind::LeaseGranted,
                Vec::new(),
                BTreeMap::from([
                    ("lease_id".to_string(), lease_id),
                    ("attempt_id".to_string(), attempt_id),
                ]),
                now_ms,
            )
            .await?;

            tx.commit().await.context("commit job assignment tx")?;
            self.notify_job_events(&job.job_id).await;
            return Ok(Some(LeaseAssignment {
                lease,
                attempt,
                job,
            }));
        }

        tx.commit()
            .await
            .context("commit empty job assignment tx")?;
        Ok(None)
    }

    async fn report_event_transition(
        &self,
        worker_id: &str,
        lease_id: &str,
        kind: EventKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<()> {
        let mut tx = self.begin_transition_tx().await?;
        let lease = self
            .get_lease_tx(&mut tx, lease_id)
            .await?
            .ok_or_else(|| anyhow!("lease '{}' does not exist", lease_id))?;
        if lease.worker_id != worker_id {
            bail!(
                "lease '{}' does not belong to worker '{}'",
                lease_id,
                worker_id
            );
        }

        let Some(mut job) = self.get_job_tx(&mut tx, &lease.job_id).await? else {
            bail!("job '{}' does not exist", lease.job_id);
        };
        let previous_job_version = job.version;
        if matches!(
            kind,
            EventKind::Started | EventKind::Progress | EventKind::OutputReady
        ) && job.state == JobState::Leased
        {
            job.state = JobState::Running;
            job.updated_at_ms = now_ms;
            job.version += 1;
            self.update_job_tx(&mut tx, &job, previous_job_version)
                .await?;
        }

        let mut attempt = self
            .get_attempt_for_lease_tx(&mut tx, lease_id)
            .await?
            .ok_or_else(|| anyhow!("no attempt found for lease '{}'", lease_id))?;
        if matches!(
            kind,
            EventKind::Started | EventKind::Progress | EventKind::OutputReady
        ) && attempt.state == AttemptState::Leased
        {
            attempt.state = AttemptState::Running;
            attempt.started_at_ms = Some(now_ms);
            self.update_attempt_tx(&mut tx, &attempt).await?;
        }

        Self::append_job_event_tx(
            self.dialect,
            &mut tx,
            &lease.job_id,
            kind,
            payload,
            metadata,
            now_ms,
        )
        .await?;

        tx.commit()
            .await
            .context("commit report event transition")?;
        self.notify_job_events(&lease.job_id).await;
        Ok(())
    }

    async fn cancel_job_transition(
        &self,
        job_id: &str,
        now_ms: u64,
    ) -> Result<StateTransition<JobRecord>> {
        let mut tx = self.begin_transition_tx().await?;
        let Some(mut job) = self.get_job_tx(&mut tx, job_id).await? else {
            bail!("job '{}' does not exist", job_id);
        };
        if job.state.is_terminal() {
            tx.commit()
                .await
                .context("commit cancel terminal job transition")?;
            return Ok(StateTransition::without_streams(job));
        }

        if let Some(lease_id) = job.lease_id.clone() {
            self.delete_lease_tx(&mut tx, &lease_id).await?;
        }

        if let Some(attempt_id) = job.current_attempt_id.clone() {
            let statement = self.sql(
                "SELECT attempt_id, job_id, worker_id, lease_id, state, created_at_ms,
                        started_at_ms, finished_at_ms, last_error, metadata_json
                 FROM attempts WHERE attempt_id = ?",
            );
            if let Some(row) = sqlx::query(statement.as_ref())
                .bind(&attempt_id)
                .fetch_optional(&mut *tx)
                .await
                .with_context(|| format!("load attempt '{attempt_id}' for cancel transition"))?
            {
                let mut attempt = attempt_from_row(row)?;
                attempt.state = AttemptState::Cancelled;
                attempt.finished_at_ms = Some(now_ms);
                self.update_attempt_tx(&mut tx, &attempt).await?;
            }
        }

        let previous_job_version = job.version;
        job.state = JobState::Cancelled;
        job.lease_id = None;
        job.updated_at_ms = now_ms;
        job.version += 1;
        self.update_job_tx(&mut tx, &job, previous_job_version)
            .await?;

        let streams_to_finalize = self
            .close_job_streams_tx(&mut tx, &job.job_id, now_ms)
            .await?;
        Self::append_job_event_tx(
            self.dialect,
            &mut tx,
            &job.job_id,
            EventKind::Cancelled,
            Vec::new(),
            Attributes::new(),
            now_ms,
        )
        .await?;

        tx.commit().await.context("commit cancel job transition")?;
        self.notify_job_events(&job.job_id).await;
        if !streams_to_finalize.is_empty() {
            self.notify_job_streams(&job.job_id).await;
        }
        Ok(StateTransition::new(job.clone(), streams_to_finalize)
            .with_job_event_updates([job.job_id.clone()])
            .with_job_stream_updates([job.job_id.clone()]))
    }

    async fn complete_lease_transition(
        &self,
        worker_id: &str,
        lease_id: &str,
        outputs: Vec<ObjectRef>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<StateTransition<()>> {
        let mut tx = self.begin_transition_tx().await?;
        let lease = self
            .get_lease_tx(&mut tx, lease_id)
            .await?
            .ok_or_else(|| anyhow!("lease '{}' does not exist", lease_id))?;
        if lease.worker_id != worker_id {
            bail!(
                "lease '{}' does not belong to worker '{}'",
                lease_id,
                worker_id
            );
        }

        let Some(mut job) = self.get_job_tx(&mut tx, &lease.job_id).await? else {
            bail!("job '{}' does not exist", lease.job_id);
        };
        let mut attempt = self
            .get_attempt_for_lease_tx(&mut tx, lease_id)
            .await?
            .ok_or_else(|| anyhow!("no attempt found for lease '{}'", lease_id))?;

        if !self.delete_lease_tx(&mut tx, lease_id).await? {
            bail!("lease '{}' no longer exists", lease_id);
        }

        let previous_job_version = job.version;
        job.state = JobState::Succeeded;
        job.outputs = outputs;
        job.lease_id = None;
        job.updated_at_ms = now_ms;
        job.version += 1;
        job.last_error = None;
        self.update_job_tx(&mut tx, &job, previous_job_version)
            .await?;

        attempt.state = AttemptState::Succeeded;
        attempt.finished_at_ms = Some(now_ms);
        attempt.last_error = None;
        self.update_attempt_tx(&mut tx, &attempt).await?;

        let streams_to_finalize = self
            .close_lease_streams_tx(&mut tx, &job.job_id, lease_id, now_ms)
            .await?;
        Self::append_job_event_tx(
            self.dialect,
            &mut tx,
            &job.job_id,
            EventKind::Succeeded,
            Vec::new(),
            metadata,
            now_ms,
        )
        .await?;

        tx.commit()
            .await
            .context("commit complete lease transition")?;
        self.notify_job_events(&job.job_id).await;
        if !streams_to_finalize.is_empty() {
            self.notify_job_streams(&job.job_id).await;
        }
        Ok(StateTransition::new((), streams_to_finalize)
            .with_job_event_updates([job.job_id.clone()])
            .with_job_stream_updates([job.job_id.clone()]))
    }

    async fn fail_lease_transition(
        &self,
        worker_id: &str,
        lease_id: &str,
        reason: String,
        retryable: bool,
        mut metadata: Attributes,
        now_ms: u64,
    ) -> Result<StateTransition<()>> {
        let mut tx = self.begin_transition_tx().await?;
        let lease = self
            .get_lease_tx(&mut tx, lease_id)
            .await?
            .ok_or_else(|| anyhow!("lease '{}' does not exist", lease_id))?;
        if lease.worker_id != worker_id {
            bail!(
                "lease '{}' does not belong to worker '{}'",
                lease_id,
                worker_id
            );
        }

        let Some(mut job) = self.get_job_tx(&mut tx, &lease.job_id).await? else {
            bail!("job '{}' does not exist", lease.job_id);
        };
        let mut attempt = self
            .get_attempt_for_lease_tx(&mut tx, lease_id)
            .await?
            .ok_or_else(|| anyhow!("no attempt found for lease '{}'", lease_id))?;

        if !self.delete_lease_tx(&mut tx, lease_id).await? {
            bail!("lease '{}' no longer exists", lease_id);
        }

        let streams_to_finalize = self
            .close_lease_streams_tx(&mut tx, &job.job_id, lease_id, now_ms)
            .await?;

        metadata.insert("reason".to_string(), reason.clone());
        Self::append_job_event_tx(
            self.dialect,
            &mut tx,
            &job.job_id,
            EventKind::Failed,
            reason.as_bytes().to_vec(),
            metadata.clone(),
            now_ms,
        )
        .await?;

        attempt.state = AttemptState::Failed;
        attempt.finished_at_ms = Some(now_ms);
        attempt.last_error = Some(reason.clone());
        self.update_attempt_tx(&mut tx, &attempt).await?;

        let previous_job_version = job.version;
        job.last_error = Some(reason);
        job.updated_at_ms = now_ms;
        job.version += 1;
        job.lease_id = None;

        if retryable {
            job.state = JobState::Pending;
            job.current_attempt_id = None;
            self.update_job_tx(&mut tx, &job, previous_job_version)
                .await?;
            Self::append_job_event_tx(
                self.dialect,
                &mut tx,
                &job.job_id,
                EventKind::Requeued,
                Vec::new(),
                metadata,
                now_ms,
            )
            .await?;
        } else {
            job.state = JobState::Failed;
            self.update_job_tx(&mut tx, &job, previous_job_version)
                .await?;
        }

        tx.commit().await.context("commit fail lease transition")?;
        self.notify_job_events(&job.job_id).await;
        if !streams_to_finalize.is_empty() {
            self.notify_job_streams(&job.job_id).await;
        }
        Ok(StateTransition::new((), streams_to_finalize)
            .with_job_event_updates([job.job_id.clone()])
            .with_job_stream_updates([job.job_id.clone()]))
    }

    async fn reap_expired_leases_transition(&self, now_ms: u64) -> Result<StateTransition<()>> {
        let has_expired_statement =
            self.sql("SELECT 1 FROM leases WHERE expires_at_ms <= ? LIMIT 1");
        let has_expired = sqlx::query_scalar::<_, i64>(has_expired_statement.as_ref())
            .bind(to_i64(now_ms)?)
            .fetch_optional(&self.pool)
            .await
            .context("probe expired leases before transition")?
            .is_some();
        if !has_expired {
            return Ok(StateTransition::without_streams(()));
        }

        let mut tx = self.begin_transition_tx().await?;
        let statement = self.sql(
            "SELECT lease_id, job_id, worker_id, issued_at_ms, expires_at_ms
             FROM leases
             WHERE expires_at_ms <= ?
             ORDER BY expires_at_ms ASC, lease_id ASC",
        );
        let expired_leases: Vec<LeaseRecord> = sqlx::query(statement.as_ref())
            .bind(to_i64(now_ms)?)
            .fetch_all(&mut *tx)
            .await
            .context("list expired leases in transition")?
            .into_iter()
            .map(lease_from_row)
            .collect::<Result<_>>()?;

        let mut notified_event_jobs = Vec::new();
        let mut notified_stream_jobs = BTreeMap::<String, ()>::new();
        let mut streams_to_finalize = Vec::new();

        for lease in expired_leases {
            if !self.delete_lease_tx(&mut tx, &lease.lease_id).await? {
                continue;
            }

            let closed_streams = self
                .close_lease_streams_tx(&mut tx, &lease.job_id, &lease.lease_id, now_ms)
                .await?;
            if !closed_streams.is_empty() {
                notified_stream_jobs.insert(lease.job_id.clone(), ());
                streams_to_finalize.extend(closed_streams);
            }

            if let Some(mut attempt) = self
                .get_attempt_for_lease_tx(&mut tx, &lease.lease_id)
                .await?
                && !attempt.state.is_terminal()
            {
                attempt.state = AttemptState::Expired;
                attempt.finished_at_ms = Some(now_ms);
                self.update_attempt_tx(&mut tx, &attempt).await?;
            }

            if let Some(mut job) = self.get_job_tx(&mut tx, &lease.job_id).await?
                && !job.state.is_terminal()
            {
                let previous_job_version = job.version;
                job.state = JobState::Pending;
                job.lease_id = None;
                job.current_attempt_id = None;
                job.updated_at_ms = now_ms;
                job.version += 1;
                self.update_job_tx(&mut tx, &job, previous_job_version)
                    .await?;
                Self::append_job_event_tx(
                    self.dialect,
                    &mut tx,
                    &job.job_id,
                    EventKind::LeaseExpired,
                    Vec::new(),
                    BTreeMap::from([("lease_id".to_string(), lease.lease_id.clone())]),
                    now_ms,
                )
                .await?;
                Self::append_job_event_tx(
                    self.dialect,
                    &mut tx,
                    &job.job_id,
                    EventKind::Requeued,
                    Vec::new(),
                    Attributes::new(),
                    now_ms,
                )
                .await?;
                notified_event_jobs.push(job.job_id);
            }
        }

        tx.commit()
            .await
            .context("commit reap expired leases transition")?;
        for job_id in &notified_event_jobs {
            self.notify_job_events(&job_id).await;
        }
        for job_id in notified_stream_jobs.keys() {
            self.notify_job_streams(job_id).await;
        }
        Ok(StateTransition::new((), streams_to_finalize)
            .with_job_event_updates(notified_event_jobs)
            .with_job_stream_updates(notified_stream_jobs.into_keys()))
    }

    async fn append_job_event(
        &self,
        job_id: &str,
        kind: EventKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<JobEvent> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin append job event tx")?;
        let event = Self::append_job_event_tx(
            self.dialect,
            &mut tx,
            job_id,
            kind,
            payload,
            metadata,
            now_ms,
        )
        .await?;
        tx.commit().await.context("commit append job event tx")?;
        self.notify_job_events(job_id).await;
        Ok(event)
    }

    async fn job_events_after(&self, job_id: &str, after_sequence: u64) -> Result<Vec<JobEvent>> {
        let statement = self.sql(
            "SELECT job_id, sequence, kind, payload, metadata_json, created_at_ms
             FROM job_events
             WHERE job_id = ? AND sequence > ?
             ORDER BY sequence ASC",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(job_id)
            .bind(to_i64(after_sequence)?)
            .fetch_all(&self.pool)
            .await
            .with_context(|| format!("job events after for '{job_id}'"))?;
        rows.into_iter().map(job_event_from_row).collect()
    }

    async fn subscribe_job_events(&self, job_id: &str) -> watch::Receiver<u64> {
        let mut watchers = self.watchers.lock().await;
        watchers
            .job_events
            .entry(job_id.to_string())
            .or_insert_with(version_sender)
            .subscribe()
    }

    async fn create_attempt(&self, attempt: AttemptRecord) -> Result<()> {
        let statement = self.sql(
            "INSERT INTO attempts (
                attempt_id, job_id, worker_id, lease_id, state, created_at_ms,
                started_at_ms, finished_at_ms, last_error, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        );
        sqlx::query(statement.as_ref())
            .bind(&attempt.attempt_id)
            .bind(&attempt.job_id)
            .bind(&attempt.worker_id)
            .bind(&attempt.lease_id)
            .bind(enum_text(&attempt.state)?)
            .bind(to_i64(attempt.created_at_ms)?)
            .bind(option_u64_to_i64(attempt.started_at_ms)?)
            .bind(option_u64_to_i64(attempt.finished_at_ms)?)
            .bind(attempt.last_error.clone())
            .bind(json_text(&attempt.metadata)?)
            .execute(&self.pool)
            .await
            .with_context(|| format!("create attempt '{}'", attempt.attempt_id))?;
        Ok(())
    }

    async fn get_attempt(&self, attempt_id: &str) -> Result<Option<AttemptRecord>> {
        let statement = self.sql(
            "SELECT attempt_id, job_id, worker_id, lease_id, state, created_at_ms,
                    started_at_ms, finished_at_ms, last_error, metadata_json
             FROM attempts WHERE attempt_id = ?",
        );
        let row = sqlx::query(statement.as_ref())
            .bind(attempt_id)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("get attempt '{attempt_id}'"))?;
        row.map(attempt_from_row).transpose()
    }

    async fn get_attempt_for_lease(&self, lease_id: &str) -> Result<Option<AttemptRecord>> {
        let statement = self.sql(
            "SELECT attempt_id, job_id, worker_id, lease_id, state, created_at_ms,
                    started_at_ms, finished_at_ms, last_error, metadata_json
             FROM attempts
             WHERE lease_id = ?
             ORDER BY created_at_ms DESC, attempt_id DESC
             LIMIT 1",
        );
        let row = sqlx::query(statement.as_ref())
            .bind(lease_id)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("get attempt for lease '{lease_id}'"))?;
        row.map(attempt_from_row).transpose()
    }

    async fn list_attempts_for_job(&self, job_id: &str) -> Result<Vec<AttemptRecord>> {
        let statement = self.sql(
            "SELECT attempt_id, job_id, worker_id, lease_id, state, created_at_ms,
                    started_at_ms, finished_at_ms, last_error, metadata_json
             FROM attempts
             WHERE job_id = ?
             ORDER BY created_at_ms ASC, attempt_id ASC",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(job_id)
            .fetch_all(&self.pool)
            .await
            .with_context(|| format!("list attempts for job '{job_id}'"))?;
        rows.into_iter().map(attempt_from_row).collect()
    }

    async fn update_attempt(&self, attempt: AttemptRecord) -> Result<()> {
        let statement = self.sql(
            "UPDATE attempts
             SET job_id = ?, worker_id = ?, lease_id = ?, state = ?, created_at_ms = ?,
                 started_at_ms = ?, finished_at_ms = ?, last_error = ?, metadata_json = ?
             WHERE attempt_id = ?",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(&attempt.job_id)
            .bind(&attempt.worker_id)
            .bind(&attempt.lease_id)
            .bind(enum_text(&attempt.state)?)
            .bind(to_i64(attempt.created_at_ms)?)
            .bind(option_u64_to_i64(attempt.started_at_ms)?)
            .bind(option_u64_to_i64(attempt.finished_at_ms)?)
            .bind(attempt.last_error.clone())
            .bind(json_text(&attempt.metadata)?)
            .bind(&attempt.attempt_id)
            .execute(&self.pool)
            .await
            .with_context(|| format!("update attempt '{}'", attempt.attempt_id))?;
        if rows.rows_affected() == 0 {
            bail!("attempt '{}' does not exist", attempt.attempt_id);
        }
        Ok(())
    }

    async fn upsert_worker(&self, worker: WorkerRecord) -> Result<()> {
        let update_statement = self.sql(
            "UPDATE workers
             SET spec_json = ?, status_json = ?, registered_at_ms = ?, expires_at_ms = ?
             WHERE worker_id = ?",
        );
        let rows = sqlx::query(update_statement.as_ref())
            .bind(json_text(&worker.spec)?)
            .bind(json_text(&worker.status)?)
            .bind(to_i64(worker.registered_at_ms)?)
            .bind(to_i64(worker.expires_at_ms)?)
            .bind(&worker.worker_id)
            .execute(&self.pool)
            .await
            .with_context(|| format!("update worker '{}'", worker.worker_id))?;
        if rows.rows_affected() == 0 {
            let insert_statement = self.sql(
                "INSERT INTO workers (worker_id, spec_json, status_json, registered_at_ms, expires_at_ms)
                 VALUES (?, ?, ?, ?, ?)",
            );
            sqlx::query(insert_statement.as_ref())
                .bind(&worker.worker_id)
                .bind(json_text(&worker.spec)?)
                .bind(json_text(&worker.status)?)
                .bind(to_i64(worker.registered_at_ms)?)
                .bind(to_i64(worker.expires_at_ms)?)
                .execute(&self.pool)
                .await
                .with_context(|| format!("insert worker '{}'", worker.worker_id))?;
        }
        Ok(())
    }

    async fn get_worker(&self, worker_id: &str) -> Result<Option<WorkerRecord>> {
        let statement = self.sql(
            "SELECT worker_id, spec_json, status_json, registered_at_ms, expires_at_ms
             FROM workers WHERE worker_id = ?",
        );
        let row = sqlx::query(statement.as_ref())
            .bind(worker_id)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("get worker '{worker_id}'"))?;
        row.map(worker_from_row).transpose()
    }

    async fn list_workers(&self) -> Result<Vec<WorkerRecord>> {
        let statement = self.sql(
            "SELECT worker_id, spec_json, status_json, registered_at_ms, expires_at_ms
             FROM workers",
        );
        let rows = sqlx::query(statement.as_ref())
            .fetch_all(&self.pool)
            .await
            .context("list workers")?;
        rows.into_iter().map(worker_from_row).collect()
    }

    async fn create_lease(&self, lease: LeaseRecord) -> Result<()> {
        let statement = self.sql(
            "INSERT INTO leases (lease_id, job_id, worker_id, issued_at_ms, expires_at_ms)
             VALUES (?, ?, ?, ?, ?)",
        );
        sqlx::query(statement.as_ref())
            .bind(&lease.lease_id)
            .bind(&lease.job_id)
            .bind(&lease.worker_id)
            .bind(to_i64(lease.issued_at_ms)?)
            .bind(to_i64(lease.expires_at_ms)?)
            .execute(&self.pool)
            .await
            .with_context(|| format!("create lease '{}'", lease.lease_id))?;
        Ok(())
    }

    async fn update_lease(&self, lease: LeaseRecord) -> Result<()> {
        let statement = self.sql(
            "UPDATE leases
             SET job_id = ?, worker_id = ?, issued_at_ms = ?, expires_at_ms = ?
             WHERE lease_id = ?",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(&lease.job_id)
            .bind(&lease.worker_id)
            .bind(to_i64(lease.issued_at_ms)?)
            .bind(to_i64(lease.expires_at_ms)?)
            .bind(&lease.lease_id)
            .execute(&self.pool)
            .await
            .with_context(|| format!("update lease '{}'", lease.lease_id))?;
        if rows.rows_affected() == 0 {
            bail!("lease '{}' does not exist", lease.lease_id);
        }
        Ok(())
    }

    async fn get_lease(&self, lease_id: &str) -> Result<Option<LeaseRecord>> {
        let statement = self.sql(
            "SELECT lease_id, job_id, worker_id, issued_at_ms, expires_at_ms
             FROM leases WHERE lease_id = ?",
        );
        let row = sqlx::query(statement.as_ref())
            .bind(lease_id)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("get lease '{lease_id}'"))?;
        row.map(lease_from_row).transpose()
    }

    async fn list_leases(&self) -> Result<Vec<LeaseRecord>> {
        let statement = self.sql(
            "SELECT lease_id, job_id, worker_id, issued_at_ms, expires_at_ms
             FROM leases",
        );
        let rows = sqlx::query(statement.as_ref())
            .fetch_all(&self.pool)
            .await
            .context("list leases")?;
        rows.into_iter().map(lease_from_row).collect()
    }

    async fn delete_lease(&self, lease_id: &str) -> Result<Option<LeaseRecord>> {
        let existing = self.get_lease(lease_id).await?;
        if existing.is_none() {
            return Ok(None);
        }
        let statement = self.sql("DELETE FROM leases WHERE lease_id = ?");
        sqlx::query(statement.as_ref())
            .bind(lease_id)
            .execute(&self.pool)
            .await
            .with_context(|| format!("delete lease '{lease_id}'"))?;
        Ok(existing)
    }

    async fn create_stream(&self, stream: StreamRecord) -> Result<()> {
        let statement = self.sql(
            "INSERT INTO streams (
                stream_id, job_id, attempt_id, lease_id, stream_name, scope, direction,
                state, metadata_json, created_at_ms, closed_at_ms, last_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        );
        sqlx::query(statement.as_ref())
            .bind(&stream.stream_id)
            .bind(&stream.job_id)
            .bind(stream.attempt_id.clone())
            .bind(stream.lease_id.clone())
            .bind(&stream.stream_name)
            .bind(enum_text(&stream.scope)?)
            .bind(enum_text(&stream.direction)?)
            .bind(enum_text(&stream.state)?)
            .bind(json_text(&stream.metadata)?)
            .bind(to_i64(stream.created_at_ms)?)
            .bind(option_u64_to_i64(stream.closed_at_ms)?)
            .bind(to_i64(stream.last_sequence)?)
            .execute(&self.pool)
            .await
            .with_context(|| format!("create stream '{}'", stream.stream_id))?;
        self.notify_job_streams(&stream.job_id).await;
        Ok(())
    }

    async fn get_stream(&self, stream_id: &str) -> Result<Option<StreamRecord>> {
        let statement = self.sql(
            "SELECT stream_id, job_id, attempt_id, lease_id, stream_name, scope, direction,
                    state, metadata_json, created_at_ms, closed_at_ms, last_sequence
             FROM streams WHERE stream_id = ?",
        );
        let row = sqlx::query(statement.as_ref())
            .bind(stream_id)
            .fetch_optional(&self.pool)
            .await
            .with_context(|| format!("get stream '{stream_id}'"))?;
        row.map(stream_from_row).transpose()
    }

    async fn list_streams_for_job(&self, job_id: &str) -> Result<Vec<StreamRecord>> {
        let statement = self.sql(
            "SELECT stream_id, job_id, attempt_id, lease_id, stream_name, scope, direction,
                    state, metadata_json, created_at_ms, closed_at_ms, last_sequence
             FROM streams
             WHERE job_id = ?
             ORDER BY created_at_ms ASC, stream_id ASC",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(job_id)
            .fetch_all(&self.pool)
            .await
            .with_context(|| format!("list streams for job '{job_id}'"))?;
        rows.into_iter().map(stream_from_row).collect()
    }

    async fn update_stream(&self, stream: StreamRecord) -> Result<()> {
        let statement = self.sql(
            "UPDATE streams
             SET job_id = ?, attempt_id = ?, lease_id = ?, stream_name = ?, scope = ?,
                 direction = ?, state = ?, metadata_json = ?, created_at_ms = ?,
                 closed_at_ms = ?, last_sequence = ?
             WHERE stream_id = ?",
        );
        let rows = sqlx::query(statement.as_ref())
            .bind(&stream.job_id)
            .bind(stream.attempt_id.clone())
            .bind(stream.lease_id.clone())
            .bind(&stream.stream_name)
            .bind(enum_text(&stream.scope)?)
            .bind(enum_text(&stream.direction)?)
            .bind(enum_text(&stream.state)?)
            .bind(json_text(&stream.metadata)?)
            .bind(to_i64(stream.created_at_ms)?)
            .bind(option_u64_to_i64(stream.closed_at_ms)?)
            .bind(to_i64(stream.last_sequence)?)
            .bind(&stream.stream_id)
            .execute(&self.pool)
            .await
            .with_context(|| format!("update stream '{}'", stream.stream_id))?;
        if rows.rows_affected() == 0 {
            bail!("stream '{}' does not exist", stream.stream_id);
        }
        self.notify_job_streams(&stream.job_id).await;
        Ok(())
    }

    async fn subscribe_job_streams(&self, job_id: &str) -> watch::Receiver<u64> {
        let mut watchers = self.watchers.lock().await;
        watchers
            .job_streams
            .entry(job_id.to_string())
            .or_insert_with(version_sender)
            .subscribe()
    }
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("value '{value}' does not fit in i64"))
}

fn from_i64(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("value '{value}' does not fit in u64"))
}

fn option_u64_to_i64(value: Option<u64>) -> Result<Option<i64>> {
    value.map(to_i64).transpose()
}

fn option_i64_to_u64(value: Option<i64>) -> Result<Option<u64>> {
    value.map(from_i64).transpose()
}

fn json_text<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).context("serialize json")
}

fn json_value<T: DeserializeOwned>(raw: &str) -> Result<T> {
    serde_json::from_str(raw).with_context(|| format!("deserialize json payload: {raw}"))
}

fn enum_text<T: Serialize>(value: &T) -> Result<String> {
    match serde_json::to_value(value).context("serialize enum")? {
        Value::String(text) => Ok(text),
        other => bail!("expected enum serialization as string, got {other}"),
    }
}

fn enum_value<T: DeserializeOwned>(raw: &str) -> Result<T> {
    serde_json::from_value(Value::String(raw.to_string()))
        .with_context(|| format!("deserialize enum payload: {raw}"))
}

fn stored_job_spec(spec: &JobSpec) -> JobSpec {
    spec.clone()
}

fn stored_job_inputs(spec: &JobSpec) -> Vec<ObjectRef> {
    if !spec.inputs.is_empty() {
        return spec.inputs.clone();
    }
    if spec.params.is_empty() {
        return Vec::new();
    }
    vec![ObjectRef::Inline {
        content: spec.params.clone(),
        metadata: Attributes::new(),
    }]
}

fn job_list_summary_from_row(row: sqlx::any::AnyRow) -> Result<JobListSummary> {
    let spec_json: String = row.try_get("spec_json")?;
    let spec: JobSpec = json_value(&spec_json)?;
    Ok(JobListSummary {
        job_id: row.try_get("job_id")?,
        state: enum_value::<JobState>(&row.try_get::<String, _>("state")?)?,
        interface_name: spec.interface_name,
        source: spec.metadata.get("source").cloned(),
        created_at_ms: from_i64(row.try_get("created_at_ms")?)?,
        updated_at_ms: from_i64(row.try_get("updated_at_ms")?)?,
        current_attempt_id: row.try_get("current_attempt_id")?,
        latest_worker_id: row.try_get("latest_worker_id")?,
        attempt_count: from_i64(row.try_get("attempt_count")?)? as usize,
        last_error: row.try_get("last_error")?,
        metadata: spec.metadata,
    })
}

fn job_from_row(row: sqlx::any::AnyRow) -> Result<JobRecord> {
    let spec_json: String = row.try_get("spec_json")?;
    let inputs_json: String = row.try_get("inputs_json")?;
    let mut spec: JobSpec = json_value(&spec_json)?;
    let inputs: Vec<ObjectRef> = json_value(&inputs_json)?;
    if spec.inputs.is_empty() && spec.params.is_empty() && !inputs.is_empty() {
        spec.inputs = inputs;
    }
    Ok(JobRecord {
        job_id: row.try_get("job_id")?,
        state: enum_value::<JobState>(&row.try_get::<String, _>("state")?)?,
        spec,
        outputs: json_value(&row.try_get::<String, _>("outputs_json")?)?,
        lease_id: row.try_get("lease_id")?,
        version: from_i64(row.try_get("version")?)?,
        created_at_ms: from_i64(row.try_get("created_at_ms")?)?,
        updated_at_ms: from_i64(row.try_get("updated_at_ms")?)?,
        last_error: row.try_get("last_error")?,
        current_attempt_id: row.try_get("current_attempt_id")?,
    })
}

fn job_from_row_without_inputs(row: sqlx::any::AnyRow) -> Result<JobRecord> {
    let spec_json: String = row.try_get("spec_json")?;
    Ok(JobRecord {
        job_id: row.try_get("job_id")?,
        state: enum_value::<JobState>(&row.try_get::<String, _>("state")?)?,
        spec: json_value(&spec_json)?,
        outputs: json_value(&row.try_get::<String, _>("outputs_json")?)?,
        lease_id: row.try_get("lease_id")?,
        version: from_i64(row.try_get("version")?)?,
        created_at_ms: from_i64(row.try_get("created_at_ms")?)?,
        updated_at_ms: from_i64(row.try_get("updated_at_ms")?)?,
        last_error: row.try_get("last_error")?,
        current_attempt_id: row.try_get("current_attempt_id")?,
    })
}

fn job_event_from_row(row: sqlx::any::AnyRow) -> Result<JobEvent> {
    Ok(JobEvent {
        job_id: row.try_get("job_id")?,
        sequence: from_i64(row.try_get("sequence")?)?,
        kind: enum_value::<EventKind>(&row.try_get::<String, _>("kind")?)?,
        payload: row.try_get("payload")?,
        metadata: json_value(&row.try_get::<String, _>("metadata_json")?)?,
        created_at_ms: from_i64(row.try_get("created_at_ms")?)?,
    })
}

fn attempt_from_row(row: sqlx::any::AnyRow) -> Result<AttemptRecord> {
    Ok(AttemptRecord {
        attempt_id: row.try_get("attempt_id")?,
        job_id: row.try_get("job_id")?,
        worker_id: row.try_get("worker_id")?,
        lease_id: row.try_get("lease_id")?,
        state: enum_value::<AttemptState>(&row.try_get::<String, _>("state")?)?,
        created_at_ms: from_i64(row.try_get("created_at_ms")?)?,
        started_at_ms: option_i64_to_u64(row.try_get("started_at_ms")?)?,
        finished_at_ms: option_i64_to_u64(row.try_get("finished_at_ms")?)?,
        last_error: row.try_get("last_error")?,
        metadata: json_value(&row.try_get::<String, _>("metadata_json")?)?,
    })
}

fn worker_from_row(row: sqlx::any::AnyRow) -> Result<WorkerRecord> {
    let spec_json: String = row.try_get("spec_json")?;
    let status_json: String = row.try_get("status_json")?;
    Ok(WorkerRecord {
        worker_id: row.try_get("worker_id")?,
        spec: json_value(&spec_json)?,
        status: json_value(&status_json)?,
        registered_at_ms: from_i64(row.try_get("registered_at_ms")?)?,
        expires_at_ms: from_i64(row.try_get("expires_at_ms")?)?,
    })
}

fn lease_from_row(row: sqlx::any::AnyRow) -> Result<LeaseRecord> {
    Ok(LeaseRecord {
        lease_id: row.try_get("lease_id")?,
        job_id: row.try_get("job_id")?,
        worker_id: row.try_get("worker_id")?,
        issued_at_ms: from_i64(row.try_get("issued_at_ms")?)?,
        expires_at_ms: from_i64(row.try_get("expires_at_ms")?)?,
    })
}

fn stream_from_row(row: sqlx::any::AnyRow) -> Result<StreamRecord> {
    Ok(StreamRecord {
        stream_id: row.try_get("stream_id")?,
        job_id: row.try_get("job_id")?,
        attempt_id: row.try_get("attempt_id")?,
        lease_id: row.try_get("lease_id")?,
        stream_name: row.try_get("stream_name")?,
        scope: enum_value::<StreamScope>(&row.try_get::<String, _>("scope")?)?,
        direction: enum_value::<StreamDirection>(&row.try_get::<String, _>("direction")?)?,
        state: enum_value::<StreamState>(&row.try_get::<String, _>("state")?)?,
        metadata: json_value(&row.try_get::<String, _>("metadata_json")?)?,
        created_at_ms: from_i64(row.try_get("created_at_ms")?)?,
        closed_at_ms: option_i64_to_u64(row.try_get("closed_at_ms")?)?,
        last_sequence: from_i64(row.try_get("last_sequence")?)?,
    })
}

fn version_sender() -> watch::Sender<u64> {
    let (sender, _) = watch::channel(0);
    sender
}

fn bump_version(sender: &watch::Sender<u64>) {
    let next = sender.borrow().saturating_add(1);
    sender.send_replace(next);
}
