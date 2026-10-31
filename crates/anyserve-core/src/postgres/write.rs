use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{Postgres as Pg, Row, Transaction};

use super::{ClaimedJob, Postgres};
use crate::error::RuntimeError;
use crate::journal::{
    AcceptRecord, CommitOutcome, JournalOp, StoredSpec, job_from_row, state_text,
};
use crate::runtime::api::{ConflictPolicy, ErrorInfo, EventKind, JobState, Object, codes};
use crate::runtime::events::{ByteBudget, Permit};

pub(super) async fn db_now(tx: &mut Transaction<'_, Pg>) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&mut **tx)
            .await?,
    )
}

async fn insert_objects(
    tx: &mut Transaction<'_, Pg>,
    id: &str,
    role: &str,
    objects: &[Object],
) -> Result<()> {
    for (idx, object) in objects.iter().enumerate() {
        let (uri, content) = match object {
            Object::Inline { data, .. } => (None, Some(data.as_ref())),
            Object::Uri { uri, .. } => (Some(uri.as_str()), None),
        };
        sqlx::query("INSERT INTO objects(job_id,role,idx,uri,content,metadata_json) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(id).bind(role).bind(idx as i64).bind(uri).bind(content)
            .bind(serde_json::to_string(object.metadata())?).execute(&mut **tx).await?;
    }
    Ok(())
}

pub(super) async fn event(
    tx: &mut Transaction<'_, Pg>,
    id: &str,
    sequence: i64,
    attempt: Option<&str>,
    at: i64,
    kind: EventKind,
) -> Result<()> {
    // Finished stores metadata here; readers attach the single copy of outputs
    // from objects, under the result-byte budget.
    sqlx::query(
        "INSERT INTO events(job_id,sequence,attempt_id,at_ms,kind_json) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(id)
    .bind(sequence)
    .bind(attempt)
    .bind(at)
    .bind(serde_json::to_string(&kind)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl Postgres {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn append_frame(
        &self,
        id: &str,
        epoch: i64,
        attempt: &str,
        frame: &anyserve_proto::v2::Frame,
        metadata: crate::runtime::api::Metadata,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<u64> {
        let kind = EventKind::Output {
            data: bytes::Bytes::new(),
            metadata,
            progress: frame.progress,
        };
        let json = serde_json::to_string(&kind)?;
        if json
            .len()
            .saturating_add(frame.data.len())
            .saturating_add(96)
            > max_bytes
            || max_events == 0
        {
            return Err(
                RuntimeError::exhausted("frame exceeds the durable event log budget").into(),
            );
        }
        self.ensure_healthy()?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT last_frame_seq,event_sequence FROM jobs
            WHERE job_id=$1 AND owner_id=$2 AND ownership_epoch=$3 AND last_attempt_id=$4
            AND state='running' AND EXISTS (SELECT 1 FROM nodes WHERE node_id=$2 AND expires_at>clock_timestamp()) FOR UPDATE",
        )
        .bind(id)
        .bind(&self.node_id)
        .bind(epoch)
        .bind(attempt)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| RuntimeError::stale("job ownership or attempt changed"))?;
        let last = row.try_get::<i64, _>("last_frame_seq")? as u64;
        if frame.sequence <= last {
            return Ok(last);
        }
        if frame.sequence != last + 1 {
            return Err(RuntimeError::invalid("frame sequence is not contiguous").into());
        }
        let sequence: i64 = row.try_get::<i64, _>("event_sequence")? + 1;
        let now = db_now(&mut tx).await?;
        sqlx::query(
            "INSERT INTO events(job_id,sequence,attempt_id,at_ms,kind_json,frame_data)
            VALUES($1,$2,$3,$4,$5,$6)",
        )
        .bind(id)
        .bind(sequence)
        .bind(attempt)
        .bind(now)
        .bind(json)
        .bind(frame.data.as_ref())
        .execute(&mut *tx)
        .await?;
        let cutoff: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(sequence) FROM (
            SELECT sequence,row_number() OVER (ORDER BY sequence DESC) AS n,
            SUM(COALESCE(octet_length(frame_data),0)+octet_length(kind_json)+96)
                OVER (ORDER BY sequence DESC) AS bytes FROM events WHERE job_id=$1
            ) retained WHERE n>$2 OR bytes>$3",
        )
        .bind(id)
        .bind(max_events as i64)
        .bind(max_bytes as i64)
        .fetch_one(&mut *tx)
        .await?;
        if let Some(cutoff) = cutoff {
            sqlx::query("DELETE FROM events WHERE job_id=$1 AND sequence<=$2")
                .bind(id)
                .bind(cutoff)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query(
            "UPDATE jobs SET last_frame_seq=$2,event_sequence=$3,
            events_floor=GREATEST(events_floor,COALESCE($4,0)) WHERE job_id=$1",
        )
        .bind(id)
        .bind(frame.sequence as i64)
        .bind(sequence)
        .bind(cutoff)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(frame.sequence)
    }

    pub(crate) async fn accept(
        &self,
        record: AcceptRecord,
        conflict: ConflictPolicy,
        queue_timeout_ms: Option<i64>,
    ) -> Result<bool> {
        self.ensure_healthy()?;
        let _permit = self
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| RuntimeError::exhausted("PostgreSQL submission queue is full"))?;
        let mut tx = self.pool.begin().await?;
        let now = db_now(&mut tx).await?;
        let spec = &record.spec;
        let slim = StoredSpec {
            demand: spec.demand.clone(),
            policy: spec.policy.clone(),
            metadata: spec.metadata.clone(),
        };
        let input_bytes =
            spec.inputs.iter().map(Object::inline_len).sum::<usize>() + spec.params.len();
        let inserted = sqlx::query("INSERT INTO jobs(job_id,state,spec_hash,spec_json,interface,
            required_attributes,preferred_attributes,required_capacity,priority,input_bytes,max_attempts,
            created_at_ms,updated_at_ms,queued_at_ms,queue_timeout_ms,total_deadline_ms,spillover_ms,event_sequence)
            VALUES($1,'queued',$2,$3,$4,$5::jsonb,$6::jsonb,$7::jsonb,$8,$9,$10,$11,$11,$11,$12,$13,$14,1)
            ON CONFLICT(job_id) DO NOTHING")
            .bind(&record.job_id).bind(&record.spec_hash).bind(serde_json::to_string(&slim)?)
            .bind(&spec.demand.interface).bind(serde_json::to_string(&spec.demand.required_attributes)?)
            .bind(serde_json::to_string(&spec.demand.preferred_attributes)?)
            .bind(serde_json::to_string(&spec.demand.required_capacity)?)
            .bind(spec.policy.priority as i64).bind(input_bytes as i64).bind(record.max_attempts as i64)
            .bind(now).bind(queue_timeout_ms)
            .bind(spec.policy.total_timeout.map(|d| now.saturating_add(d.as_millis().min(i64::MAX as u128) as i64)))
            .bind(spec.policy.spillover_after.map(|d| d.as_millis().min(i64::MAX as u128) as i64))
            .execute(&mut *tx).await?.rows_affected() == 1;
        if inserted {
            insert_objects(&mut tx, &record.job_id, "input", &spec.inputs).await?;
            if !spec.params.is_empty() {
                insert_objects(
                    &mut tx,
                    &record.job_id,
                    "params",
                    &[Object::inline(spec.params.clone())],
                )
                .await?;
            }
            event(&mut tx, &record.job_id, 1, None, now, EventKind::Accepted).await?;
        } else {
            let hash: String = sqlx::query_scalar("SELECT spec_hash FROM jobs WHERE job_id=$1")
                .bind(&record.job_id)
                .fetch_one(&mut *tx)
                .await?;
            if conflict == ConflictPolicy::Fail || hash != record.spec_hash {
                return Err(RuntimeError::conflict(format!(
                    "job '{}' already exists{}",
                    record.job_id,
                    if hash != record.spec_hash {
                        " with a different specification"
                    } else {
                        ""
                    }
                ))
                .into());
            }
        }
        tx.commit().await?;
        Ok(inserted)
    }

    pub(crate) async fn claim(
        &self,
        workers: Value,
        limit: usize,
        budget: Arc<ByteBudget>,
    ) -> Result<Vec<ClaimedJob>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.ensure_healthy()?;
        let mut tx = self.pool.begin().await?;
        let now = db_now(&mut tx).await?;
        let rows = sqlx::query(
            "SELECT jobs.* FROM jobs
            WHERE state IN ('queued','running')
              AND EXISTS (SELECT 1 FROM nodes WHERE node_id=$4 AND expires_at>clock_timestamp())
              AND (owner_id IS NULL OR NOT EXISTS (
                  SELECT 1 FROM nodes WHERE node_id=owner_id AND expires_at>clock_timestamp()))
              AND (not_before_ms<=$1 OR total_deadline_ms<=$1) AND (
                attempt_count>=max_attempts OR total_deadline_ms<=$1 OR
                (state='queued' AND queued_at_ms+queue_timeout_ms<=$1) OR
                EXISTS (SELECT 1 FROM jsonb_array_elements($2::jsonb) w
                    WHERE w->'interfaces' ? jobs.interface
                      AND (w->'attributes') @> required_attributes
                      AND (spillover_ms IS NULL OR queued_at_ms+spillover_ms<=$1
                           OR (w->'attributes') @> preferred_attributes)
                      AND NOT EXISTS(SELECT 1 FROM jsonb_each_text(required_capacity) r
                          WHERE r.value::bigint>COALESCE((w->'capacity'->>r.key)::bigint,0))))
            ORDER BY priority DESC,created_at_ms,job_id LIMIT $3 FOR UPDATE OF jobs SKIP LOCKED",
        )
        .bind(now)
        .bind(workers.to_string())
        .bind(limit.min(256) as i64)
        .bind(&self.node_id)
        .fetch_all(&mut *tx)
        .await?;
        let mut claimed = Vec::new();
        for row in rows {
            let total_deadline: Option<i64> = row.try_get("total_deadline_ms")?;
            let queue_timeout: Option<i64> = row.try_get("queue_timeout_ms")?;
            let queued_at: i64 = row.try_get("queued_at_ms")?;
            let failure = if total_deadline.is_some_and(|t| t <= now) {
                Some(ErrorInfo::new(
                    codes::TOTAL_TIMEOUT,
                    "job exceeded its total timeout",
                    false,
                ))
            } else if row.try_get::<&str, _>("state")? == "queued"
                && queue_timeout.is_some_and(|t| queued_at.saturating_add(t) <= now)
            {
                Some(ErrorInfo::new(
                    codes::QUEUE_TIMEOUT,
                    "job exceeded its queue timeout",
                    false,
                ))
            } else if row.try_get::<i64, _>("attempt_count")?
                >= row.try_get::<i64, _>("max_attempts")?
            {
                Some(ErrorInfo::new(
                    codes::ATTEMPTS_EXHAUSTED,
                    "previous replica exhausted the attempt budget",
                    false,
                ))
            } else {
                None
            };
            let mut permit = Permit::new(Arc::clone(&budget));
            if failure.is_none()
                && !permit.grow(usize::try_from(row.try_get::<i64, _>("input_bytes")?)?)
            {
                continue;
            }
            let mut job = job_from_row(&row)?;
            let was_running = job.state == JobState::Running;
            let queued_at = if was_running {
                now
            } else {
                row.try_get("queued_at_ms")?
            };
            let ownership = sqlx::query(
                "UPDATE jobs SET owner_id=$2,ownership_epoch=ownership_epoch+1,
                state='queued',queued_at_ms=$3,
                event_sequence=event_sequence+CASE WHEN state='running' THEN 1 ELSE 0 END
                WHERE job_id=$1 RETURNING ownership_epoch,event_sequence",
            )
            .bind(&job.job_id)
            .bind(&self.node_id)
            .bind(queued_at)
            .fetch_one(&mut *tx)
            .await?;
            if was_running {
                event(
                    &mut tx,
                    &job.job_id,
                    ownership.try_get("event_sequence")?,
                    job.last_attempt_id.as_deref(),
                    now,
                    EventKind::AttemptRestarted {
                        previous_attempt_id: job.last_attempt_id.clone().unwrap_or_default(),
                        reason: ErrorInfo::new(
                            codes::RECOVERED,
                            "previous replica lease expired",
                            true,
                        ),
                    },
                )
                .await?;
            }
            if failure.is_none() {
                super::read::load_inputs(&mut tx, &mut job).await?;
            }
            job.state = JobState::Queued;
            claimed.push(ClaimedJob {
                job,
                epoch: ownership.try_get("ownership_epoch")?,
                queued_at_ms: queued_at as u64,
                input_permit: permit,
                failure,
            });
        }
        if let Err(error) = tx.commit().await {
            // The claim may have committed although no local job was installed.
            // Stop renewal so another node can recover every ambiguous claim.
            self.fence();
            return Err(error.into());
        }
        Ok(claimed)
    }

    pub(crate) async fn release(&self, id: &str, epoch: i64) -> Result<bool> {
        Ok(sqlx::query(
            "UPDATE jobs SET owner_id=NULL WHERE job_id=$1 AND owner_id=$2
            AND ownership_epoch=$3 AND state='queued'",
        )
        .bind(id)
        .bind(&self.node_id)
        .bind(epoch)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1)
    }

    pub(super) async fn apply(&self, op: JournalOp) -> CommitOutcome {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(e) => return CommitOutcome::RolledBack(e.to_string()),
        };
        let result = self.apply_in_tx(&mut tx, &op).await;
        match result {
            Ok(false) => CommitOutcome::Fenced("job ownership or state changed".into()),
            Err(e) => CommitOutcome::RolledBack(e.to_string()),
            Ok(true) => match tx.commit().await {
                Ok(()) => CommitOutcome::Committed,
                // A lost COMMIT response is ambiguous. Fence this node; the
                // authoritative row/receipt remains readable on every replica.
                Err(e) => CommitOutcome::Unknown(e.to_string()),
            },
        }
    }

    async fn apply_in_tx(&self, tx: &mut Transaction<'_, Pg>, op: &JournalOp) -> Result<bool> {
        self.ensure_healthy()?;
        let now = db_now(tx).await?;
        match op {
            JournalOp::Probe => Ok(true),
            JournalOp::Accept(_) => {
                anyhow::bail!("PostgreSQL acceptance uses atomic idempotent submission")
            }
            JournalOp::AttemptStart(record) => {
                let seq: Option<i64> = sqlx::query_scalar("UPDATE jobs SET state='running',
                    attempt_count=$4,last_attempt_id=$5,last_worker_id=$6,updated_at_ms=$7,event_sequence=event_sequence+1,last_frame_seq=0
                    WHERE job_id=$1 AND owner_id=$2 AND ownership_epoch=$3
                      AND state='queued' AND attempt_count=$4-1
                      AND EXISTS (SELECT 1 FROM nodes WHERE node_id=$2 AND expires_at>clock_timestamp())
                    RETURNING event_sequence")
                    .bind(&record.job_id).bind(&self.node_id).bind(record.ownership_epoch)
                    .bind(record.attempt_number as i64).bind(&record.attempt_id).bind(&record.worker_id)
                    .bind(now).fetch_optional(&mut **tx).await?;
                let Some(seq) = seq else {
                    return Ok(false);
                };
                event(
                    tx,
                    &record.job_id,
                    seq,
                    record.attempt_id.as_deref(),
                    now,
                    EventKind::Assigned {
                        worker_id: record.worker_id.clone().unwrap_or_default(),
                        attempt_number: record.attempt_number,
                    },
                )
                .await?;
                Ok(true)
            }
            JournalOp::Terminal(record) => {
                let retry = record.state == JobState::Queued;
                let state = state_text(record.state);
                let seq: Option<i64> = sqlx::query_scalar("UPDATE jobs SET state=$4,error_json=$5,
                    updated_at_ms=$6,owner_id=NULL,event_sequence=event_sequence+1,
                    queued_at_ms=$6,not_before_ms=$7
                    WHERE job_id=$1 AND owner_id=$2 AND ownership_epoch=$3 AND state IN ('queued','running')
                      AND EXISTS (SELECT 1 FROM nodes WHERE node_id=$2 AND expires_at>clock_timestamp())
                    RETURNING event_sequence")
                    .bind(&record.job_id).bind(&self.node_id).bind(record.ownership_epoch).bind(state)
                    .bind(if retry { None } else { record.error.as_ref().map(serde_json::to_string).transpose()? })
                    .bind(now).bind(record.retry_at_ms.map(|at| now.saturating_add(at.saturating_sub(record.at_ms).min(i64::MAX as u64) as i64)).unwrap_or_default())
                    .fetch_optional(&mut **tx).await?;
                let Some(seq) = seq else {
                    return Ok(false);
                };
                if record.state == JobState::Succeeded {
                    insert_objects(tx, &record.job_id, "output", &record.outputs).await?;
                }
                if let (Some(lease), Some(digest)) =
                    (&record.winning_lease_id, &record.outcome_digest)
                {
                    sqlx::query("INSERT INTO receipts(lease_id,job_id,digest) VALUES($1,$2,$3)")
                        .bind(lease)
                        .bind(&record.job_id)
                        .bind(digest)
                        .execute(&mut **tx)
                        .await?;
                }
                let kind = if retry {
                    EventKind::AttemptRestarted {
                        previous_attempt_id: record.attempt_id.clone().unwrap_or_default(),
                        reason: record.error.clone().context("retry reason")?,
                    }
                } else {
                    EventKind::Finished {
                        state: record.state,
                        outputs: Vec::new(),
                        error: record.error.clone(),
                    }
                };
                event(
                    tx,
                    &record.job_id,
                    seq,
                    record.attempt_id.as_deref(),
                    now,
                    kind,
                )
                .await?;
                Ok(true)
            }
        }
    }

    pub(crate) async fn cancel(&self, id: &str, reason: String) -> Result<()> {
        self.ensure_healthy()?;
        let mut tx = self.pool.begin().await?;
        let now = db_now(&mut tx).await?;
        let error = ErrorInfo::new(
            codes::CANCELLED,
            if reason.trim().is_empty() {
                "cancelled".into()
            } else {
                reason
            },
            false,
        );
        let row = sqlx::query("UPDATE jobs SET state='cancelled',error_json=$2,updated_at_ms=$3,
            owner_id=NULL,ownership_epoch=ownership_epoch+1,event_sequence=event_sequence+1
            WHERE job_id=$1 AND state IN ('queued','running') RETURNING event_sequence,last_attempt_id")
            .bind(id).bind(serde_json::to_string(&error)?).bind(now).fetch_optional(&mut *tx).await?;
        if let Some(row) = row {
            let attempt: Option<String> = row.try_get("last_attempt_id")?;
            event(
                &mut tx,
                id,
                row.try_get("event_sequence")?,
                attempt.as_deref(),
                now,
                EventKind::Finished {
                    state: JobState::Cancelled,
                    outputs: Vec::new(),
                    error: Some(error),
                },
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn purge(&self, before_ms: u64) -> Result<usize> {
        // Bounded cleanup, safe when several replicas run maintenance together.
        let mut removed = 0;
        loop {
            let count = sqlx::query(
                "DELETE FROM jobs WHERE job_id IN (SELECT job_id FROM jobs
            WHERE state IN ('succeeded','failed','cancelled') AND updated_at_ms<$1
            ORDER BY updated_at_ms LIMIT 500 FOR UPDATE SKIP LOCKED)",
            )
            .bind(before_ms.min(i64::MAX as u64) as i64)
            .execute(&self.pool)
            .await?
            .rows_affected() as usize;
            removed += count;
            if count < 500 {
                return Ok(removed);
            }
            tokio::task::yield_now().await;
        }
    }
}
