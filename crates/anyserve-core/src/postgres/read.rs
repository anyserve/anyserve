use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use sqlx::{Postgres as Pg, QueryBuilder, Row, Transaction};

use super::{ClaimedJob, Postgres};
use crate::error::RuntimeError;
use crate::journal::{StoredJob, job_from_row, object_from_row, state_text};
use crate::runtime::api::{
    EventKind, ExecutionEvent, INSPECTION_PREVIEW_BYTES, JobState, Object, ObjectPreview,
};
use crate::runtime::events::{ByteBudget, Permit, budgeted_bytes};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadMode {
    Metadata,
    Full,
    Events,
    Preview,
}

#[derive(Debug)]
pub(crate) struct JobSnapshot {
    pub job: StoredJob,
    pub events: Vec<ExecutionEvent>,
    pub previews: Vec<ObjectPreview>,
}

pub(super) async fn load_inputs(
    tx: &mut Transaction<'_, Pg>,
    claimed: &mut [ClaimedJob],
) -> Result<()> {
    let mut jobs: std::collections::HashMap<_, _> = claimed
        .iter_mut()
        .filter(|claimed| claimed.failure.is_none())
        .map(|claimed| (claimed.job.job_id.clone(), &mut claimed.job))
        .collect();
    if jobs.is_empty() {
        return Ok(());
    }
    let ids: Vec<_> = jobs.keys().cloned().collect();
    let mut rows = sqlx::query(
        "SELECT job_id,role,uri,content,metadata_json FROM objects
        WHERE job_id=ANY($1) AND role IN ('input','params') ORDER BY job_id,role,idx",
    )
    .bind(ids)
    .fetch(&mut **tx);
    while let Some(row) = rows.try_next().await? {
        let job = jobs
            .get_mut(row.try_get::<&str, _>("job_id")?)
            .context("unexpected input job")?;
        let object = object_from_row(&row)?;
        match row.try_get::<&str, _>("role")? {
            "input" => job.spec.inputs.push(object),
            "params" => {
                if let Object::Inline { data, .. } = object {
                    job.spec.params = data;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

async fn load_outputs(
    tx: &mut Transaction<'_, Pg>,
    job: &mut StoredJob,
    budget: Arc<ByteBudget>,
    size: i64,
) -> Result<()> {
    let mut permit = Permit::new(budget);
    if !permit.grow(usize::try_from(size).context("PostgreSQL result size")?) {
        return Err(
            RuntimeError::exhausted("result budget is exhausted; retry the read later").into(),
        );
    }
    let mut rows = sqlx::query("SELECT uri,content,metadata_json FROM objects WHERE job_id=$1 AND role='output' ORDER BY idx")
        .bind(&job.job_id).fetch(&mut **tx);
    while let Some(row) = rows.try_next().await? {
        let mut object = object_from_row(&row)?;
        if let Object::Inline { data, .. } = &mut object {
            let reserved = permit.split(data.len());
            *data = budgeted_bytes(std::mem::take(data), reserved);
        }
        job.outputs.push(object);
    }
    Ok(())
}

impl Postgres {
    pub(crate) async fn snapshot(
        &self,
        id: &str,
        mode: ReadMode,
        after: Option<u64>,
        budget: Arc<ByteBudget>,
        event_budget: Arc<ByteBudget>,
    ) -> Result<Option<JobSnapshot>> {
        if mode == ReadMode::Metadata && after.is_none() {
            return sqlx::query("SELECT * FROM jobs WHERE job_id=$1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
                .as_ref()
                .map(|row| {
                    Ok(JobSnapshot {
                        job: job_from_row(row)?,
                        events: Vec::new(),
                        previews: Vec::new(),
                    })
                })
                .transpose();
        }
        // Size reservation, payloads, events and retention observe one snapshot.
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        let frame_limit = if matches!(mode, ReadMode::Full | ReadMode::Events) {
            i32::MAX
        } else {
            INSPECTION_PREVIEW_BYTES as i32
        };
        let Some(row) = sqlx::query(
            "SELECT jobs.*,
            CASE WHEN state='succeeded' AND ($2 OR ($3 AND $4::bigint<event_sequence))
                THEN (SELECT COALESCE(SUM(octet_length(content)),0)::bigint FROM objects
                      WHERE objects.job_id=jobs.job_id AND role='output' AND uri IS NULL)
                ELSE 0 END AS result_bytes,
            CASE WHEN $4::bigint<event_sequence
                THEN (SELECT COALESCE(SUM(LEAST(COALESCE(octet_length(frame_data),0),$5)),0)::bigint
                      FROM events WHERE events.job_id=jobs.job_id AND sequence>$4)
                ELSE 0 END AS frame_bytes
            FROM jobs WHERE job_id=$1",
        )
        .bind(id)
        .bind(mode == ReadMode::Full)
        .bind(mode == ReadMode::Events)
        .bind(after.map(|after| after.min(i64::MAX as u64) as i64))
        .bind(frame_limit)
        .fetch_optional(&mut *tx)
        .await?
        else {
            return Ok(None);
        };
        let mut job = job_from_row(&row)?;
        let floor = row.try_get::<i64, _>("events_floor")? as u64;
        if after.is_some_and(|after| after != 0 && after < floor) {
            return Err(RuntimeError::cursor_expired(floor + 1).into());
        }
        let sequence = row.try_get::<i64, _>("event_sequence")? as u64;
        let after = after.filter(|after| *after < sequence);
        if job.state == JobState::Succeeded
            && (mode == ReadMode::Full || (mode == ReadMode::Events && after.is_some()))
        {
            load_outputs(&mut tx, &mut job, budget, row.try_get("result_bytes")?).await?;
        }
        let mut previews = Vec::new();
        if mode == ReadMode::Preview {
            let rows = sqlx::query(
                "SELECT uri,substring(content FROM 1 FOR $2) AS content,
                octet_length(content)::bigint AS size_bytes,metadata_json FROM objects
                WHERE job_id=$1 AND role='output' ORDER BY idx",
            )
            .bind(id)
            .bind(INSPECTION_PREVIEW_BYTES as i32)
            .fetch_all(&mut *tx)
            .await?;
            for row in rows {
                previews.push(ObjectPreview {
                    prefix: object_from_row(&row)?,
                    size_bytes: row
                        .try_get::<Option<i64>, _>("size_bytes")?
                        .map(|v| v as usize),
                });
            }
        }
        let mut events = Vec::new();
        if let Some(after) = after {
            let mut permit = Permit::new(event_budget);
            if !permit.grow(usize::try_from(row.try_get::<i64, _>("frame_bytes")?)?) {
                return Err(RuntimeError::exhausted("event read budget is exhausted").into());
            }
            let rows = sqlx::query("SELECT sequence,attempt_id,at_ms,kind_json,substring(frame_data FROM 1 FOR $3) AS frame_data
            FROM events WHERE job_id=$1 AND sequence>$2 ORDER BY sequence")
            .bind(id).bind(after.min(i64::MAX as u64) as i64).bind(frame_limit).fetch_all(&mut *tx).await?;
            for row in rows {
                let mut kind: EventKind = serde_json::from_str(row.try_get("kind_json")?)?;
                if let EventKind::Output { data, .. } = &mut kind {
                    let content: Vec<u8> = row.try_get("frame_data")?;
                    let reserved = permit.split(content.len());
                    *data = budgeted_bytes(Bytes::from(content), reserved);
                }
                if let EventKind::Finished { outputs, .. } = &mut kind {
                    *outputs = job.outputs.clone();
                }
                events.push(ExecutionEvent {
                    job_id: id.to_string(),
                    sequence: row.try_get::<i64, _>("sequence")? as u64,
                    attempt_id: row.try_get("attempt_id")?,
                    at_ms: row.try_get::<i64, _>("at_ms")? as u64,
                    kind,
                });
            }
        }
        tx.commit().await?;
        Ok(Some(JobSnapshot {
            job,
            events,
            previews,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn scan_jobs(
        &self,
        from: u64,
        to: u64,
        before: Option<(u64, String)>,
        states: &[JobState],
        interface: Option<&str>,
        limit: usize,
    ) -> Result<Vec<StoredJob>> {
        let mut query = QueryBuilder::<Pg>::new("SELECT * FROM jobs WHERE created_at_ms>=");
        query
            .push_bind(from.min(i64::MAX as u64) as i64)
            .push(" AND created_at_ms<")
            .push_bind(to.min(i64::MAX as u64) as i64);
        if let Some((at, id)) = before {
            query
                .push(" AND (created_at_ms,job_id)<(")
                .push_bind(at.min(i64::MAX as u64) as i64)
                .push(",")
                .push_bind(id)
                .push(")");
        }
        if !states.is_empty() {
            query
                .push(" AND state=ANY(")
                .push_bind(states.iter().copied().map(state_text).collect::<Vec<_>>())
                .push(")");
        }
        if let Some(interface) = interface {
            query.push(" AND interface=").push_bind(interface);
        }
        query
            .push(" ORDER BY created_at_ms DESC,job_id DESC LIMIT ")
            .push_bind(limit as i64);
        query
            .build()
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(job_from_row)
            .collect()
    }

    pub(crate) async fn receipt(&self, lease_id: &str) -> Result<Option<(String, String)>> {
        Ok(
            sqlx::query_as("SELECT job_id,digest FROM receipts WHERE lease_id=$1")
                .bind(lease_id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }
}
