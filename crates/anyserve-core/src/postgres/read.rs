use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use sqlx::{Postgres as Pg, QueryBuilder, Row, Transaction};

use super::Postgres;
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
    Preview,
}

#[derive(Debug)]
pub(crate) struct JobSnapshot {
    pub job: StoredJob,
    pub events: Vec<ExecutionEvent>,
    pub previews: Vec<ObjectPreview>,
}

pub(super) async fn load_inputs(tx: &mut Transaction<'_, Pg>, job: &mut StoredJob) -> Result<()> {
    let mut rows = sqlx::query(
        "SELECT role,uri,content,metadata_json FROM objects
        WHERE job_id=$1 AND role IN ('input','params') ORDER BY role,idx",
    )
    .bind(&job.job_id)
    .fetch(&mut **tx);
    while let Some(row) = rows.try_next().await? {
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
) -> Result<()> {
    let size: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(octet_length(content)),0)::bigint FROM objects
        WHERE job_id=$1 AND role='output' AND uri IS NULL",
    )
    .bind(&job.job_id)
    .fetch_one(&mut **tx)
    .await?;
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
        let Some(row) = sqlx::query("SELECT * FROM jobs WHERE job_id=$1")
            .bind(id)
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
        if mode == ReadMode::Full && job.state == JobState::Succeeded {
            load_outputs(&mut tx, &mut job, budget).await?;
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
            let frame_limit = if mode == ReadMode::Full {
                i32::MAX
            } else {
                INSPECTION_PREVIEW_BYTES as i32
            };
            let size: i64 = sqlx::query_scalar(
                "SELECT COALESCE(SUM(LEAST(COALESCE(octet_length(frame_data),0),$3)),0)::bigint
            FROM events WHERE job_id=$1 AND sequence>$2",
            )
            .bind(id)
            .bind(after.min(i64::MAX as u64) as i64)
            .bind(frame_limit)
            .fetch_one(&mut *tx)
            .await?;
            let mut permit = Permit::new(event_budget);
            if !permit.grow(usize::try_from(size)?) {
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
