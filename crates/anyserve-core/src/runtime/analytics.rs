use super::api::{JobPage, JobScan};
use super::{Runtime, view_from_stored};
use crate::error::RuntimeError;

impl Runtime {
    /// Pages across creation-time indexes, without an OFFSET walk or payload
    /// reads. Pages are independent observations, not a long-lived transaction.
    pub async fn scan_job_metadata(&self, scan: JobScan) -> Result<JobPage, RuntimeError> {
        if scan.from_ms >= scan.to_ms
            || scan.to_ms > i64::MAX as u64
            || scan.to_ms - scan.from_ms > 31 * 24 * 60 * 60 * 1000
        {
            return Err(RuntimeError::invalid(
                "choose a time range of at most 31 days",
            ));
        }
        let limit = scan.limit.clamp(1, 250);
        let before: Option<(u64, String)> = scan
            .cursor
            .as_deref()
            .map(|value| {
                if value.len() > 2048 {
                    return Err(RuntimeError::invalid("invalid metadata cursor"));
                }
                let cursor: (u64, String) = serde_json::from_str(value)
                    .map_err(|_| RuntimeError::invalid("invalid metadata cursor"))?;
                if cursor.0 < scan.from_ms
                    || cursor.0 >= scan.to_ms
                    || cursor.1.is_empty()
                    || cursor.1.len() > self.inner.config.limits.max_identifier_bytes as usize
                {
                    return Err(RuntimeError::invalid(
                        "metadata cursor is outside the time range",
                    ));
                }
                Ok(cursor)
            })
            .transpose()?;
        let mut jobs = if let Some(pg) = &self.inner.postgres {
            let states = if scan.archived {
                vec![
                    super::api::JobState::Succeeded,
                    super::api::JobState::Failed,
                    super::api::JobState::Cancelled,
                ]
            } else {
                vec![super::api::JobState::Queued, super::api::JobState::Running]
            };
            pg.scan_jobs(scan.from_ms, scan.to_ms, before, &states, None, limit + 1)
                .await
                .map_err(super::distributed::read_error)?
                .into_iter()
                .map(|job| view_from_stored(job, false))
                .collect::<Vec<_>>()
        } else if scan.archived {
            let journal = self
                .inner
                .journal
                .as_ref()
                .ok_or_else(|| RuntimeError::invalid("durable history requires a journal"))?;
            journal
                .scan_jobs(scan.from_ms, scan.to_ms, before, limit + 1)
                .await
                .map_err(|error| RuntimeError::unavailable(format!("read history: {error:#}")))?
                .into_iter()
                .map(|job| view_from_stored(job, false))
                .collect::<Vec<_>>()
        } else {
            let st = self.inner.lock();
            st.job_index
                .scan(scan.from_ms, scan.to_ms, before, limit + 1)
                .into_iter()
                .map(|id| st.jobs[id].view(false))
                .collect()
        };
        let has_more = jobs.len() > limit;
        jobs.truncate(limit);
        let next_page_token = if has_more {
            jobs.last().map(|job| {
                serde_json::to_string(&(job.created_at_ms, &job.job_id))
                    .expect("cursor contains only an integer and string")
            })
        } else {
            None
        };
        Ok(JobPage {
            jobs,
            next_page_token,
        })
    }
}
