use std::borrow::Cow;

use super::api::{JobPage, JobScan};
use super::identity::encode_scoped_id;
use super::{Runtime, view_from_stored};
use crate::error::RuntimeError;

impl Runtime {
    /// Pages across creation-time indexes, without an OFFSET walk or payload
    /// reads. Pages are independent observations, not a long-lived transaction.
    pub async fn scan_job_metadata(&self, scan: JobScan) -> Result<JobPage, RuntimeError> {
        self.ensure_active_leader()?;
        if scan.from_ms >= scan.to_ms
            || scan.to_ms > i64::MAX as u64
            || scan.to_ms - scan.from_ms > 31 * 24 * 60 * 60 * 1000
        {
            return Err(RuntimeError::invalid(
                "choose a time range of at most 31 days",
            ));
        }
        let limit = scan.limit.clamp(1, 250);
        let max_cursor_id_len = if scan.namespace.is_some() {
            self.inner.config.limits.max_identifier_bytes as usize
        } else {
            self.inner.config.limits.max_identifier_bytes as usize + 70
        };
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
                    || cursor.1.len() > max_cursor_id_len
                {
                    return Err(RuntimeError::invalid(
                        "metadata cursor is outside the time range",
                    ));
                }
                Ok(cursor)
            })
            .transpose()?;
        let mut jobs = if scan.archived {
            let journal = self
                .inner
                .journal
                .as_ref()
                .ok_or_else(|| RuntimeError::invalid("durable history requires a journal"))?;
            journal
                .scan_jobs(
                    scan.from_ms,
                    scan.to_ms,
                    before,
                    scan.namespace.as_deref(),
                    limit + 1,
                )
                .await
                .map_err(|error| RuntimeError::unavailable(format!("read history: {error:#}")))?
                .into_iter()
                .map(|job| view_from_stored(job, false))
                .collect::<Vec<_>>()
        } else {
            let st = self.inner.lock();
            st.job_index
                .scan(
                    scan.namespace.as_deref(),
                    scan.from_ms,
                    scan.to_ms,
                    before,
                    limit + 1,
                )
                .into_iter()
                .map(|id| st.jobs[id].view(false))
                .collect()
        };
        let has_more = jobs.len() > limit;
        jobs.truncate(limit);
        let next_page_token = if has_more {
            jobs.last().map(|job| {
                let cursor_id = if scan.namespace.is_some() {
                    Cow::Borrowed(job.job_id.as_str())
                } else {
                    Cow::Owned(encode_scoped_id(&job.namespace, &job.job_id))
                };
                serde_json::to_string(&(job.created_at_ms, cursor_id.as_ref()))
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

#[cfg(test)]
mod tests {
    use super::super::api::{Durability, JobSpec, JournalConfig, RuntimeConfig, SubmitRequest};
    use super::*;

    #[tokio::test]
    async fn multi_namespace_metadata_scan_paginates_without_duplicates_or_skips() {
        let path = std::env::temp_dir()
            .join(format!("anyserve-analytics-{}", uuid::Uuid::new_v4()))
            .join("journal.sqlite");
        let runtime = Runtime::start(RuntimeConfig {
            journal: Some(JournalConfig::new(&path)),
            ..Default::default()
        })
        .await
        .unwrap();

        let from_ms = crate::now_ms().saturating_sub(1000);
        let mut expected = Vec::new();
        for ns in ["default", "alpha", "beta"] {
            for i in 0..3 {
                let mut spec = JobSpec::default();
                spec.demand.interface = "demo".into();
                spec.policy.durability = Some(Durability::Durable);
                let job_id = format!("job-{i}");
                let job = runtime
                    .submit(SubmitRequest {
                        namespace: Some(ns.into()),
                        job_id: Some(job_id),
                        spec,
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .job;
                runtime
                    .cancel_in(ns, &job.job_id, "done".into())
                    .await
                    .unwrap();
                expected.push((ns.to_string(), job.job_id));
            }
        }
        let to_ms = crate::now_ms() + 1000;
        expected.sort();

        for archived in [false, true] {
            if archived {
                let mut st = runtime.inner.lock();
                runtime.inner.evict_terminal_locked(&mut st, u64::MAX);
            }
            let mut cursor = None;
            let mut got = Vec::new();
            let mut pages = 0;
            loop {
                pages += 1;
                assert!(pages <= 10, "pagination looped infinitely");
                let page = runtime
                    .scan_job_metadata(JobScan {
                        archived,
                        from_ms,
                        to_ms,
                        limit: 2,
                        cursor,
                        namespace: None,
                    })
                    .await
                    .unwrap();
                for job in page.jobs {
                    got.push((job.namespace, job.job_id));
                }
                cursor = page.next_page_token;
                if cursor.is_none() {
                    break;
                }
            }
            got.sort();
            assert_eq!(got, expected, "archived={archived}");
        }
        runtime.shutdown().await;
    }
}
