use std::cmp::Reverse;

use super::api::{JobHistogram, JobHistogramBucket, JobQuery, JobQueryResult, JobState};
use super::{DEFAULT_PAGE_SIZE, Runtime, state};
use crate::error::RuntimeError;
use crate::now_ms;

impl Runtime {
    /// Queries retained jobs with optional search, worker, and time-window filters,
    /// exact total count, and an optional stacked time histogram.
    pub fn query_jobs(&self, query: JobQuery) -> Result<JobQueryResult, RuntimeError> {
        self.ensure_active_leader()?;
        if let (Some(from), Some(to)) = (query.from_ms, query.to_ms)
            && from >= to
        {
            return Err(RuntimeError::invalid("from_ms must be less than to_ms"));
        }
        let offset = match query.page_token.as_deref().filter(|t| !t.is_empty()) {
            Some(token) => token
                .parse::<usize>()
                .map_err(|_| RuntimeError::invalid("invalid page_token"))?,
            None => 0,
        };
        let limit = if query.limit == 0 {
            DEFAULT_PAGE_SIZE
        } else {
            query.limit
        };
        let needle = query
            .search
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_lowercase);
        let worker_filter = query
            .worker_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());

        let has_extra_filters = worker_filter.is_some()
            || needle.is_some()
            || query.from_ms.is_some()
            || query.to_ms.is_some();

        let guard = self.inner.lock();
        let st = &*guard;
        if !has_extra_filters && query.histogram_buckets == 0 {
            let (ids, total) = st.job_index.page_raw(
                &query.states,
                query.namespace.as_deref(),
                query.interface.as_deref(),
                offset,
                limit,
            );
            let jobs = ids.into_iter().map(|id| st.jobs[id].view(false)).collect();
            let next_offset = offset.saturating_add(limit);
            return Ok(JobQueryResult {
                jobs,
                total,
                next_page_token: (next_offset < total).then(|| next_offset.to_string()),
                histogram: None,
            });
        }

        let need_samples = query.histogram_buckets > 0;
        let mut samples = Vec::new();
        let (total, page_views) = if query.from_ms.is_some() || query.to_ms.is_some() {
            let mut matched: Vec<(Reverse<u64>, &str)> = Vec::new();
            st.job_index.visit_created_range(
                query.namespace.as_deref(),
                query.from_ms,
                query.to_ms,
                |id| {
                    let job = &st.jobs[id];
                    if matches_job(job, &query, worker_filter, needle.as_deref()) {
                        matched.push((Reverse(job.updated_at_ms), id));
                        if need_samples {
                            samples.push((job.created_at_ms, job.state()));
                        }
                    }
                },
            );
            matched.sort_unstable();
            let total = matched.len();
            let page_views = matched
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(_, id)| st.jobs[id].view(false))
                .collect();
            (total, page_views)
        } else {
            let mut total = 0usize;
            let mut page_ids: Vec<&str> = Vec::new();
            st.job_index.visit_ordered(
                &query.states,
                query.namespace.as_deref(),
                query.interface.as_deref(),
                |id| {
                    let job = &st.jobs[id];
                    if matches_job(job, &query, worker_filter, needle.as_deref()) {
                        if total >= offset && page_ids.len() < limit {
                            page_ids.push(id);
                        }
                        if need_samples {
                            samples.push((job.created_at_ms, job.state()));
                        }
                        total += 1;
                    }
                },
            );
            let page_views = page_ids
                .into_iter()
                .map(|id| st.jobs[id].view(false))
                .collect();
            (total, page_views)
        };
        drop(guard);

        let histogram = build_histogram(
            query.from_ms,
            query.to_ms,
            query.histogram_buckets,
            &samples,
        );
        let next_offset = offset.saturating_add(limit);
        Ok(JobQueryResult {
            jobs: page_views,
            total,
            next_page_token: (next_offset < total).then(|| next_offset.to_string()),
            histogram,
        })
    }
}

fn matches_job(
    job: &state::Job,
    query: &JobQuery,
    worker_filter: Option<&str>,
    needle_lower: Option<&str>,
) -> bool {
    let state = job.state();
    if !query.states.is_empty() && !query.states.contains(&state) {
        return false;
    }
    if let Some(iface) = query.interface.as_deref()
        && job.spec.demand.interface != iface
    {
        return false;
    }
    if let Some(ns) = query.namespace.as_deref()
        && job.namespace != ns
    {
        return false;
    }
    if let Some(from) = query.from_ms
        && job.created_at_ms < from
    {
        return false;
    }
    if let Some(to) = query.to_ms
        && job.created_at_ms >= to
    {
        return false;
    }
    if let Some(worker) = worker_filter
        && job.current_worker_id() != Some(worker)
    {
        return false;
    }
    if let Some(needle) = needle_lower {
        let hit =
            contains_ignore_case(&job.raw_id, needle)
                || contains_ignore_case(&job.spec.demand.interface, needle)
                || contains_ignore_case(&job.namespace, needle)
                || job
                    .current_worker_id()
                    .is_some_and(|w| contains_ignore_case(w, needle))
                || job.error.as_ref().is_some_and(|err| {
                    contains_ignore_case(&err.code, needle)
                        || contains_ignore_case(&err.message, needle)
                })
                || job.spec.metadata.iter().any(|(k, v)| {
                    contains_ignore_case(k, needle) || contains_ignore_case(v, needle)
                });
        if !hit {
            return false;
        }
    }
    true
}

fn contains_ignore_case(haystack: &str, needle_lower: &str) -> bool {
    if haystack.is_ascii() && needle_lower.is_ascii() {
        let n = needle_lower.as_bytes();
        let h = haystack.as_bytes();
        if n.is_empty() {
            return true;
        }
        return h.len() >= n.len()
            && h.windows(n.len())
                .any(|w| w.iter().zip(n).all(|(a, b)| a.to_ascii_lowercase() == *b));
    }
    haystack.to_lowercase().contains(needle_lower)
}

fn build_histogram(
    from_ms: Option<u64>,
    to_ms: Option<u64>,
    buckets_requested: usize,
    samples: &[(u64, JobState)],
) -> Option<JobHistogram> {
    if buckets_requested == 0 {
        return None;
    }
    let (mut min_ts, mut max_ts) = (u64::MAX, 0u64);
    for &(ts, _) in samples {
        min_ts = min_ts.min(ts);
        max_ts = max_ts.max(ts);
    }
    let (start_ms, raw_end_ms) = match (from_ms, to_ms) {
        (Some(f), Some(t)) => (f, t.max(f.saturating_add(1))),
        (Some(f), None) => {
            let upper = if samples.is_empty() { now_ms() } else { max_ts };
            (f, upper.max(f).saturating_add(1))
        }
        (None, Some(t)) => {
            if samples.is_empty() {
                return None;
            }
            (min_ts.min(t.saturating_sub(1)), t.max(1))
        }
        (None, None) => {
            if samples.is_empty() {
                return None;
            }
            (min_ts, max_ts.saturating_add(1))
        }
    };
    let num_buckets = buckets_requested.clamp(8, 96);
    let b = num_buckets as u128;
    let total_span = raw_end_ms.saturating_sub(start_ms).max(num_buckets as u64);
    let end_ms = start_ms.saturating_add(total_span);
    let span = u128::from(total_span);
    let bound = |i: usize| -> u64 { start_ms.saturating_add(((i as u128 * span) / b) as u64) };
    let mut buckets: Vec<JobHistogramBucket> = (0..num_buckets)
        .map(|i| JobHistogramBucket {
            start_ms: bound(i),
            end_ms: bound(i + 1),
            ..Default::default()
        })
        .collect();
    for &(ts, state) in samples {
        if ts < start_ms || ts >= end_ms {
            continue;
        }
        let rel = u128::from(ts - start_ms);
        let idx = ((((rel + 1) * b) - 1) / span) as usize;
        let bucket = &mut buckets[idx.min(num_buckets - 1)];
        match state {
            JobState::Queued | JobState::Running => bucket.active += 1,
            JobState::Succeeded => bucket.succeeded += 1,
            JobState::Failed => bucket.failed += 1,
            JobState::Cancelled => bucket.cancelled += 1,
        }
    }
    Some(JobHistogram {
        start_ms,
        end_ms,
        bucket_ms: (total_span / num_buckets as u64).max(1),
        buckets,
    })
}
