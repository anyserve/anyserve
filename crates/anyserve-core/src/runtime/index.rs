//! Read indexes maintained alongside job transitions under the runtime lock.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};

use super::api::{InterfaceStats, JobFilter, JobState};
use super::state::Job;

type OrderKey = (Reverse<u64>, String);
type ByState = [BTreeSet<OrderKey>; 5];
type Position = (JobState, u64, u64);

const STATES: [JobState; 5] = [
    JobState::Queued,
    JobState::Running,
    JobState::Succeeded,
    JobState::Failed,
    JobState::Cancelled,
];

fn slot(state: JobState) -> usize {
    match state {
        JobState::Queued => 0,
        JobState::Running => 1,
        JobState::Succeeded => 2,
        JobState::Failed => 3,
        JobState::Cancelled => 4,
    }
}

#[derive(Default)]
struct InterfaceIndex {
    jobs: ByState,
    queued_since: BTreeSet<(u64, String)>,
}

#[derive(Default)]
struct NamespaceIndex {
    jobs: ByState,
    interfaces: BTreeMap<String, InterfaceIndex>,
    /// Keyed by `(created_at_ms, raw_job_id)` -> `scoped_job_id`.
    created: BTreeMap<(u64, String), String>,
}

#[derive(Default)]
pub(crate) struct JobIndex {
    namespaces: HashMap<String, NamespaceIndex>,
    counts: [usize; 5],
}

impl JobIndex {
    #[cfg(test)]
    pub(crate) fn unfinished_ids(&self) -> impl Iterator<Item = &str> + Clone {
        self.namespaces
            .values()
            .flat_map(|ns| ns.jobs[0].iter().chain(ns.jobs[1].iter()))
            .map(|(_, id)| id.as_str())
    }

    pub(crate) fn position(job: &Job) -> Position {
        (job.state(), job.updated_at_ms, job.queued_since_ms)
    }

    pub(crate) fn insert(&mut self, job: &Job) {
        self.namespaces
            .entry(job.namespace.clone())
            .or_default()
            .created
            .insert((job.created_at_ms, job.raw_id.clone()), job.id.clone());
        self.insert_state(job);
    }

    pub(crate) fn insert_state(&mut self, job: &Job) {
        let state = slot(job.state());
        let key = (Reverse(job.updated_at_ms), job.id.clone());
        self.counts[state] += 1;
        let ns = self.namespaces.entry(job.namespace.clone()).or_default();
        ns.jobs[state].insert(key.clone());
        let ns_iface = ns
            .interfaces
            .entry(job.spec.demand.interface.clone())
            .or_default();
        ns_iface.jobs[state].insert(key);
        if job.state() == JobState::Queued {
            ns_iface
                .queued_since
                .insert((job.queued_since_ms, job.id.clone()));
        }
    }

    pub(crate) fn remove(&mut self, job: &Job, prune: bool) {
        if let Some(ns) = self.namespaces.get_mut(&job.namespace) {
            ns.created.remove(&(job.created_at_ms, job.raw_id.clone()));
        }
        self.remove_position(job, Self::position(job), prune);
        if prune
            && let Some(ns) = self.namespaces.get(&job.namespace)
            && ns.jobs.iter().all(BTreeSet::is_empty)
            && ns.created.is_empty()
        {
            self.namespaces.remove(&job.namespace);
        }
    }

    pub(crate) fn scan(
        &self,
        namespace: Option<&str>,
        from_ms: u64,
        to_ms: u64,
        before: Option<(u64, String)>,
        limit: usize,
    ) -> Vec<&str> {
        use std::ops::Bound::{Excluded, Included};
        if limit == 0 || self.namespaces.is_empty() {
            return Vec::new();
        }
        let lower = (from_ms, String::new());
        let upper = before.unwrap_or((to_ms, String::new()));
        if upper <= lower {
            return Vec::new();
        }
        if let Some(ns) = namespace {
            return match self.namespaces.get(ns) {
                Some(index) => index
                    .created
                    .range((Included(lower), Excluded(upper)))
                    .rev()
                    .take(limit)
                    .map(|(_, scoped)| scoped.as_str())
                    .collect(),
                None => Vec::new(),
            };
        }
        if self.namespaces.len() == 1
            && let Some((ns_name, index)) = self.namespaces.iter().next()
            && ns_name == crate::auth::DEFAULT_NAMESPACE
        {
            return index
                .created
                .range((Included(lower), Excluded(upper)))
                .rev()
                .take(limit)
                .map(|(_, scoped)| scoped.as_str())
                .collect();
        }
        let range_upper = if upper.0 < to_ms {
            (upper.0.saturating_add(1), String::new())
        } else {
            (to_ms, String::new())
        };
        let mut items: Vec<(u64, &str)> = Vec::new();
        for index in self.namespaces.values() {
            let mut taken = 0usize;
            for ((ts, _raw), scoped) in index
                .created
                .range((Included(lower.clone()), Excluded(range_upper.clone())))
                .rev()
            {
                if (*ts, scoped.as_str()) >= (upper.0, upper.1.as_str()) {
                    continue;
                }
                items.push((*ts, scoped.as_str()));
                taken += 1;
                if taken >= limit && *ts < upper.0 {
                    break;
                }
            }
        }
        items.sort_by(|a, b| b.cmp(a));
        items.truncate(limit);
        items.into_iter().map(|(_, id)| id).collect()
    }

    pub(crate) fn remove_position(&mut self, job: &Job, position: Position, prune: bool) {
        let (previous_state, updated_at_ms, queued_since_ms) = position;
        let state = slot(previous_state);
        self.counts[state] = self.counts[state].saturating_sub(1);
        let key = (Reverse(updated_at_ms), job.id.clone());
        let name = &job.spec.demand.interface;
        if let Some(ns) = self.namespaces.get_mut(&job.namespace) {
            ns.jobs[state].remove(&key);
            if let Some(ns_iface) = ns.interfaces.get_mut(name) {
                ns_iface.jobs[state].remove(&key);
                if previous_state == JobState::Queued {
                    ns_iface
                        .queued_since
                        .remove(&(queued_since_ms, job.id.clone()));
                }
                if prune && ns_iface.jobs.iter().all(BTreeSet::is_empty) {
                    ns.interfaces.remove(name);
                }
            }
        }
    }

    /// Merge sorted state indexes across selected namespace/interface sets.
    pub(crate) fn page(
        &self,
        filter: &JobFilter,
        offset: usize,
        limit: usize,
    ) -> (Vec<&str>, usize) {
        let mut selected: Vec<&BTreeSet<OrderKey>> = Vec::new();
        match (&filter.namespace, &filter.interface) {
            (Some(ns), Some(name)) => {
                if let Some(interface) =
                    self.namespaces.get(ns).and_then(|i| i.interfaces.get(name))
                {
                    for (state, set) in STATES.iter().zip(&interface.jobs) {
                        if filter.states.is_empty() || filter.states.contains(state) {
                            selected.push(set);
                        }
                    }
                }
            }
            (Some(ns), None) => {
                if let Some(index) = self.namespaces.get(ns) {
                    for (state, set) in STATES.iter().zip(&index.jobs) {
                        if filter.states.is_empty() || filter.states.contains(state) {
                            selected.push(set);
                        }
                    }
                }
            }
            (None, Some(name)) => {
                for index in self.namespaces.values() {
                    if let Some(interface) = index.interfaces.get(name) {
                        for (state, set) in STATES.iter().zip(&interface.jobs) {
                            if filter.states.is_empty() || filter.states.contains(state) {
                                selected.push(set);
                            }
                        }
                    }
                }
            }
            (None, None) => {
                for index in self.namespaces.values() {
                    for (state, set) in STATES.iter().zip(&index.jobs) {
                        if filter.states.is_empty() || filter.states.contains(state) {
                            selected.push(set);
                        }
                    }
                }
            }
        }
        if selected.is_empty() {
            return (Vec::new(), 0);
        }
        let total = selected.iter().map(|jobs| jobs.len()).sum();
        if offset >= total || limit == 0 {
            return (Vec::new(), total);
        }
        if selected.len() == 1 {
            return (
                selected[0]
                    .iter()
                    .skip(offset)
                    .take(limit)
                    .map(|(_, id)| id.as_str())
                    .collect(),
                total,
            );
        }
        if selected.len() <= 8 {
            let mut cursors: Vec<_> = selected.iter().map(|jobs| jobs.iter().peekable()).collect();
            let ordered = std::iter::from_fn(|| {
                let next = cursors
                    .iter_mut()
                    .enumerate()
                    .filter_map(|(i, cursor)| cursor.peek().map(|key| (i, *key)))
                    .min_by(|(_, a), (_, b)| a.cmp(b))
                    .map(|(i, _)| i)?;
                cursors[next].next().map(|(_, id)| id.as_str())
            });
            return (ordered.skip(offset).take(limit).collect(), total);
        }
        let mut iters: Vec<_> = selected.iter().map(|jobs| jobs.iter()).collect();
        let mut heap: BinaryHeap<Reverse<(&OrderKey, usize)>> =
            BinaryHeap::with_capacity(iters.len());
        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some(key) = iter.next() {
                heap.push(Reverse((key, idx)));
            }
        }
        let take = limit.min(total - offset);
        let mut out = Vec::with_capacity(take);
        let mut skipped = 0usize;
        while out.len() < take {
            let Some(Reverse((key, idx))) = heap.pop() else {
                break;
            };
            if skipped < offset {
                skipped += 1;
            } else {
                out.push(key.1.as_str());
            }
            if let Some(next_key) = iters[idx].next() {
                heap.push(Reverse((next_key, idx)));
            }
        }
        (out, total)
    }

    pub(crate) fn counts(&self) -> [usize; 5] {
        self.counts
    }

    pub(crate) fn interfaces(&self, now: u64) -> BTreeMap<String, InterfaceStats> {
        let mut merged: BTreeMap<String, InterfaceStats> = BTreeMap::new();
        for ns in self.namespaces.values() {
            for (name, index) in &ns.interfaces {
                let oldest = index
                    .queued_since
                    .first()
                    .map(|(since, _)| now.saturating_sub(*since))
                    .unwrap_or(0);
                let entry = merged.entry(name.clone()).or_default();
                entry.queued += index.jobs[0].len();
                entry.running += index.jobs[1].len();
                entry.oldest_queued_age_ms = entry.oldest_queued_age_ms.max(oldest);
            }
        }
        merged
    }
}

#[cfg(test)]
pub(crate) fn assert_matches_jobs(st: &super::state::State) {
    assert_eq!(
        st.job_index.unfinished_ids().collect::<BTreeSet<_>>(),
        st.jobs
            .values()
            .filter(|job| !job.is_terminal())
            .map(|job| job.id.as_str())
            .collect()
    );
    assert_eq!(
        st.job_index
            .namespaces
            .values()
            .flat_map(|ns| ns.created.iter().map(|((ts, _), id)| (*ts, id.clone())))
            .collect::<BTreeSet<_>>(),
        st.jobs
            .values()
            .map(|job| (job.created_at_ms, job.id.clone()))
            .collect()
    );
    let mut expected_counts = [0; 5];
    let now = crate::now_ms();
    let mut expected_interfaces: BTreeMap<String, InterfaceStats> = BTreeMap::new();
    for job in st.jobs.values() {
        expected_counts[slot(job.state())] += 1;
        let entry = expected_interfaces
            .entry(job.spec.demand.interface.clone())
            .or_default();
        match job.state() {
            JobState::Queued => {
                entry.queued += 1;
                entry.oldest_queued_age_ms = entry
                    .oldest_queued_age_ms
                    .max(now.saturating_sub(job.queued_since_ms));
            }
            JobState::Running => entry.running += 1,
            _ => {}
        }
    }
    assert_eq!(st.job_index.counts(), expected_counts);
    assert_eq!(st.job_index.interfaces(now), expected_interfaces);
    let interfaces = [None, Some("missing".to_string())]
        .into_iter()
        .chain(expected_interfaces.keys().cloned().map(Some));
    for interface in interfaces {
        for states in [
            Vec::new(),
            STATES.to_vec(),
            vec![JobState::Queued],
            vec![JobState::Running, JobState::Cancelled, JobState::Running],
        ] {
            let filter = JobFilter {
                interface: interface.clone(),
                states,
                ..Default::default()
            };
            let mut expected: Vec<_> = st
                .jobs
                .values()
                .filter(|job| {
                    filter
                        .interface
                        .as_ref()
                        .is_none_or(|name| name == &job.spec.demand.interface)
                        && (filter.states.is_empty() || filter.states.contains(&job.state()))
                })
                .collect();
            expected.sort_by_key(|job| (Reverse(job.updated_at_ms), &job.id));
            for offset in [0, 1, 7, usize::MAX] {
                let (actual, total) = st.job_index.page(&filter, offset, 3);
                assert_eq!(total, expected.len());
                assert_eq!(
                    actual,
                    expected
                        .iter()
                        .skip(offset)
                        .take(3)
                        .map(|job| job.id.as_str())
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::Runtime;
    use super::super::api::{Durability, JobSpec, RuntimeConfig, SubmitRequest};
    use super::super::state::{Phase, TerminalPlan};
    use super::*;

    #[tokio::test]
    async fn indexes_follow_admission_cancellation_eviction_and_equal_timestamps() {
        let runtime = Runtime::start(RuntimeConfig {
            max_terminal_jobs: 3,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut ids = Vec::new();
        for i in 0..40 {
            let mut spec = JobSpec::default();
            spec.demand.interface = format!("interface-{}", i % 3);
            spec.policy.durability = Some(Durability::Ephemeral);
            let id = format!("job-{i:03}");
            runtime
                .submit(SubmitRequest {
                    job_id: Some(id.clone()),
                    spec,
                    ..Default::default()
                })
                .await
                .unwrap();
            // Ensure the secondary sort and oldest-queue tracking are exercised.
            runtime.inner.lock().update_job(&id, |job| {
                job.updated_at_ms = 5;
                job.queued_since_ms = i % 4;
            });
            ids.push(id);
        }
        assert_matches_jobs(&runtime.inner.lock());
        {
            let mut st = runtime.inner.lock();
            for previous in [
                Phase::Accepting,
                Phase::Queued,
                Phase::Starting,
                Phase::Assigned,
                Phase::Running,
            ] {
                st.update_job(&ids[0], |job| job.phase = previous.clone());
                assert_matches_jobs(&st);
                let mut plan = TerminalPlan::new(JobState::Succeeded, None);
                *plan.previous = previous;
                st.update_job(&ids[0], |job| {
                    job.phase = Phase::Terminating(Box::new(plan))
                });
                assert_matches_jobs(&st);
            }
            st.update_job(&ids[0], |job| job.phase = Phase::Queued);
        }
        for id in ids {
            runtime.cancel(&id, "test".into()).await.unwrap();
            assert_matches_jobs(&runtime.inner.lock());
        }
        assert_eq!(runtime.stats().cancelled, 3);
        assert_eq!(runtime.stats().queued, 0);
    }

    #[tokio::test]
    async fn multi_namespace_scan_and_many_namespace_paging_preserve_global_order() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let mut expected_scan = Vec::new();
        for ns_idx in 0..10 {
            let ns = format!("tenant-{ns_idx}");
            for j in 0..4 {
                let mut spec = JobSpec::default();
                spec.demand.interface = "demo".into();
                spec.policy.durability = Some(Durability::Ephemeral);
                let id = format!("job-{j}");
                runtime
                    .submit(SubmitRequest {
                        namespace: Some(ns.clone()),
                        job_id: Some(id.clone()),
                        spec,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                let ts = (ns_idx * 10 + j + 1) as u64;
                let scoped = super::super::identity::encode_scoped_id(&ns, &id);
                let mut st = runtime.inner.lock();
                let st = &mut *st;
                if let Some(job) = st.jobs.get_mut(&scoped) {
                    let pos = JobIndex::position(job);
                    st.job_index.remove_position(job, pos, false);
                    if let Some(ns_idx_map) = st.job_index.namespaces.get_mut(&ns) {
                        ns_idx_map
                            .created
                            .remove(&(job.created_at_ms, job.raw_id.clone()));
                        job.created_at_ms = ts;
                        job.updated_at_ms = ts * 2;
                        ns_idx_map
                            .created
                            .insert((job.created_at_ms, job.raw_id.clone()), job.id.clone());
                    }
                    st.job_index.insert_state(job);
                }
                expected_scan.push((ts, scoped));
            }
        }
        expected_scan.sort_by(|a, b| b.cmp(a));
        let st = runtime.inner.lock();
        // Global scan across 10 namespaces with small limit must not drop newer namespaces.
        let page1 = st.job_index.scan(None, 0, 1_000, None, 3);
        assert_eq!(
            page1,
            expected_scan[..3]
                .iter()
                .map(|(_, id)| id.as_str())
                .collect::<Vec<_>>()
        );
        let cursor = expected_scan[2].clone();
        let page2 = st.job_index.scan(None, 0, 1_000, Some(cursor), 5);
        assert_eq!(
            page2,
            expected_scan[3..8]
                .iter()
                .map(|(_, id)| id.as_str())
                .collect::<Vec<_>>()
        );
        // BinaryHeap merge (>8 selected sets) matches full sorted order across pages.
        let mut paged = Vec::new();
        for offset in (0..40).step_by(7) {
            let (ids, total) = st.job_index.page(&JobFilter::default(), offset, 7);
            assert_eq!(total, 40);
            paged.extend(ids.into_iter().map(str::to_string));
        }
        assert_eq!(
            paged,
            expected_scan
                .into_iter()
                .map(|(_, id)| id)
                .collect::<Vec<_>>()
        );
    }
}
