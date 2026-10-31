//! Read indexes maintained alongside job transitions under the runtime lock.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

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
pub(crate) struct JobIndex {
    jobs: ByState,
    interfaces: BTreeMap<String, InterfaceIndex>,
}

impl JobIndex {
    pub(crate) fn position(job: &Job) -> Position {
        (job.state(), job.updated_at_ms, job.queued_since_ms)
    }

    pub(crate) fn insert(&mut self, job: &Job) {
        let state = slot(job.state());
        let key = (Reverse(job.updated_at_ms), job.id.clone());
        self.jobs[state].insert(key.clone());
        let interface = self
            .interfaces
            .entry(job.spec.demand.interface.clone())
            .or_default();
        interface.jobs[state].insert(key);
        if job.state() == JobState::Queued {
            interface
                .queued_since
                .insert((job.queued_since_ms, job.id.clone()));
        }
    }

    pub(crate) fn remove(&mut self, job: &Job, prune: bool) {
        self.remove_position(job, Self::position(job), prune);
    }

    pub(crate) fn remove_position(&mut self, job: &Job, position: Position, prune: bool) {
        let (previous_state, updated_at_ms, queued_since_ms) = position;
        let state = slot(previous_state);
        let key = (Reverse(updated_at_ms), job.id.clone());
        self.jobs[state].remove(&key);
        let name = &job.spec.demand.interface;
        if let Some(interface) = self.interfaces.get_mut(name) {
            interface.jobs[state].remove(&key);
            if previous_state == JobState::Queued {
                interface
                    .queued_since
                    .remove(&(queued_since_ms, job.id.clone()));
            }
            if prune && interface.jobs.iter().all(BTreeSet::is_empty) {
                self.interfaces.remove(name);
            }
        }
    }

    /// Merge at most five already sorted state indexes. Only the requested
    /// page's views need cloning; duplicate state filters do not duplicate jobs.
    pub(crate) fn page(
        &self,
        filter: &JobFilter,
        offset: usize,
        limit: usize,
    ) -> (Vec<&str>, usize) {
        let jobs = match &filter.interface {
            Some(name) => match self.interfaces.get(name) {
                Some(interface) => &interface.jobs,
                None => return (Vec::new(), 0),
            },
            None => &self.jobs,
        };
        let selected: Vec<_> = STATES
            .iter()
            .zip(jobs)
            .filter(|(state, _)| filter.states.is_empty() || filter.states.contains(state))
            .map(|(_, jobs)| jobs)
            .collect();
        let total = selected.iter().map(|jobs| jobs.len()).sum();
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
        (ordered.skip(offset).take(limit).collect(), total)
    }

    pub(crate) fn counts(&self) -> [usize; 5] {
        std::array::from_fn(|i| self.jobs[i].len())
    }

    pub(crate) fn interfaces(&self, now: u64) -> BTreeMap<String, InterfaceStats> {
        self.interfaces
            .iter()
            .map(|(name, index)| {
                (
                    name.clone(),
                    InterfaceStats {
                        queued: index.jobs[0].len(),
                        running: index.jobs[1].len(),
                        oldest_queued_age_ms: index
                            .queued_since
                            .first()
                            .map(|(since, _)| now.saturating_sub(*since))
                            .unwrap_or(0),
                        ..Default::default()
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
pub(crate) fn assert_matches_jobs(st: &super::state::State) {
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
        for id in ids {
            runtime.cancel(&id, "test".into()).await.unwrap();
            assert_matches_jobs(&runtime.inner.lock());
        }
        assert_eq!(runtime.stats().cancelled, 3);
        assert_eq!(runtime.stats().queued, 0);
    }
}
