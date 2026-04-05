use std::cmp::Reverse;

use crate::model::{Capacity, JobRecord, JobState, WorkerRecord};

pub trait Scheduler: Send + Sync {
    fn ordered_jobs_for_worker(&self, worker: &WorkerRecord, jobs: &[JobRecord]) -> Vec<JobRecord>;
}

#[derive(Clone, Debug, Default)]
pub struct BasicScheduler;

impl Scheduler for BasicScheduler {
    fn ordered_jobs_for_worker(&self, worker: &WorkerRecord, jobs: &[JobRecord]) -> Vec<JobRecord> {
        let mut candidates: Vec<JobRecord> = jobs
            .iter()
            .filter(|job| job.state == JobState::Pending)
            .filter(|job| worker.spec.interfaces.contains(&job.spec.interface_name))
            .filter(|job| attributes_match(worker, job))
            .filter(|job| {
                capacity_match(
                    &worker.status.available_capacity,
                    &job.spec.demand.required_capacity,
                )
            })
            .cloned()
            .collect();

        candidates.sort_by_key(|job| {
            let preferred_score = preferred_attribute_matches(worker, job);
            (
                Reverse(job.spec.policy.priority),
                Reverse(preferred_score),
                job.created_at_ms,
                job.job_id.clone(),
            )
        });

        candidates
    }
}

fn attributes_match(worker: &WorkerRecord, job: &JobRecord) -> bool {
    job.spec
        .demand
        .required_attributes
        .iter()
        .all(|(key, expected)| worker.spec.attributes.get(key) == Some(expected))
}

fn preferred_attribute_matches(worker: &WorkerRecord, job: &JobRecord) -> usize {
    job.spec
        .demand
        .preferred_attributes
        .iter()
        .filter(|(key, expected)| worker.spec.attributes.get(*key) == Some(*expected))
        .count()
}

fn capacity_match(available: &Capacity, required: &Capacity) -> bool {
    required
        .iter()
        .all(|(key, value)| available.get(key).copied().unwrap_or_default() >= *value)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{BasicScheduler, Scheduler};
    use crate::model::{
        Demand, ExecutionPolicy, JobRecord, JobSpec, JobState, WorkerRecord, WorkerSpec,
        WorkerStatus,
    };

    #[test]
    fn schedules_using_generic_attributes_and_capacity() {
        let worker = WorkerRecord {
            worker_id: "worker-1".to_string(),
            spec: WorkerSpec {
                interfaces: BTreeSet::from(["demo.execute.v1".to_string()]),
                attributes: BTreeMap::from([
                    ("runtime".to_string(), "demo".to_string()),
                    ("accelerator.vendor".to_string(), "nvidia".to_string()),
                ]),
                total_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                max_active_leases: 1,
                metadata: BTreeMap::new(),
            },
            status: WorkerStatus {
                available_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                active_leases: 0,
                metadata: BTreeMap::new(),
                last_seen_at_ms: 0,
            },
            registered_at_ms: 0,
            expires_at_ms: u64::MAX,
        };
        let job = JobRecord {
            job_id: "job-1".to_string(),
            state: JobState::Pending,
            spec: JobSpec {
                interface_name: "demo.execute.v1".to_string(),
                demand: Demand {
                    required_attributes: BTreeMap::from([(
                        "accelerator.vendor".to_string(),
                        "nvidia".to_string(),
                    )]),
                    preferred_attributes: BTreeMap::from([(
                        "runtime".to_string(),
                        "demo".to_string(),
                    )]),
                    required_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                },
                policy: ExecutionPolicy::default(),
                ..JobSpec::default()
            },
            created_at_ms: 1,
            updated_at_ms: 1,
            version: 1,
            ..JobRecord::default()
        };

        let selected = BasicScheduler.ordered_jobs_for_worker(&worker, std::slice::from_ref(&job));

        assert_eq!(selected, vec![job]);
    }
}
