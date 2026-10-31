//! Bridge between the shared queue and each replica's local worker sessions.
use std::sync::{Arc, Weak};

use anyserve_proto::v2 as pb;
use serde_json::json;

use super::api::{
    ConflictPolicy, Durability, JobFilter, JobPage, JobSpec, JobState, SubmitOutcome,
};
use super::machine::ms;
use super::state::{FinishStatus, Phase, State, TerminalPlan};
use super::{Inner, Runtime, view_from_stored};
use crate::error::RuntimeError;
use crate::journal::AcceptRecord;
use crate::now_ms;
use crate::postgres::{JobSnapshot, ReadMode};

pub(super) const EPHEMERAL_PREFIX: &str = "ephemeral:";

#[cfg(test)]
mod tests;

pub(super) fn read_error(error: anyhow::Error) -> RuntimeError {
    error
        .downcast_ref::<RuntimeError>()
        .cloned()
        .unwrap_or_else(|| RuntimeError::unavailable(format!("PostgreSQL: {error:#}")))
}

pub(super) async fn heartbeat_loop(me: Weak<Inner>) {
    loop {
        let Some(inner) = me.upgrade() else {
            return;
        };
        let pg = Arc::clone(inner.postgres.as_ref().expect("PG"));
        if inner.lock().shutting_down {
            return;
        }
        drop(inner);
        tokio::time::sleep(pg.config.lease_duration / 3).await;
        if let Err(error) = pg.heartbeat().await {
            tracing::error!(%error, "replica lost PostgreSQL membership");
            return;
        }
    }
}

pub(super) async fn cluster_loop(me: Weak<Inner>) {
    loop {
        let Some(inner) = me.upgrade() else {
            return;
        };
        let pg = Arc::clone(inner.postgres.as_ref().expect("PG"));
        if inner.lock().shutting_down {
            return;
        }
        if !pg.healthy() {
            let mut st = inner.lock();
            let ids: Vec<_> = st
                .job_index
                .unfinished_ids()
                .filter_map(|id| st.jobs.get(id))
                .filter(|j| j.ownership_epoch.is_some() && !j.is_terminal())
                .map(|j| j.id.clone())
                .collect();
            for id in ids {
                inner.forget_owned_locked(&mut st, &id, "replica node lease lost");
            }
            return;
        }
        let busy =
            match tokio::time::timeout(std::time::Duration::from_secs(10), inner.cluster_tick())
                .await
            {
                Ok(Ok(busy)) => busy,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "PostgreSQL scheduling pass failed");
                    false
                }
                Err(_) => {
                    pg.fence();
                    tracing::error!("PostgreSQL scheduling timed out; replica fenced");
                    false
                }
            };
        let poll = pg.config.poll_interval;
        let wakeup = Arc::clone(&inner.cluster_wakeup);
        drop(inner);
        drop(pg);
        if busy {
            tokio::task::yield_now().await;
        } else {
            tokio::select! {
                _ = tokio::time::sleep(poll) => {},
                _ = wakeup.notified() => {},
            }
        }
    }
}

impl Inner {
    async fn cluster_tick(&self) -> anyhow::Result<bool> {
        let pg = self.postgres.as_ref().expect("PG");
        let (ids, queued) = {
            let st = self.lock();
            let owned = st
                .job_index
                .unfinished_ids()
                .filter_map(|id| st.jobs.get(id))
                .filter(|j| j.ownership_epoch.is_some() && !j.is_terminal());
            (
                owned.clone().map(|j| j.id.clone()).collect::<Vec<_>>(),
                owned
                    .filter(|j| matches!(j.phase, Phase::Queued))
                    .map(|j| (j.id.clone(), j.ownership_epoch.expect("owned")))
                    .collect::<Vec<_>>(),
            )
        };
        if !ids.is_empty() {
            let lost = pg.lost_jobs(&ids).await?;
            let mut st = self.lock();
            for id in lost {
                // The terminal callback can have finished since the query.
                // Do not discard its cached receipt or worker acknowledgement.
                if st
                    .jobs
                    .get(&id)
                    .is_some_and(|j| matches!(j.phase, Phase::Terminating(_) | Phase::Terminal(_)))
                {
                    continue;
                }
                self.forget_owned_locked(&mut st, &id, "job cancelled or ownership moved");
            }
        }
        // Capacity may have changed between claiming and dispatching. Return
        // unassigned work so a replica with an eligible worker can take it.
        // The SQL queued-state CAS races safely with an attempt-start commit.
        for (id, epoch) in queued {
            if pg.release(&id, epoch).await? {
                self.forget_owned_locked(
                    &mut self.lock(),
                    &id,
                    "returned unassigned job to shared queue",
                );
            }
        }
        let (workers, limit) = {
            let st = self.lock();
            let mut slots = 0usize;
            let workers: Vec<_> = st.sessions.values().filter(|s| s.free_slots() > 0).map(|s| {
                slots = slots.saturating_add(s.free_slots() as usize);
                let capacity: std::collections::BTreeMap<_,_> = s.total_capacity.iter()
                    .map(|(k,v)| (k, v.saturating_sub(s.reserved.get(k).copied().unwrap_or(0)))) .collect();
                json!({ "interfaces": s.interfaces, "attributes": s.attributes, "capacity": capacity })
            }).collect();
            let available = self
                .config
                .max_pending_jobs
                .saturating_sub(st.unfinished_jobs);
            // A small sweep also expires orphaned jobs when no worker is free.
            (
                json!(workers),
                available.min(if slots == 0 { 16 } else { slots }).min(256),
            )
        };
        let claimed = pg
            .claim(workers, limit, Arc::clone(&self.input_budget))
            .await?;
        let busy = !claimed.is_empty();
        for claimed in claimed {
            if !pg.healthy() {
                break;
            }
            let job = claimed.job;
            let id = job.job_id.clone();
            let mut st = self.lock();
            if st.shutting_down {
                break;
            }
            if st.jobs.contains_key(&id) {
                self.forget_owned_locked(&mut st, &id, "new ownership epoch");
            }
            self.insert_job(
                &mut st,
                id.clone(),
                job.spec,
                Durability::Durable,
                job.spec_hash,
                job.max_attempts,
                None,
                claimed.input_permit,
                job.created_at_ms,
            );
            let local = st.jobs.get_mut(&id).expect("claimed job");
            local.ownership_epoch = Some(claimed.epoch);
            local.attempt_count = job.attempt_count;
            local.last_attempt_id = job.last_attempt_id;
            local.last_worker_id = job.last_worker_id;
            self.accepted_locked(&mut st, &id, claimed.queued_at_ms);
            if let Some(error) = claimed.failure {
                self.finish_job_locked(
                    &mut st,
                    &id,
                    TerminalPlan::new(JobState::Failed, Some(error)),
                    now_ms(),
                );
            }
            self.dispatch_locked(&mut st, now_ms());
        }
        Ok(busy)
    }

    pub(crate) fn forget_owned_locked(&self, st: &mut State, id: &str, reason: &str) {
        self.dequeue_locked(st, id);
        let Some(mut job) = st.remove_job(id) else {
            return;
        };
        self.timers.cancel(&job.timers);
        if !job.is_terminal() {
            st.unfinished_jobs = st.unfinished_jobs.saturating_sub(1);
        }
        if matches!(job.phase, Phase::Accepting)
            && let Some(count) = st.accepting.get_mut(&job.spec.demand.interface)
        {
            *count = count.saturating_sub(1);
        }
        if let Some(attempt) = &job.attempt
            && st.leases.contains_key(&attempt.lease_id)
        {
            self.send_locked(
                st,
                &attempt.session_id,
                pb::server_message::Msg::Fence(pb::Fence {
                    lease_id: attempt.lease_id.clone(),
                    reason: reason.to_string(),
                }),
                now_ms(),
            );
            self.release_lease_locked(st, &attempt.lease_id);
        }
        for lease in &job.finished_leases {
            st.finished_leases.remove(lease);
        }
        for waiter in job.accept_waiters.drain(..).chain(job.waiters.drain(..)) {
            let _ = waiter.send(Err(RuntimeError::stale(reason)));
        }
        for waiter in job.finish_waiters.drain(..) {
            let _ = waiter.send(Ok(FinishStatus::Fenced(reason.to_string())));
        }
        st.released.jobs.push(job);
    }
}

impl Runtime {
    pub(super) fn generated_job_id(&self, durability: Durability) -> String {
        let id = super::machine::new_id();
        if self.inner.postgres.is_some() && durability == Durability::Ephemeral {
            format!("{EPHEMERAL_PREFIX}{id}")
        } else {
            id
        }
    }

    pub(super) fn is_postgres_job(&self, id: &str) -> bool {
        self.inner.postgres.is_some() && !id.starts_with(EPHEMERAL_PREFIX)
    }

    pub(super) async fn submit_postgres(
        &self,
        id: String,
        spec: JobSpec,
        hash: String,
        attempts: u32,
        conflict: ConflictPolicy,
    ) -> Result<SubmitOutcome, RuntimeError> {
        let admission = self.inner.admit(&self.inner.lock(), &spec);
        let permit = match admission {
            Ok(permit) => permit,
            Err(error) => {
                // Replaying an accepted request needs no new admission capacity.
                let job = match self.get_job(&id, false).await {
                    Ok(job) => job,
                    Err(read) if read.kind == crate::error::ErrorKind::NotFound => {
                        return Err(error);
                    }
                    Err(read) => return Err(read),
                };
                if conflict == ConflictPolicy::Fail || job.spec_hash != hash {
                    return Err(RuntimeError::conflict(format!("job '{id}' already exists")));
                }
                return Ok(SubmitOutcome {
                    job,
                    created: false,
                });
            }
        };
        let queue_timeout = spec
            .policy
            .queue_timeout
            .or_else(|| {
                self.inner
                    .config
                    .interfaces
                    .get(&spec.demand.interface)
                    .and_then(|p| p.queue_timeout)
            })
            .map(|d| ms(d).min(i64::MAX as u64) as i64);
        let accepted = self.inner.postgres.as_ref().expect("PG").accept(
            AcceptRecord {
                job_id: id.clone(),
                spec_hash: hash,
                spec: Arc::new(spec),
                max_attempts: attempts,
                created_at_ms: now_ms(),
            },
            conflict,
            queue_timeout,
        );
        let (job, created) = tokio::time::timeout(std::time::Duration::from_secs(10), accepted)
            .await
            .map_err(|_| {
                RuntimeError::unavailable(
                    "PostgreSQL submission timed out; retry with the same job_id",
                )
            })?
            .map_err(read_error)?;
        drop(permit);
        self.inner.cluster_wakeup.notify_one();
        Ok(SubmitOutcome {
            job: view_from_stored(job, false),
            created,
        })
    }

    pub(super) async fn postgres_snapshot(
        &self,
        id: &str,
        mode: ReadMode,
        after: Option<u64>,
    ) -> Result<JobSnapshot, RuntimeError> {
        self.inner
            .postgres
            .as_ref()
            .expect("PG")
            .snapshot(
                id,
                mode,
                after,
                self.result_budget(),
                Arc::clone(&self.inner.event_budget),
            )
            .await
            .map_err(read_error)?
            .ok_or_else(|| RuntimeError::not_found("job", id))
    }

    pub(super) async fn list_postgres_jobs(
        &self,
        filter: JobFilter,
    ) -> Result<JobPage, RuntimeError> {
        let limit = if filter.limit == 0 {
            100
        } else {
            filter.limit.min(1000)
        };
        let before: Option<(u64, String)> = filter
            .page_token
            .as_deref()
            .filter(|s| !s.is_empty() && *s != "0")
            .map(|s| {
                serde_json::from_str(s)
                    .map_err(|_| RuntimeError::invalid("invalid PostgreSQL page token"))
            })
            .transpose()?;
        if before.as_ref().is_some_and(|(at, id)| {
            *at > i64::MAX as u64
                || id.is_empty()
                || id.len() > self.inner.config.limits.max_identifier_bytes as usize
        }) {
            return Err(RuntimeError::invalid("invalid PostgreSQL page token"));
        }
        let mut jobs: Vec<_> = self
            .inner
            .postgres
            .as_ref()
            .expect("PG")
            .scan_jobs(
                0,
                i64::MAX as u64,
                before.clone(),
                &filter.states,
                filter.interface.as_deref(),
                limit + 1,
            )
            .await
            .map_err(read_error)?
            .into_iter()
            .map(|j| view_from_stored(j, false))
            .collect();
        jobs.extend(
            self.inner
                .lock()
                .jobs
                .values()
                .filter(|j| {
                    j.durability == Durability::Ephemeral
                        && (filter.states.is_empty() || filter.states.contains(&j.state()))
                        && filter
                            .interface
                            .as_ref()
                            .is_none_or(|i| *i == j.spec.demand.interface)
                        && before
                            .as_ref()
                            .is_none_or(|(at, id)| (j.created_at_ms, &j.id) < (*at, id))
                })
                .map(|j| j.view(false)),
        );
        jobs.sort_by(|a, b| (b.created_at_ms, &b.job_id).cmp(&(a.created_at_ms, &a.job_id)));
        let more = jobs.len() > limit;
        jobs.truncate(limit);
        let next_page_token = if more {
            jobs.last()
                .map(|j| serde_json::to_string(&(j.created_at_ms, &j.job_id)).expect("cursor"))
        } else {
            None
        };
        Ok(JobPage {
            jobs,
            next_page_token,
        })
    }
}
