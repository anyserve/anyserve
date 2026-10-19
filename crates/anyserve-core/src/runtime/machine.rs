//! The single execution state machine. Every transition runs under the runtime
//! lock and performs only non-blocking side effects: sending to worker
//! sessions, appending to event logs, scheduling timers and queueing journal
//! commits. Commit results come back through continuations owned by the
//! runtime, never through the RPC that caused them.

use std::cmp::Reverse;
use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use anyserve_proto::v2 as pb;
use bytes::Bytes;
use uuid::Uuid;

use super::api::{
    Durability, ErrorInfo, EventKind, JobSpec, JobState, JournalHealth, Object, codes,
};
use super::events::{JobLog, Permit};
use super::input::InputLog;
use super::state::{Attempt, FinishStatus, Job, Lease, Phase, QueueKey, State, TerminalPlan};
use super::{Inner, JournalStatus, TimerKey};
use crate::error::RuntimeError;
use crate::journal::{AcceptRecord, AttemptStartRecord, CommitOutcome, JournalOp, TerminalRecord};

/// Jobs examined per interface on each dispatch pass.
const DISPATCH_SCAN: usize = 256;
/// Delay before retrying work after a journal rollback.
const ROLLBACK_RETRY_MS: u64 = 1_000;
/// Terminal jobs past their retention evicted per maintenance pass, so one
/// pass never holds the lock for a whole backlog.
const MAINTENANCE_EVICTIONS: usize = 1_024;

/// Milliseconds of `duration`, saturating instead of wrapping.
pub(crate) fn ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn new_id() -> String {
    Uuid::new_v4().to_string()
}

fn spec_payload_bytes(spec: &JobSpec) -> u64 {
    spec.inputs
        .iter()
        .map(|o| o.inline_len() as u64)
        .sum::<u64>()
        + spec.params.len() as u64
}

fn without_payload(spec: &JobSpec) -> JobSpec {
    JobSpec {
        demand: spec.demand.clone(),
        inputs: Vec::new(),
        params: Bytes::new(),
        policy: spec.policy.clone(),
        metadata: spec.metadata.clone(),
    }
}

pub(crate) fn object_to_proto(object: &Object) -> pb::Object {
    match object {
        Object::Inline { data, metadata } => pb::Object {
            body: Some(pb::object::Body::Inline(data.clone())),
            metadata: metadata.clone().into_iter().collect(),
        },
        Object::Uri { uri, metadata } => pb::Object {
            body: Some(pb::object::Body::Uri(uri.clone())),
            metadata: metadata.clone().into_iter().collect(),
        },
    }
}

impl Inner {
    pub(crate) fn journal_writable(&self) -> bool {
        matches!(
            *self.journal_status.lock().expect("journal status poisoned"),
            JournalStatus::Healthy
        )
    }

    pub(crate) fn journal_health(&self) -> JournalHealth {
        match &*self.journal_status.lock().expect("journal status poisoned") {
            JournalStatus::Disabled => JournalHealth::Disabled,
            JournalStatus::Healthy => JournalHealth::Healthy,
            JournalStatus::Degraded(reason) | JournalStatus::Failed(reason) => {
                JournalHealth::Unavailable(reason.clone())
            }
        }
    }

    fn record_commit(&self, outcome: &CommitOutcome) {
        let mut status = self.journal_status.lock().expect("journal status poisoned");
        match outcome {
            CommitOutcome::Committed => {
                if matches!(*status, JournalStatus::Degraded(_)) {
                    tracing::info!("journal writes succeed again");
                    *status = JournalStatus::Healthy;
                    // Durable jobs were held back from dispatch meanwhile.
                    self.timers.schedule(crate::now_ms(), TimerKey::Dispatch);
                }
            }
            CommitOutcome::RolledBack(reason) => {
                if !matches!(*status, JournalStatus::Failed(_)) {
                    tracing::warn!(reason = %reason, "journal commit rolled back");
                    *status = JournalStatus::Degraded(reason.clone());
                }
            }
            CommitOutcome::Unknown(reason) => {
                tracing::error!(
                    reason = %reason,
                    "journal commit outcome unknown; durable work stops until restart"
                );
                *status = JournalStatus::Failed(reason.clone());
            }
        }
    }

    /// Queues `op`; `then` runs with the outcome once the writer is done,
    /// independently of whoever triggered the operation.
    pub(crate) fn commit(
        &self,
        op: JournalOp,
        then: impl FnOnce(&Arc<Inner>, CommitOutcome) + Send + 'static,
    ) -> Result<(), RuntimeError> {
        let journal = self
            .journal
            .as_ref()
            .ok_or_else(|| RuntimeError::unavailable("durable execution requires a journal"))?;
        let receiver = journal.enqueue(op)?;
        let me = self.me.clone();
        tokio::spawn(async move {
            let outcome = receiver
                .await
                .unwrap_or_else(|_| CommitOutcome::Unknown("journal writer stopped".to_string()));
            if let Some(inner) = me.upgrade() {
                inner.record_commit(&outcome);
                then(&inner, outcome);
            }
        });
        Ok(())
    }

    pub(crate) fn new_log(&self, st: &State, job_id: &str) -> Arc<JobLog> {
        Arc::new(JobLog::new(
            job_id.to_string(),
            st.generation << 32,
            self.config.event_log_max_events,
            self.config.event_log_max_bytes,
            Arc::clone(&self.budget),
        ))
    }

    pub(crate) fn queue_timeout(&self, job: &Job) -> Option<Duration> {
        job.spec.policy.queue_timeout.or_else(|| {
            self.config
                .interfaces
                .get(&job.spec.demand.interface)
                .and_then(|policy| policy.queue_timeout)
        })
    }

    pub(crate) fn max_queue(&self, interface: &str) -> usize {
        self.config
            .interfaces
            .get(interface)
            .and_then(|policy| policy.max_queue)
            .unwrap_or(self.config.default_max_queue)
    }

    /// Admission checks shared by every new job. On success the job's input
    /// bytes are reserved from the input budget by the returned permit.
    pub(crate) fn admit(&self, st: &State, spec: &JobSpec) -> Result<Permit, RuntimeError> {
        if st.shutting_down {
            return Err(RuntimeError::unavailable("runtime is shutting down"));
        }
        if st.unfinished_jobs >= self.config.max_pending_jobs {
            return Err(RuntimeError::exhausted("unfinished job limit reached"));
        }
        let interface = &spec.demand.interface;
        let queued = st.queues.get(interface).map(|q| q.len()).unwrap_or(0)
            + st.accepting.get(interface).copied().unwrap_or(0);
        if queued >= self.max_queue(interface) {
            return Err(RuntimeError::exhausted(format!(
                "queue for interface '{interface}' is full"
            )));
        }
        let mut permit = Permit::new(Arc::clone(&self.input_budget));
        if !permit.grow(spec_payload_bytes(spec) as usize) {
            return Err(RuntimeError::exhausted("input budget is exhausted"));
        }
        Ok(permit)
    }

    /// Inserts a new job. Ephemeral jobs are queued at once; durable ones wait
    /// in `Accepting` for the caller to start their acceptance commit.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_job(
        &self,
        st: &mut State,
        job_id: String,
        spec: JobSpec,
        durability: Durability,
        spec_hash: String,
        max_attempts: u32,
        input: Option<Arc<InputLog>>,
        input_permit: Permit,
        now: u64,
    ) {
        st.unfinished_jobs += 1;
        *st.accepting
            .entry(spec.demand.interface.clone())
            .or_default() += 1;
        let log = self.new_log(st, &job_id);
        let job = Job {
            id: job_id.clone(),
            spec: Arc::new(spec),
            durability,
            spec_hash,
            max_attempts,
            phase: Phase::Accepting,
            created_at_ms: now,
            updated_at_ms: now,
            attempt_count: 0,
            attempt: None,
            last_attempt_id: None,
            last_worker_id: None,
            queue_key: None,
            queued_since_ms: now,
            not_before_ms: 0,
            rejected_by: HashSet::new(),
            cancel_requested: None,
            indeterminate: false,
            outputs: None,
            error: None,
            log,
            streamed: false,
            input,
            input_permit: Some(input_permit),
            output_bytes: 0,
            accept_waiters: Vec::new(),
            waiters: Vec::new(),
            finish_waiters: Vec::new(),
            finished_leases: Vec::new(),
        };
        st.jobs.insert(job_id.clone(), job);
        if durability == Durability::Ephemeral {
            self.accepted_locked(st, &job_id, now);
        }
    }

    /// Makes an accepted job schedulable.
    pub(crate) fn accepted_locked(&self, st: &mut State, job_id: &str, now: u64) {
        let Some(job) = st.jobs.get_mut(job_id) else {
            return;
        };
        if let Some(count) = st.accepting.get_mut(&job.spec.demand.interface) {
            *count = count.saturating_sub(1);
        }
        job.log.append(None, EventKind::Accepted, now);
        if let Some(total) = job.spec.policy.total_timeout {
            self.timers.schedule(
                job.created_at_ms.saturating_add(ms(total)),
                TimerKey::TotalDeadline {
                    job_id: job_id.to_string(),
                },
            );
        }
        self.enqueue_locked(st, job_id, now);
        if st.jobs[job_id].cancel_requested.is_some() {
            self.cancel_locked(st, job_id, now);
        }
    }

    pub(crate) fn begin_accept(&self, st: &mut State, job_id: &str) -> Result<(), RuntimeError> {
        let job = &st.jobs[job_id];
        let op = JournalOp::Accept(Box::new(AcceptRecord {
            job_id: job.id.clone(),
            spec_hash: job.spec_hash.clone(),
            spec: Arc::clone(&job.spec),
            max_attempts: job.max_attempts,
            created_at_ms: job.created_at_ms,
        }));
        let id = job_id.to_string();
        self.commit(op, move |inner, outcome| {
            inner.on_accept_committed(&id, outcome)
        })
    }

    fn on_accept_committed(&self, job_id: &str, outcome: CommitOutcome) {
        let mut st = self.lock();
        let now = crate::now_ms();
        let Some(job) = st.jobs.get(job_id) else {
            return;
        };
        if !matches!(job.phase, Phase::Accepting) {
            return;
        }
        match outcome {
            CommitOutcome::Committed => {
                self.accepted_locked(&mut st, job_id, now);
                let job = st.jobs.get_mut(job_id).expect("job exists");
                let view = job.view(false);
                for waiter in job.accept_waiters.drain(..) {
                    let _ = waiter.send(Ok(view.clone()));
                }
                self.dispatch_locked(&mut st, now);
            }
            CommitOutcome::RolledBack(reason) => {
                self.remove_unaccepted_locked(&mut st, job_id, &reason);
            }
            // It may be on disk: keep the id reserved and the job frozen.
            CommitOutcome::Unknown(reason) => self.freeze_locked(&mut st, job_id, &reason),
        }
    }

    pub(crate) fn freeze_locked(&self, st: &mut State, job_id: &str, reason: &str) {
        let Some(job) = st.jobs.get_mut(job_id) else {
            return;
        };
        job.indeterminate = true;
        // Hand the capacity back and stop the worker. The lease records stay,
        // marked cancelling, so a repeated Finish is told Unavailable instead
        // of waiting for a commit that will never be resolved.
        let mut leases: Vec<String> = job.attempt.iter().map(|a| a.lease_id.clone()).collect();
        if let Phase::Terminating(plan) = &job.phase {
            leases.extend(plan.release_lease.clone());
            leases.extend(plan.cancel_lease.as_ref().map(|(lease, _)| lease.clone()));
        }
        let now = crate::now_ms();
        for lease_id in leases {
            let Some(lease) = st.leases.get_mut(&lease_id) else {
                continue;
            };
            lease.cancelling = true;
            let session_id = lease.session_id.clone();
            if let Some(session) = st.sessions.get_mut(&session_id) {
                session.detach(&lease_id, &lease.reserved);
            }
            self.send_locked(
                st,
                &session_id,
                pb::server_message::Msg::Fence(pb::Fence {
                    lease_id,
                    reason: "commit_outcome_unknown".to_string(),
                }),
                now,
            );
        }
        let job = st.jobs.get_mut(job_id).expect("job");
        let error = RuntimeError::unavailable(format!(
            "outcome of a journal commit for job '{job_id}' is unknown until restart: {reason}"
        ));
        for waiter in job.accept_waiters.drain(..).chain(job.waiters.drain(..)) {
            let _ = waiter.send(Err(error.clone()));
        }
        for waiter in job.finish_waiters.drain(..) {
            let _ = waiter.send(Err(error.clone()));
        }
    }

    pub(crate) fn remove_unaccepted_locked(&self, st: &mut State, job_id: &str, reason: &str) {
        if let Some(mut job) = st.jobs.remove(job_id) {
            st.unfinished_jobs -= 1;
            if let Some(count) = st.accepting.get_mut(&job.spec.demand.interface) {
                *count = count.saturating_sub(1);
            }
            let error = RuntimeError::unavailable(format!("job was not persisted: {reason}"));
            for waiter in job.accept_waiters.drain(..).chain(job.waiters.drain(..)) {
                let _ = waiter.send(Err(error.clone()));
            }
        }
    }

    pub(crate) fn enqueue_locked(&self, st: &mut State, job_id: &str, now: u64) {
        st.queue_seq += 1;
        let seq = st.queue_seq;
        let priority_cap = self.config.priority_levels.saturating_sub(1);
        let Some(job) = st.jobs.get_mut(job_id) else {
            return;
        };
        job.phase = Phase::Queued;
        job.queued_since_ms = now;
        job.updated_at_ms = now;
        let key = QueueKey {
            priority: Reverse(job.spec.policy.priority.min(priority_cap)),
            seq,
            job_id: job_id.to_string(),
        };
        job.queue_key = Some(key.clone());
        let interface = job.spec.demand.interface.clone();
        let spillover = (!job.spec.demand.preferred_attributes.is_empty())
            .then_some(job.spec.policy.spillover_after)
            .flatten();
        let not_before = job.not_before_ms;
        let queue_timeout = self.queue_timeout(job);
        st.queues.entry(interface).or_default().insert(key);
        if let Some(timeout) = queue_timeout {
            self.timers.schedule(
                now.saturating_add(ms(timeout)),
                TimerKey::QueueDeadline {
                    job_id: job_id.to_string(),
                    since: now,
                },
            );
        }
        if let Some(spillover) = spillover {
            self.timers
                .schedule(now.saturating_add(ms(spillover)), TimerKey::Dispatch);
        }
        if not_before > now {
            self.timers.schedule(not_before, TimerKey::Dispatch);
        }
    }

    fn dequeue_locked(&self, st: &mut State, job_id: &str) {
        let Some(job) = st.jobs.get_mut(job_id) else {
            return;
        };
        if let Some(key) = job.queue_key.take()
            && let Some(queue) = st.queues.get_mut(&job.spec.demand.interface)
        {
            queue.remove(&key);
        }
    }

    // ---------------------------------------------------------------- dispatch

    /// Assigns queued jobs to sessions with free slots. Each pass looks at the
    /// head of every queue plus a window that rotates through the rest, so
    /// jobs behind many unmatchable ones are still reached; a pass that did
    /// not finish its rotation schedules the next one.
    pub(crate) fn dispatch_locked(&self, st: &mut State, now: u64) {
        if st.shutting_down || st.queues.values().all(|queue| queue.is_empty()) {
            return;
        }
        let durable_ok = self.journal_writable();
        let interfaces: Vec<String> = st
            .queues
            .iter()
            .filter(|(_, queue)| !queue.is_empty())
            .map(|(interface, _)| interface.clone())
            .collect();
        let mut rotation_pending = false;
        for interface in interfaces {
            if !st
                .sessions
                .values()
                .any(|s| s.free_slots() > 0 && s.interfaces.contains(&interface))
            {
                continue;
            }
            let queue = &st.queues[&interface];
            let mut keys: Vec<QueueKey> = queue.iter().take(DISPATCH_SCAN).cloned().collect();
            if queue.len() > DISPATCH_SCAN {
                let head_end = keys.last().cloned().expect("non-empty head");
                let start = st
                    .scan_cursor
                    .get(&interface)
                    .filter(|cursor| **cursor > head_end)
                    .cloned()
                    .unwrap_or(head_end);
                let window: Vec<QueueKey> = queue
                    .range((Bound::Excluded(start), Bound::Unbounded))
                    .take(DISPATCH_SCAN)
                    .cloned()
                    .collect();
                if window.len() == DISPATCH_SCAN {
                    st.scan_cursor
                        .insert(interface.clone(), window.last().cloned().expect("window"));
                    rotation_pending = true;
                } else {
                    st.scan_cursor.remove(&interface);
                }
                keys.extend(window);
            }
            for key in keys {
                if !st
                    .sessions
                    .values()
                    .any(|s| s.free_slots() > 0 && s.interfaces.contains(&interface))
                {
                    break;
                }
                let Some(job) = st.jobs.get(&key.job_id) else {
                    continue;
                };
                if job.not_before_ms > now || (job.durability == Durability::Durable && !durable_ok)
                {
                    continue;
                }
                let mut rng = st.rng;
                let picked = pick_session(st, job, now, &mut rng);
                st.rng = rng;
                if let Some(session_id) = picked {
                    self.begin_attempt_locked(st, &key.job_id, &session_id, now);
                }
            }
        }
        if rotation_pending {
            self.timers.schedule(now, super::TimerKey::Dispatch);
        }
    }

    fn begin_attempt_locked(&self, st: &mut State, job_id: &str, session_id: &str, now: u64) {
        self.dequeue_locked(st, job_id);
        let limit = self.config.limits.assign_inline_limit as u64;
        let reserved = st.jobs[job_id].spec.demand.required_capacity.clone();
        let session = st.sessions.get_mut(session_id).expect("picked session");
        let worker_id = session.worker_id.clone();
        let lease_id = new_id();
        let attempt_id = new_id();
        session.attach(lease_id.clone(), &reserved);
        session.assigned_since_report += 1;
        let job = st.jobs.get_mut(job_id).expect("queued job");
        job.attempt_count += 1;
        job.updated_at_ms = now;
        let inline_bytes = spec_payload_bytes(&job.spec)
            + job
                .spec
                .inputs
                .iter()
                .flat_map(|o| o.metadata())
                .map(|(k, v)| (k.len() + v.len()) as u64)
                .sum::<u64>();
        job.attempt = Some(Attempt {
            attempt_id: attempt_id.clone(),
            lease_id: lease_id.clone(),
            number: job.attempt_count,
            session_id: session_id.to_string(),
            worker_id: worker_id.clone(),
            started_at_ms: now,
            inputs_via_attempt: job.input.is_some() || inline_bytes > limit,
            last_frame_seq: 0,
            exposed: false,
        });
        job.last_attempt_id = Some(attempt_id.clone());
        job.last_worker_id = Some(worker_id.clone());
        let durable = job.durability == Durability::Durable;
        let number = job.attempt_count;
        st.leases.insert(
            lease_id.clone(),
            Lease {
                job_id: job_id.to_string(),
                session_id: session_id.to_string(),
                reserved,
                cancelling: false,
            },
        );
        if !durable {
            self.send_assign_locked(st, job_id, now);
            return;
        }
        st.jobs.get_mut(job_id).expect("job").phase = Phase::Starting;
        let op = JournalOp::AttemptStart(AttemptStartRecord {
            job_id: job_id.to_string(),
            attempt_id,
            lease_id: lease_id.clone(),
            attempt_number: number,
            worker_id,
            generation: st.generation,
            at_ms: now,
        });
        let (id, lease) = (job_id.to_string(), lease_id.clone());
        if let Err(error) = self.commit(op, move |inner, outcome| {
            inner.on_attempt_started(&id, &lease, outcome)
        }) {
            self.undo_start_locked(st, job_id, &lease_id, &error.to_string(), now);
        }
    }

    /// Reverts an attempt whose start was never committed.
    fn undo_start_locked(
        &self,
        st: &mut State,
        job_id: &str,
        lease_id: &str,
        reason: &str,
        now: u64,
    ) {
        tracing::warn!(job_id, reason, "attempt start was not persisted");
        // The assignment never happened: give back the slot it consumed.
        if let Some(session) = st
            .leases
            .get(lease_id)
            .and_then(|lease| st.sessions.get_mut(&lease.session_id))
        {
            session.assigned_since_report = session.assigned_since_report.saturating_sub(1);
        }
        self.release_lease_locked(st, lease_id);
        let job = st.jobs.get_mut(job_id).expect("job");
        job.attempt = None;
        job.attempt_count -= 1;
        job.not_before_ms = now.saturating_add(ROLLBACK_RETRY_MS);
        self.enqueue_locked(st, job_id, now);
        if st.jobs[job_id].cancel_requested.is_some() {
            self.cancel_locked(st, job_id, now);
        }
    }

    fn on_attempt_started(&self, job_id: &str, lease_id: &str, outcome: CommitOutcome) {
        let mut st = self.lock();
        let now = crate::now_ms();
        let current = st.jobs.get(job_id).is_some_and(|job| {
            matches!(job.phase, Phase::Starting)
                && job.attempt.as_ref().is_some_and(|a| a.lease_id == lease_id)
        });
        if !current {
            self.release_lease_locked(&mut st, lease_id);
            return;
        }
        match outcome {
            CommitOutcome::Committed => {
                if st.jobs[job_id].cancel_requested.is_some() {
                    self.cancel_locked(&mut st, job_id, now);
                } else if !st.leases.get(lease_id).is_some_and(|lease| {
                    // A session in its reconnect grace has no stream to
                    // carry the Assign; the worker cannot hold this lease.
                    st.sessions
                        .get(&lease.session_id)
                        .is_some_and(|session| session.tx.is_some())
                }) {
                    self.attempt_ended_locked(
                        &mut st,
                        job_id,
                        ErrorInfo::new(codes::WORKER_LOST, "worker session ended", true),
                        LeaseEnd::Release,
                        None,
                        now,
                    );
                } else {
                    self.send_assign_locked(&mut st, job_id, now);
                }
            }
            CommitOutcome::RolledBack(reason) => {
                self.undo_start_locked(&mut st, job_id, lease_id, &reason, now);
            }
            CommitOutcome::Unknown(reason) => {
                self.release_lease_locked(&mut st, lease_id);
                self.freeze_locked(&mut st, job_id, &reason);
            }
        }
        self.dispatch_locked(&mut st, now);
    }

    pub(crate) fn build_assign(&self, st: &State, job: &Job) -> pb::Assign {
        let attempt = job.attempt.as_ref().expect("assigned attempt");
        let spec = &job.spec;
        let via = attempt.inputs_via_attempt;
        pb::Assign {
            lease_id: attempt.lease_id.clone(),
            attempt_id: attempt.attempt_id.clone(),
            job_id: job.id.clone(),
            attempt_number: attempt.number,
            generation: st.generation,
            interface: spec.demand.interface.clone(),
            params: if via {
                Bytes::new()
            } else {
                spec.params.clone()
            },
            reserved_capacity: spec.demand.required_capacity.clone().into_iter().collect(),
            metadata: spec.metadata.clone().into_iter().collect(),
            inline_inputs: if via {
                Vec::new()
            } else {
                spec.inputs.iter().map(object_to_proto).collect()
            },
            inputs_via_attempt: via,
            streaming_input: job.input.is_some(),
            execution_deadline_ms: spec
                .policy
                .execution_timeout
                .map(|timeout| attempt.started_at_ms.saturating_add(ms(timeout)))
                .unwrap_or(0),
            total_deadline_ms: spec
                .policy
                .total_timeout
                .map(|timeout| job.created_at_ms.saturating_add(ms(timeout)))
                .unwrap_or(0),
        }
    }

    fn send_assign_locked(&self, st: &mut State, job_id: &str, now: u64) {
        let job = st.jobs.get_mut(job_id).expect("job");
        job.phase = Phase::Assigned;
        job.updated_at_ms = now;
        let job = &st.jobs[job_id];
        let attempt = job.attempt.as_ref().expect("attempt");
        let assign = self.build_assign(st, job);
        let session_id = attempt.session_id.clone();
        job.log.append(
            Some(attempt.attempt_id.clone()),
            EventKind::Assigned {
                worker_id: attempt.worker_id.clone(),
                attempt_number: attempt.number,
            },
            now,
        );
        if let Some(timeout) = job.spec.policy.execution_timeout {
            self.timers.schedule(
                now.saturating_add(ms(timeout)),
                TimerKey::ExecutionDeadline {
                    job_id: job_id.to_string(),
                    attempt_id: attempt.attempt_id.clone(),
                },
            );
        }
        self.send_locked(
            st,
            &session_id,
            pb::server_message::Msg::Assign(assign),
            now,
        );
    }

    /// Sends a control message to a session without waiting. A worker whose
    /// bounded control queue is full is not reading it: it is disconnected
    /// and its leases follow the reconnect-or-lost path.
    pub(crate) fn send_locked(
        &self,
        st: &mut State,
        session_id: &str,
        message: pb::server_message::Msg,
        now: u64,
    ) {
        let delivered = st
            .sessions
            .get(session_id)
            .is_none_or(|session| session.try_send(message));
        if !delivered {
            tracing::warn!(
                session_id,
                "worker control queue is full; disconnecting session"
            );
            self.disconnect_locked(st, session_id, now);
        }
    }

    /// Ends the session's current connection; its leases wait one heartbeat
    /// timeout for a reconnect.
    pub(crate) fn disconnect_locked(&self, st: &mut State, session_id: &str, now: u64) {
        let Some(session) = st.sessions.get_mut(session_id) else {
            return;
        };
        if session.tx.is_none() {
            return;
        }
        session.tx = None;
        session.disconnected_at_ms = Some(now);
        if session.active.is_empty() {
            st.sessions.remove(session_id);
            return;
        }
        let connection = session.connection;
        self.timers.schedule(
            now.saturating_add(ms(self.config.heartbeat_timeout)),
            TimerKey::SessionGrace {
                session_id: session_id.to_string(),
                connection,
            },
        );
    }

    // ------------------------------------------------------- attempt endings

    /// Ends the current attempt with `error`, then retries the job or fails it.
    /// `receipt` records the worker's reported outcome for its lease. Returns
    /// whether the job was queued for another attempt.
    pub(crate) fn attempt_ended_locked(
        &self,
        st: &mut State,
        job_id: &str,
        error: ErrorInfo,
        lease_end: LeaseEnd,
        receipt: Option<(String, String)>,
        now: u64,
    ) -> bool {
        let Some(job) = st.jobs.get(job_id) else {
            return false;
        };
        let Some(attempt) = job.attempt.as_ref() else {
            return false;
        };
        let lease_id = attempt.lease_id.clone();
        let exposed = attempt.exposed;
        let retry = error.retryable
            && !exposed
            && job.cancel_requested.is_none()
            && !st.shutting_down
            && job.attempt_count < job.max_attempts;
        if retry {
            let job = st.jobs.get_mut(job_id).expect("job");
            let attempt = job.attempt.take().expect("attempt");
            let backoff = self.backoff(job);
            job.not_before_ms = now.saturating_add(backoff);
            job.log.append(
                Some(attempt.attempt_id.clone()),
                EventKind::AttemptRestarted {
                    previous_attempt_id: attempt.attempt_id,
                    reason: error,
                },
                now,
            );
            if let Some((lease, digest)) = receipt {
                job.finished_leases.push(lease.clone());
                st.finished_leases
                    .insert(lease, (job_id.to_string(), digest));
            }
            self.end_lease_locked(st, &lease_id, lease_end, now);
            self.enqueue_locked(st, job_id, now);
            return true;
        }
        let error = if exposed && error.retryable {
            ErrorInfo::new(
                codes::OUTPUT_INTERRUPTED,
                format!(
                    "attempt failed after its output was delivered: {}",
                    error.message
                ),
                false,
            )
        } else if error.retryable && job.attempt_count >= job.max_attempts {
            ErrorInfo::new(
                codes::ATTEMPTS_EXHAUSTED,
                format!(
                    "{} ({}) after {} attempts",
                    error.message, error.code, job.attempt_count
                ),
                false,
            )
        } else {
            error
        };
        let (release_lease, cancel_lease) = match lease_end {
            LeaseEnd::Release => (Some(lease_id), None),
            LeaseEnd::Cancel(reason) => (None, Some((lease_id, reason))),
        };
        self.finish_job_locked(
            st,
            job_id,
            TerminalPlan {
                state: JobState::Failed,
                outputs: Vec::new(),
                error: Some(error),
                release_lease,
                cancel_lease,
                finished: receipt,
                previous: Box::new(Phase::Running),
            },
            now,
        );
        false
    }

    fn backoff(&self, job: &Job) -> u64 {
        let initial = ms(self.config.default_initial_backoff);
        let max = ms(self.config.default_max_backoff);
        let exponent = job.attempt_count.saturating_sub(1).min(16);
        initial.saturating_mul(1 << exponent).min(max)
    }

    fn end_lease_locked(&self, st: &mut State, lease_id: &str, lease_end: LeaseEnd, now: u64) {
        match lease_end {
            LeaseEnd::Release => self.release_lease_locked(st, lease_id),
            LeaseEnd::Cancel(reason) => self.cancel_lease_locked(st, lease_id, &reason, now),
        }
    }

    pub(crate) fn release_lease_locked(&self, st: &mut State, lease_id: &str) {
        if let Some(lease) = st.leases.remove(lease_id)
            && let Some(session) = st.sessions.get_mut(&lease.session_id)
        {
            session.detach(lease_id, &lease.reserved);
        }
    }

    /// Tells the worker to stop; the reservation is held until it acknowledges,
    /// finishes, or `cancel_grace` passes.
    pub(crate) fn cancel_lease_locked(
        &self,
        st: &mut State,
        lease_id: &str,
        reason: &str,
        now: u64,
    ) {
        let Some(lease) = st.leases.get_mut(lease_id) else {
            return;
        };
        lease.cancelling = true;
        let session_id = lease.session_id.clone();
        self.send_locked(
            st,
            &session_id,
            pb::server_message::Msg::Cancel(pb::Cancel {
                lease_id: lease_id.to_string(),
                reason: reason.to_string(),
            }),
            now,
        );
        self.timers.schedule(
            now.saturating_add(ms(self.config.cancel_grace)),
            TimerKey::CancelGrace {
                lease_id: lease_id.to_string(),
            },
        );
    }

    // --------------------------------------------------------- terminal path

    /// Moves a job to a terminal state: immediately when ephemeral, after the
    /// terminal commit when durable.
    pub(crate) fn finish_job_locked(
        &self,
        st: &mut State,
        job_id: &str,
        mut plan: TerminalPlan,
        now: u64,
    ) {
        let Some(job) = st.jobs.get(job_id) else {
            return;
        };
        if matches!(job.phase, Phase::Terminal(_) | Phase::Terminating(_)) {
            return;
        }
        plan.previous = Box::new(job.phase.clone());
        self.dequeue_locked(st, job_id);
        let job = st.jobs.get_mut(job_id).expect("job");
        if job.durability == Durability::Ephemeral {
            self.apply_terminal_locked(st, job_id, plan, now);
            return;
        }
        job.phase = Phase::Terminating(Box::new(plan));
        self.commit_terminal_locked(st, job_id, now);
    }

    /// Queues the terminal commit of a `Terminating` job. The outcome is
    /// final: a rollback retries the same plan rather than undoing it.
    fn commit_terminal_locked(&self, st: &mut State, job_id: &str, now: u64) {
        let job = &st.jobs[job_id];
        let Phase::Terminating(plan) = &job.phase else {
            return;
        };
        let attempt = job.attempt.as_ref();
        let op = JournalOp::Terminal(Box::new(TerminalRecord {
            job_id: job_id.to_string(),
            state: plan.state,
            outputs: plan.outputs.clone(),
            error: plan.error.clone(),
            attempt_id: attempt
                .map(|a| a.attempt_id.clone())
                .or_else(|| job.last_attempt_id.clone()),
            worker_id: attempt
                .map(|a| a.worker_id.clone())
                .or_else(|| job.last_worker_id.clone()),
            winning_lease_id: plan.finished.as_ref().map(|(lease, _)| lease.clone()),
            outcome_digest: plan.finished.as_ref().map(|(_, digest)| digest.clone()),
            at_ms: now,
        }));
        let id = job_id.to_string();
        if let Err(error) = self.commit(op, move |inner, outcome| {
            inner.on_terminal_committed(&id, outcome)
        }) {
            self.terminal_rolled_back_locked(st, job_id, &error.to_string(), now);
        }
    }

    fn on_terminal_committed(&self, job_id: &str, outcome: CommitOutcome) {
        let mut st = self.lock();
        let now = crate::now_ms();
        let Some(job) = st.jobs.get_mut(job_id) else {
            return;
        };
        if !matches!(job.phase, Phase::Terminating(_)) {
            return;
        }
        match outcome {
            CommitOutcome::Committed => {
                let Phase::Terminating(plan) = std::mem::replace(&mut job.phase, Phase::Queued)
                else {
                    unreachable!()
                };
                self.apply_terminal_locked(&mut st, job_id, *plan, now);
            }
            CommitOutcome::RolledBack(reason) => {
                self.terminal_rolled_back_locked(&mut st, job_id, &reason, now);
            }
            // The outcome may be on disk: stay frozen until a restart recovers it.
            CommitOutcome::Unknown(reason) => self.freeze_locked(&mut st, job_id, &reason),
        }
    }

    /// The terminal commit did not happen. Callers waiting on it are told to
    /// retry; the job stays `Terminating` and the same plan is committed again
    /// after `ROLLBACK_RETRY_MS`.
    fn terminal_rolled_back_locked(&self, st: &mut State, job_id: &str, reason: &str, now: u64) {
        let job = st.jobs.get_mut(job_id).expect("job");
        let error =
            RuntimeError::unavailable(format!("terminal state was not persisted yet: {reason}"));
        for waiter in job.waiters.drain(..) {
            let _ = waiter.send(Err(error.clone()));
        }
        for waiter in job.finish_waiters.drain(..) {
            let _ = waiter.send(Err(error.clone()));
        }
        self.timers.schedule(
            now.saturating_add(ROLLBACK_RETRY_MS),
            TimerKey::RetryTerminal {
                job_id: job_id.to_string(),
            },
        );
    }

    /// Timer: commit a `Terminating` job's plan again.
    pub(crate) fn retry_terminal_locked(&self, st: &mut State, job_id: &str, now: u64) {
        if st
            .jobs
            .get(job_id)
            .is_some_and(|job| matches!(job.phase, Phase::Terminating(_)) && !job.indeterminate)
        {
            self.commit_terminal_locked(st, job_id, now);
        }
    }

    fn apply_terminal_locked(&self, st: &mut State, job_id: &str, plan: TerminalPlan, now: u64) {
        self.dequeue_locked(st, job_id);
        let job = st.jobs.get_mut(job_id).expect("job");
        job.phase = Phase::Terminal(plan.state);
        job.updated_at_ms = now;
        job.error = plan.error.clone();
        let outputs = if plan.state == JobState::Succeeded {
            plan.outputs
        } else {
            Vec::new()
        };
        let output_bytes = outputs.iter().map(|o| o.inline_len() as u64).sum::<u64>();
        job.outputs = (plan.state == JobState::Succeeded).then(|| Arc::new(outputs.clone()));
        if let Some(attempt) = job.attempt.take() {
            job.last_attempt_id = Some(attempt.attempt_id);
            job.last_worker_id = Some(attempt.worker_id);
        }
        job.spec = Arc::new(without_payload(&job.spec));
        if let Some(input) = job.input.take() {
            input.abort();
        }
        job.log.append(
            job.last_attempt_id.clone(),
            EventKind::Finished {
                state: plan.state,
                outputs,
                error: plan.error,
            },
            now,
        );
        job.log.set_primary(false);
        job.output_bytes = output_bytes;
        let view = job.view(false);
        for waiter in job.waiters.drain(..) {
            let _ = waiter.send(Ok(view.clone()));
        }
        for waiter in job.finish_waiters.drain(..) {
            let _ = waiter.send(Ok(FinishStatus::Committed));
        }
        job.input_permit = None;
        st.unfinished_jobs -= 1;
        st.terminal_order.push_back((now, job_id.to_string()));
        if let Some((lease_id, digest)) = plan.finished {
            st.finished_leases
                .insert(lease_id.clone(), (job_id.to_string(), digest));
            st.jobs
                .get_mut(job_id)
                .expect("job")
                .finished_leases
                .push(lease_id);
        }
        if let Some(lease_id) = plan.release_lease {
            self.release_lease_locked(st, &lease_id);
        }
        if let Some((lease_id, reason)) = plan.cancel_lease {
            self.cancel_lease_locked(st, &lease_id, &reason, now);
        }
        self.dispatch_locked(st, now);
        // Keep the count bound as jobs finish, one eviction each, instead of
        // leaving a second's worth to the maintenance pass.
        while st.terminal_order.len() > self.config.max_terminal_jobs {
            if self.evict_oldest_terminal_locked(st).is_none() {
                break;
            }
        }
    }

    /// Cancels a job on behalf of a caller. Ephemeral jobs end at once; durable
    /// ones once the terminal commit succeeds. Deferred while a commit for the
    /// job is in flight.
    pub(crate) fn cancel_locked(&self, st: &mut State, job_id: &str, now: u64) {
        let Some(job) = st.jobs.get(job_id) else {
            return;
        };
        let reason = job
            .cancel_requested
            .clone()
            .unwrap_or_else(|| "cancelled".to_string());
        let error = Some(ErrorInfo::new(codes::CANCELLED, reason.clone(), false));
        let plan = |release_lease, cancel_lease| TerminalPlan {
            state: JobState::Cancelled,
            outputs: Vec::new(),
            error: error.clone(),
            release_lease,
            cancel_lease,
            finished: None,
            previous: Box::new(Phase::Queued),
        };
        match &job.phase {
            Phase::Queued => self.finish_job_locked(st, job_id, plan(None, None), now),
            Phase::Assigned | Phase::Running => {
                let lease_id = job.attempt.as_ref().expect("attempt").lease_id.clone();
                self.finish_job_locked(st, job_id, plan(None, Some((lease_id, reason))), now);
            }
            Phase::Starting => {
                // The start committed but the worker was not told yet.
                let lease_id = job.attempt.as_ref().expect("attempt").lease_id.clone();
                self.finish_job_locked(st, job_id, plan(Some(lease_id), None), now);
            }
            Phase::Accepting | Phase::Terminating(_) | Phase::Terminal(_) => {}
        }
    }

    // ------------------------------------------------------------ retention

    /// Maintenance: drops terminal jobs past their retention, at most
    /// `MAINTENANCE_EVICTIONS` per pass. `max_terminal_jobs` is enforced as
    /// jobs finish.
    pub(crate) fn evict_terminal_locked(&self, st: &mut State, now: u64) {
        let retention = ms(self.config.terminal_retention);
        for _ in 0..MAINTENANCE_EVICTIONS {
            match st.terminal_order.front() {
                Some((finished_at, _)) if now.saturating_sub(*finished_at) >= retention => {
                    self.evict_oldest_terminal_locked(st);
                }
                _ => break,
            }
        }
    }

    /// Removes the oldest terminal job from memory; returns the result and
    /// event bytes its references held. Durable jobs, their receipts
    /// included, stay readable from the journal.
    fn evict_oldest_terminal_locked(&self, st: &mut State) -> Option<(u64, u64)> {
        loop {
            let (_, job_id) = st.terminal_order.pop_front()?;
            if !st.jobs.get(&job_id).is_some_and(|job| job.is_terminal()) {
                continue;
            }
            let job = st.jobs.remove(&job_id).expect("job");
            for lease in &job.finished_leases {
                st.finished_leases.remove(lease);
            }
            return Some((job.output_bytes, job.log.bytes() as u64));
        }
    }

    /// Evicts the oldest terminal jobs until they held `needed` bytes of the
    /// result budget (`results`) or of the event budget, or none are left.
    /// Only this runtime's own references go; copies held elsewhere keep
    /// their bytes until dropped.
    pub(crate) fn reclaim_locked(&self, st: &mut State, needed: u64, results: bool) {
        let mut freed = 0u64;
        while freed < needed {
            let Some((output_bytes, event_bytes)) = self.evict_oldest_terminal_locked(st) else {
                return;
            };
            freed += if results { output_bytes } else { event_bytes };
        }
    }
}

pub(crate) enum LeaseEnd {
    Release,
    Cancel(String),
}

fn load_score(session: &super::state::Session) -> f64 {
    let concurrency = session.max_concurrency.max(1) as f64;
    session.active.len() as f64 / concurrency
        + session.load.utilization * 0.5
        + session.load.queued as f64 / concurrency * 0.25
}

fn next_random(rng: &mut u64) -> u64 {
    // xorshift64*
    let mut x = if *rng == 0 {
        0x9E37_79B9_7F4A_7C15
    } else {
        *rng
    };
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *rng = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// Picks a session for `job`: eligible sessions with the most preferred
/// attributes, then the less loaded of two random ones.
fn pick_session(st: &State, job: &Job, now: u64, rng: &mut u64) -> Option<String> {
    let demand = &job.spec.demand;
    let spill_open = match job.spec.policy.spillover_after {
        Some(after) if !demand.preferred_attributes.is_empty() => {
            now >= job.queued_since_ms.saturating_add(ms(after))
        }
        _ => true,
    };
    // Sessions that rejected this job are used only when no other fits; the
    // retry backoff already spaced the new attempt out.
    let mut best: Vec<&super::state::Session> = Vec::new();
    let mut best_rank = (false, 0usize);
    for session in st.sessions.values() {
        if session.free_slots() == 0
            || !session.interfaces.contains(&demand.interface)
            || !demand
                .required_attributes
                .iter()
                .all(|(key, value)| session.attributes.get(key) == Some(value))
        {
            continue;
        }
        let matches = demand
            .preferred_attributes
            .iter()
            .filter(|(key, value)| session.attributes.get(*key) == Some(value))
            .count();
        if !spill_open && matches < demand.preferred_attributes.len() {
            continue;
        }
        if !session.fits(&demand.required_capacity) {
            continue;
        }
        let rank = (!job.rejected_by.contains(&session.id), matches);
        if best.is_empty() || rank > best_rank {
            best_rank = rank;
            best.clear();
        }
        if rank == best_rank {
            best.push(session);
        }
    }
    match best.len() {
        0 => None,
        1 => Some(best[0].id.clone()),
        n => {
            let first = (next_random(rng) % n as u64) as usize;
            let mut second = (next_random(rng) % (n as u64 - 1)) as usize;
            if second >= first {
                second += 1;
            }
            let (a, b) = (best[first], best[second]);
            Some(if load_score(b) < load_score(a) { b } else { a }.id.clone())
        }
    }
}
