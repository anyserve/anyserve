//! The single execution state machine. Every transition runs under the runtime
//! lock and performs only non-blocking side effects: sending to worker
//! sessions, appending to event logs, scheduling timers and queueing journal
//! commits. Commit results come back through continuations owned by the
//! runtime, never through the RPC that caused them.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyserve_proto::v2 as pb;
use bytes::Bytes;
use uuid::Uuid;

use super::api::{
    Durability, ErrorInfo, EventKind, InterfacePolicy, JobSpec, JobState, JournalHealth, Object,
    PoolTier, codes,
};
use super::events::{JobLog, Permit};
use super::identity::decode_scoped_id;
use super::input::InputLog;
use super::state::{
    Attempt, FinishStatus, Job, JobWaiter, Lease, Phase, QueueKey, Session, State, TerminalPlan,
};
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

fn assign_inline_bytes(spec: &JobSpec) -> u64 {
    spec_payload_bytes(spec)
        + spec
            .inputs
            .iter()
            .flat_map(|o| o.metadata())
            .map(|(k, v)| (k.len() + v.len()) as u64)
            .sum::<u64>()
}

pub(crate) fn to_proto_map<V: Clone>(map: &BTreeMap<String, V>) -> HashMap<String, V> {
    map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
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

fn notify_job_waiters(waiters: &mut Vec<JobWaiter>, job: &Job) {
    if waiters.is_empty() {
        return;
    }
    if waiters.len() == 1 {
        let _ = waiters.pop().expect("non-empty").send(Ok(job.view(false)));
        return;
    }
    let view = job.view(false);
    for waiter in waiters.drain(..) {
        let _ = waiter.send(Ok(view.clone()));
    }
}

pub(crate) fn object_to_proto(object: &Object) -> pb::Object {
    match object {
        Object::Inline { data, metadata } => pb::Object {
            body: Some(pb::object::Body::Inline(data.clone())),
            metadata: to_proto_map(metadata),
        },
        Object::Uri { uri, metadata } => pb::Object {
            body: Some(pb::object::Body::Uri(uri.clone())),
            metadata: to_proto_map(metadata),
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
        if let CommitOutcome::Fenced(reason) = outcome {
            self.mark_lease_lost(reason);
            return;
        }
        let mut status = self.journal_status.lock().expect("journal status poisoned");
        match outcome {
            CommitOutcome::Fenced(_) => unreachable!(),
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
        let term = self.generation.load(std::sync::atomic::Ordering::Acquire);
        let receiver = journal.enqueue(term, op)?;
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
            Arc::clone(&self.event_budget),
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

    pub(crate) fn max_in_flight_per_key(&self, interface: &str) -> Option<usize> {
        self.config
            .interfaces
            .get(interface)
            .and_then(|policy| policy.max_in_flight_per_key)
            .or(self.config.default_max_in_flight_per_key)
    }

    pub(crate) fn session_reconnect_grace(&self, session: &super::state::Session) -> Duration {
        let mut override_grace: Option<Duration> = None;
        for interface in &session.interfaces {
            if let Some(policy) = self.config.interfaces.get(interface)
                && let Some(grace) = policy
                    .pool_tiers
                    .iter()
                    .find(|tier| tier.pool == session.pool)
                    .and_then(|tier| tier.reconnect_grace)
            {
                override_grace = Some(match override_grace {
                    Some(prev) => prev.min(grace),
                    None => grace,
                });
            }
        }
        override_grace
            .or(self.config.reconnect_grace)
            .unwrap_or(self.config.heartbeat_timeout)
    }

    pub(crate) fn release_attempt_concurrency_locked(&self, st: &mut State, job_id: &str) {
        let Some(job) = st.jobs.get_mut(job_id) else {
            return;
        };
        if let Some(attempt) = job.attempt.as_mut()
            && std::mem::replace(&mut attempt.concurrency_counted, false)
        {
            State::dec_in(
                &mut st.in_flight_concurrency,
                &job.namespace,
                &job.spec.policy.concurrency_key,
            );
        }
    }

    /// Admission checks shared by every new job. On success the job's input
    /// bytes are reserved from the input budget by the returned permit.
    pub(crate) fn admit(
        &self,
        st: &State,
        namespace: &str,
        spec: &JobSpec,
    ) -> Result<Permit, RuntimeError> {
        if st.shutting_down {
            return Err(RuntimeError::unavailable("runtime is shutting down"));
        }
        if st.unfinished_jobs >= self.config.max_pending_jobs {
            return Err(RuntimeError::exhausted("unfinished job limit reached"));
        }
        let quotas = self.config.tenant_quotas.resolve_for(namespace);
        if let Some(ns_pending_limit) = quotas.max_pending_jobs_per_namespace
            && st.job_index.namespace_pending_count(namespace) >= ns_pending_limit
        {
            return Err(RuntimeError::exhausted(format!(
                "unfinished job limit reached for namespace '{namespace}'"
            )));
        }
        if let (Some(prefix), Some(prefix_pending_limit)) =
            (quotas.matched_prefix, quotas.max_total_pending_jobs)
            && st.job_index.prefix_pending_count(prefix) >= prefix_pending_limit
        {
            return Err(RuntimeError::exhausted(format!(
                "unfinished job limit reached for namespace prefix '{prefix}'"
            )));
        }
        let interface = &spec.demand.interface;
        if let Some(ns_iface_limit) = quotas.max_interfaces_per_namespace {
            let already_tracked = st.queue(namespace, interface).is_some()
                || st.accepting_count(namespace, interface) > 0;
            if !already_tracked {
                let queued_ifaces = st.queues.get(namespace);
                let mut distinct = queued_ifaces.map_or(0, HashMap::len);
                if let Some(accepting_ifaces) = st.accepting.get(namespace) {
                    for iface in accepting_ifaces.keys() {
                        if queued_ifaces.is_none_or(|q| !q.contains_key(iface)) {
                            distinct += 1;
                        }
                    }
                }
                if distinct >= ns_iface_limit {
                    return Err(RuntimeError::exhausted(format!(
                        "queued interface limit reached for namespace '{namespace}'"
                    )));
                }
            }
        }
        let queued = st.queue(namespace, interface).map(|q| q.len()).unwrap_or(0)
            + st.accepting_count(namespace, interface);
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
        let (namespace, raw_id) = decode_scoped_id(&job_id);
        let namespace = namespace.to_string();
        let raw_id = raw_id.to_string();
        st.unfinished_jobs += 1;
        if durability == Durability::Durable {
            st.inc_accepting(&namespace, &spec.demand.interface);
        }
        let log = self.new_log(st, &raw_id);
        let job = Job {
            id: job_id.clone(),
            raw_id,
            namespace,
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
            timers: Vec::new(),
            total_timer: None,
            rejected_sessions: HashSet::new(),
            failed_workers: BTreeSet::new(),
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
        st.insert_job(job);
        if durability == Durability::Ephemeral {
            st.counters.jobs_submitted += 1;
            self.accepted_locked(st, &job_id, now);
        }
    }

    /// Makes an accepted job schedulable.
    pub(crate) fn accepted_locked(&self, st: &mut State, job_id: &str, now: u64) {
        {
            let Some(job) = st.jobs.get_mut(job_id) else {
                return;
            };
            if job.durability == Durability::Durable {
                State::dec_in(
                    &mut st.accepting,
                    &job.namespace,
                    &job.spec.demand.interface,
                );
            }
            job.log.append(None, EventKind::Accepted, now);
            if let Some(tc) = job
                .spec
                .metadata
                .get("traceparent")
                .and_then(|v| crate::trace::parse_traceparent(v))
            {
                tracing::debug!(
                    job_id = %job.raw_id,
                    namespace = %job.namespace,
                    interface = %job.spec.demand.interface,
                    trace_id = %tc.trace_id,
                    parent_span_id = %tc.parent_id,
                    sampled = tc.sampled,
                    "job accepted"
                );
            }
            if let Some(total) = job.spec.policy.total_timeout {
                let timer = self.timers.schedule(
                    job.created_at_ms.saturating_add(ms(total)),
                    TimerKey::TotalDeadline {
                        job_id: job_id.to_string(),
                    },
                );
                job.total_timer = Some(timer);
            }
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
            CommitOutcome::Fenced(reason) | CommitOutcome::RolledBack(reason) => {
                self.remove_unaccepted_locked(&mut st, job_id, &reason);
            }
            CommitOutcome::Committed => {
                st.counters.jobs_submitted += 1;
                self.accepted_locked(&mut st, job_id, now);
                let (ns, iface, mut waiters) = {
                    let job = st.jobs.get_mut(job_id).expect("job exists");
                    (
                        job.namespace.clone(),
                        job.spec.demand.interface.clone(),
                        std::mem::take(&mut job.accept_waiters),
                    )
                };
                notify_job_waiters(&mut waiters, &st.jobs[job_id]);
                self.dispatch_pair_locked(&mut st, &ns, &iface, now);
            }
            // It may be on disk: keep the id reserved and the job frozen.
            CommitOutcome::Unknown(reason) => self.freeze_locked(&mut st, job_id, &reason),
        }
    }

    pub(crate) fn freeze_locked(&self, st: &mut State, job_id: &str, reason: &str) {
        self.release_attempt_concurrency_locked(st, job_id);
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
                crate::now_ms(),
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
        if let Some(mut job) = st.remove_job(job_id) {
            self.timers.cancel(&std::mem::take(&mut job.timers));
            if let Some(timer) = job.total_timer.take() {
                self.timers.cancel(&[timer]);
            }
            st.unfinished_jobs -= 1;
            if job.durability == Durability::Durable {
                State::dec_in(
                    &mut st.accepting,
                    &job.namespace,
                    &job.spec.demand.interface,
                );
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
        st.update_job(job_id, |job| {
            job.phase = Phase::Queued;
            job.queued_since_ms = now;
            job.updated_at_ms = now;
        });
        let Some(job) = st.jobs.get_mut(job_id) else {
            return;
        };
        if !job.timers.is_empty() {
            self.timers.cancel(&std::mem::take(&mut job.timers));
        }
        let priority = job.spec.policy.priority.min(priority_cap);
        let key = QueueKey {
            priority: Reverse(priority),
            seq,
            job_id: job_id.to_string(),
        };
        job.queue_key = Some(key.clone());
        let namespace = job.namespace.clone();
        let interface = job.spec.demand.interface.clone();
        let fairness_key = job.spec.policy.fairness_key.clone();
        let spillover = (!job.spec.demand.preferred_attributes.is_empty())
            .then_some(job.spec.policy.spillover_after)
            .flatten();
        let has_tier_spillovers = self
            .config
            .interfaces
            .get(&interface)
            .is_some_and(|policy| {
                policy
                    .pool_tiers
                    .iter()
                    .any(|tier| !tier.spillover_after.is_zero())
            });
        let not_before = job.not_before_ms;
        if let Some(timeout) = self.queue_timeout(job) {
            let timer = self.timers.schedule(
                now.saturating_add(ms(timeout)),
                TimerKey::QueueDeadline {
                    job_id: job_id.to_string(),
                    since: now,
                },
            );
            job.timers.push(timer);
        }
        if has_tier_spillovers && let Some(policy) = self.config.interfaces.get(&interface) {
            for tier in &policy.pool_tiers {
                if !tier.spillover_after.is_zero() {
                    let timer = self.timers.schedule(
                        now.saturating_add(ms(tier.spillover_after)),
                        TimerKey::Dispatch,
                    );
                    job.timers.push(timer);
                }
            }
        }
        if let Some(spillover) = spillover {
            let timer = self
                .timers
                .schedule(now.saturating_add(ms(spillover)), TimerKey::Dispatch);
            job.timers.push(timer);
        }
        if not_before > now {
            let timer = self.timers.schedule(not_before, TimerKey::Dispatch);
            job.timers.push(timer);
        }
        st.queue_or_insert(&namespace, &interface).insert(
            self.config.priority_levels as usize,
            priority,
            &fairness_key,
            key,
        );
    }

    pub(crate) fn dequeue_locked(&self, st: &mut State, job_id: &str) {
        let Some(job) = st.jobs.get_mut(job_id) else {
            return;
        };
        let Some(key) = job.queue_key.take() else {
            return;
        };
        let priority_cap = self.config.priority_levels.saturating_sub(1);
        let priority = job.spec.policy.priority.min(priority_cap);
        let namespace = job.namespace.as_str();
        let interface = job.spec.demand.interface.as_str();
        // Field access (not `State::queue_mut`) keeps `st.queues` disjoint from the `st.jobs` borrow.
        let Some(queue) = st
            .queues
            .get_mut(namespace)
            .and_then(|m| m.get_mut(interface))
        else {
            return;
        };
        queue.remove(priority, &job.spec.policy.fairness_key, &key);
        if queue.is_empty() {
            State::remove_nested(&mut st.queues, namespace, interface);
            State::remove_nested(&mut st.scan_cursor, namespace, interface);
        }
    }

    // ---------------------------------------------------------------- dispatch

    /// Assigns queued jobs across all `(namespace, interface)` queues with free worker slots.
    pub(crate) fn dispatch_locked(&self, st: &mut State, now: u64) {
        if st.shutting_down || st.job_index.counts()[0] == 0 {
            return;
        }
        let mut queue_keys: Vec<(String, String)> = Vec::new();
        for (ns, ifaces) in &st.queues {
            for (iface, queue) in ifaces {
                if !queue.is_empty()
                    && st.interface_sessions(ns, iface).is_some_and(|ids| {
                        ids.iter()
                            .any(|id| st.sessions.get(id).is_some_and(|s| s.free_slots() > 0))
                    })
                {
                    queue_keys.push((ns.clone(), iface.clone()));
                }
            }
        }
        let mut rotation_pending = false;
        let mut durable_ok = None;
        for (namespace, interface) in queue_keys {
            rotation_pending |=
                self.dispatch_pair_inner_locked(st, &namespace, &interface, now, &mut durable_ok);
        }
        if rotation_pending {
            self.timers.schedule(now, super::TimerKey::Dispatch);
        }
    }

    /// Assigns queued jobs for a single `(namespace, interface)` without scanning unrelated tenants.
    pub(crate) fn dispatch_pair_locked(
        &self,
        st: &mut State,
        namespace: &str,
        interface: &str,
        now: u64,
    ) {
        if st.shutting_down {
            return;
        }
        if self.dispatch_pair_inner_locked(st, namespace, interface, now, &mut None) {
            self.timers.schedule(now, super::TimerKey::Dispatch);
        }
    }

    /// `durable_ok` memoizes `journal_writable()` across the pairs of one dispatch pass.
    fn dispatch_pair_inner_locked(
        &self,
        st: &mut State,
        namespace: &str,
        interface: &str,
        now: u64,
        durable_ok: &mut Option<bool>,
    ) -> bool {
        let Some(queue_len) = st
            .queue(namespace, interface)
            .filter(|q| !q.is_empty())
            .map(|q| q.len())
        else {
            return false;
        };
        let mut candidates: Vec<String> = st
            .interface_sessions(namespace, interface)
            .into_iter()
            .flatten()
            .filter(|id| st.sessions.get(*id).is_some_and(|s| s.free_slots() > 0))
            .cloned()
            .collect();
        if candidates.is_empty() {
            return false;
        }
        let mut rotation_pending = false;
        let queue = &st.queues[namespace][interface];
        let mut keys = queue.collect_candidates(DISPATCH_SCAN);
        if queue_len > DISPATCH_SCAN {
            let cursor = st.scan_cursor.get(namespace).and_then(|m| m.get(interface));
            let (mut window, next_cursor) = queue.collect_window(cursor, DISPATCH_SCAN);
            if let Some(next_cursor) = next_cursor {
                st.scan_cursor
                    .entry(namespace.to_string())
                    .or_default()
                    .insert(interface.to_string(), next_cursor);
                rotation_pending = true;
            } else {
                State::remove_nested(&mut st.scan_cursor, namespace, interface);
            }
            {
                let seen: HashSet<&str> = keys.iter().map(|k| k.job_id.as_str()).collect();
                window.retain(|k| !seen.contains(k.job_id.as_str()));
            }
            keys.extend(window);
        } else {
            State::remove_nested(&mut st.scan_cursor, namespace, interface);
        }
        let priority_cap = self.config.priority_levels.saturating_sub(1);
        let policy = self.config.interfaces.get(interface);
        for key in keys {
            let Some(job) = st.jobs.get(&key.job_id) else {
                continue;
            };
            if job.not_before_ms > now
                || (job.durability == Durability::Durable
                    && !*durable_ok.get_or_insert_with(|| self.journal_writable()))
            {
                continue;
            }
            let ckey = &job.spec.policy.concurrency_key;
            if !ckey.is_empty() {
                let cap = self.max_in_flight_per_key(interface).unwrap_or(1);
                if st.in_flight_key_count(namespace, ckey) >= cap {
                    continue;
                }
            }
            let mut rng = st.rng;
            let picked = pick_session(st, job, policy, &mut candidates, now, &mut rng);
            st.rng = rng;
            if let Some(session_id) = picked {
                let prio = job.spec.policy.priority.min(priority_cap);
                let fairness_key = job.spec.policy.fairness_key.clone();
                self.begin_attempt_locked(st, &key.job_id, &session_id, now);
                if let Some(queue) = st.queue_mut(namespace, interface) {
                    queue.rotate_key_to_back(prio, &fairness_key);
                }
                if st
                    .sessions
                    .get(&session_id)
                    .is_none_or(|s| s.free_slots() == 0)
                    && let Some(pos) = candidates.iter().position(|id| id == &session_id)
                {
                    candidates.swap_remove(pos);
                }
            }
            if candidates.is_empty() {
                break;
            }
        }
        rotation_pending
    }

    /// Direct zero-queue ephemeral dispatch path: when no queued backlog exists for
    /// `(namespace, interface)` and a ready worker matches immediately, transitions
    /// straight to `Phase::Assigned` and inserts into `JobIndex` once (avoiding
    /// `InterfaceQueue` + `QueueKey` + redundant `BTreeSet` churn).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn invoke_ephemeral_direct_locked(
        &self,
        st: &mut State,
        namespace: &str,
        raw_id: String,
        scoped_id: String,
        spec: &mut Option<JobSpec>,
        max_attempts: u32,
        input: Option<Arc<InputLog>>,
        input_permit: &mut Option<Permit>,
        now: u64,
    ) -> Option<(Arc<JobLog>, tokio::sync::watch::Receiver<u64>)> {
        let spec_ref = spec.as_ref().expect("spec");
        if st.shutting_down
            || st
                .queue(namespace, &spec_ref.demand.interface)
                .is_some_and(|q| !q.is_empty())
        {
            return None;
        }
        if !spec_ref.policy.concurrency_key.is_empty() {
            let cap = self
                .max_in_flight_per_key(&spec_ref.demand.interface)
                .unwrap_or(1);
            if st.in_flight_key_count(namespace, &spec_ref.policy.concurrency_key) >= cap {
                return None;
            }
        }
        let policy = self.config.interfaces.get(&spec_ref.demand.interface);
        let mut rng = st.rng;
        let picked = pick_direct_session(st, namespace, spec_ref, policy, now, &mut rng);
        st.rng = rng;
        let session_id = picked?;

        let spec = spec.take().expect("spec");
        let input_permit = input_permit.take().expect("permit");
        let ckey = &spec.policy.concurrency_key;

        st.unfinished_jobs += 1;
        let limit = self.config.limits.assign_inline_limit as u64;
        let lease_id = new_id();
        let attempt_id = new_id();
        let session = st.sessions.get_mut(&session_id).expect("picked session");
        let worker_id = session.worker_id.clone();
        session.attach(lease_id.clone(), &spec.demand.required_capacity);
        session.assigned_since_report += 1;
        let concurrency_counted = if !ckey.is_empty() {
            st.inc_in_flight_key(namespace, ckey);
            true
        } else {
            false
        };
        let log = self.new_log(st, &raw_id);
        log.set_primary(true);
        let changes = log.subscribe();
        log.append(None, EventKind::Accepted, now);
        log.append(
            Some(attempt_id.clone()),
            EventKind::Assigned {
                worker_id: worker_id.clone(),
                attempt_number: 1,
            },
            now,
        );
        let total_timer = spec.policy.total_timeout.map(|total| {
            self.timers.schedule(
                now.saturating_add(ms(total)),
                TimerKey::TotalDeadline {
                    job_id: scoped_id.clone(),
                },
            )
        });
        let mut timers = Vec::new();
        if let Some(exec_timeout) = spec.policy.execution_timeout {
            timers.push(self.timers.schedule(
                now.saturating_add(ms(exec_timeout)),
                TimerKey::ExecutionDeadline {
                    job_id: scoped_id.clone(),
                    attempt_id: attempt_id.clone(),
                },
            ));
        }
        let inline_bytes = assign_inline_bytes(&spec);
        let reserved = spec.demand.required_capacity.clone();
        let job = Job {
            id: scoped_id.clone(),
            raw_id,
            namespace: namespace.to_string(),
            spec: Arc::new(spec),
            durability: Durability::Ephemeral,
            spec_hash: String::new(),
            max_attempts,
            phase: Phase::Assigned,
            created_at_ms: now,
            updated_at_ms: now,
            attempt_count: 1,
            attempt: Some(Attempt {
                attempt_id: attempt_id.clone(),
                lease_id: lease_id.clone(),
                number: 1,
                session_id: session_id.clone(),
                worker_id: worker_id.clone(),
                started_at_ms: now,
                inputs_via_attempt: input.is_some() || inline_bytes > limit,
                last_frame_seq: 0,
                exposed: false,
                concurrency_counted,
            }),
            last_attempt_id: Some(attempt_id),
            last_worker_id: Some(worker_id),
            queue_key: None,
            queued_since_ms: now,
            not_before_ms: 0,
            timers,
            total_timer,
            rejected_sessions: HashSet::new(),
            failed_workers: BTreeSet::new(),
            cancel_requested: None,
            indeterminate: false,
            outputs: None,
            error: None,
            log: Arc::clone(&log),
            streamed: true,
            input,
            input_permit: Some(input_permit),
            output_bytes: 0,
            accept_waiters: Vec::new(),
            waiters: Vec::new(),
            finish_waiters: Vec::new(),
            finished_leases: Vec::new(),
        };
        let assign = build_assign(st.generation, &job);
        if let Some(tc) = job
            .spec
            .metadata
            .get("traceparent")
            .and_then(|v| crate::trace::parse_traceparent(v))
        {
            tracing::debug!(
                job_id = %job.raw_id,
                namespace = %job.namespace,
                attempt_id = %assign.attempt_id,
                worker_id = %job.last_worker_id.as_deref().unwrap_or(""),
                trace_id = %tc.trace_id,
                parent_span_id = %tc.parent_id,
                sampled = tc.sampled,
                "ephemeral invoke dispatched"
            );
        }
        st.leases.insert(
            lease_id,
            Lease {
                job_id: scoped_id,
                session_id: session_id.clone(),
                reserved,
                cancelling: false,
            },
        );
        st.insert_job(job);
        st.counters.jobs_submitted += 1;
        st.counters.attempts_started += 1;
        self.send_locked(
            st,
            &session_id,
            pb::server_message::Msg::Assign(assign),
            now,
        );
        Some((log, changes))
    }

    fn begin_attempt_locked(&self, st: &mut State, job_id: &str, session_id: &str, now: u64) {
        self.dequeue_locked(st, job_id);
        let limit = self.config.limits.assign_inline_limit as u64;
        let (reserved, ckey, ns) = {
            let job = st.jobs.get_mut(job_id).expect("queued job");
            if !job.timers.is_empty() {
                self.timers.cancel(&std::mem::take(&mut job.timers));
            }
            (
                job.spec.demand.required_capacity.clone(),
                job.spec.policy.concurrency_key.clone(),
                job.namespace.clone(),
            )
        };
        let session = st.sessions.get_mut(session_id).expect("picked session");
        let worker_id = session.worker_id.clone();
        let lease_id = new_id();
        let attempt_id = new_id();
        session.attach(lease_id.clone(), &reserved);
        session.assigned_since_report += 1;
        let concurrency_counted = if !ckey.is_empty() {
            st.inc_in_flight_key(&ns, &ckey);
            true
        } else {
            false
        };
        let job = st.jobs.get_mut(job_id).expect("queued job");
        job.attempt_count += 1;
        let inline_bytes = assign_inline_bytes(&job.spec);
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
            concurrency_counted,
        });
        job.last_attempt_id = Some(attempt_id);
        job.last_worker_id = Some(worker_id);
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
            st.counters.attempts_started += 1;
            self.send_assign_locked(st, job_id, now);
            return;
        }
        st.update_job(job_id, |job| {
            job.phase = Phase::Starting;
            job.updated_at_ms = now;
        });
        let job = &st.jobs[job_id];
        let op = JournalOp::AttemptStart(AttemptStartRecord {
            job_id: job_id.to_string(),
            attempt_number: number,
            at_ms: now,
            attempt_id: job.last_attempt_id.clone(),
            worker_id: job.last_worker_id.clone(),
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
        self.release_attempt_concurrency_locked(st, job_id);
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
            CommitOutcome::Fenced(_) => {
                self.release_attempt_concurrency_locked(&mut st, job_id);
                self.release_lease_locked(&mut st, lease_id);
            }
            CommitOutcome::Committed => {
                st.counters.attempts_started += 1;
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

    fn send_assign_locked(&self, st: &mut State, job_id: &str, now: u64) {
        let generation = st.generation;
        st.update_job(job_id, |job| {
            job.phase = Phase::Assigned;
            job.updated_at_ms = now;
        });
        let job = st.jobs.get_mut(job_id).expect("job");
        let assign = build_assign(generation, job);
        let attempt = job.attempt.as_ref().expect("attempt");
        let session_id = attempt.session_id.clone();
        let attempt_id = attempt.attempt_id.clone();
        job.log.append(
            Some(attempt_id.clone()),
            EventKind::Assigned {
                worker_id: attempt.worker_id.clone(),
                attempt_number: attempt.number,
            },
            now,
        );
        if let Some(tc) = job
            .spec
            .metadata
            .get("traceparent")
            .and_then(|v| crate::trace::parse_traceparent(v))
        {
            tracing::debug!(
                job_id = %job.raw_id,
                namespace = %job.namespace,
                attempt_id = %attempt_id,
                worker_id = %attempt.worker_id,
                attempt_number = attempt.number,
                trace_id = %tc.trace_id,
                parent_span_id = %tc.parent_id,
                sampled = tc.sampled,
                "attempt assigned"
            );
        }
        if let Some(timeout) = job.spec.policy.execution_timeout {
            let timer = self.timers.schedule(
                now.saturating_add(ms(timeout)),
                TimerKey::ExecutionDeadline {
                    job_id: job_id.to_string(),
                    attempt_id,
                },
            );
            job.timers.push(timer);
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
}

fn build_assign(generation: u64, job: &Job) -> pb::Assign {
    let attempt = job.attempt.as_ref().expect("assigned attempt");
    let spec = &job.spec;
    let via = attempt.inputs_via_attempt;
    pb::Assign {
        lease_id: attempt.lease_id.clone(),
        attempt_id: attempt.attempt_id.clone(),
        job_id: job.raw_id.clone(),
        attempt_number: attempt.number,
        generation,
        interface: spec.demand.interface.clone(),
        params: if via {
            Bytes::new()
        } else {
            spec.params.clone()
        },
        reserved_capacity: to_proto_map(&spec.demand.required_capacity),
        metadata: to_proto_map(&spec.metadata),
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

impl Inner {
    /// Drops the control stream, immediately fences and releases every active
    /// lease without entering `reconnect_grace`, and removes the session.
    pub(crate) fn revoke_session_locked(&self, st: &mut State, session_id: &str, now: u64) {
        self.evict_session_locked(st, session_id, "worker session revoked or expired", now);
    }

    pub(crate) fn evict_session_locked(
        &self,
        st: &mut State,
        session_id: &str,
        reason: &str,
        now: u64,
    ) {
        let Some(session) = st.sessions.get_mut(session_id) else {
            return;
        };
        session.tx = None;
        let leases: Vec<String> = session.active.iter().cloned().collect();
        for lease_id in leases {
            let Some(lease) = st.leases.get(&lease_id) else {
                continue;
            };
            if lease.cancelling {
                self.release_lease_locked(st, &lease_id);
                continue;
            }
            let job_id = lease.job_id.clone();
            let current = st.jobs.get(&job_id).is_some_and(|job| {
                job.attempt.as_ref().is_some_and(|a| a.lease_id == lease_id)
                    && matches!(job.phase, Phase::Assigned | Phase::Running)
            });
            if current {
                self.attempt_ended_locked(
                    st,
                    &job_id,
                    ErrorInfo::new(codes::WORKER_LOST, reason, true),
                    LeaseEnd::Release,
                    None,
                    now,
                );
            }
            // Guarantee immediate lease removal from `st.leases` even if a durable
            // job transitioned to `Phase::Terminating` or `Phase::Starting`, so
            // in-flight `push_frame`/`finish_attempt` are fenced immediately.
            self.release_lease_locked(st, &lease_id);
        }
        st.remove_session(session_id);
    }

    /// Ends the session's current connection; its leases wait `reconnect_grace`
    /// (falling back to `heartbeat_timeout`) for a reconnect.
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
            st.remove_session(session_id);
            return;
        }
        let grace = self.session_reconnect_grace(session);
        let connection = session.connection;
        if grace.is_zero() {
            self.session_expired_locked(st, session_id, connection, now);
            return;
        }
        self.timers.schedule(
            now.saturating_add(ms(grace)),
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
        let (lease_id, exposed, attempt_session_id, attempt_worker_id, retry) = {
            let Some(job) = st.jobs.get(job_id) else {
                return false;
            };
            let Some(attempt) = job.attempt.as_ref() else {
                return false;
            };
            let retry = error.retryable
                && !attempt.exposed
                && job.cancel_requested.is_none()
                && !st.shutting_down
                && job.attempt_count < job.max_attempts;
            (
                attempt.lease_id.clone(),
                attempt.exposed,
                attempt.session_id.clone(),
                attempt.worker_id.clone(),
                retry,
            )
        };
        self.release_attempt_concurrency_locked(st, job_id);
        {
            let job = st.jobs.get_mut(job_id).expect("job");
            if !job.timers.is_empty() {
                self.timers.cancel(&std::mem::take(&mut job.timers));
            }
            job.failed_workers.insert(attempt_worker_id);
        }
        if matches!(
            error.code.as_str(),
            codes::WORKER_FAILED
                | codes::WORKER_REJECTED
                | codes::EXECUTION_TIMEOUT
                | codes::WORKER_LOST
        ) && let Some(session) = st.sessions.get_mut(&attempt_session_id)
        {
            session.consecutive_failures = session.consecutive_failures.saturating_add(1);
            if session.consecutive_failures >= 3 {
                session.cooldown_until_ms = now.saturating_add(2_000);
                self.timers
                    .schedule(session.cooldown_until_ms, TimerKey::Dispatch);
            }
        }
        let backoff_ms = if error.code == codes::WORKER_PREEMPTED {
            0
        } else {
            self.backoff(&st.jobs[job_id])
        };
        if retry {
            let job = st.jobs.get_mut(job_id).expect("job");
            let attempt = job.attempt.take().expect("attempt");
            job.not_before_ms = now.saturating_add(backoff_ms);
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
        let job_attempt_count = st.jobs[job_id].attempt_count;
        let job_max_attempts = st.jobs[job_id].max_attempts;
        let error = if exposed && error.retryable {
            ErrorInfo::new(
                codes::OUTPUT_INTERRUPTED,
                format!(
                    "attempt failed after its output was delivered: {}",
                    error.message
                ),
                false,
            )
        } else if error.retryable && job_attempt_count >= job_max_attempts {
            ErrorInfo::new(
                codes::ATTEMPTS_EXHAUSTED,
                format!(
                    "{} ({}) after {} attempts",
                    error.message, error.code, job_attempt_count
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
                release_lease,
                cancel_lease,
                finished: receipt,
                ..TerminalPlan::new(JobState::Failed, Some(error))
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
        let Phase::Terminating(plan) = &job.phase else {
            return;
        };
        match outcome {
            CommitOutcome::Fenced(reason) => {
                let release = plan.release_lease.clone();
                let cancel = plan.cancel_lease.as_ref().map(|(l, _)| l.clone());
                let err =
                    RuntimeError::unavailable(format!("control plane lost leadership: {reason}"));
                for waiter in job.waiters.drain(..) {
                    let _ = waiter.send(Err(err.clone()));
                }
                for waiter in job.finish_waiters.drain(..) {
                    let _ = waiter.send(Err(err.clone()));
                }
                self.release_attempt_concurrency_locked(&mut st, job_id);
                if let Some(lease) = release {
                    self.release_lease_locked(&mut st, &lease);
                }
                if let Some(lease) = cancel {
                    self.release_lease_locked(&mut st, &lease);
                }
            }
            CommitOutcome::Committed => {
                // Taking the plan must preserve the public state so the terminal
                // transition removes the original entry from the read indexes.
                let Phase::Terminating(mut plan) = std::mem::replace(&mut job.phase, Phase::Queued)
                else {
                    unreachable!()
                };
                job.phase = std::mem::replace(&mut *plan.previous, Phase::Queued);
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
        self.release_attempt_concurrency_locked(st, job_id);
        st.update_job(job_id, |job| {
            job.phase = Phase::Terminal(plan.state);
            job.updated_at_ms = now;
        });
        match plan.state {
            JobState::Succeeded => st.counters.jobs_succeeded += 1,
            JobState::Failed => st.counters.jobs_failed += 1,
            JobState::Cancelled => st.counters.jobs_cancelled += 1,
            JobState::Queued | JobState::Running => {}
        }
        let job = st.jobs.get_mut(job_id).expect("job");
        self.timers.cancel(&std::mem::take(&mut job.timers));
        if let Some(timer) = job.total_timer.take() {
            self.timers.cancel(&[timer]);
        }
        job.error = plan.error.clone();
        let outputs = if plan.state == JobState::Succeeded {
            plan.outputs
        } else {
            st.released.outputs.push(plan.outputs);
            Vec::new()
        };
        let output_bytes = outputs.iter().map(|o| o.inline_len() as u64).sum::<u64>();
        job.outputs = (plan.state == JobState::Succeeded).then(|| Arc::new(outputs.clone()));
        if let Some(attempt) = job.attempt.take() {
            job.last_attempt_id = Some(attempt.attempt_id);
            job.last_worker_id = Some(attempt.worker_id);
        }
        // Inputs and params leave with the full spec, freed after the lock.
        let slim = Arc::new(without_payload(&job.spec));
        let full_spec = std::mem::replace(&mut job.spec, slim);
        if let Some(input) = job.input.take() {
            input.abort();
        }
        job.log.append(
            job.last_attempt_id.clone(),
            EventKind::Finished {
                state: plan.state,
                outputs,
                error: plan.error,
                handler_duration_ms: plan.handler_duration_ms,
            },
            now,
        );
        if let Some(tc) = job
            .spec
            .metadata
            .get("traceparent")
            .and_then(|v| crate::trace::parse_traceparent(v))
        {
            tracing::debug!(
                job_id = %job.raw_id,
                namespace = %job.namespace,
                state = ?plan.state,
                attempt_count = job.attempt_count,
                trace_id = %tc.trace_id,
                parent_span_id = %tc.parent_id,
                sampled = tc.sampled,
                "job terminal committed"
            );
        }
        // A finished ephemeral job keeps only its outcome, as a durable job read
        // back from the journal does, once its current readers are done.
        let mut compacted = if job.durability == Durability::Ephemeral {
            job.log.finish_ephemeral()
        } else {
            job.log.set_primary(false);
            Vec::new()
        };
        job.output_bytes = output_bytes;
        let mut waiters = std::mem::take(&mut job.waiters);
        notify_job_waiters(&mut waiters, job);
        for waiter in job.finish_waiters.drain(..) {
            let _ = waiter.send(Ok(FinishStatus::Committed));
        }
        job.input_permit = None;
        st.released.events.append(&mut compacted);
        st.released.specs.push(full_spec);
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
            let Some(evicted) = self.evict_oldest_terminal_locked(st) else {
                break;
            };
            st.released.jobs.push(evicted);
        }
    }

    /// Cancels a job on behalf of a caller. Ephemeral jobs end at once; durable
    /// ones once the terminal commit succeeds. Deferred while a commit for the
    /// job is in flight.
    pub(crate) fn cancel_locked(&self, st: &mut State, job_id: &str, now: u64) {
        let Some(job) = st.jobs.get(job_id) else {
            return;
        };
        let (release_lease, cancel_lease) = match &job.phase {
            Phase::Queued => (None, None),
            Phase::Assigned | Phase::Running => {
                let lease_id = job.attempt.as_ref().expect("attempt").lease_id.clone();
                (None, Some(lease_id))
            }
            Phase::Starting => {
                // The start committed but the worker was not told yet.
                let lease_id = job.attempt.as_ref().expect("attempt").lease_id.clone();
                (Some(lease_id), None)
            }
            Phase::Accepting | Phase::Terminating(_) | Phase::Terminal(_) => return,
        };
        let reason = job
            .cancel_requested
            .clone()
            .unwrap_or_else(|| "cancelled".to_string());
        let error = Some(ErrorInfo::new(codes::CANCELLED, reason.clone(), false));
        let plan = TerminalPlan {
            release_lease,
            cancel_lease: cancel_lease.map(|lease_id| (lease_id, reason)),
            ..TerminalPlan::new(JobState::Cancelled, error)
        };
        self.finish_job_locked(st, job_id, plan, now);
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
                    if let Some(evicted) = self.evict_oldest_terminal_locked(st) {
                        st.released.jobs.push(evicted);
                    }
                }
                _ => break,
            }
        }
    }

    /// Takes the oldest terminal job out of memory and returns it for the
    /// caller to free. Durable jobs, their receipts included, stay readable
    /// from the journal.
    fn evict_oldest_terminal_locked(&self, st: &mut State) -> Option<Job> {
        loop {
            let (_, job_id) = st.terminal_order.pop_front()?;
            if !st.jobs.get(&job_id).is_some_and(|job| job.is_terminal()) {
                continue;
            }
            let job = st.remove_job(&job_id).expect("job");
            for lease in &job.finished_leases {
                st.finished_leases.remove(lease);
            }
            return Some(job);
        }
    }

    /// Evicts the oldest terminal jobs until they held `needed` bytes of the
    /// result budget (`results`) or of the event budget, or none are left.
    /// Only this runtime's own references go; copies held elsewhere keep
    /// their bytes until dropped.
    pub(crate) fn reclaim_locked(&self, st: &mut State, needed: u64, results: bool) {
        let mut freed = 0u64;
        while freed < needed {
            let Some(evicted) = self.evict_oldest_terminal_locked(st) else {
                return;
            };
            freed += if results {
                evicted.output_bytes
            } else {
                evicted.log.bytes() as u64
            };
            // Freed here, not after the lock: the caller retries at once.
            drop(evicted);
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

fn rendezvous_hash(affinity_key: &str, worker_id: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in affinity_key
        .as_bytes()
        .iter()
        .chain([0xffu8].iter())
        .chain(worker_id.as_bytes().iter())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

fn tier_rank_for_session(
    tiers: &[PoolTier],
    pool: &str,
    elapsed_ms: u64,
) -> Option<Reverse<usize>> {
    if tiers.is_empty() {
        return Some(Reverse(0));
    }
    let idx = tiers.iter().position(|t| t.pool == pool)?;
    (elapsed_ms >= ms(tiers[idx].spillover_after)).then_some(Reverse(idx))
}

fn session_rank(
    demand: &super::api::Demand,
    tiers: &[PoolTier],
    session: &Session,
    elapsed_ms: u64,
    spill_open: bool,
) -> Option<(Reverse<usize>, usize)> {
    let tier_rank = tier_rank_for_session(tiers, &session.pool, elapsed_ms)?;
    if !demand
        .required_attributes
        .iter()
        .all(|(key, value)| session.attributes.get(key) == Some(value))
    {
        return None;
    }
    let matches = demand
        .preferred_attributes
        .iter()
        .filter(|(key, value)| session.attributes.get(*key) == Some(value))
        .count();
    if !spill_open && matches < demand.preferred_attributes.len() {
        return None;
    }
    session
        .fits(&demand.required_capacity)
        .then_some((tier_rank, matches))
}

fn pick_two_choices(best: &[&Session], rng: &mut u64) -> Option<String> {
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

/// Picks a session for `job`: avoids previously failed workers/sessions and
/// workers in cooldown, prefers earlier unlocked pool tiers, matches the most
/// preferred attributes, routes `affinity_key` via bounded-load Rendezvous
/// Hashing, and otherwise picks the less loaded of two random sessions.
fn pick_session(
    st: &State,
    job: &Job,
    policy: Option<&InterfacePolicy>,
    candidates: &mut Vec<String>,
    now: u64,
    rng: &mut u64,
) -> Option<String> {
    let demand = &job.spec.demand;
    let spill_open = match job.spec.policy.spillover_after {
        Some(after) if !demand.preferred_attributes.is_empty() => {
            now >= job.queued_since_ms.saturating_add(ms(after))
        }
        _ => true,
    };
    let tiers = policy.map(|p| p.pool_tiers.as_slice()).unwrap_or(&[]);
    let elapsed_ms = now.saturating_sub(job.queued_since_ms);
    let mut best: Vec<&Session> = Vec::new();
    let mut best_rank = (false, false, Reverse(usize::MAX), 0usize);
    candidates.retain(|id| {
        let Some(session) = st.sessions.get(id).filter(|s| s.free_slots() > 0) else {
            return false;
        };
        let Some((tier_rank, matches)) =
            session_rank(demand, tiers, session, elapsed_ms, spill_open)
        else {
            return true;
        };
        let healthy = !job.failed_workers.contains(&session.worker_id)
            && !job.rejected_sessions.contains(&session.id);
        let not_in_cooldown = now >= session.cooldown_until_ms;
        let rank = (healthy, not_in_cooldown, tier_rank, matches);
        if best.is_empty() || rank > best_rank {
            best_rank = rank;
            best.clear();
        }
        if rank == best_rank {
            best.push(session);
        }
        true
    });
    if best.is_empty() {
        return None;
    }
    let (best_healthy, best_not_cooled, _, _) = best_rank;
    if (!best_healthy || !best_not_cooled) && now.saturating_sub(job.queued_since_ms) < 5_000 {
        let has_better_peer = st
            .interface_sessions(&job.namespace, &demand.interface)
            .into_iter()
            .flatten()
            .filter_map(|id| st.sessions.get(id))
            .any(|peer| {
                peer.tx.is_some()
                    && !peer.draining
                    && tier_rank_for_session(tiers, &peer.pool, elapsed_ms).is_some()
                    && demand
                        .required_attributes
                        .iter()
                        .all(|(key, value)| peer.attributes.get(key) == Some(value))
                    && demand.required_capacity.iter().all(|(name, needed)| {
                        *needed <= 0
                            || peer
                                .total_capacity
                                .get(name)
                                .is_some_and(|total| *total >= *needed)
                    })
                    && if !best_healthy {
                        !job.failed_workers.contains(&peer.worker_id)
                            && !job.rejected_sessions.contains(&peer.id)
                    } else {
                        now >= peer.cooldown_until_ms
                    }
            });
        if has_better_peer {
            return None;
        }
    }
    if best.len() == 1 {
        return Some(best[0].id.clone());
    }
    if !demand.affinity_key.is_empty()
        && let Some(primary) = best
            .iter()
            .filter(|s| (s.active.len() as f64 / s.max_concurrency.max(1) as f64) < 0.85)
            .max_by_key(|s| {
                (
                    rendezvous_hash(&demand.affinity_key, &s.worker_id),
                    s.worker_id.as_str(),
                )
            })
    {
        return Some(primary.id.clone());
    }
    pick_two_choices(&best, rng)
}

/// Zero-allocation session selector for immediate ephemeral dispatch (`elapsed_ms == 0`).
fn pick_direct_session(
    st: &State,
    namespace: &str,
    spec: &JobSpec,
    policy: Option<&InterfacePolicy>,
    now: u64,
    rng: &mut u64,
) -> Option<String> {
    let demand = &spec.demand;
    let sessions = st.interface_sessions(namespace, &demand.interface)?;
    let spill_open = demand.preferred_attributes.is_empty()
        || spec.policy.spillover_after.is_some_and(|d| d.is_zero());
    let tiers = policy.map(|p| p.pool_tiers.as_slice()).unwrap_or(&[]);

    if !demand.affinity_key.is_empty() {
        let mut best_rank = (false, Reverse(usize::MAX), 0usize);
        let mut under_load: Option<((u64, &str), &Session)> = None;
        let mut fallback: Option<((u64, &str), &Session)> = None;
        for id in sessions {
            let Some(s) = st.sessions.get(id).filter(|s| s.free_slots() > 0) else {
                continue;
            };
            let Some((tier_rank, matches)) = session_rank(demand, tiers, s, 0, spill_open) else {
                continue;
            };
            let rank = (now >= s.cooldown_until_ms, tier_rank, matches);
            if (under_load.is_none() && fallback.is_none()) || rank > best_rank {
                best_rank = rank;
                under_load = None;
                fallback = None;
            }
            if rank == best_rank {
                let score = (
                    rendezvous_hash(&demand.affinity_key, &s.worker_id),
                    s.worker_id.as_str(),
                );
                let healthy_load = (s.active.len() as f64 / s.max_concurrency.max(1) as f64) < 0.85;
                if healthy_load && under_load.is_none_or(|(best_s, _)| score > best_s) {
                    under_load = Some((score, s));
                }
                if fallback.is_none_or(|(best_s, _)| score > best_s) {
                    fallback = Some((score, s));
                }
            }
        }
        return best_rank
            .0
            .then(|| under_load.or(fallback).map(|(_, s)| s.id.clone()))
            .flatten();
    }

    let mut stack_buf: [std::mem::MaybeUninit<&Session>; 32] =
        [const { std::mem::MaybeUninit::uninit() }; 32];
    let mut stack_len = 0usize;
    let mut overflow: Vec<&Session> = Vec::new();
    let mut best_rank = (false, Reverse(usize::MAX), 0usize);
    for id in sessions {
        let Some(s) = st.sessions.get(id).filter(|s| s.free_slots() > 0) else {
            continue;
        };
        let Some((tier_rank, matches)) = session_rank(demand, tiers, s, 0, spill_open) else {
            continue;
        };
        let rank = (now >= s.cooldown_until_ms, tier_rank, matches);
        if (stack_len == 0 && overflow.is_empty()) || rank > best_rank {
            best_rank = rank;
            stack_len = 0;
            overflow.clear();
        }
        if rank == best_rank {
            if stack_len < 32 && overflow.is_empty() {
                stack_buf[stack_len].write(s);
                stack_len += 1;
            } else {
                if overflow.is_empty() {
                    // SAFETY: `stack_buf[..stack_len]` was initialized before advancing `stack_len`.
                    let slice = unsafe {
                        std::slice::from_raw_parts(stack_buf.as_ptr().cast::<&Session>(), stack_len)
                    };
                    overflow.extend_from_slice(slice);
                }
                overflow.push(s);
            }
        }
    }
    if !best_rank.0 && (stack_len > 0 || !overflow.is_empty()) {
        // Cooldown peer present: defer to normal queue so `has_better_peer` wait applies.
        return None;
    }
    if overflow.is_empty() {
        // SAFETY: `stack_buf[..stack_len]` was initialized when `stack_len` incremented.
        let slice =
            unsafe { std::slice::from_raw_parts(stack_buf.as_ptr().cast::<&Session>(), stack_len) };
        pick_two_choices(slice, rng)
    } else {
        pick_two_choices(&overflow, rng)
    }
}
