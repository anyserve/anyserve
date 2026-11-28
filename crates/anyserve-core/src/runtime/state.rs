//! In-memory execution state. Only `machine.rs` and `worker.rs` mutate it, and
//! always under the single runtime lock.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, Bound, VecDeque};
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use anyserve_proto::v2 as pb;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use super::api::{
    Capacity, Durability, ErrorInfo, ExecutionEvent, JobSpec, JobState, JobView, Metadata, Object,
    WorkerLoad, WorkerRecord, WorkerView,
};
use super::events::{JobLog, Permit};
use super::index::JobIndex;
use super::input::InputLog;
use crate::error::RuntimeError;

pub(crate) const DEFAULT_FAIRNESS_KEY: &str = "_default";

pub(crate) type JobWaiter = oneshot::Sender<Result<JobView, RuntimeError>>;
pub(crate) type FinishWaiter = oneshot::Sender<Result<FinishStatus, RuntimeError>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FinishStatus {
    Committed,
    Fenced(String),
    Rejected(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct QueueKey {
    pub priority: Reverse<u32>,
    pub seq: u64,
    pub job_id: String,
}

#[derive(Default)]
pub(crate) struct PriorityFairQueue {
    pub ring: VecDeque<String>,
    pub subqueues: AHashMap<String, BTreeSet<QueueKey>>,
    pub total_len: usize,
}

impl PriorityFairQueue {
    pub(crate) fn insert(&mut self, fairness_key: &str, key: QueueKey) {
        let fkey = if fairness_key.is_empty() {
            DEFAULT_FAIRNESS_KEY
        } else {
            fairness_key
        };
        let subqueue = self.subqueues.entry(fkey.to_string()).or_default();
        let was_empty = subqueue.is_empty();
        if subqueue.insert(key) {
            self.total_len += 1;
            if was_empty {
                self.ring.push_back(fkey.to_string());
            }
        }
    }

    pub(crate) fn remove(&mut self, fairness_key: &str, key: &QueueKey) -> bool {
        let fkey = if fairness_key.is_empty() {
            DEFAULT_FAIRNESS_KEY
        } else {
            fairness_key
        };
        let Some(subqueue) = self.subqueues.get_mut(fkey) else {
            return false;
        };
        if subqueue.remove(key) {
            self.total_len -= 1;
            if subqueue.is_empty() {
                self.subqueues.remove(fkey);
                self.ring.retain(|k| k != fkey);
            }
            return true;
        }
        false
    }

    pub(crate) fn rotate_key_to_back(&mut self, fairness_key: &str) {
        let fkey = if fairness_key.is_empty() {
            DEFAULT_FAIRNESS_KEY
        } else {
            fairness_key
        };
        if self.ring.len() > 1
            && self.ring.front().is_some_and(|k| k == fkey)
            && let Some(front) = self.ring.pop_front()
        {
            self.ring.push_back(front);
        }
    }

    /// Collects up to `limit` queued entries by visiting active `fairness_key`
    /// subqueues in round-robin order.
    pub(crate) fn collect_fair(&self, limit: usize) -> Vec<QueueKey> {
        if self.total_len == 0 || limit == 0 {
            return Vec::new();
        }
        if self.ring.len() == 1
            && let Some(front) = self.ring.front()
            && let Some(subqueue) = self.subqueues.get(front)
        {
            return subqueue.iter().take(limit).cloned().collect();
        }
        let mut iters: Vec<_> = self
            .ring
            .iter()
            .filter_map(|k| self.subqueues.get(k).map(|q| q.iter()))
            .collect();
        let mut out = Vec::with_capacity(limit.min(self.total_len));
        while out.len() < limit && !iters.is_empty() {
            let mut progressed = false;
            iters.retain_mut(|iter| {
                if out.len() >= limit {
                    return true;
                }
                if let Some(item) = iter.next() {
                    out.push(item.clone());
                    progressed = true;
                    true
                } else {
                    false
                }
            });
            if !progressed {
                break;
            }
        }
        out
    }
}

#[derive(Default)]
pub(crate) struct InterfaceQueue {
    pub levels: Vec<PriorityFairQueue>,
    pub total_len: usize,
}

impl InterfaceQueue {
    pub(crate) fn len(&self) -> usize {
        self.total_len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    pub(crate) fn insert(
        &mut self,
        priority_levels: usize,
        priority: u32,
        fairness_key: &str,
        key: QueueKey,
    ) {
        let levels = priority_levels.max(1);
        if self.levels.len() < levels {
            self.levels.resize_with(levels, PriorityFairQueue::default);
        }
        let idx = (priority as usize).min(levels - 1);
        let before = self.levels[idx].total_len;
        self.levels[idx].insert(fairness_key, key);
        self.total_len += self.levels[idx].total_len - before;
    }

    pub(crate) fn remove(&mut self, priority: u32, fairness_key: &str, key: &QueueKey) {
        let idx = (priority as usize).min(self.levels.len().saturating_sub(1));
        if let Some(level) = self.levels.get_mut(idx)
            && level.remove(fairness_key, key)
        {
            self.total_len -= 1;
        }
    }

    pub(crate) fn rotate_key_to_back(&mut self, priority: u32, fairness_key: &str) {
        let idx = (priority as usize).min(self.levels.len().saturating_sub(1));
        if let Some(level) = self.levels.get_mut(idx) {
            level.rotate_key_to_back(fairness_key);
        }
    }

    pub(crate) fn collect_candidates(&self, limit: usize) -> Vec<QueueKey> {
        let mut out = Vec::new();
        for level in self.levels.iter().rev() {
            if out.len() >= limit {
                break;
            }
            if level.total_len == 0 {
                continue;
            }
            out.extend(level.collect_fair(limit - out.len()));
        }
        out
    }

    /// Collects the rotating scan window past `DISPATCH_SCAN` so unmatchable
    /// jobs at the head never starve jobs deeper in a large queue.
    pub(crate) fn collect_window(
        &self,
        cursor: Option<&QueueKey>,
        limit: usize,
    ) -> (Vec<QueueKey>, Option<QueueKey>) {
        if limit == 0 || self.total_len == 0 {
            return (Vec::new(), None);
        }
        let mut out = Vec::with_capacity(limit);
        for (prio_idx, level) in self.levels.iter().enumerate().rev() {
            if out.len() >= limit {
                break;
            }
            if level.total_len == 0 {
                continue;
            }
            let level_prio = Reverse(prio_idx as u32);
            if let Some(cur) = cursor
                && level_prio < cur.priority
            {
                continue;
            }
            let mut iters: Vec<_> = level
                .subqueues
                .values()
                .map(|q| match cursor {
                    Some(cur) if cur.priority == level_prio => {
                        q.range((Bound::Excluded(cur.clone()), Bound::Unbounded))
                    }
                    _ => q.range(..),
                })
                .collect();
            let mut heap: BinaryHeap<Reverse<(&QueueKey, usize)>> = BinaryHeap::new();
            for (idx, iter) in iters.iter_mut().enumerate() {
                if let Some(k) = iter.next() {
                    heap.push(Reverse((k, idx)));
                }
            }
            while out.len() < limit {
                let Some(Reverse((key, idx))) = heap.pop() else {
                    break;
                };
                out.push(key.clone());
                if let Some(next_key) = iters[idx].next() {
                    heap.push(Reverse((next_key, idx)));
                }
            }
        }
        let next_cursor = if out.len() == limit {
            out.last().cloned()
        } else {
            None
        };
        (out, next_cursor)
    }
}

pub(crate) type NsIfaceMap<T> = AHashMap<String, AHashMap<String, T>>;

#[derive(Default)]
pub(crate) struct State {
    pub generation: u64,
    pub jobs: AHashMap<String, Job>,
    pub job_index: JobIndex,
    /// Queued jobs per `namespace -> interface`, priority + round-robin fairness order.
    pub queues: NsIfaceMap<InterfaceQueue>,
    /// Where the scan past the dispatch head left off per `namespace -> interface`.
    pub scan_cursor: NsIfaceMap<QueueKey>,
    pub queue_seq: u64,
    /// Durable jobs awaiting their acceptance commit, per `namespace -> interface`.
    pub accepting: NsIfaceMap<usize>,
    /// Jobs not yet terminal, including those being accepted.
    pub unfinished_jobs: usize,
    /// Active in-flight attempts keyed by `namespace -> concurrency_key`.
    pub in_flight_concurrency: NsIfaceMap<usize>,
    pub sessions: AHashMap<String, Session>,
    pub sessions_by_interface: NsIfaceMap<AHashSet<String>>,
    /// Worker registrations keyed by `(namespace, worker_id)`, retained after
    /// sessions end so operators can inspect offline workers and historical jobs.
    pub workers: AHashMap<(String, String), WorkerRecord>,
    pub leases: AHashMap<String, Lease>,
    /// Completion receipts of jobs held in memory: lease id -> (scoped job id,
    /// outcome digest). Receipts of evicted durable jobs are read from the
    /// journal.
    pub finished_leases: AHashMap<String, (String, String)>,
    pub terminal_order: VecDeque<(u64, Arc<str>)>,
    pub rng: u64,
    pub connection_seq: u64,
    pub shutting_down: bool,
    pub counters: LifecycleCounters,
    /// Taken out under the lock, freed once it is released.
    pub released: Released,
}

/// Monotonic totals since this Runtime started.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LifecycleCounters {
    pub jobs_submitted: u64,
    pub jobs_succeeded: u64,
    pub jobs_failed: u64,
    pub jobs_cancelled: u64,
    pub attempts_started: u64,
}

impl State {
    pub(crate) fn queue(&self, namespace: &str, interface: &str) -> Option<&InterfaceQueue> {
        self.queues.get(namespace).and_then(|m| m.get(interface))
    }

    pub(crate) fn queue_mut(
        &mut self,
        namespace: &str,
        interface: &str,
    ) -> Option<&mut InterfaceQueue> {
        self.queues
            .get_mut(namespace)
            .and_then(|m| m.get_mut(interface))
    }

    pub(crate) fn queue_or_insert(
        &mut self,
        namespace: &str,
        interface: &str,
    ) -> &mut InterfaceQueue {
        if !self.queues.contains_key(namespace) {
            self.queues.insert(namespace.to_string(), AHashMap::new());
        }
        let m = self
            .queues
            .get_mut(namespace)
            .expect("namespace map inserted");
        if !m.contains_key(interface) {
            m.insert(interface.to_string(), InterfaceQueue::default());
        }
        m.get_mut(interface).expect("interface queue inserted")
    }

    pub(crate) fn count_in(map: &NsIfaceMap<usize>, namespace: &str, key: &str) -> usize {
        map.get(namespace)
            .and_then(|m| m.get(key))
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn inc_in(map: &mut NsIfaceMap<usize>, namespace: &str, key: &str) {
        if let Some(m) = map.get_mut(namespace) {
            if let Some(c) = m.get_mut(key) {
                *c += 1;
            } else {
                m.insert(key.to_string(), 1);
            }
            return;
        }
        map.entry(namespace.to_string())
            .or_default()
            .insert(key.to_string(), 1);
    }

    /// Removes `namespace -> key` and drops the namespace map once it is empty.
    pub(crate) fn remove_nested<T>(map: &mut NsIfaceMap<T>, namespace: &str, key: &str) {
        if let Some(m) = map.get_mut(namespace) {
            m.remove(key);
            if m.is_empty() {
                map.remove(namespace);
            }
        }
    }

    pub(crate) fn dec_in(map: &mut NsIfaceMap<usize>, namespace: &str, key: &str) {
        if let Some(m) = map.get_mut(namespace) {
            if let Some(c) = m.get_mut(key) {
                *c = c.saturating_sub(1);
                if *c == 0 {
                    m.remove(key);
                }
            }
            if m.is_empty() {
                map.remove(namespace);
            }
        }
    }

    pub(crate) fn accepting_count(&self, namespace: &str, interface: &str) -> usize {
        Self::count_in(&self.accepting, namespace, interface)
    }

    pub(crate) fn inc_accepting(&mut self, namespace: &str, interface: &str) {
        Self::inc_in(&mut self.accepting, namespace, interface);
    }

    pub(crate) fn in_flight_key_count(&self, namespace: &str, ckey: &str) -> usize {
        Self::count_in(&self.in_flight_concurrency, namespace, ckey)
    }

    pub(crate) fn inc_in_flight_key(&mut self, namespace: &str, ckey: &str) {
        Self::inc_in(&mut self.in_flight_concurrency, namespace, ckey);
    }

    pub(crate) fn interface_sessions(
        &self,
        namespace: &str,
        interface: &str,
    ) -> Option<&AHashSet<String>> {
        self.sessions_by_interface
            .get(namespace)
            .and_then(|m| m.get(interface))
    }

    pub(crate) fn insert_job(&mut self, job: Job) {
        self.job_index.insert(&job);
        self.jobs.insert(job.id.clone(), job);
    }

    /// All changes to a job's public state or ordering timestamps go here.
    pub(crate) fn update_job(&mut self, id: &str, update: impl FnOnce(&mut Job)) {
        if let Some(job) = self.jobs.get_mut(id) {
            let previous = JobIndex::position(job);
            update(job);
            // Internal phases often share a public state and millisecond
            // timestamp. Their identity and interface never change.
            if previous != JobIndex::position(job) {
                self.job_index.move_position(job, previous);
            }
        }
    }

    pub(crate) fn remove_job(&mut self, id: &str) -> Option<Job> {
        let job = self.jobs.remove(id)?;
        self.job_index.remove(&job, true);
        Some(job)
    }

    pub(crate) fn index_session(&mut self, id: &str) {
        let ns = self.sessions[id].namespace.clone();
        let ns_map = self.sessions_by_interface.entry(ns).or_default();
        for interface in &self.sessions[id].interfaces {
            if let Some(ids) = ns_map.get_mut(interface.as_str()) {
                ids.insert(id.to_string());
            } else {
                let mut set = AHashSet::new();
                set.insert(id.to_string());
                ns_map.insert(interface.clone(), set);
            }
        }
    }

    pub(crate) fn unindex_session(&mut self, id: &str) {
        if let Some(session) = self.sessions.get(id) {
            let ns = &session.namespace;
            if let Some(ns_map) = self.sessions_by_interface.get_mut(ns.as_str()) {
                for interface in &session.interfaces {
                    if let Some(ids) = ns_map.get_mut(interface.as_str()) {
                        ids.remove(id);
                        if ids.is_empty() {
                            ns_map.remove(interface.as_str());
                        }
                    }
                }
                if ns_map.is_empty() {
                    self.sessions_by_interface.remove(ns.as_str());
                }
            }
        }
    }

    pub(crate) fn remove_session(&mut self, id: &str) -> Option<Session> {
        self.unindex_session(id);
        self.sessions.remove(id)
    }

    pub(crate) fn has_live_session(&self, namespace: &str, worker_id: &str) -> bool {
        self.sessions
            .values()
            .any(|s| s.namespace == namespace && s.worker_id == worker_id)
    }

    pub(crate) fn upsert_live_worker(
        &mut self,
        session_id: &str,
        now: u64,
    ) -> Option<WorkerRecord> {
        let session = self.sessions.get(session_id)?;
        let key = (session.namespace.clone(), session.worker_id.clone());
        let first_seen_ms = self
            .workers
            .get(&key)
            .map(|existing| existing.first_seen_ms)
            .filter(|&ts| ts > 0)
            .unwrap_or(now);
        let record = WorkerRecord {
            namespace: session.namespace.clone(),
            worker_id: session.worker_id.clone(),
            pool: session.pool.clone(),
            interfaces: session.interfaces.iter().cloned().collect(),
            attributes: session.attributes.clone(),
            total_capacity: session.total_capacity.clone(),
            max_concurrency: session.max_concurrency,
            first_seen_ms,
            last_seen_ms: session.last_seen_ms.max(now),
            last_session_id: session.id.clone(),
            last_connected_at_ms: session.connected_at_ms,
            offline_since_ms: None,
            offline_reason: None,
        };
        self.workers.insert(key, record.clone());
        Some(record)
    }

    pub(crate) fn mark_worker_session_ended(
        &mut self,
        removed: &Session,
        reason: &str,
        now: u64,
        retain_offline: bool,
    ) -> Option<crate::journal::JournalOp> {
        let key = (removed.namespace.clone(), removed.worker_id.clone());
        if let Some(other) = self
            .sessions
            .values()
            .filter(|s| s.namespace == removed.namespace && s.worker_id == removed.worker_id)
            .max_by_key(|s| s.last_seen_ms)
            .map(|s| s.id.clone())
        {
            return self
                .upsert_live_worker(&other, now)
                .filter(|_| retain_offline)
                .map(|r| crate::journal::JournalOp::WorkerUpsert(Box::new(r)));
        }
        if !retain_offline {
            self.workers.remove(&key);
            return Some(crate::journal::JournalOp::WorkerForget {
                namespace: removed.namespace.clone(),
                worker_id: removed.worker_id.clone(),
            });
        }
        let first_seen_ms = self
            .workers
            .get(&key)
            .map(|existing| existing.first_seen_ms)
            .filter(|&ts| ts > 0)
            .unwrap_or(removed.connected_at_ms);
        let last_seen_ms = self
            .workers
            .get(&key)
            .map(|existing| existing.last_seen_ms)
            .unwrap_or(0)
            .max(removed.last_seen_ms);
        let record = WorkerRecord {
            namespace: removed.namespace.clone(),
            worker_id: removed.worker_id.clone(),
            pool: removed.pool.clone(),
            interfaces: removed.interfaces.iter().cloned().collect(),
            attributes: removed.attributes.clone(),
            total_capacity: removed.total_capacity.clone(),
            max_concurrency: removed.max_concurrency,
            first_seen_ms,
            last_seen_ms,
            last_session_id: removed.id.clone(),
            last_connected_at_ms: removed.connected_at_ms,
            offline_since_ms: Some(now),
            offline_reason: Some(reason.to_owned()),
        };
        self.workers.insert(key, record.clone());
        Some(crate::journal::JournalOp::WorkerUpsert(Box::new(record)))
    }
}

pub(crate) fn offline_worker_view(record: &WorkerRecord) -> WorkerView {
    WorkerView {
        session_id: record.last_session_id.clone(),
        worker_id: record.worker_id.clone(),
        namespace: record.namespace.clone(),
        pool: record.pool.clone(),
        interfaces: record.interfaces.clone(),
        attributes: record.attributes.clone(),
        total_capacity: record.total_capacity.clone(),
        max_concurrency: record.max_concurrency,
        active_leases: 0,
        free_slots: 0,
        load: WorkerLoad::default(),
        draining: false,
        connected: false,
        connected_at_ms: record.last_connected_at_ms,
        last_seen_ms: record.last_seen_ms,
        first_seen_ms: if record.first_seen_ms > 0 {
            record.first_seen_ms
        } else {
            record.last_connected_at_ms
        },
        offline_since_ms: Some(record.offline_since_ms.unwrap_or(record.last_seen_ms)),
        offline_reason: record.offline_reason.clone(),
    }
}

/// What lock holders set aside instead of freeing it in place: freeing an
/// evicted job or a compacted log is work nobody else has to wait for. The
/// runtime's lock guard drops it right after unlocking.
#[derive(Default)]
pub(crate) struct Released {
    pub jobs: Vec<Job>,
    pub events: Vec<ExecutionEvent>,
    /// Full specifications of jobs that just ended; their inputs go with them.
    pub specs: Vec<Arc<JobSpec>>,
    pub outputs: Vec<Vec<Object>>,
}

impl Released {
    pub(crate) fn is_empty(&self) -> bool {
        self.jobs.is_empty()
            && self.events.is_empty()
            && self.specs.is_empty()
            && self.outputs.is_empty()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Phase {
    /// Durable acceptance commit in flight.
    Accepting,
    Queued,
    /// Durable attempt-start commit in flight; the lease is reserved.
    Starting,
    Assigned,
    Running,
    /// Durable terminal commit in flight.
    Terminating(Box<TerminalPlan>),
    Terminal(JobState),
}

#[derive(Clone, Debug)]
pub(crate) struct TerminalPlan {
    pub state: JobState,
    pub outputs: Vec<Object>,
    pub error: Option<ErrorInfo>,
    /// Lease whose reservation ends with the commit.
    pub release_lease: Option<String>,
    /// Lease to cancel at the worker; its reservation ends on acknowledgement.
    pub cancel_lease: Option<(String, String)>,
    /// Lease and digest of the completion that produced this outcome.
    pub finished: Option<(String, String)>,
    /// Worker-measured monotonic handler execution time, when reported.
    pub handler_duration_ms: Option<u64>,
    /// Phase the job was in when the plan was made; `finish_job_locked` sets
    /// it, so the public state stays the same while the commit is in flight.
    pub previous: Box<Phase>,
}

impl TerminalPlan {
    /// A plan with no outputs, receipt or lease to end.
    pub(crate) fn new(state: JobState, error: Option<ErrorInfo>) -> Self {
        Self {
            state,
            outputs: Vec::new(),
            error,
            release_lease: None,
            cancel_lease: None,
            finished: None,
            handler_duration_ms: None,
            previous: Box::new(Phase::Queued),
        }
    }
}

pub(crate) struct AttemptStreamState {
    pub active: bool,
    pub last_frame_seq: u64,
    pub exposed: bool,
    /// Cleared on deactivation so live gRPC/WS attempt handlers never keep an
    /// evicted job's `JobLog` (and its `ByteBudget` reservation) alive.
    pub log: Option<Arc<JobLog>>,
}

/// Per-attempt streaming state shared between `Attempt`/`Lease` and live
/// attempt handlers so streaming frame ingestion bypasses the global runtime
/// state lock on the fast path.
///
/// Lock ordering: always `State` -> `AttemptStream::state` -> `JobLog::inner`.
pub(crate) struct AttemptStream {
    pub job_id: String,
    pub attempt_id: String,
    pub streamed: bool,
    pub state: Mutex<AttemptStreamState>,
}

impl AttemptStream {
    pub(crate) fn new(
        job_id: String,
        attempt_id: String,
        streamed: bool,
        log: Arc<JobLog>,
    ) -> Self {
        Self {
            job_id,
            attempt_id,
            streamed,
            state: Mutex::new(AttemptStreamState {
                active: true,
                last_frame_seq: 0,
                exposed: false,
                log: Some(log),
            }),
        }
    }

    pub(crate) fn deactivate(&self) {
        let log = {
            let mut st = self.state.lock();
            st.active = false;
            st.log.take()
        };
        drop(log);
    }

    /// Marks the stream inactive and returns whether it had already exposed
    /// output to a live caller before deactivation.
    pub(crate) fn deactivate_and_read_exposed(&self) -> bool {
        let (exposed, log) = {
            let mut st = self.state.lock();
            st.active = false;
            (st.exposed, st.log.take())
        };
        drop(log);
        exposed
    }

    /// Atomically deactivates the stream only if it has not yet exposed output.
    pub(crate) fn try_deactivate_if_unexposed(&self) -> bool {
        let log = {
            let mut st = self.state.lock();
            if st.exposed {
                return false;
            }
            st.active = false;
            st.log.take()
        };
        drop(log);
        true
    }

    pub(crate) fn last_frame_seq(&self) -> u64 {
        self.state.lock().last_frame_seq
    }
}

pub(crate) struct Attempt {
    pub attempt_id: String,
    pub lease_id: String,
    pub number: u32,
    pub session_id: String,
    pub worker_id: String,
    pub started_at_ms: u64,
    pub inputs_via_attempt: bool,
    pub stream: Arc<AttemptStream>,
    pub concurrency_counted: bool,
}

pub(crate) struct Job {
    /// Internal scoped storage ID (`encode_scoped_id(namespace, raw_id)`).
    pub id: String,
    pub raw_id: String,
    /// Reference-counted copies shared by `JobIndex` keys so state transitions
    /// avoid heap-allocating `job_id` / `raw_id` strings.
    pub index_id: Arc<str>,
    pub raw_index_id: Arc<str>,
    pub namespace: String,
    pub spec: Arc<JobSpec>,
    pub durability: Durability,
    pub spec_hash: String,
    pub max_attempts: u32,
    pub phase: Phase,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub attempt_count: u32,
    pub attempt: Option<Attempt>,
    pub last_attempt_id: Option<String>,
    pub last_worker_id: Option<String>,
    pub queue_key: Option<QueueKey>,
    pub queued_since_ms: u64,
    pub not_before_ms: u64,
    /// Phase/attempt timers (`QueueDeadline`, `ExecutionDeadline`, spillover/backoff
    /// `Dispatch`); cancelled when leaving the phase or when the job ends.
    pub timers: Vec<u64>,
    /// `TotalDeadline` timer across the whole job lifetime; cancelled when terminal.
    pub total_timer: Option<u64>,
    /// Sessions that explicitly rejected this job (`AssignReply.accepted = false`).
    pub rejected_sessions: AHashSet<String>,
    /// Worker IDs that failed, timed out, crashed on, or rejected this job.
    pub failed_workers: BTreeSet<String>,
    pub cancel_requested: Option<String>,
    /// A commit for this job had an unknown outcome; the id stays reserved
    /// and the job frozen until a restart reconciles it with the journal.
    pub indeterminate: bool,
    /// Metadata-only terminal job restored from the journal on startup; its
    /// outputs and replayable event history live on disk and must be read from
    /// the journal on demand.
    pub cold_terminal: bool,
    pub outputs: Option<Arc<Vec<Object>>>,
    pub error: Option<ErrorInfo>,
    pub log: Arc<JobLog>,
    /// Output goes to a live Invoke caller as it arrives: an
    /// attempt counts as exposed as soon as its first output is logged.
    pub streamed: bool,
    pub input: Option<Arc<InputLog>>,
    /// Reserves the job's input bytes from the input budget until it ends.
    pub input_permit: Option<Permit>,
    /// Result bytes this job keeps once terminal; what evicting it can free.
    pub output_bytes: u64,
    /// Submitters waiting for a durable acceptance.
    pub accept_waiters: Vec<JobWaiter>,
    /// Callers waiting for the terminal state (cancellation).
    pub waiters: Vec<JobWaiter>,
    pub finish_waiters: Vec<FinishWaiter>,
    pub finished_leases: Vec<String>,
}

impl Job {
    pub(crate) fn state(&self) -> JobState {
        match &self.phase {
            Phase::Accepting | Phase::Queued | Phase::Starting => JobState::Queued,
            Phase::Assigned | Phase::Running => JobState::Running,
            Phase::Terminating(plan) => match plan.previous.as_ref() {
                Phase::Assigned | Phase::Running => JobState::Running,
                _ => JobState::Queued,
            },
            Phase::Terminal(state) => *state,
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(self.phase, Phase::Terminal(_))
    }

    pub(crate) fn current_worker_id(&self) -> Option<&str> {
        self.attempt
            .as_ref()
            .map(|attempt| attempt.worker_id.as_str())
            .or(self.last_worker_id.as_deref())
    }

    pub(crate) fn view(&self, include_outputs: bool) -> JobView {
        JobView {
            job_id: self.raw_id.clone(),
            namespace: self.namespace.clone(),
            state: self.state(),
            durability: self.durability,
            interface: self.spec.demand.interface.clone(),
            attempt_count: self.attempt_count,
            current_attempt_id: self
                .attempt
                .as_ref()
                .map(|attempt| attempt.attempt_id.clone())
                .or_else(|| self.last_attempt_id.clone()),
            current_worker_id: self.current_worker_id().map(str::to_owned),
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            error: self.error.clone(),
            outputs: include_outputs
                .then(|| {
                    self.outputs
                        .as_ref()
                        .map(|outputs| outputs.as_ref().clone())
                })
                .flatten(),
            metadata: self.spec.metadata.clone(),
            spec_hash: self.spec_hash.clone(),
        }
    }
}

pub(crate) struct Lease {
    pub job_id: String,
    pub session_id: String,
    pub reserved: Capacity,
    pub cancelling: bool,
    pub stream: Arc<AttemptStream>,
}

pub(crate) struct Session {
    pub id: String,
    pub worker_id: String,
    pub namespace: String,
    pub pool: String,
    pub forced_pool: Option<String>,
    pub token_jti: Option<String>,
    pub token_exp: Option<u64>,
    pub interfaces: BTreeSet<String>,
    pub attributes: Metadata,
    pub metadata: Metadata,
    pub total_capacity: Capacity,
    pub max_concurrency: u32,
    pub reported_free: u32,
    pub assigned_since_report: u32,
    /// Leases holding this session's capacity; change only through
    /// `attach` / `detach` so `reserved` stays their sum.
    pub active: BTreeSet<String>,
    /// Capacity reserved by `active`, kept incrementally so dispatch does not
    /// walk the leases.
    pub reserved: Capacity,
    pub load: WorkerLoad,
    pub draining: bool,
    /// Current connection, bounded; `None` while disconnected (in grace).
    pub tx: Option<mpsc::Sender<pb::ServerMessage>>,
    pub connection: u64,
    pub connected_at_ms: u64,
    pub last_seen_ms: u64,
    pub disconnected_at_ms: Option<u64>,
    pub consecutive_failures: u32,
    pub cooldown_until_ms: u64,
}

impl Session {
    pub(crate) fn free_slots(&self) -> u32 {
        if self.tx.is_none() || self.draining {
            return 0;
        }
        let by_server = self
            .max_concurrency
            .saturating_sub(self.active.len() as u32);
        let by_worker = self
            .reported_free
            .saturating_sub(self.assigned_since_report);
        by_server.min(by_worker)
    }

    pub(crate) fn attach(&mut self, lease_id: String, capacity: &Capacity) {
        if self.active.insert(lease_id) {
            for (name, value) in capacity {
                if let Some(held) = self.reserved.get_mut(name) {
                    *held = held.saturating_add(*value);
                } else {
                    self.reserved.insert(name.clone(), *value);
                }
            }
        }
    }

    pub(crate) fn worker_view(&self, first_seen_ms: Option<u64>) -> WorkerView {
        WorkerView {
            session_id: self.id.clone(),
            worker_id: self.worker_id.clone(),
            namespace: self.namespace.clone(),
            pool: self.pool.clone(),
            interfaces: self.interfaces.iter().cloned().collect(),
            attributes: self.attributes.clone(),
            total_capacity: self.total_capacity.clone(),
            max_concurrency: self.max_concurrency,
            active_leases: self.active.len() as u32,
            free_slots: self.free_slots(),
            load: self.load.clone(),
            draining: self.draining,
            connected: self.tx.is_some(),
            connected_at_ms: self.connected_at_ms,
            last_seen_ms: self.last_seen_ms,
            first_seen_ms: first_seen_ms
                .filter(|&ts| ts > 0)
                .unwrap_or(self.connected_at_ms),
            offline_since_ms: None,
            offline_reason: None,
        }
    }

    pub(crate) fn detach(&mut self, lease_id: &str, capacity: &Capacity) {
        if self.active.remove(lease_id) {
            for (name, value) in capacity {
                if let Some(held) = self.reserved.get_mut(name) {
                    *held = held.saturating_sub(*value);
                    if *held == 0 {
                        self.reserved.remove(name);
                    }
                }
            }
        }
    }

    /// Whether the capacity not yet reserved covers `demand`. A resource the
    /// session does not declare has none.
    pub(crate) fn fits(&self, demand: &Capacity) -> bool {
        demand.iter().all(|(name, needed)| {
            *needed <= 0
                || self.total_capacity.get(name).is_some_and(|total| {
                    total.saturating_sub(self.reserved.get(name).copied().unwrap_or(0)) >= *needed
                })
        })
    }

    /// Queues a control message without waiting. False means the worker is
    /// not reading its control stream; the caller must disconnect it.
    pub(crate) fn try_send(&self, message: pb::server_message::Msg) -> bool {
        match &self.tx {
            Some(tx) => tx
                .try_send(pb::ServerMessage { msg: Some(message) })
                .is_ok(),
            None => true,
        }
    }
}

/// Test check: every session's `reserved` equals the sum over its leases.
#[cfg(test)]
pub(crate) fn assert_reservations(st: &State) {
    super::index::assert_matches_jobs(st);
    let mut expected_interfaces: NsIfaceMap<AHashSet<String>> = AHashMap::new();
    for session in st.sessions.values() {
        for interface in &session.interfaces {
            expected_interfaces
                .entry(session.namespace.clone())
                .or_default()
                .entry(interface.clone())
                .or_default()
                .insert(session.id.clone());
        }
    }
    assert_eq!(st.sessions_by_interface, expected_interfaces);
    for session in st.sessions.values() {
        let mut expected = Capacity::new();
        for lease_id in &session.active {
            for (name, value) in &st.leases[lease_id].reserved {
                *expected.entry(name.clone()).or_insert(0) += value;
            }
        }
        expected.retain(|_, value| *value != 0);
        assert_eq!(session.reserved, expected, "session {}", session.id);
    }
}
