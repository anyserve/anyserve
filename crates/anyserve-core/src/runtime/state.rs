//! In-memory execution state. Only `machine.rs` and `worker.rs` mutate it, and
//! always under the single runtime lock.

use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use anyserve_proto::v2 as pb;
use tokio::sync::{mpsc, oneshot};

use super::api::{
    Capacity, Durability, ErrorInfo, ExecutionEvent, JobSpec, JobState, JobView, Metadata, Object,
    WorkerLoad,
};
use super::events::{JobLog, Permit};
use super::index::JobIndex;
use super::input::InputLog;
use crate::error::RuntimeError;

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
pub(crate) struct State {
    pub generation: u64,
    pub jobs: HashMap<String, Job>,
    pub job_index: JobIndex,
    /// Queued jobs per interface, highest priority then oldest first.
    pub queues: HashMap<String, BTreeSet<QueueKey>>,
    pub queue_seq: u64,
    /// Where the rotating part of the next dispatch scan starts, per interface.
    pub scan_cursor: HashMap<String, QueueKey>,
    /// Durable jobs awaiting their acceptance commit, per interface.
    pub accepting: HashMap<String, usize>,
    /// Jobs not yet terminal, including those being accepted.
    pub unfinished_jobs: usize,
    pub sessions: HashMap<String, Session>,
    pub sessions_by_interface: HashMap<String, HashSet<String>>,
    pub leases: HashMap<String, Lease>,
    /// Completion receipts of jobs held in memory: lease id -> (job id,
    /// outcome digest). Receipts of evicted durable jobs are read from the
    /// journal.
    pub finished_leases: HashMap<String, (String, String)>,
    pub terminal_order: VecDeque<(u64, String)>,
    pub rng: u64,
    pub connection_seq: u64,
    pub shutting_down: bool,
    /// Taken out under the lock, freed once it is released.
    pub released: Released,
}

impl State {
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
                self.job_index.remove_position(job, previous, false);
                self.job_index.insert_state(job);
            }
        }
    }

    pub(crate) fn remove_job(&mut self, id: &str) -> Option<Job> {
        let job = self.jobs.remove(id)?;
        self.job_index.remove(&job, true);
        Some(job)
    }

    pub(crate) fn index_session(&mut self, id: &str) {
        for interface in &self.sessions[id].interfaces {
            self.sessions_by_interface
                .entry(interface.clone())
                .or_default()
                .insert(id.to_string());
        }
    }

    pub(crate) fn unindex_session(&mut self, id: &str) {
        if let Some(session) = self.sessions.get(id) {
            for interface in &session.interfaces {
                if let Some(ids) = self.sessions_by_interface.get_mut(interface) {
                    ids.remove(id);
                    if ids.is_empty() {
                        self.sessions_by_interface.remove(interface);
                    }
                }
            }
        }
    }

    pub(crate) fn remove_session(&mut self, id: &str) {
        self.unindex_session(id);
        self.sessions.remove(id);
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
            previous: Box::new(Phase::Queued),
        }
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
    pub last_frame_seq: u64,
    pub exposed: bool,
}

pub(crate) struct Job {
    pub id: String,
    pub ownership_epoch: Option<i64>,
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
    /// Deadline timers scheduled for this job; cancelled when it ends.
    pub timers: Vec<u64>,
    pub rejected_by: HashSet<String>,
    pub cancel_requested: Option<String>,
    /// A commit for this job had an unknown outcome; the id stays reserved
    /// and the job frozen until a restart reconciles it with the journal.
    pub indeterminate: bool,
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

    pub(crate) fn view(&self, include_outputs: bool) -> JobView {
        JobView {
            job_id: self.id.clone(),
            state: self.state(),
            durability: self.durability,
            interface: self.spec.demand.interface.clone(),
            attempt_count: self.attempt_count,
            current_attempt_id: self
                .attempt
                .as_ref()
                .map(|attempt| attempt.attempt_id.clone())
                .or_else(|| self.last_attempt_id.clone()),
            current_worker_id: self
                .attempt
                .as_ref()
                .map(|attempt| attempt.worker_id.clone())
                .or_else(|| self.last_worker_id.clone()),
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
}

pub(crate) struct Session {
    pub id: String,
    pub worker_id: String,
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
                let held = self.reserved.entry(name.clone()).or_insert(0);
                *held = held.saturating_add(*value);
            }
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
    let mut expected_interfaces: HashMap<String, HashSet<String>> = HashMap::new();
    for session in st.sessions.values() {
        for interface in &session.interfaces {
            expected_interfaces
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
