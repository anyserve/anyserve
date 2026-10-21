//! The execution runtime: one in-memory state machine for ephemeral and
//! durable jobs, worker sessions and attempt streams, with an optional SQLite
//! journal for durable jobs.

pub mod api;
pub(crate) mod events;
mod identity;
pub(crate) mod input;
mod machine;
pub(crate) mod state;
pub(crate) mod worker;

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use tokio::sync::{Notify, oneshot, watch};

use self::api::{
    Durability, ErrorInfo, EventKind, EventStream, Execution, ExecutionEvent, Health, InputMode,
    InputSink, InterfaceStats, InvokeRequest, JobFilter, JobPage, JobSpec, JobState, JobView,
    RuntimeConfig, RuntimeStats, SubmitOutcome, SubmitRequest, WorkerView, codes,
};
use self::events::{EventBudget, JobLog, Permit};
use self::input::InputLog;
use self::machine::{LeaseEnd, ms, new_id};
use self::state::{Phase, State, TerminalPlan};
use crate::error::RuntimeError;
use crate::journal::{Journal, Recovery, StoredJob};
use crate::now_ms;

const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
const PURGE_INTERVAL_MS: u64 = 3_600_000;
const TIMER_MAX_SLEEP: Duration = Duration::from_secs(1);
const DEFAULT_PAGE_SIZE: usize = 100;

#[derive(Clone, Debug)]
pub(crate) enum JournalStatus {
    Disabled,
    Healthy,
    /// Commits are rolling back (for example, the disk is full); probed.
    Degraded(String),
    /// A commit outcome could not be determined; needs a restart.
    Failed(String),
}

#[derive(Clone, Debug)]
pub(crate) enum TimerKey {
    Dispatch,
    QueueDeadline { job_id: String, since: u64 },
    ExecutionDeadline { job_id: String, attempt_id: String },
    TotalDeadline { job_id: String },
    SessionGrace { session_id: String, connection: u64 },
    CancelGrace { lease_id: String },
    RetryTerminal { job_id: String },
}

#[derive(Default)]
struct TimerHeap {
    heap: BinaryHeap<Reverse<(u64, u64)>>,
    keys: HashMap<u64, TimerKey>,
    next: u64,
}

pub(crate) struct Timers {
    inner: Mutex<TimerHeap>,
    notify: Arc<Notify>,
}

impl Timers {
    fn new() -> Self {
        Self {
            inner: Mutex::new(TimerHeap::default()),
            notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn schedule(&self, at_ms: u64, key: TimerKey) {
        let mut inner = self.inner.lock().expect("timers poisoned");
        inner.next += 1;
        let id = inner.next;
        let earliest = inner.heap.peek().map(|Reverse((at, _))| *at);
        inner.heap.push(Reverse((at_ms, id)));
        inner.keys.insert(id, key);
        drop(inner);
        if earliest.is_none_or(|earliest| at_ms < earliest) {
            self.notify.notify_one();
        }
    }

    fn pop_due(&self, now: u64) -> Vec<TimerKey> {
        let mut inner = self.inner.lock().expect("timers poisoned");
        let mut due = Vec::new();
        while let Some(Reverse((at, id))) = inner.heap.peek().copied() {
            if at > now {
                break;
            }
            inner.heap.pop();
            if let Some(key) = inner.keys.remove(&id) {
                due.push(key);
            }
        }
        due
    }

    fn next_due(&self) -> Option<u64> {
        let inner = self.inner.lock().expect("timers poisoned");
        inner.heap.peek().map(|Reverse((at, _))| *at)
    }

    fn len(&self) -> usize {
        self.inner.lock().expect("timers poisoned").keys.len()
    }

    /// Drops timers for which `keep` is false and rebuilds the heap.
    fn compact(&self, keep: impl Fn(&TimerKey) -> bool) {
        let mut inner = self.inner.lock().expect("timers poisoned");
        inner.keys.retain(|_, key| keep(key));
        let entries: Vec<Reverse<(u64, u64)>> = inner.heap.drain().collect();
        let heap: BinaryHeap<Reverse<(u64, u64)>> = entries
            .into_iter()
            .filter(|Reverse((_, id))| inner.keys.contains_key(id))
            .collect();
        inner.heap = heap;
    }
}

pub(crate) struct Inner {
    pub(crate) config: RuntimeConfig,
    pub(crate) generation: u64,
    state: Mutex<State>,
    pub(crate) journal: Option<Journal>,
    pub(crate) journal_status: Mutex<JournalStatus>,
    pub(crate) timers: Timers,
    /// Event log bytes across all jobs.
    pub(crate) budget: Arc<EventBudget>,
    /// Streaming input bytes across all jobs.
    pub(crate) input_budget: Arc<EventBudget>,
    /// Result payload bytes alive anywhere in the control plane.
    pub(crate) result_budget: Arc<EventBudget>,
    pub(crate) me: Weak<Inner>,
}

/// The runtime lock. What the holder moved to `State::released` is dropped
/// after the lock is let go.
pub(crate) struct StateGuard<'a>(Option<MutexGuard<'a, State>>);

impl Deref for StateGuard<'_> {
    type Target = State;

    fn deref(&self) -> &State {
        self.0.as_ref().expect("lock held")
    }
}

impl DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut State {
        self.0.as_mut().expect("lock held")
    }
}

impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        if let Some(mut st) = self.0.take() {
            let released = std::mem::take(&mut st.released);
            drop(st);
            drop(released);
        }
    }
}

impl Inner {
    pub(crate) fn lock(&self) -> StateGuard<'_> {
        StateGuard(Some(self.state.lock().expect("runtime state poisoned")))
    }

    fn fire(&self, key: TimerKey) {
        let mut st = self.lock();
        let now = now_ms();
        match key {
            TimerKey::Dispatch => {}
            TimerKey::QueueDeadline { job_id, since } => {
                if st.jobs.get(&job_id).is_some_and(|job| {
                    matches!(job.phase, Phase::Queued) && job.queued_since_ms == since
                }) {
                    self.fail_job_locked(
                        &mut st,
                        &job_id,
                        codes::QUEUE_TIMEOUT,
                        "no worker took the job within its queue timeout",
                        now,
                    );
                }
            }
            TimerKey::ExecutionDeadline { job_id, attempt_id } => {
                if st.jobs.get(&job_id).is_some_and(|job| {
                    matches!(job.phase, Phase::Assigned | Phase::Running)
                        && job
                            .attempt
                            .as_ref()
                            .is_some_and(|a| a.attempt_id == attempt_id)
                }) {
                    self.attempt_ended_locked(
                        &mut st,
                        &job_id,
                        ErrorInfo::new(
                            codes::EXECUTION_TIMEOUT,
                            "attempt exceeded its execution timeout",
                            true,
                        ),
                        LeaseEnd::Cancel(codes::EXECUTION_TIMEOUT.to_string()),
                        None,
                        now,
                    );
                }
            }
            TimerKey::TotalDeadline { job_id } => self.total_deadline_locked(&mut st, &job_id, now),
            TimerKey::SessionGrace {
                session_id,
                connection,
            } => self.session_expired_locked(&mut st, &session_id, connection, now),
            TimerKey::CancelGrace { lease_id } => {
                if let Some(lease) = st.leases.get(&lease_id)
                    && lease.cancelling
                {
                    let session_id = lease.session_id.clone();
                    self.release_lease_locked(&mut st, &lease_id);
                    self.send_locked(
                        &mut st,
                        &session_id,
                        anyserve_proto::v2::server_message::Msg::Fence(anyserve_proto::v2::Fence {
                            lease_id: lease_id.clone(),
                            reason: "cancel_not_acknowledged".to_string(),
                        }),
                        now,
                    );
                }
            }
            TimerKey::RetryTerminal { job_id } => self.retry_terminal_locked(&mut st, &job_id, now),
        }
        self.dispatch_locked(&mut st, now);
    }

    fn fail_job_locked(&self, st: &mut State, job_id: &str, code: &str, message: &str, now: u64) {
        let lease = st
            .jobs
            .get(job_id)
            .and_then(|job| job.attempt.as_ref())
            .map(|attempt| attempt.lease_id.clone());
        self.finish_job_locked(
            st,
            job_id,
            TerminalPlan {
                state: JobState::Failed,
                outputs: Vec::new(),
                error: Some(ErrorInfo::new(code, message, false)),
                release_lease: None,
                cancel_lease: lease.map(|lease| (lease, code.to_string())),
                finished: None,
                previous: Box::new(Phase::Queued),
            },
            now,
        );
    }

    fn total_deadline_locked(&self, st: &mut State, job_id: &str, now: u64) {
        let Some(job) = st.jobs.get(job_id) else {
            return;
        };
        match job.phase {
            Phase::Queued | Phase::Assigned | Phase::Running => self.fail_job_locked(
                st,
                job_id,
                codes::TOTAL_TIMEOUT,
                "job exceeded its total timeout",
                now,
            ),
            // A commit is in flight; look again shortly.
            Phase::Accepting | Phase::Starting | Phase::Terminating(_) => self.timers.schedule(
                now.saturating_add(1_000),
                TimerKey::TotalDeadline {
                    job_id: job_id.to_string(),
                },
            ),
            Phase::Terminal(_) => {}
        }
    }

    fn session_expired_locked(&self, st: &mut State, session_id: &str, connection: u64, now: u64) {
        let Some(session) = st.sessions.get(session_id) else {
            return;
        };
        if session.tx.is_some() || session.connection != connection {
            return;
        }
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
                    ErrorInfo::new(codes::WORKER_LOST, "worker session ended", true),
                    LeaseEnd::Release,
                    None,
                    now,
                );
            }
        }
        st.sessions.remove(session_id);
    }

    /// Whether a timer can still have an effect.
    fn timer_live(st: &State, key: &TimerKey) -> bool {
        match key {
            TimerKey::Dispatch => true,
            TimerKey::QueueDeadline { job_id, since } => st.jobs.get(job_id).is_some_and(|job| {
                matches!(job.phase, Phase::Queued) && job.queued_since_ms == *since
            }),
            TimerKey::ExecutionDeadline { job_id, attempt_id } => {
                st.jobs.get(job_id).is_some_and(|job| {
                    job.attempt
                        .as_ref()
                        .is_some_and(|a| &a.attempt_id == attempt_id)
                })
            }
            TimerKey::TotalDeadline { job_id } => {
                st.jobs.get(job_id).is_some_and(|job| !job.is_terminal())
            }
            TimerKey::RetryTerminal { job_id } => st
                .jobs
                .get(job_id)
                .is_some_and(|job| matches!(job.phase, Phase::Terminating(_))),
            TimerKey::SessionGrace {
                session_id,
                connection,
            } => st
                .sessions
                .get(session_id)
                .is_some_and(|s| s.tx.is_none() && s.connection == *connection),
            TimerKey::CancelGrace { lease_id } => st
                .leases
                .get(lease_id)
                .is_some_and(|lease| lease.cancelling),
        }
    }

    fn maintain(&self) {
        let now = now_ms();
        {
            let mut st = self.lock();
            self.evict_terminal_locked(&mut st, now);
            let live = st.jobs.len() * 3 + st.sessions.len() + st.leases.len() + 1_024;
            if self.timers.len() > live * 2 {
                self.timers.compact(|key| Self::timer_live(&st, key));
            }
        }
        if matches!(
            *self.journal_status.lock().expect("journal status poisoned"),
            JournalStatus::Degraded(_)
        ) {
            let _ = self.commit(crate::journal::JournalOp::Probe, |_, _| {});
        }
    }

    async fn purge(&self) {
        let Some(journal) = &self.journal else {
            return;
        };
        let Some(retention) = self.config.journal.as_ref().map(|c| c.retention) else {
            return;
        };
        match journal.purge(now_ms().saturating_sub(ms(retention))).await {
            Ok(removed) if removed > 0 => tracing::info!(removed, "purged terminal journal jobs"),
            Ok(_) => {}
            Err(error) => tracing::warn!(error = %error, "journal purge failed"),
        }
    }

    fn recover(&self, recovery: Recovery) {
        let mut st = self.lock();
        let now = now_ms();
        for stored in recovery.active {
            let job_id = stored.job_id.clone();
            // Already accepted: reserve its input even beyond the budget.
            let payload = stored
                .spec
                .inputs
                .iter()
                .map(|o| o.inline_len())
                .sum::<usize>()
                + stored.spec.params.len();
            let input_permit = Permit::forced(Arc::clone(&self.input_budget), payload);
            self.insert_job(
                &mut st,
                job_id.clone(),
                stored.spec,
                Durability::Durable,
                stored.spec_hash,
                stored.max_attempts,
                None,
                input_permit,
                now,
            );
            let job = st.jobs.get_mut(&job_id).expect("recovered job");
            // Events of earlier generations are gone: older cursors get
            // `CursorExpired` rather than this generation's events.
            job.log.mark_lost_before_base();
            job.created_at_ms = stored.created_at_ms;
            job.attempt_count = stored.attempt_count;
            job.last_attempt_id = stored.last_attempt_id;
            job.last_worker_id = stored.last_worker_id;
            self.accepted_locked(&mut st, &job_id, now);
            let job = &st.jobs[&job_id];
            if job.attempt_count >= job.max_attempts {
                let message = format!(
                    "{} attempts were started before the control plane restarted",
                    job.attempt_count
                );
                self.fail_job_locked(&mut st, &job_id, codes::ATTEMPTS_EXHAUSTED, &message, now);
            }
        }
        self.dispatch_locked(&mut st, now);
    }
}

async fn timer_loop(me: Weak<Inner>) {
    loop {
        let Some(inner) = me.upgrade() else {
            return;
        };
        let now = now_ms();
        for key in inner.timers.pop_due(now) {
            inner.fire(key);
        }
        let notify = Arc::clone(&inner.timers.notify);
        let wait = inner
            .timers
            .next_due()
            .map(|at| Duration::from_millis(at.saturating_sub(now_ms())))
            .unwrap_or(TIMER_MAX_SLEEP)
            .min(TIMER_MAX_SLEEP);
        drop(inner);
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = notify.notified() => {}
        }
    }
}

async fn maintenance_loop(me: Weak<Inner>) {
    let mut last_purge = 0u64;
    loop {
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
        let Some(inner) = me.upgrade() else {
            return;
        };
        inner.maintain();
        let now = now_ms();
        if now.saturating_sub(last_purge) >= PURGE_INTERVAL_MS {
            last_purge = now;
            inner.purge().await;
        }
    }
}

/// Cancels an `Invoke` job when its `Execution` is dropped before the end.
struct CancelOnDrop {
    me: Weak<Inner>,
    job_id: String,
    log: Arc<JobLog>,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.log.set_primary(false);
        // The caller's own follower is already gone (the event stream drops
        // before this guard); nothing it left unread is needed any more.
        self.log.compact_if_unfollowed();
        // The usual case: the caller read through Finished. Nothing to cancel,
        // so the global lock is not needed.
        if self.log.is_finished() {
            return;
        }
        if let Some(inner) = self.me.upgrade() {
            let mut st = inner.lock();
            if let Some(job) = st.jobs.get_mut(&self.job_id)
                && !job.is_terminal()
            {
                job.cancel_requested
                    .get_or_insert_with(|| "caller disconnected".to_string());
                inner.cancel_locked(&mut st, &self.job_id, now_ms());
            }
        }
    }
}

/// Handle to the runtime. Cloning shares the same state.
#[derive(Clone)]
pub struct Runtime {
    pub(crate) inner: Arc<Inner>,
}

/// Called for each event just before a follower yields it.
type EventHook = Arc<dyn Fn(&ExecutionEvent) + Send + Sync>;

/// One reader of a job log. Its receiver counts it as a follower; when the
/// reader finishes or goes away, the last follower of a finished ephemeral job
/// compacts the log.
struct Follower {
    log: Arc<JobLog>,
    changes: Option<watch::Receiver<u64>>,
}

impl Follower {
    fn mark_seen(&mut self) {
        if let Some(changes) = self.changes.as_mut() {
            changes.borrow_and_update();
        }
    }

    async fn changed(&mut self) -> bool {
        match self.changes.as_mut() {
            Some(changes) => changes.changed().await.is_ok(),
            None => false,
        }
    }
}

impl Drop for Follower {
    fn drop(&mut self) {
        self.changes.take();
        self.log.compact_if_unfollowed();
    }
}

/// Follows `log` from `after` with a receiver subscribed by the caller (while
/// it held the runtime lock, so a follower is counted before its first poll).
/// With `skip_gaps`, events dropped from the log are skipped instead of ending
/// the stream with `CursorExpired`.
fn follow(
    log: Arc<JobLog>,
    changes: watch::Receiver<u64>,
    after: u64,
    on_yield: Option<EventHook>,
    skip_gaps: bool,
) -> EventStream {
    EventStream::new(async_stream::stream! {
        let mut follower = Follower {
            log: Arc::clone(&log),
            changes: Some(changes),
        };
        let mut after = after;
        loop {
            follower.mark_seen();
            match log.read_after(after) {
                Err(oldest) if skip_gaps => {
                    after = oldest - 1;
                    continue;
                }
                Err(oldest) => {
                    yield Err(RuntimeError::cursor_expired(oldest));
                    return;
                }
                Ok((events, finished)) => {
                    for event in events {
                        after = event.sequence;
                        if let Some(callback) = &on_yield {
                            callback(&event);
                        }
                        let last = matches!(event.kind, EventKind::Finished { .. });
                        yield Ok(event);
                        if last {
                            return;
                        }
                    }
                    if finished {
                        return;
                    }
                }
            }
            if !follower.changed().await {
                return;
            }
        }
    })
}

fn view_from_stored(stored: StoredJob, include_outputs: bool) -> JobView {
    JobView {
        job_id: stored.job_id,
        state: stored.state,
        durability: Durability::Durable,
        interface: stored.spec.demand.interface.clone(),
        attempt_count: stored.attempt_count,
        current_attempt_id: stored.last_attempt_id,
        current_worker_id: stored.last_worker_id,
        created_at_ms: stored.created_at_ms,
        updated_at_ms: stored.updated_at_ms,
        error: stored.error,
        outputs: include_outputs.then_some(stored.outputs),
        metadata: stored.spec.metadata,
        spec_hash: stored.spec_hash,
    }
}

impl Runtime {
    /// Starts the runtime, recovering durable jobs when a journal is
    /// configured.
    pub async fn start(config: RuntimeConfig) -> anyhow::Result<Runtime> {
        let (journal, recovery) = match &config.journal {
            Some(journal_config) => {
                let (journal, recovery) = Journal::open(journal_config).await?;
                (Some(journal), Some(recovery))
            }
            None => (None, None),
        };
        let generation = recovery
            .as_ref()
            .map(|recovery| recovery.generation)
            // Without a journal nothing survives a restart; a timestamp in
            // seconds still distinguishes this process from the previous one
            // and keeps event sequences (`generation << 32`) in range.
            .unwrap_or_else(|| now_ms() / 1000);
        let status = if journal.is_some() {
            JournalStatus::Healthy
        } else {
            JournalStatus::Disabled
        };
        let event_budget = config.max_event_bytes;
        let input_budget = config.max_pending_bytes;
        let result_budget = config.max_result_bytes;
        let inner = Arc::new_cyclic(|me| Inner {
            generation,
            state: Mutex::new(State {
                generation,
                rng: generation ^ 0x5DEE_CE66_D1CE_4E5B,
                ..State::default()
            }),
            journal,
            journal_status: Mutex::new(status),
            timers: Timers::new(),
            budget: Arc::new(EventBudget::new(event_budget)),
            input_budget: Arc::new(EventBudget::new(input_budget)),
            result_budget: Arc::new(EventBudget::new(result_budget)),
            me: me.clone(),
            config,
        });
        if let Some(recovery) = recovery {
            inner.recover(recovery);
        }
        tokio::spawn(timer_loop(Arc::downgrade(&inner)));
        tokio::spawn(maintenance_loop(Arc::downgrade(&inner)));
        Ok(Runtime { inner })
    }

    pub fn config(&self) -> &RuntimeConfig {
        &self.inner.config
    }

    pub fn generation(&self) -> u64 {
        self.inner.generation
    }

    pub(crate) fn result_budget(&self) -> Arc<EventBudget> {
        Arc::clone(&self.inner.result_budget)
    }

    fn normalize(
        &self,
        mut spec: JobSpec,
        durability: Durability,
    ) -> Result<(JobSpec, u32), RuntimeError> {
        let limits = &self.inner.config.limits;
        if spec.demand.interface.trim().is_empty() {
            return Err(RuntimeError::invalid("demand.interface is required"));
        }
        if spec.demand.interface.len() > limits.max_identifier_bytes as usize {
            return Err(RuntimeError::invalid(format!(
                "demand.interface exceeds {} bytes",
                limits.max_identifier_bytes
            )));
        }
        if spec.inputs.len() > limits.max_objects as usize {
            return Err(RuntimeError::invalid(format!(
                "a job may carry at most {} input objects",
                limits.max_objects
            )));
        }
        let mut total = spec.params.len() as u64;
        for input in &spec.inputs {
            let size = input.inline_len() as u64;
            if size > limits.max_inline_object_bytes {
                return Err(RuntimeError::invalid(format!(
                    "input object of {size} bytes exceeds {} bytes",
                    limits.max_inline_object_bytes
                )));
            }
            total += size;
        }
        if total > limits.max_inline_total_bytes {
            return Err(RuntimeError::invalid(format!(
                "inputs and params total {total} bytes, above {}",
                limits.max_inline_total_bytes
            )));
        }
        let metadata_bytes =
            |m: &api::Metadata| m.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
        let metadata = spec.demand.interface.len()
            + metadata_bytes(&spec.metadata)
            + metadata_bytes(&spec.demand.required_attributes)
            + metadata_bytes(&spec.demand.preferred_attributes)
            + spec
                .demand
                .required_capacity
                .keys()
                .map(String::len)
                .sum::<usize>()
            + spec
                .inputs
                .iter()
                .map(|o| metadata_bytes(o.metadata()))
                .sum::<usize>();
        if metadata > limits.max_metadata_bytes as usize {
            return Err(RuntimeError::invalid(format!(
                "metadata and attributes total {metadata} bytes, above {}",
                limits.max_metadata_bytes
            )));
        }
        if spec
            .demand
            .required_capacity
            .values()
            .any(|value| *value < 0)
        {
            return Err(RuntimeError::invalid(
                "required capacity cannot be negative",
            ));
        }
        spec.policy.durability = Some(durability);
        spec.policy.priority = spec
            .policy
            .priority
            .min(self.inner.config.priority_levels.saturating_sub(1));
        let max_attempts = spec
            .policy
            .max_attempts
            .filter(|attempts| *attempts > 0)
            .unwrap_or(self.inner.config.default_max_attempts)
            .clamp(1, self.inner.config.max_attempts_limit.max(1));
        Ok((spec, max_attempts))
    }

    /// Accepts a job. Durable jobs return once acceptance is committed.
    pub async fn submit(&self, request: SubmitRequest) -> Result<SubmitOutcome, RuntimeError> {
        let durability = request
            .spec
            .policy
            .durability
            .unwrap_or(Durability::Durable);
        let (spec, max_attempts) = self.normalize(request.spec, durability)?;
        let requested = request.job_id.filter(|id| !id.trim().is_empty());
        // The fingerprint serves idempotent resubmission (caller-chosen ids)
        // and the journal (durable jobs); other jobs skip the hashing.
        let spec_hash = if requested.is_some() || durability == Durability::Durable {
            identity::spec_hash(&spec, durability)
        } else {
            String::new()
        };
        let mut spec = Some(spec);
        if requested
            .as_ref()
            .is_some_and(|id| id.len() > self.inner.config.limits.max_identifier_bytes as usize)
        {
            return Err(RuntimeError::invalid("job_id is too long"));
        }
        // A caller-chosen id may belong to a durable job that already left
        // memory; generated ids cannot.
        let mut cold_checked = requested.is_none() || self.inner.journal.is_none();
        let job_id = requested.unwrap_or_else(new_id);
        let conflict = |existing: &str| -> Result<(), RuntimeError> {
            if existing.is_empty() {
                return Err(RuntimeError::conflict(format!(
                    "job '{job_id}' exists and was not created by an idempotent submission"
                )));
            }
            if existing != spec_hash {
                return Err(RuntimeError::conflict(format!(
                    "job '{job_id}' exists with a different specification"
                )));
            }
            if request.conflict == api::ConflictPolicy::Fail {
                return Err(RuntimeError::conflict(format!(
                    "job '{job_id}' already exists"
                )));
            }
            Ok(())
        };
        enum Next {
            Done(Box<SubmitOutcome>),
            Wait(oneshot::Receiver<Result<JobView, RuntimeError>>, bool),
            ColdCheck,
        }
        loop {
            let next = {
                let mut st = self.inner.lock();
                let now = now_ms();
                if let Some(job) = st.jobs.get_mut(&job_id) {
                    if job.indeterminate {
                        return Err(RuntimeError::unavailable(format!(
                            "job '{job_id}' has an unknown commit outcome until restart"
                        )));
                    }
                    conflict(&job.spec_hash)?;
                    if !matches!(job.phase, Phase::Accepting) {
                        Next::Done(Box::new(SubmitOutcome {
                            job: job.view(false),
                            created: false,
                        }))
                    } else {
                        let (tx, rx) = oneshot::channel();
                        job.accept_waiters.push(tx);
                        Next::Wait(rx, false)
                    }
                } else if !cold_checked {
                    Next::ColdCheck
                } else {
                    if durability == Durability::Durable && !self.inner.journal_writable() {
                        return Err(match self.inner.journal {
                            None => {
                                RuntimeError::unavailable("durable execution requires a journal")
                            }
                            Some(_) => RuntimeError::unavailable(format!(
                                "journal is unavailable: {:?}",
                                self.inner.journal_health()
                            )),
                        });
                    }
                    let spec = spec.take().expect("a job is inserted at most once");
                    let input_permit = self.inner.admit(&st, &spec)?;
                    self.inner.insert_job(
                        &mut st,
                        job_id.clone(),
                        spec,
                        durability,
                        spec_hash.clone(),
                        max_attempts,
                        None,
                        input_permit,
                        now,
                    );
                    if durability == Durability::Ephemeral {
                        let view = st.jobs[&job_id].view(false);
                        self.inner.dispatch_locked(&mut st, now);
                        Next::Done(Box::new(SubmitOutcome {
                            job: view,
                            created: true,
                        }))
                    } else {
                        let (tx, rx) = oneshot::channel();
                        st.jobs
                            .get_mut(&job_id)
                            .expect("job")
                            .accept_waiters
                            .push(tx);
                        if let Err(error) = self.inner.begin_accept(&mut st, &job_id) {
                            self.inner.remove_unaccepted_locked(
                                &mut st,
                                &job_id,
                                &error.to_string(),
                            );
                            return Err(error);
                        }
                        Next::Wait(rx, true)
                    }
                }
            };
            let (receiver, created) = match next {
                Next::Done(outcome) => return Ok(*outcome),
                Next::ColdCheck => {
                    let journal = self.inner.journal.as_ref().expect("journal");
                    let stored = journal.spec_hash(&job_id).await.map_err(|error| {
                        RuntimeError::unavailable(format!("read journal: {error:#}"))
                    })?;
                    if let Some(existing) = stored {
                        conflict(&existing)?;
                        return Ok(SubmitOutcome {
                            job: self.get_job(&job_id, false).await?,
                            created: false,
                        });
                    }
                    cold_checked = true;
                    continue;
                }
                Next::Wait(receiver, created) => (receiver, created),
            };
            let job = receiver
                .await
                .map_err(|_| RuntimeError::unavailable("runtime stopped"))??;
            return Ok(SubmitOutcome { job, created });
        }
    }

    /// Starts an ephemeral execution whose events the caller follows.
    pub async fn invoke(&self, request: InvokeRequest) -> Result<Execution, RuntimeError> {
        let (spec, max_attempts) = self.normalize(request.spec, Durability::Ephemeral)?;
        let job_id = new_id();
        let limits = &self.inner.config.limits;
        let input = (request.input == InputMode::Streaming).then(|| {
            let used = spec
                .inputs
                .iter()
                .map(|o| o.inline_len() as u64)
                .sum::<u64>()
                + spec.params.len() as u64;
            Arc::new(InputLog::new(
                limits.max_inline_total_bytes.saturating_sub(used),
                Arc::clone(&self.inner.input_budget),
            ))
        });
        let (log, changes) = {
            let mut st = self.inner.lock();
            let now = now_ms();
            let input_permit = self.inner.admit(&st, &spec)?;
            self.inner.insert_job(
                &mut st,
                job_id.clone(),
                spec,
                Durability::Ephemeral,
                String::new(),
                max_attempts,
                input.clone(),
                input_permit,
                now,
            );
            let job = st.jobs.get_mut(&job_id).expect("job");
            job.streamed = true;
            let log = Arc::clone(&job.log);
            log.set_primary(true);
            let changes = log.subscribe();
            self.inner.dispatch_locked(&mut st, now);
            (log, changes)
        };
        // A streamed consumer's reading position releases back-pressure.
        let on_yield = Some({
            let log = Arc::clone(&log);
            Arc::new(move |event: &ExecutionEvent| log.mark_consumed(event.sequence)) as EventHook
        });
        Ok(Execution {
            job_id: job_id.clone(),
            events: follow(Arc::clone(&log), changes, 0, on_yield, false),
            input: input.map(|log| InputSink { log }),
            _guard: Box::new(CancelOnDrop {
                me: Arc::downgrade(&self.inner),
                job_id,
                log,
            }),
        })
    }

    /// Follows a job's events after `after_sequence` until it finishes.
    /// `after_sequence = 0` starts at the oldest event still held and never
    /// reports `CursorExpired`; a non-zero sequence older than what is held
    /// ends the stream with `CursorExpired{oldest}`.
    pub fn watch(&self, job_id: &str, after_sequence: u64) -> Result<EventStream, RuntimeError> {
        let st = self.inner.lock();
        if let Some(job) = st.jobs.get(job_id) {
            let changes = job.log.subscribe();
            return Ok(follow(
                Arc::clone(&job.log),
                changes,
                after_sequence,
                None,
                false,
            ));
        }
        if self.inner.journal.is_none() {
            return Err(RuntimeError::not_found("job", job_id));
        }
        drop(st);
        // Possibly an evicted durable job: replay its acceptance and outcome.
        let runtime = self.clone();
        let job_id = job_id.to_string();
        Ok(EventStream::new(async_stream::stream! {
            match runtime.get_job(&job_id, true).await {
                Err(error) => yield Err(error),
                Ok(job) => {
                    for event in runtime.replayed_events(job, after_sequence) {
                        yield Ok(event);
                    }
                }
            }
        }))
    }

    /// Events after `after_sequence`, and whether the job's log has ended.
    /// Evicted durable jobs are read back from the journal as their
    /// acceptance and outcome. `after_sequence = 0` starts at the oldest event
    /// still held and never fails with `CursorExpired`; a non-zero sequence
    /// older than what is held gets `CursorExpired{oldest}`.
    pub async fn events(
        &self,
        job_id: &str,
        after_sequence: u64,
    ) -> Result<(Vec<ExecutionEvent>, bool), RuntimeError> {
        let log = {
            let st = self.inner.lock();
            match st.jobs.get(job_id) {
                Some(job) if job.indeterminate => {
                    return Err(RuntimeError::unavailable(format!(
                        "job '{job_id}' has an unknown commit outcome until restart"
                    )));
                }
                Some(job) => Some(Arc::clone(&job.log)),
                None => None,
            }
        };
        if let Some(log) = log {
            return log
                .read_after(after_sequence)
                .map_err(RuntimeError::cursor_expired);
        }
        if self.inner.journal.is_none() {
            return Err(RuntimeError::not_found("job", job_id));
        }
        let job = self.get_job(job_id, true).await?;
        Ok((self.replayed_events(job, after_sequence), true))
    }

    /// The specification of a job held in memory. Inputs and params are
    /// dropped once the job is terminal.
    pub fn spec(&self, job_id: &str) -> Result<Arc<JobSpec>, RuntimeError> {
        let st = self.inner.lock();
        st.jobs
            .get(job_id)
            .map(|job| Arc::clone(&job.spec))
            .ok_or_else(|| RuntimeError::not_found("job", job_id))
    }

    /// Acceptance and outcome of a job known only to the journal.
    fn replayed_events(&self, job: JobView, after_sequence: u64) -> Vec<ExecutionEvent> {
        // Finished takes the generation's last sequence: above anything the
        // job's live log handed out, below the next generation.
        let base = self.inner.generation << 32;
        [
            (base + 1, EventKind::Accepted),
            (
                base + 0xFFFF_FFFF,
                EventKind::Finished {
                    state: job.state,
                    outputs: job.outputs.unwrap_or_default(),
                    error: job.error,
                },
            ),
        ]
        .into_iter()
        .filter(|(sequence, _)| *sequence > after_sequence)
        .map(|(sequence, kind)| ExecutionEvent {
            job_id: job.job_id.clone(),
            sequence,
            attempt_id: job.current_attempt_id.clone(),
            at_ms: job.updated_at_ms,
            kind,
        })
        .collect()
    }

    pub async fn get_job(
        &self,
        job_id: &str,
        include_outputs: bool,
    ) -> Result<JobView, RuntimeError> {
        {
            let st = self.inner.lock();
            if let Some(job) = st.jobs.get(job_id) {
                if job.indeterminate {
                    return Err(RuntimeError::unavailable(format!(
                        "job '{job_id}' has an unknown commit outcome until restart"
                    )));
                }
                return Ok(job.view(include_outputs));
            }
        }
        let journal = self
            .inner
            .journal
            .as_ref()
            .ok_or_else(|| RuntimeError::not_found("job", job_id))?;
        match journal.load_job(job_id, include_outputs).await {
            Ok(Some(stored)) => Ok(view_from_stored(stored, include_outputs)),
            Ok(None) => Err(RuntimeError::not_found("job", job_id)),
            Err(error) => Err(RuntimeError::unavailable(format!(
                "read journal: {error:#}"
            ))),
        }
    }

    /// Cancels a job and returns its final view. Durable jobs return once the
    /// cancellation is committed.
    pub async fn cancel(&self, job_id: &str, reason: String) -> Result<JobView, RuntimeError> {
        let receiver = {
            let mut st = self.inner.lock();
            let now = now_ms();
            match st.jobs.get_mut(job_id) {
                None => None,
                Some(_) => Some(Self::start_cancel(
                    &self.inner,
                    &mut st,
                    job_id,
                    reason,
                    now,
                )?),
            }
        };
        match receiver {
            None => self.get_job(job_id, false).await,
            Some(receiver) => receiver
                .await
                .map_err(|_| RuntimeError::unavailable("runtime stopped"))?,
        }
    }

    fn start_cancel(
        inner: &Inner,
        st: &mut State,
        job_id: &str,
        reason: String,
        now: u64,
    ) -> Result<oneshot::Receiver<Result<JobView, RuntimeError>>, RuntimeError> {
        {
            let job = st.jobs.get_mut(job_id).expect("job");
            if job.indeterminate {
                return Err(RuntimeError::unavailable(format!(
                    "job '{job_id}' has an unknown commit outcome until restart"
                )));
            }
            let (tx, rx) = oneshot::channel();
            if job.is_terminal() {
                let _ = tx.send(Ok(job.view(false)));
                return Ok(rx);
            }
            let reason = if reason.trim().is_empty() {
                "cancelled".to_string()
            } else {
                reason
            };
            job.cancel_requested.get_or_insert(reason);
            job.waiters.push(tx);
            inner.cancel_locked(st, job_id, now);
            Ok(rx)
        }
    }

    pub async fn list_jobs(&self, filter: JobFilter) -> Result<JobPage, RuntimeError> {
        let offset = match filter.page_token.as_deref().filter(|t| !t.is_empty()) {
            Some(token) => token
                .parse::<usize>()
                .map_err(|_| RuntimeError::invalid("invalid page_token"))?,
            None => 0,
        };
        let limit = if filter.limit == 0 {
            DEFAULT_PAGE_SIZE
        } else {
            filter.limit
        };
        let st = self.inner.lock();
        let mut jobs: Vec<&state::Job> = st
            .jobs
            .values()
            .filter(|job| filter.states.is_empty() || filter.states.contains(&job.state()))
            .filter(|job| {
                filter
                    .interface
                    .as_ref()
                    .is_none_or(|interface| &job.spec.demand.interface == interface)
            })
            .collect();
        jobs.sort_by(|a, b| {
            b.updated_at_ms
                .cmp(&a.updated_at_ms)
                .then_with(|| a.id.cmp(&b.id))
        });
        let total = jobs.len();
        let page = jobs
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|job| job.view(false))
            .collect();
        Ok(JobPage {
            jobs: page,
            next_page_token: (offset.saturating_add(limit) < total)
                .then(|| (offset + limit).to_string()),
        })
    }

    pub fn list_workers(&self) -> Vec<WorkerView> {
        let st = self.inner.lock();
        let mut workers: Vec<WorkerView> = st
            .sessions
            .values()
            .map(|session| WorkerView {
                session_id: session.id.clone(),
                worker_id: session.worker_id.clone(),
                interfaces: session.interfaces.iter().cloned().collect(),
                attributes: session.attributes.clone(),
                total_capacity: session.total_capacity.clone(),
                max_concurrency: session.max_concurrency,
                active_leases: session.active.len() as u32,
                free_slots: session.free_slots(),
                load: session.load.clone(),
                draining: session.draining,
                connected: session.tx.is_some(),
                connected_at_ms: session.connected_at_ms,
                last_seen_ms: session.last_seen_ms,
            })
            .collect();
        workers.sort_by(|a, b| {
            a.worker_id
                .cmp(&b.worker_id)
                .then(a.session_id.cmp(&b.session_id))
        });
        workers
    }

    pub fn health(&self) -> Health {
        let journal = self.inner.journal_health();
        let st = self.inner.lock();
        Health {
            live: true,
            ready: !st.shutting_down
                && matches!(
                    journal,
                    api::JournalHealth::Disabled | api::JournalHealth::Healthy
                ),
            journal,
        }
    }

    pub fn stats(&self) -> RuntimeStats {
        let now = now_ms();
        let st = self.inner.lock();
        let mut interfaces: BTreeMap<String, InterfaceStats> = BTreeMap::new();
        let (mut queued, mut running) = (0, 0);
        let (mut succeeded, mut failed, mut cancelled) = (0, 0, 0);
        for job in st.jobs.values() {
            let entry = interfaces
                .entry(job.spec.demand.interface.clone())
                .or_default();
            match job.state() {
                JobState::Queued => {
                    queued += 1;
                    entry.queued += 1;
                    entry.oldest_queued_age_ms = entry
                        .oldest_queued_age_ms
                        .max(now.saturating_sub(job.queued_since_ms));
                }
                JobState::Running => {
                    running += 1;
                    entry.running += 1;
                }
                JobState::Succeeded => succeeded += 1,
                JobState::Failed => failed += 1,
                JobState::Cancelled => cancelled += 1,
            }
        }
        for session in st.sessions.values() {
            for interface in &session.interfaces {
                let entry = interfaces.entry(interface.clone()).or_default();
                entry.sessions += 1;
                entry.free_slots += session.free_slots() as u64;
            }
        }
        RuntimeStats {
            generation: st.generation,
            queued,
            running,
            succeeded,
            failed,
            cancelled,
            retained_jobs: st.terminal_order.len(),
            sessions: st.sessions.len(),
            interfaces,
            journal: self.inner.journal.as_ref().map(|journal| journal.stats()),
            input_bytes: self.inner.input_budget.used(),
            result_bytes: self.inner.result_budget.used(),
            event_bytes: self.inner.budget.used(),
        }
    }

    /// Stops accepting work, asks workers to drain and closes the journal
    /// after its queued commits.
    pub async fn shutdown(&self) {
        {
            let mut st = self.inner.lock();
            st.shutting_down = true;
            let now = now_ms();
            let sessions: Vec<String> = st.sessions.keys().cloned().collect();
            for session_id in sessions {
                self.inner.send_locked(
                    &mut st,
                    &session_id,
                    anyserve_proto::v2::server_message::Msg::Drain(anyserve_proto::v2::Drain {}),
                    now,
                );
            }
        }
        if let Some(journal) = &self.inner.journal {
            journal.close().await;
        }
    }
}
