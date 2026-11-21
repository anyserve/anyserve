//! The execution runtime: one in-memory state machine for ephemeral and
//! durable jobs, worker sessions and attempt streams. Durable jobs commit to a
//! pluggable journal backend (local SQLite or single-leader PostgreSQL).

mod analytics;
pub mod api;
pub(crate) mod events;
pub(crate) mod identity;
mod index;
pub(crate) mod input;
pub(crate) mod machine;
pub(crate) mod state;
pub(crate) mod worker;

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use tokio::sync::{Notify, oneshot, watch};

use self::api::{
    Durability, ErrorInfo, EventKind, EventStream, Execution, ExecutionEvent, Health, InputMode,
    InputSink, InterfaceStats, InvokeRequest, JobFilter, JobInspection, JobPage, JobPreview,
    JobSpec, JobState, JobView, ObjectPreview, PoolStats, RuntimeConfig, RuntimeStats,
    SubmitOutcome, SubmitRequest, WorkerView, codes,
};
use self::events::{ByteBudget, JobLog, Permit};
use self::input::InputLog;
use self::machine::{LeaseEnd, ms};
use self::state::{Phase, State, TerminalPlan};
use crate::auth::{DEFAULT_NAMESPACE, validate_namespace};
use crate::error::RuntimeError;
use crate::journal::{JournalBackend, Leadership, LeaseHandle, Recovery, SqliteJournal, StoredJob};
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

/// Cancelled entries the heap may hold before it is rebuilt without them.
const TIMER_COMPACT_SLACK: usize = 4_096;

#[derive(Default)]
struct TimerHeap {
    /// Due time and id; an id missing from `keys` was cancelled or fired.
    heap: BinaryHeap<Reverse<(u64, u64)>>,
    keys: HashMap<u64, TimerKey>,
    /// Cancelled ids whose heap entries are still in place.
    cancelled: HashSet<u64>,
    next: u64,
}

/// Timers are scheduled and cancelled by lock holders in O(log n) and O(1);
/// the timer loop alone pops them, and it alone rebuilds the heap when
/// cancelled entries pile up, without the runtime lock.
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

    fn lock(&self) -> MutexGuard<'_, TimerHeap> {
        self.inner.lock().expect("timers poisoned")
    }

    /// Schedules `key` and returns the id that `cancel` takes.
    pub(crate) fn schedule(&self, at_ms: u64, key: TimerKey) -> u64 {
        let mut inner = self.lock();
        inner.next += 1;
        let id = inner.next;
        let earliest = inner.heap.peek().map(|Reverse((at, _))| *at);
        inner.heap.push(Reverse((at_ms, id)));
        inner.keys.insert(id, key);
        drop(inner);
        if earliest.is_none_or(|earliest| at_ms < earliest) {
            self.notify.notify_one();
        }
        id
    }

    /// Forgets timers that can no longer have an effect. Ids that already
    /// fired are ignored.
    pub(crate) fn cancel(&self, ids: &[u64]) {
        if ids.is_empty() {
            return;
        }
        let mut inner = self.lock();
        for id in ids {
            if inner.keys.remove(id).is_some() {
                inner.cancelled.insert(*id);
            }
        }
    }

    fn pop_due(&self, now: u64) -> Vec<TimerKey> {
        let mut inner = self.lock();
        let mut due = Vec::new();
        while let Some(Reverse((at, id))) = inner.heap.peek().copied() {
            if at > now {
                break;
            }
            inner.heap.pop();
            match inner.keys.remove(&id) {
                Some(key) => due.push(key),
                None => {
                    inner.cancelled.remove(&id);
                }
            }
        }
        due
    }

    #[cfg(test)]
    fn compact(&self) {
        let _ = self.compact_and_next_due();
    }

    /// Rebuilds the heap without cancelled entries once they outnumber the
    /// live ones and returns the next due timestamp. The heap is filtered
    /// outside the timers lock so that lock holders scheduling meanwhile are
    /// not held up; their entries land in a fresh heap that is merged back
    /// afterwards. Ids cancelled meanwhile stay in `cancelled` for the next
    /// rebuild.
    fn compact_and_next_due(&self) -> Option<u64> {
        let (mut heap, cancelled) = {
            let mut inner = self.lock();
            if inner.cancelled.len() <= inner.keys.len() + TIMER_COMPACT_SLACK {
                return inner.heap.peek().map(|Reverse((at, _))| *at);
            }
            (
                std::mem::take(&mut inner.heap),
                std::mem::take(&mut inner.cancelled),
            )
        };
        heap.retain(|Reverse((_, id))| !cancelled.contains(id));
        let mut inner = self.lock();
        inner.heap.append(&mut heap);
        inner.heap.peek().map(|Reverse((at, _))| *at)
    }
}

pub(crate) struct Inner {
    pub(crate) config: RuntimeConfig,
    pub(crate) generation: AtomicU64,
    pub(crate) leader_active: AtomicBool,
    pub(crate) lease_lost: AtomicBool,
    pub(crate) leadership_tx: watch::Sender<bool>,
    pub(crate) lease_lost_tx: watch::Sender<bool>,
    pub(crate) stop_tx: watch::Sender<bool>,
    pub(crate) lease_handle: Mutex<Option<LeaseHandle>>,
    state: Mutex<State>,
    pub(crate) journal: Option<Arc<dyn JournalBackend>>,
    pub(crate) journal_status: Mutex<JournalStatus>,
    pub(crate) timers: Timers,
    /// Event log bytes across all jobs.
    pub(crate) event_budget: Arc<ByteBudget>,
    /// Streaming input bytes across all jobs.
    pub(crate) input_budget: Arc<ByteBudget>,
    /// Result payload bytes alive anywhere in the control plane.
    pub(crate) result_budget: Arc<ByteBudget>,
    pub(crate) me: Weak<Inner>,
}

/// The runtime lock. What the holder moved to `State::released` is dropped
/// after the lock is let go.
pub(crate) struct StateGuard<'a> {
    state: Option<MutexGuard<'a, State>>,
}

impl Deref for StateGuard<'_> {
    type Target = State;

    fn deref(&self) -> &State {
        self.state.as_ref().expect("lock held")
    }
}

impl DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut State {
        self.state.as_mut().expect("lock held")
    }
}

impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        if let Some(mut st) = self.state.take() {
            let released = std::mem::take(&mut st.released);
            drop(st);
            drop(released);
        }
    }
}

impl Inner {
    pub(crate) fn lock(&self) -> StateGuard<'_> {
        StateGuard {
            state: Some(self.state.lock().expect("runtime state poisoned")),
        }
    }

    #[cfg(test)]
    fn fire(&self, key: TimerKey) {
        let mut st = self.lock();
        let now = now_ms();
        self.fire_one_locked(&mut st, key, now);
        self.dispatch_locked(&mut st, now);
    }

    fn fire_due(&self, keys: Vec<TimerKey>) {
        if keys.is_empty() {
            return;
        }
        let mut iter = keys.into_iter().peekable();
        while iter.peek().is_some() {
            let mut st = self.lock();
            let now = now_ms();
            let mut handled = 0usize;
            for key in iter.by_ref() {
                if matches!(key, TimerKey::Dispatch) {
                    continue;
                }
                self.fire_one_locked(&mut st, key, now);
                handled += 1;
                if handled >= 64 {
                    break;
                }
            }
            self.dispatch_locked(&mut st, now);
        }
    }

    fn fire_one_locked(&self, st: &mut State, key: TimerKey, now: u64) {
        match key {
            TimerKey::Dispatch => {}
            TimerKey::QueueDeadline { job_id, since } => {
                if st.jobs.get(&job_id).is_some_and(|job| {
                    matches!(job.phase, Phase::Queued) && job.queued_since_ms == since
                }) {
                    self.fail_job_locked(
                        st,
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
                        st,
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
            TimerKey::TotalDeadline { job_id } => self.total_deadline_locked(st, &job_id, now),
            TimerKey::SessionGrace {
                session_id,
                connection,
            } => self.session_expired_locked(st, &session_id, connection, now),
            TimerKey::CancelGrace { lease_id } => {
                if let Some(lease) = st.leases.get(&lease_id)
                    && lease.cancelling
                {
                    let session_id = lease.session_id.clone();
                    self.release_lease_locked(st, &lease_id);
                    self.send_locked(
                        st,
                        &session_id,
                        anyserve_proto::v2::server_message::Msg::Fence(anyserve_proto::v2::Fence {
                            lease_id: lease_id.clone(),
                            reason: "cancel_not_acknowledged".to_string(),
                        }),
                        now,
                    );
                }
            }
            TimerKey::RetryTerminal { job_id } => self.retry_terminal_locked(st, &job_id, now),
        }
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
                cancel_lease: lease.map(|lease| (lease, code.to_string())),
                ..TerminalPlan::new(JobState::Failed, Some(ErrorInfo::new(code, message, false)))
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
            // A commit is in flight; look again shortly. This retry is not
            // tracked by the job: it fires or expires within a second.
            Phase::Accepting | Phase::Starting | Phase::Terminating(_) => {
                self.timers.schedule(
                    now.saturating_add(1_000),
                    TimerKey::TotalDeadline {
                        job_id: job_id.to_string(),
                    },
                );
            }
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
        st.remove_session(session_id);
    }

    fn maintain(&self) {
        let now = now_ms();
        {
            let mut st = self.lock();
            self.evict_terminal_locked(&mut st, now);
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
        if !self.leader_active.load(Ordering::Acquire) {
            return;
        }
        let Some(retention) = self.config.journal.as_ref().map(|c| c.retention) else {
            return;
        };
        let term = self.generation.load(Ordering::Acquire);
        match journal
            .purge(term, now_ms().saturating_sub(ms(retention)))
            .await
        {
            Ok(removed) if removed > 0 => tracing::info!(removed, "purged terminal journal jobs"),
            Ok(_) => {}
            Err(error) => tracing::warn!(error = %error, "journal purge failed"),
        }
    }

    pub(crate) fn activate_leadership(&self, leadership: Leadership) {
        if *self.stop_tx.borrow() || self.lease_lost.load(Ordering::Acquire) {
            let lease = leadership.lease;
            tokio::spawn(async move {
                lease.relinquish().await;
            });
            return;
        }
        let term = leadership.term;
        self.generation.store(term, Ordering::Release);
        {
            let mut st = self.lock();
            st.generation = term;
            st.rng = term ^ 0x5DEE_CE66_D1CE_4E5B;
        }
        self.recover(leadership.recovery);
        let lost_rx = leadership.lease.lost();
        *self
            .lease_handle
            .lock()
            .expect("lease handle mutex poisoned") = Some(leadership.lease);
        *self.journal_status.lock().expect("journal status poisoned") = JournalStatus::Healthy;
        self.leader_active.store(true, Ordering::Release);
        self.leadership_tx.send_replace(true);
        tokio::spawn(lease_loss_watcher(
            self.me.clone(),
            lost_rx,
            self.stop_tx.subscribe(),
        ));
    }

    pub(crate) fn mark_lease_lost(&self, reason: &str) {
        if self.lease_lost.swap(true, Ordering::AcqRel) {
            return;
        }
        tracing::error!(reason = %reason, "control plane lost leadership lease; fencing local state");
        self.leader_active.store(false, Ordering::Release);
        self.leadership_tx.send_replace(false);
        self.stop_tx.send_replace(true);
        *self.journal_status.lock().expect("journal status poisoned") =
            JournalStatus::Failed(reason.to_string());
        {
            let mut st = self.lock();
            st.shutting_down = true;
            for session in st.sessions.values_mut() {
                session.tx = None;
            }
            let err = RuntimeError::unavailable(format!("control plane lost leadership: {reason}"));
            for job in st.jobs.values_mut() {
                for waiter in job.accept_waiters.drain(..).chain(job.waiters.drain(..)) {
                    let _ = waiter.send(Err(err.clone()));
                }
                for waiter in job.finish_waiters.drain(..) {
                    let _ = waiter.send(Err(err.clone()));
                }
            }
        }
        self.lease_lost_tx.send_replace(true);
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
                // Creation time is immutable once the read index is populated.
                // accepted_locked below resets queue and update time to now.
                stored.created_at_ms,
            );
            let job = st.jobs.get_mut(&job_id).expect("recovered job");
            // Events of earlier generations are gone: older cursors get
            // `CursorExpired` rather than this generation's events.
            job.log.mark_lost_before_base();
            job.attempt_count = stored.attempt_count;
            job.last_attempt_id = stored.last_attempt_id;
            job.last_worker_id = stored.last_worker_id.clone();
            job.failed_workers = stored.failed_workers;
            if stored.attempt_count > 0
                && let Some(worker_id) = stored.last_worker_id
            {
                job.failed_workers.insert(worker_id);
            }
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

async fn standby_acquire_loop(
    me: Weak<Inner>,
    backend: Arc<dyn JournalBackend>,
    stop: watch::Receiver<bool>,
) {
    match backend.acquire_leadership(stop).await {
        Ok(Some(leadership)) => {
            if let Some(inner) = me.upgrade() {
                tracing::info!(term = leadership.term, "acquired control-plane leadership");
                inner.activate_leadership(leadership);
            } else {
                leadership.lease.relinquish().await;
            }
        }
        Ok(None) => {}
        Err(error) => {
            if let Some(inner) = me.upgrade() {
                inner.mark_lease_lost(&format!("leadership acquisition failed: {error:#}"));
            }
        }
    }
}

async fn lease_loss_watcher(
    me: Weak<Inner>,
    mut lost: watch::Receiver<bool>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        if *lost.borrow() {
            if let Some(inner) = me.upgrade() {
                inner.mark_lease_lost("leadership lease expired or renewal failed");
            }
            return;
        }
        if *stop.borrow() {
            return;
        }
        tokio::select! {
            res = lost.changed() => {
                if res.is_err() {
                    return;
                }
            }
            res = stop.changed() => {
                if res.is_err() || *stop.borrow() {
                    return;
                }
            }
        }
    }
}

async fn timer_loop(me: Weak<Inner>) {
    loop {
        let Some(inner) = me.upgrade() else {
            return;
        };
        let now = now_ms();
        let due = inner.timers.pop_due(now);
        inner.fire_due(due);
        let next = inner.timers.compact_and_next_due();
        let notify = Arc::clone(&inner.timers.notify);
        let wait = next
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
    let (namespace, raw_id) = identity::decode_scoped_id(&stored.job_id);
    JobView {
        job_id: raw_id.to_string(),
        namespace: namespace.to_string(),
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

fn check_namespace(namespace: &str) -> Result<String, RuntimeError> {
    let ns = if namespace.trim().is_empty() {
        DEFAULT_NAMESPACE
    } else {
        namespace.trim()
    };
    validate_namespace(ns).map_err(|status| RuntimeError::invalid(status.message()))?;
    Ok(ns.to_string())
}

impl Runtime {
    /// Starts the runtime, recovering durable jobs when leadership on a
    /// configured journal backend is acquired.
    pub async fn start(config: RuntimeConfig) -> anyhow::Result<Runtime> {
        let journal: Option<Arc<dyn JournalBackend>> = match &config.journal {
            Some(c) if c.postgres.is_some() => Some(crate::postgres::PgJournal::open(c).await?),
            Some(c) => Some(SqliteJournal::open(c).await?),
            None => None,
        };
        Self::start_with_backend(config, journal).await
    }

    pub(crate) async fn start_with_backend(
        config: RuntimeConfig,
        journal: Option<Arc<dyn JournalBackend>>,
    ) -> anyhow::Result<Runtime> {
        let initial_leadership = match &journal {
            Some(backend) => backend.try_acquire_leadership().await?,
            None => None,
        };
        let generation = initial_leadership
            .as_ref()
            .map(|l| l.term)
            // Without an active journal term yet, a timestamp in seconds
            // distinguishes ephemeral runs and keeps sequences in range.
            .unwrap_or_else(|| now_ms() / 1000);
        let initially_active = journal.is_none() || initial_leadership.is_some();
        let status = if journal.is_some() {
            JournalStatus::Healthy
        } else {
            JournalStatus::Disabled
        };
        let event_budget = config.max_event_bytes;
        let input_budget = config.max_pending_bytes;
        let result_budget = config.max_result_bytes;
        let (leadership_tx, _) = watch::channel(initially_active);
        let (lease_lost_tx, _) = watch::channel(false);
        let (stop_tx, stop_rx) = watch::channel(false);
        let standby_backend = (!initially_active)
            .then(|| journal.as_ref().cloned())
            .flatten();
        let inner = Arc::new_cyclic(|me| Inner {
            generation: AtomicU64::new(generation),
            leader_active: AtomicBool::new(initially_active),
            lease_lost: AtomicBool::new(false),
            leadership_tx,
            lease_lost_tx,
            stop_tx,
            lease_handle: Mutex::new(None),
            state: Mutex::new(State {
                generation,
                rng: generation ^ 0x5DEE_CE66_D1CE_4E5B,
                ..State::default()
            }),
            journal,
            journal_status: Mutex::new(status),
            timers: Timers::new(),
            event_budget: Arc::new(ByteBudget::new(event_budget)),
            input_budget: Arc::new(ByteBudget::new(input_budget)),
            result_budget: Arc::new(ByteBudget::new(result_budget)),
            me: me.clone(),
            config,
        });
        if let Some(leadership) = initial_leadership {
            inner.activate_leadership(leadership);
        } else if let Some(backend) = standby_backend {
            tokio::spawn(standby_acquire_loop(
                Arc::downgrade(&inner),
                backend,
                stop_rx,
            ));
        }
        tokio::spawn(timer_loop(Arc::downgrade(&inner)));
        tokio::spawn(maintenance_loop(Arc::downgrade(&inner)));
        Ok(Runtime { inner })
    }

    pub fn config(&self) -> &RuntimeConfig {
        &self.inner.config
    }

    pub fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::Acquire)
    }

    pub fn is_leader_active(&self) -> bool {
        self.inner.leader_active.load(Ordering::Acquire)
            && !self.inner.lease_lost.load(Ordering::Acquire)
    }

    pub fn subscribe_leadership(&self) -> watch::Receiver<bool> {
        self.inner.leadership_tx.subscribe()
    }

    pub async fn wait_for_lease_loss(&self) {
        if self.inner.lease_lost.load(Ordering::Acquire) {
            return;
        }
        let mut rx = self.inner.lease_lost_tx.subscribe();
        loop {
            if *rx.borrow() || self.inner.lease_lost.load(Ordering::Acquire) {
                return;
            }
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    pub fn ensure_active_leader(&self) -> Result<(), RuntimeError> {
        if self.inner.lease_lost.load(Ordering::Acquire) {
            return Err(RuntimeError::unavailable(
                "control plane lost its leadership lease",
            ));
        }
        if !self.inner.leader_active.load(Ordering::Acquire) {
            return Err(RuntimeError::unavailable(
                "control plane replica is on standby",
            ));
        }
        Ok(())
    }

    pub(crate) fn result_budget(&self) -> Arc<ByteBudget> {
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
            if matches!(input, api::Object::Uri { uri, .. } if uri.is_empty()) {
                return Err(RuntimeError::invalid("object URI must not be empty"));
            }
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
        self.ensure_active_leader()?;
        let namespace = check_namespace(request.namespace.as_deref().unwrap_or_default())?;
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
        let raw_id = requested.unwrap_or_else(machine::new_id);
        let scoped_id = identity::encode_scoped_id(&namespace, &raw_id);
        let conflict = |existing: &str| -> Result<(), RuntimeError> {
            if existing.is_empty() {
                return Err(RuntimeError::conflict(format!(
                    "job '{raw_id}' exists and was not created by an idempotent submission"
                )));
            }
            if existing != spec_hash {
                return Err(RuntimeError::conflict(format!(
                    "job '{raw_id}' exists with a different specification"
                )));
            }
            if request.conflict == api::ConflictPolicy::Fail {
                return Err(RuntimeError::conflict(format!(
                    "job '{raw_id}' already exists"
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
                if let Some(job) = st.jobs.get_mut(&scoped_id) {
                    if job.indeterminate {
                        return Err(RuntimeError::unavailable(format!(
                            "job '{raw_id}' has an unknown commit outcome until restart"
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
                    let input_permit = self.inner.admit(&st, &namespace, &spec)?;
                    self.inner.insert_job(
                        &mut st,
                        scoped_id.clone(),
                        spec,
                        durability,
                        spec_hash.clone(),
                        max_attempts,
                        None,
                        input_permit,
                        now,
                    );
                    if durability == Durability::Ephemeral {
                        let view = st.jobs[&scoped_id].view(false);
                        self.inner.dispatch_locked(&mut st, now);
                        Next::Done(Box::new(SubmitOutcome {
                            job: view,
                            created: true,
                        }))
                    } else {
                        if let Err(error) = self.inner.begin_accept(&mut st, &scoped_id) {
                            self.inner.remove_unaccepted_locked(
                                &mut st,
                                &scoped_id,
                                &error.to_string(),
                            );
                            return Err(error);
                        }
                        let (tx, rx) = oneshot::channel();
                        st.jobs
                            .get_mut(&scoped_id)
                            .expect("job")
                            .accept_waiters
                            .push(tx);
                        Next::Wait(rx, true)
                    }
                }
            };
            let (receiver, created) = match next {
                Next::Done(outcome) => return Ok(*outcome),
                Next::ColdCheck => {
                    let journal = self.inner.journal.as_ref().expect("journal");
                    let stored = journal.spec_hash(&scoped_id).await.map_err(|error| {
                        RuntimeError::unavailable(format!("read journal: {error:#}"))
                    })?;
                    if let Some(existing) = stored {
                        conflict(&existing)?;
                        return Ok(SubmitOutcome {
                            job: self.get_job_in(&namespace, &raw_id, false).await?,
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
        self.ensure_active_leader()?;
        let namespace = check_namespace(request.namespace.as_deref().unwrap_or_default())?;
        let (spec, max_attempts) = self.normalize(request.spec, Durability::Ephemeral)?;
        let raw_id = machine::new_id();
        let scoped_id = identity::encode_scoped_id(&namespace, &raw_id);
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
            let input_permit = self.inner.admit(&st, &namespace, &spec)?;
            let mut spec_slot = Some(spec);
            let mut permit_slot = Some(input_permit);
            match self.inner.invoke_ephemeral_direct_locked(
                &mut st,
                &namespace,
                raw_id.clone(),
                scoped_id.clone(),
                &mut spec_slot,
                max_attempts,
                input.clone(),
                &mut permit_slot,
                now,
            ) {
                Some(direct) => direct,
                None => {
                    self.inner.insert_job(
                        &mut st,
                        scoped_id.clone(),
                        spec_slot.expect("spec"),
                        Durability::Ephemeral,
                        String::new(),
                        max_attempts,
                        input.clone(),
                        permit_slot.expect("permit"),
                        now,
                    );
                    let job = st.jobs.get_mut(&scoped_id).expect("job");
                    job.streamed = true;
                    let log = Arc::clone(&job.log);
                    log.set_primary(true);
                    let changes = log.subscribe();
                    self.inner.dispatch_locked(&mut st, now);
                    (log, changes)
                }
            }
        };
        // A streamed consumer's reading position releases back-pressure.
        let on_yield = Some({
            let log = Arc::clone(&log);
            Arc::new(move |event: &ExecutionEvent| log.mark_consumed(event.sequence)) as EventHook
        });
        Ok(Execution {
            job_id: raw_id,
            events: follow(Arc::clone(&log), changes, 0, on_yield, false),
            input: input.map(|log| InputSink { log }),
            _guard: Box::new(CancelOnDrop {
                me: Arc::downgrade(&self.inner),
                job_id: scoped_id,
                log,
            }),
        })
    }

    pub fn watch(&self, job_id: &str, after_sequence: u64) -> Result<EventStream, RuntimeError> {
        self.watch_in(DEFAULT_NAMESPACE, job_id, after_sequence)
    }

    /// Follows a job's events after `after_sequence` until it finishes.
    /// `after_sequence = 0` starts at the oldest event still held and never
    /// reports `CursorExpired`; a non-zero sequence older than what is held
    /// ends the stream with `CursorExpired{oldest}`.
    pub fn watch_in(
        &self,
        namespace: &str,
        job_id: &str,
        after_sequence: u64,
    ) -> Result<EventStream, RuntimeError> {
        self.ensure_active_leader()?;
        let ns = check_namespace(namespace)?;
        let scoped_id = identity::encode_scoped_id(&ns, job_id);
        let st = self.inner.lock();
        if let Some(job) = st.jobs.get(&scoped_id) {
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
        let raw = job_id.to_string();
        Ok(EventStream::new(async_stream::stream! {
            match runtime.get_job_in(&ns, &raw, true).await {
                Err(error) => yield Err(error),
                Ok(job) => {
                    for event in runtime.replayed_events(&job, after_sequence) {
                        yield Ok(event);
                    }
                }
            }
        }))
    }

    pub async fn events(
        &self,
        job_id: &str,
        after_sequence: u64,
    ) -> Result<(Vec<ExecutionEvent>, bool), RuntimeError> {
        self.events_in(DEFAULT_NAMESPACE, job_id, after_sequence)
            .await
    }

    /// Events after `after_sequence`, and whether the job's log has ended.
    /// Evicted durable jobs are read back from the journal as their
    /// acceptance and outcome. `after_sequence = 0` starts at the oldest event
    /// still held and never fails with `CursorExpired`; a non-zero sequence
    /// older than what is held gets `CursorExpired{oldest}`.
    pub async fn events_in(
        &self,
        namespace: &str,
        job_id: &str,
        after_sequence: u64,
    ) -> Result<(Vec<ExecutionEvent>, bool), RuntimeError> {
        self.ensure_active_leader()?;
        let ns = check_namespace(namespace)?;
        let scoped_id = identity::encode_scoped_id(&ns, job_id);
        let log = {
            let st = self.inner.lock();
            match st.jobs.get(&scoped_id) {
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
        let job = self.get_job_in(&ns, job_id, true).await?;
        Ok((self.replayed_events(&job, after_sequence), true))
    }

    /// The specification of a job held in memory. Inputs and params are
    /// dropped once the job is terminal.
    pub fn spec(&self, job_id: &str) -> Result<Arc<JobSpec>, RuntimeError> {
        self.spec_in(DEFAULT_NAMESPACE, job_id)
    }

    pub fn spec_in(&self, namespace: &str, job_id: &str) -> Result<Arc<JobSpec>, RuntimeError> {
        let ns = check_namespace(namespace)?;
        let scoped_id = identity::encode_scoped_id(&ns, job_id);
        let st = self.inner.lock();
        st.jobs
            .get(&scoped_id)
            .map(|job| Arc::clone(&job.spec))
            .ok_or_else(|| RuntimeError::not_found("job", job_id))
    }

    /// Acceptance and outcome of a job known only to the journal.
    fn replayed_events(&self, job: &JobView, after_sequence: u64) -> Vec<ExecutionEvent> {
        // Finished takes the generation's last sequence: above anything the
        // job's live log handed out, below the next generation.
        let base = self.generation() << 32;
        let accepted_seq = base + 1;
        let finished_seq = base + 0xFFFF_FFFF;
        let mut out = Vec::with_capacity(
            usize::from(accepted_seq > after_sequence) + usize::from(finished_seq > after_sequence),
        );
        if accepted_seq > after_sequence {
            out.push(ExecutionEvent {
                job_id: job.job_id.clone(),
                sequence: accepted_seq,
                attempt_id: job.current_attempt_id.clone(),
                at_ms: job.updated_at_ms,
                kind: EventKind::Accepted,
            });
        }
        if finished_seq > after_sequence {
            out.push(ExecutionEvent {
                job_id: job.job_id.clone(),
                sequence: finished_seq,
                attempt_id: job.current_attempt_id.clone(),
                at_ms: job.updated_at_ms,
                kind: EventKind::Finished {
                    state: job.state,
                    outputs: job.outputs.clone().unwrap_or_default(),
                    error: job.error.clone(),
                },
            });
        }
        out
    }

    pub async fn get_job(
        &self,
        job_id: &str,
        include_outputs: bool,
    ) -> Result<JobView, RuntimeError> {
        self.get_job_in(DEFAULT_NAMESPACE, job_id, include_outputs)
            .await
    }

    pub async fn get_job_in(
        &self,
        namespace: &str,
        job_id: &str,
        include_outputs: bool,
    ) -> Result<JobView, RuntimeError> {
        self.ensure_active_leader()?;
        let ns = check_namespace(namespace)?;
        let scoped_id = identity::encode_scoped_id(&ns, job_id);
        {
            let st = self.inner.lock();
            if let Some(job) = st.jobs.get(&scoped_id) {
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
        match journal
            .load_job(&scoped_id, include_outputs, self.result_budget())
            .await
        {
            Ok(Some(stored)) => Ok(view_from_stored(stored, include_outputs)),
            Ok(None) => Err(RuntimeError::not_found("job", job_id)),
            Err(error) => Err(error
                .downcast_ref::<RuntimeError>()
                .cloned()
                .unwrap_or_else(|| RuntimeError::unavailable(format!("read journal: {error:#}")))),
        }
    }

    fn inspect_retained_job(
        &self,
        scoped_id: &str,
        raw_id: &str,
    ) -> Result<Option<JobInspection>, RuntimeError> {
        let retained = {
            let st = self.inner.lock();
            match st.jobs.get(scoped_id) {
                Some(job) if job.indeterminate => {
                    return Err(RuntimeError::unavailable(format!(
                        "job '{raw_id}' has an unknown commit outcome until restart"
                    )));
                }
                Some(job) => Some((job.view(true), Arc::clone(&job.spec), Arc::clone(&job.log))),
                None => None,
            }
        };
        if let Some((job, spec, log)) = retained {
            let (events, _) = log.read_after(0).map_err(RuntimeError::cursor_expired)?;
            return Ok(Some(JobInspection {
                job,
                spec: Some(spec),
                events,
            }));
        }
        Ok(None)
    }

    /// Reads a detail view without loading a cold result twice for its view
    /// and replayed events. Event cloning happens outside the runtime lock.
    pub async fn inspect_job(&self, job_id: &str) -> Result<JobInspection, RuntimeError> {
        self.inspect_job_in(DEFAULT_NAMESPACE, job_id).await
    }

    pub async fn inspect_job_in(
        &self,
        namespace: &str,
        job_id: &str,
    ) -> Result<JobInspection, RuntimeError> {
        self.ensure_active_leader()?;
        let ns = check_namespace(namespace)?;
        let scoped_id = identity::encode_scoped_id(&ns, job_id);
        loop {
            if let Some(retained) = self.inspect_retained_job(&scoped_id, job_id)? {
                return Ok(retained);
            }
            let job = self.get_job_in(&ns, job_id, true).await?;
            if !job.state.is_terminal() {
                // Admission raced the lookup: read its live log on the next pass.
                continue;
            }
            let events = self.replayed_events(&job, 0);
            return Ok(JobInspection {
                job,
                spec: None,
                events,
            });
        }
    }

    /// Management inspection: cold inline outputs are read as bounded prefixes
    /// directly in storage and never consume the business result-byte budget.
    pub async fn inspect_job_preview(&self, job_id: &str) -> Result<JobPreview, RuntimeError> {
        self.inspect_job_preview_in(DEFAULT_NAMESPACE, job_id).await
    }

    pub async fn inspect_job_preview_in(
        &self,
        namespace: &str,
        job_id: &str,
    ) -> Result<JobPreview, RuntimeError> {
        self.ensure_active_leader()?;
        let ns = check_namespace(namespace)?;
        let scoped_id = identity::encode_scoped_id(&ns, job_id);
        loop {
            if let Some(JobInspection {
                mut job,
                spec,
                events,
            }) = self.inspect_retained_job(&scoped_id, job_id)?
            {
                let outputs = job
                    .outputs
                    .take()
                    .unwrap_or_default()
                    .iter()
                    .map(ObjectPreview::from)
                    .collect();
                return Ok(JobPreview {
                    job,
                    spec,
                    events,
                    outputs,
                });
            }
            let journal = self
                .inner
                .journal
                .as_ref()
                .ok_or_else(|| RuntimeError::not_found("job", job_id))?;
            let (stored, outputs) = journal
                .load_job_preview(&scoped_id)
                .await
                .map_err(|error| {
                    RuntimeError::unavailable(format!("read journal preview: {error:#}"))
                })?
                .ok_or_else(|| RuntimeError::not_found("job", job_id))?;
            if !stored.state.is_terminal() {
                // Admission raced the lookup: use its live spec and events.
                continue;
            }
            let job = view_from_stored(stored, false);
            let events = self.replayed_events(&job, 0);
            return Ok(JobPreview {
                job,
                spec: None,
                events,
                outputs,
            });
        }
    }

    /// Cancels a job and returns its final view. Durable jobs return once the
    /// cancellation is committed.
    pub async fn cancel(&self, job_id: &str, reason: String) -> Result<JobView, RuntimeError> {
        self.cancel_in(DEFAULT_NAMESPACE, job_id, reason).await
    }

    pub async fn cancel_in(
        &self,
        namespace: &str,
        job_id: &str,
        reason: String,
    ) -> Result<JobView, RuntimeError> {
        self.ensure_active_leader()?;
        let ns = check_namespace(namespace)?;
        let scoped_id = identity::encode_scoped_id(&ns, job_id);
        let receiver = {
            let mut st = self.inner.lock();
            let now = now_ms();
            match st.jobs.get_mut(&scoped_id) {
                None => None,
                Some(_) => Some(Self::start_cancel(
                    &self.inner,
                    &mut st,
                    &scoped_id,
                    reason,
                    now,
                )?),
            }
        };
        match receiver {
            None => self.get_job_in(&ns, job_id, false).await,
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
        self.ensure_active_leader()?;
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
        let (ids, total) = st.job_index.page(&filter, offset, limit);
        let page = ids.into_iter().map(|id| st.jobs[id].view(false)).collect();
        let next_offset = offset.saturating_add(limit);
        Ok(JobPage {
            jobs: page,
            next_page_token: (next_offset < total).then(|| next_offset.to_string()),
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
                namespace: session.namespace.clone(),
                pool: session.pool.clone(),
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
        drop(st);
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
        let live = !self.inner.lease_lost.load(Ordering::Acquire);
        let active = self.inner.leader_active.load(Ordering::Acquire);
        Health {
            live,
            ready: live
                && active
                && !st.shutting_down
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
        let mut interfaces = st.job_index.interfaces(now);
        let [queued, running, succeeded, failed, cancelled] = st.job_index.counts();
        let generation = st.generation;
        let retained_jobs = st.terminal_order.len();
        let sessions = st.sessions.len();
        let mut pools_tmp: HashMap<(&str, &str, &str), PoolStats> = HashMap::new();
        let mut session_lease_ifaces: HashMap<&str, usize> = HashMap::new();
        for session in st.sessions.values() {
            let free = session.free_slots() as u64;
            if session.interfaces.len() > 1 && !session.active.is_empty() {
                session_lease_ifaces.clear();
                for lease_id in &session.active {
                    if let Some(iface) = st
                        .leases
                        .get(lease_id)
                        .and_then(|lease| st.jobs.get(&lease.job_id))
                        .map(|job| job.spec.demand.interface.as_str())
                    {
                        *session_lease_ifaces.entry(iface).or_insert(0) += 1;
                    }
                }
            }
            for interface in &session.interfaces {
                if let Some(entry) = interfaces.get_mut(interface.as_str()) {
                    entry.sessions += 1;
                    entry.free_slots += free;
                } else {
                    interfaces.insert(
                        interface.clone(),
                        InterfaceStats {
                            sessions: 1,
                            free_slots: free,
                            ..Default::default()
                        },
                    );
                }

                let active_leases = if session.active.is_empty() {
                    0
                } else if session.interfaces.len() == 1 {
                    session
                        .active
                        .iter()
                        .filter(|lease_id| {
                            st.leases
                                .get(*lease_id)
                                .and_then(|lease| st.jobs.get(&lease.job_id))
                                .is_some_and(|job| job.spec.demand.interface == *interface)
                        })
                        .count()
                } else {
                    session_lease_ifaces
                        .get(interface.as_str())
                        .copied()
                        .unwrap_or(0)
                };

                let pool_entry = pools_tmp
                    .entry((
                        session.namespace.as_str(),
                        interface.as_str(),
                        session.pool.as_str(),
                    ))
                    .or_default();
                pool_entry.sessions += 1;
                pool_entry.free_slots += free;
                pool_entry.active_leases += active_leases;
            }
        }
        for (namespace, ifaces) in &st.queues {
            for (interface, queue) in ifaces {
                if queue.is_empty() {
                    continue;
                }
                let primary_pool = self
                    .inner
                    .config
                    .interfaces
                    .get(interface)
                    .and_then(|policy| policy.pool_tiers.first())
                    .map(|tier| tier.pool.as_str())
                    .unwrap_or("");
                let entry = pools_tmp
                    .entry((namespace.as_str(), interface.as_str(), primary_pool))
                    .or_default();
                entry.desired_additional_slots =
                    queue.len().saturating_sub(entry.free_slots as usize);
            }
        }
        let pools: BTreeMap<(String, String, String), PoolStats> = pools_tmp
            .into_iter()
            .map(|((ns, iface, pool), stats)| {
                ((ns.to_string(), iface.to_string(), pool.to_string()), stats)
            })
            .collect();
        drop(st);
        RuntimeStats {
            generation,
            queued,
            running,
            succeeded,
            failed,
            cancelled,
            retained_jobs,
            sessions,
            interfaces,
            pools,
            journal: self.inner.journal.as_ref().map(|journal| journal.stats()),
            input_bytes: self.inner.input_budget.used(),
            result_bytes: self.inner.result_budget.used(),
            event_bytes: self.inner.event_budget.used(),
        }
    }

    /// Stops accepting work, asks workers to drain, flushes queued commits, and
    /// relinquishes leadership so a standby can take over immediately.
    pub async fn shutdown(&self) {
        self.inner.stop_tx.send_replace(true);
        self.inner.leader_active.store(false, Ordering::Release);
        self.inner.leadership_tx.send_replace(false);
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
        let lease = self
            .inner
            .lease_handle
            .lock()
            .expect("lease handle mutex poisoned")
            .take();
        if let Some(lease) = lease {
            lease.relinquish().await;
        }
        {
            let mut st = self.inner.lock();
            for session in st.sessions.values_mut() {
                session.tx = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_uri_inputs_are_rejected_without_rejecting_empty_inline() {
        let runtime = Runtime::start(RuntimeConfig::default()).await.unwrap();
        let mut spec = JobSpec::default();
        spec.demand.interface = "opaque-uri".into();
        spec.policy.durability = Some(Durability::Ephemeral);
        spec.inputs.push(api::Object::Uri {
            uri: String::new(),
            metadata: Default::default(),
        });
        assert_eq!(
            runtime
                .submit(SubmitRequest {
                    spec: spec.clone(),
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .kind,
            crate::error::ErrorKind::InvalidArgument
        );
        assert_eq!(
            runtime
                .invoke(InvokeRequest {
                    spec: spec.clone(),
                    ..Default::default()
                })
                .await
                .err()
                .unwrap()
                .kind,
            crate::error::ErrorKind::InvalidArgument
        );
        spec.inputs = vec![
            api::Object::inline(bytes::Bytes::new()),
            api::Object::Uri {
                uri: "relative/video.mp4".into(),
                metadata: Default::default(),
            },
        ];
        runtime
            .submit(SubmitRequest {
                spec,
                ..Default::default()
            })
            .await
            .unwrap();
        runtime.shutdown().await;
    }

    #[test]
    fn cancelled_timers_leave_the_heap_without_firing() {
        let timers = Timers::new();
        timers.schedule(10, TimerKey::Dispatch);
        let dead: Vec<u64> = (0..TIMER_COMPACT_SLACK + 2)
            .map(|_| timers.schedule(5, TimerKey::Dispatch))
            .collect();
        timers.cancel(&dead);
        timers.compact();
        {
            let inner = timers.lock();
            assert_eq!(inner.heap.len(), 1);
            assert_eq!(inner.keys.len(), 1);
            assert!(inner.cancelled.is_empty());
        }
        // Cancelling again is harmless, and a cancelled entry still in the
        // heap is skipped when its time comes.
        let late = timers.schedule(7, TimerKey::Dispatch);
        timers.cancel(&[late, dead[0]]);
        assert_eq!(timers.lock().cancelled.len(), 1);
        let due = timers.pop_due(10);
        assert!(matches!(due.as_slice(), [TimerKey::Dispatch]), "{due:?}");
        let inner = timers.lock();
        assert!(inner.heap.is_empty() && inner.keys.is_empty() && inner.cancelled.is_empty());
    }

    #[tokio::test]
    async fn standby_gate_takeover_fencing_and_graceful_relinquish() {
        use crate::journal::{BoxFuture, CommitOutcome, JournalOp};
        use std::sync::atomic::AtomicUsize;

        struct MockLeasedBackend {
            allow_acquire: AtomicBool,
            term: AtomicU64,
            lost_tx: watch::Sender<bool>,
            relinquished: Arc<AtomicUsize>,
            fence_next: AtomicBool,
        }

        impl JournalBackend for MockLeasedBackend {
            fn try_acquire_leadership(&self) -> BoxFuture<'_, anyhow::Result<Option<Leadership>>> {
                if !self.allow_acquire.load(Ordering::Acquire) {
                    return Box::pin(async { Ok(None) });
                }
                let term = self.term.fetch_add(1, Ordering::AcqRel) + 1;
                let lost_rx = self.lost_tx.subscribe();
                let rel = Arc::clone(&self.relinquished);
                let mut spec = JobSpec::default();
                spec.demand.interface = "demo.v1".into();
                spec.policy.durability = Some(Durability::Durable);
                let recovered = StoredJob {
                    job_id: identity::encode_scoped_id(DEFAULT_NAMESPACE, "recovered-1"),
                    state: JobState::Queued,
                    spec,
                    spec_hash: "h1".into(),
                    attempt_count: 0,
                    max_attempts: 3,
                    created_at_ms: 100,
                    updated_at_ms: 100,
                    error: None,
                    outputs: Vec::new(),
                    last_attempt_id: None,
                    last_worker_id: None,
                    failed_workers: Default::default(),
                };
                Box::pin(async move {
                    Ok(Some(Leadership {
                        term,
                        recovery: Recovery {
                            generation: term,
                            active: vec![recovered],
                        },
                        lease: LeaseHandle::new(lost_rx, move || {
                            Box::pin(async move {
                                rel.fetch_add(1, Ordering::SeqCst);
                            })
                        }),
                    }))
                })
            }

            fn standby_poll_interval(&self) -> Duration {
                Duration::from_millis(15)
            }

            fn enqueue(
                &self,
                _term: u64,
                _op: JournalOp,
            ) -> Result<oneshot::Receiver<CommitOutcome>, RuntimeError> {
                let (tx, rx) = oneshot::channel();
                if self.fence_next.load(Ordering::Acquire) {
                    let _ = tx.send(CommitOutcome::Fenced("mock lease stolen".into()));
                } else {
                    let _ = tx.send(CommitOutcome::Committed);
                }
                Ok(rx)
            }

            fn spec_hash<'a>(
                &'a self,
                _id: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<String>>> {
                Box::pin(async { Ok(None) })
            }
            fn receipt<'a>(
                &'a self,
                _lease_id: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<(String, String)>>> {
                Box::pin(async { Ok(None) })
            }
            fn load_job<'a>(
                &'a self,
                _id: &'a str,
                _include_outputs: bool,
                _budget: Arc<ByteBudget>,
            ) -> BoxFuture<'a, anyhow::Result<Option<StoredJob>>> {
                Box::pin(async { Ok(None) })
            }
            fn load_job_preview<'a>(
                &'a self,
                _id: &'a str,
            ) -> BoxFuture<'a, anyhow::Result<Option<(StoredJob, Vec<ObjectPreview>)>>>
            {
                Box::pin(async { Ok(None) })
            }
            fn scan_jobs<'a>(
                &'a self,
                _from: u64,
                _to: u64,
                _before: Option<(u64, String)>,
                _ns: Option<&'a str>,
                _limit: usize,
            ) -> BoxFuture<'a, anyhow::Result<Vec<StoredJob>>> {
                Box::pin(async { Ok(Vec::new()) })
            }
            fn purge<'a>(
                &'a self,
                _term: u64,
                _before: u64,
            ) -> BoxFuture<'a, anyhow::Result<usize>> {
                Box::pin(async { Ok(0) })
            }
            fn stats(&self) -> api::JournalStats {
                api::JournalStats::default()
            }
            fn close(&self) -> BoxFuture<'_, ()> {
                Box::pin(async {})
            }
        }

        let (lost_tx, _) = watch::channel(false);
        let relinquished = Arc::new(AtomicUsize::new(0));
        let mock = Arc::new(MockLeasedBackend {
            allow_acquire: AtomicBool::new(false),
            term: AtomicU64::new(41),
            lost_tx: lost_tx.clone(),
            relinquished: Arc::clone(&relinquished),
            fence_next: AtomicBool::new(false),
        });

        // 1. Starts on standby: live=true, ready=false, RPCs fail with Unavailable.
        let runtime = Runtime::start_with_backend(
            RuntimeConfig::default(),
            Some(Arc::clone(&mock) as Arc<dyn JournalBackend>),
        )
        .await
        .unwrap();
        assert!(!runtime.is_leader_active());
        assert!(runtime.health().live);
        assert!(!runtime.health().ready);
        assert_eq!(
            runtime
                .list_jobs(JobFilter::default())
                .await
                .unwrap_err()
                .kind,
            crate::error::ErrorKind::PersistenceUnavailable
        );
        assert_eq!(
            runtime
                .open_session(anyserve_proto::v2::Hello {
                    interfaces: vec!["demo.v1".into()],
                    ..Default::default()
                })
                .err()
                .unwrap()
                .kind,
            crate::error::ErrorKind::PersistenceUnavailable
        );

        // 2. Grant leadership -> standby takes over, recovers queued job, flips ready=true.
        let mut leader_sub = runtime.subscribe_leadership();
        mock.allow_acquire.store(true, Ordering::Release);
        while !*leader_sub.borrow() {
            leader_sub.changed().await.unwrap();
        }
        assert!(runtime.is_leader_active());
        assert_eq!(runtime.generation(), 42);
        assert!(runtime.health().ready);
        let view = runtime.get_job("recovered-1", false).await.unwrap();
        assert_eq!(view.state, JobState::Queued);

        // 3. Graceful shutdown invokes relinquish() cleanly.
        runtime.shutdown().await;
        assert_eq!(relinquished.load(Ordering::SeqCst), 1);

        // 4. Fenced write or lease expiry triggers wait_for_lease_loss() immediately.
        let runtime2 = Runtime::start_with_backend(
            RuntimeConfig::default(),
            Some(Arc::clone(&mock) as Arc<dyn JournalBackend>),
        )
        .await
        .unwrap();
        assert_eq!(runtime2.generation(), 43);
        mock.fence_next.store(true, Ordering::Release);
        let mut spec = JobSpec::default();
        spec.demand.interface = "demo.v1".into();
        let err = runtime2
            .submit(SubmitRequest {
                spec,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind, crate::error::ErrorKind::PersistenceUnavailable);
        tokio::time::timeout(Duration::from_secs(2), runtime2.wait_for_lease_loss())
            .await
            .expect("fenced write must trigger lease loss notification");
        assert!(!runtime2.health().live);
        assert!(!runtime2.health().ready);
    }
}
