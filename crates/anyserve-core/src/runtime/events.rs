//! Per-job event log: lifecycle events and attempt output, bounded by count and
//! bytes per job and by a byte budget shared by all jobs.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::watch;

use super::api::{EventKind, ExecutionEvent, Metadata};

/// A byte budget shared by many owners; reservations never exceed the limit
/// unless forced.
pub(crate) struct ByteBudget {
    used: AtomicU64,
    limit: u64,
}

impl ByteBudget {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            used: AtomicU64::new(0),
            limit,
        }
    }

    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    /// Reserves `bytes` only if they fit under the limit.
    #[allow(deprecated, reason = "AtomicU64::try_update is still unstable")]
    pub(crate) fn try_reserve(&self, bytes: usize) -> bool {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                let next = used + bytes as u64;
                (next <= self.limit).then_some(next)
            })
            .is_ok()
    }

    /// Reserves `bytes` regardless of the limit.
    pub(crate) fn force_reserve(&self, bytes: usize) {
        self.used.fetch_add(bytes as u64, Ordering::AcqRel);
    }

    pub(crate) fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes as u64, Ordering::AcqRel);
    }
}

/// Bytes reserved from a budget until dropped.
pub(crate) struct Permit {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl Permit {
    pub(crate) fn new(budget: Arc<ByteBudget>) -> Self {
        Self { budget, bytes: 0 }
    }

    /// Reserves `bytes` even beyond the limit (for state recovered at startup).
    pub(crate) fn forced(budget: Arc<ByteBudget>, bytes: usize) -> Self {
        budget.force_reserve(bytes);
        Self { budget, bytes }
    }

    /// Reserves `more` bytes; false (reserving nothing) when they do not fit.
    pub(crate) fn grow(&mut self, more: usize) -> bool {
        if !self.budget.try_reserve(more) {
            return false;
        }
        self.bytes += more;
        true
    }

    /// Transfers part of an existing reservation without releasing its budget.
    pub(crate) fn split(&mut self, bytes: usize) -> Self {
        assert!(bytes <= self.bytes, "split exceeds reserved bytes");
        self.bytes -= bytes;
        Self {
            budget: Arc::clone(&self.budget),
            bytes,
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

struct Budgeted {
    data: Bytes,
    _permit: Permit,
}

impl AsRef<[u8]> for Budgeted {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// `data` as `Bytes` that keep `permit` reserved until the last clone drops,
/// wherever the clones travel (journal operations, retained results, events).
pub(crate) fn budgeted_bytes(data: Bytes, permit: Permit) -> Bytes {
    Bytes::from_owner(Budgeted {
        data,
        _permit: permit,
    })
}

pub(crate) struct JobLog {
    job_id: String,
    inner: Mutex<LogInner>,
    /// Last appended sequence, for followers.
    changed: watch::Sender<u64>,
    /// Last sequence taken by the primary consumer, for back-pressured writers.
    consumed: watch::Sender<u64>,
    budget: Arc<ByteBudget>,
    max_events: usize,
    max_bytes: usize,
    /// Set once an ephemeral job finishes: the log may then shrink to its
    /// outcome as soon as nobody follows it.
    compactable: AtomicBool,
}

struct LogInner {
    events: VecDeque<(ExecutionEvent, usize)>,
    bytes: usize,
    last_sequence: u64,
    /// Highest sequence no longer readable; 0 when nothing was dropped.
    evicted_through: u64,
    finished: bool,
    /// Set while a streamed consumer must see every output event.
    primary: Option<u64>,
}

/// Why an output event could not be appended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LogFull {
    /// The primary consumer has not read this job's earlier output; the
    /// writer waits for it.
    ConsumerBehind,
    /// The shared event budget (or the job's own byte bound) is full and this
    /// job has nothing left to give up. With a primary consumer the writer
    /// retries until room appears; without one the event is dropped on the
    /// last try.
    BudgetFull,
}

const EVENT_OVERHEAD: usize = 96;

fn metadata_bytes(metadata: &Metadata) -> usize {
    metadata.iter().map(|(k, v)| k.len() + v.len()).sum()
}

impl JobLog {
    pub(crate) fn new(
        job_id: String,
        base_sequence: u64,
        max_events: usize,
        max_bytes: usize,
        budget: Arc<ByteBudget>,
    ) -> Self {
        Self {
            job_id,
            inner: Mutex::new(LogInner {
                events: VecDeque::new(),
                bytes: 0,
                last_sequence: base_sequence,
                evicted_through: 0,
                finished: false,
                primary: None,
            }),
            changed: watch::channel(base_sequence).0,
            consumed: watch::channel(0).0,
            budget,
            max_events: max_events.max(2),
            max_bytes,
            compactable: AtomicBool::new(false),
        }
    }

    /// Marks every sequence up to the log's base as unreadable.
    pub(crate) fn mark_lost_before_base(&self) {
        let mut inner = self.lock();
        if inner.events.is_empty() {
            inner.evicted_through = inner.last_sequence;
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LogInner> {
        self.inner.lock().expect("job log poisoned")
    }

    fn evict_front(&self, inner: &mut LogInner) -> ExecutionEvent {
        let (event, size) = inner.events.pop_front().expect("front exists");
        inner.bytes -= size;
        self.budget.release(size);
        inner.evicted_through = event.sequence;
        event
    }

    /// Whether the front event may be dropped: always without a primary
    /// consumer, otherwise only once that consumer has read it.
    fn front_evictable(inner: &LogInner) -> bool {
        match (inner.events.front(), inner.primary) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some((front, _)), Some(consumed)) => front.sequence <= consumed,
        }
    }

    /// Drops evictable events until `needed` more bytes fit this log and the
    /// shared budget, then reserves them. Returns false without reserving when
    /// they cannot fit.
    fn reserve(&self, inner: &mut LogInner, needed: usize) -> bool {
        loop {
            let over_local =
                inner.events.len() >= self.max_events || inner.bytes + needed > self.max_bytes;
            if !over_local && self.budget.try_reserve(needed) {
                return true;
            }
            if !Self::front_evictable(inner) {
                return false;
            }
            self.evict_front(inner);
        }
    }

    fn push(&self, inner: &mut LogInner, event: ExecutionEvent, size: usize) {
        inner.last_sequence = event.sequence;
        inner.bytes += size;
        inner.events.push_back((event, size));
    }

    /// Appends a lifecycle event (Accepted, Assigned, AttemptRestarted,
    /// Finished). These are never refused and may take the shared budget past
    /// its limit. Each costs `EVENT_OVERHEAD`, and a job with N attempts logs
    /// at most 2N + 1 of them: Accepted, Finished, one Assigned per attempt
    /// and one AttemptRestarted per retried attempt. Only unfinished and
    /// retained terminal jobs have logs, so with M = `max_attempts_limit` the
    /// allowance stays below `(max_pending_jobs + max_terminal_jobs) ×
    /// (2M + 1) × EVENT_OVERHEAD`: 110 000 × 41 × 96 B ≈ 433 MB with the
    /// defaults, under the default `max_event_bytes`. Outputs carried by
    /// `Finished` share their buffers with the job's result, which the result
    /// budget covers.
    pub(crate) fn append(&self, attempt_id: Option<String>, kind: EventKind, at_ms: u64) -> u64 {
        let mut inner = self.lock();
        if inner.finished {
            return inner.last_sequence;
        }
        let size = EVENT_OVERHEAD;
        if !self.reserve(&mut inner, size) {
            self.budget.force_reserve(size);
        }
        let sequence = inner.last_sequence + 1;
        let finished = matches!(kind, EventKind::Finished { .. });
        let event = ExecutionEvent {
            job_id: self.job_id.clone(),
            sequence,
            attempt_id,
            at_ms,
            kind,
        };
        self.push(&mut inner, event, size);
        inner.finished = finished;
        drop(inner);
        self.changed.send_replace(sequence);
        sequence
    }

    /// Appends attempt output without ever reserving beyond the shared
    /// budget. A primary consumer's unread output is never dropped: when it
    /// is in the way the result is `ConsumerBehind`. Otherwise a full budget
    /// gives `BudgetFull`, except on the `last_try` of a log without a primary
    /// consumer, where the event is dropped and followers see a cursor gap.
    /// Followers are not woken here: on `Ok` the caller passes the sequence to
    /// `notify_appended` once it has let go of its own locks.
    pub(crate) fn append_output(
        &self,
        attempt_id: String,
        data: Bytes,
        metadata: Metadata,
        progress: bool,
        at_ms: u64,
        last_try: bool,
    ) -> Result<u64, LogFull> {
        let mut inner = self.lock();
        if inner.finished {
            return Ok(inner.last_sequence);
        }
        let size = EVENT_OVERHEAD + data.len() + metadata_bytes(&metadata);
        let sequence = inner.last_sequence + 1;
        if self.reserve(&mut inner, size) {
            let event = ExecutionEvent {
                job_id: self.job_id.clone(),
                sequence,
                attempt_id: Some(attempt_id),
                at_ms,
                kind: EventKind::Output {
                    data,
                    metadata,
                    progress,
                },
            };
            self.push(&mut inner, event, size);
        } else if inner.primary.is_some() && !inner.events.is_empty() {
            // `reserve` already dropped everything evictable: what remains is
            // unread.
            return Err(LogFull::ConsumerBehind);
        } else if inner.primary.is_some() || !last_try {
            return Err(LogFull::BudgetFull);
        } else {
            // No reader must see it: record it as dropped so followers get a
            // cursor error instead of a silent gap.
            inner.last_sequence = sequence;
            inner.evicted_through = sequence;
        }
        Ok(sequence)
    }

    /// Wakes followers for output appended up to `sequence`.
    pub(crate) fn notify_appended(&self, sequence: u64) {
        self.changed.send_replace(sequence);
    }

    /// Events after `after`, and whether the log is finished. `after == 0`
    /// starts at the oldest event still held and never fails; a non-zero
    /// `after` that was already dropped gives `Err` with the oldest readable
    /// sequence.
    pub(crate) fn read_after(&self, after: u64) -> Result<(Vec<ExecutionEvent>, bool), u64> {
        let inner = self.lock();
        if after != 0 && after < inner.evicted_through {
            return Err(inner.evicted_through + 1);
        }
        let events = inner
            .events
            .iter()
            .filter(|(event, _)| event.sequence > after)
            .map(|(event, _)| event.clone())
            .collect();
        Ok((events, inner.finished))
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub(crate) fn subscribe_consumed(&self) -> watch::Receiver<u64> {
        self.consumed.subscribe()
    }

    pub(crate) fn set_primary(&self, enabled: bool) {
        let mut inner = self.lock();
        inner.primary = enabled.then_some(0);
        drop(inner);
        self.consumed.send_modify(|value| *value += 1);
    }

    pub(crate) fn mark_consumed(&self, sequence: u64) {
        let mut inner = self.lock();
        if let Some(consumed) = inner.primary.as_mut()
            && sequence > *consumed
        {
            *consumed = sequence;
        }
        drop(inner);
        // Only a writer that found this consumer behind waits for it, and it
        // subscribes before retrying its append under the log lock: either
        // that retry sees this progress, or the subscription is counted here.
        if self.consumed.receiver_count() > 0 {
            self.consumed.send_replace(sequence);
        }
    }

    /// Removes what a finished log no longer needs to serve: every event
    /// before `Finished`, except those a primary consumer has not read yet.
    /// Only a prefix goes, so a later reader with an older cursor gets
    /// `CursorExpired`, never a silent gap. The removed events are returned
    /// for the caller to free outside its locks.
    pub(crate) fn compact_finished(&self) -> Vec<ExecutionEvent> {
        let mut inner = self.lock();
        let mut removed = Vec::new();
        if !inner.finished {
            return removed;
        }
        let finished_at = inner.last_sequence;
        let keep_from = inner
            .primary
            .map_or(finished_at, |consumed| (consumed + 1).min(finished_at));
        while inner
            .events
            .front()
            .is_some_and(|(event, _)| event.sequence < keep_from)
        {
            removed.push(self.evict_front(&mut inner));
        }
        removed
    }

    /// Readers currently following this log; each holds a `changed` receiver.
    pub(crate) fn follower_count(&self) -> usize {
        self.changed.receiver_count()
    }

    /// An ephemeral job finished: its log keeps only the outcome once nobody
    /// follows it any more (now, or when the last follower lets go). Writers
    /// waiting on the consumer are woken to see the end. Returns what was
    /// compacted now.
    pub(crate) fn finish_ephemeral(&self) -> Vec<ExecutionEvent> {
        self.compactable.store(true, Ordering::Release);
        let removed = self.compact_if_unfollowed();
        self.consumed.send_modify(|value| *value += 1);
        removed
    }

    pub(crate) fn compact_if_unfollowed(&self) -> Vec<ExecutionEvent> {
        if self.compactable.load(Ordering::Acquire) && self.follower_count() == 0 {
            self.compact_finished()
        } else {
            Vec::new()
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.lock().finished
    }

    pub(crate) fn bytes(&self) -> usize {
        self.lock().bytes
    }
}

impl Drop for JobLog {
    fn drop(&mut self) {
        let bytes = self.inner.get_mut().map(|inner| inner.bytes).unwrap_or(0);
        self.budget.release(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(max_events: usize, max_bytes: usize, global: u64) -> JobLog {
        JobLog::new(
            "job".to_string(),
            1 << 32,
            max_events,
            max_bytes,
            Arc::new(ByteBudget::new(global)),
        )
    }

    fn output_try(log: &JobLog, size: usize, last_try: bool) -> Result<u64, LogFull> {
        log.append_output(
            "a".to_string(),
            Bytes::from(vec![0; size]),
            Metadata::new(),
            false,
            0,
            last_try,
        )
    }

    fn output(log: &JobLog, size: usize) -> Result<u64, LogFull> {
        output_try(log, size, true)
    }

    #[test]
    fn primary_consumer_back_pressures_instead_of_losing_output() {
        let log = log(4, 1 << 20, 1 << 30);
        log.set_primary(true);
        for _ in 0..4 {
            assert!(output(&log, 10).is_ok());
        }
        assert_eq!(output(&log, 10), Err(LogFull::ConsumerBehind));
        let (events, _) = log.read_after(0).unwrap();
        log.mark_consumed(events[1].sequence);
        assert!(output(&log, 10).is_ok());
        // A cursor older than what is held reports the gap; 0 starts at the
        // oldest event still held.
        assert!(log.read_after(events[0].sequence - 1).is_err());
        assert_eq!(log.read_after(0).unwrap().0[0].sequence, events[1].sequence);
    }

    #[test]
    fn without_primary_oldest_events_are_dropped_and_reported() {
        let log = log(2, 1 << 20, 1 << 30);
        let first = output(&log, 10).ok().unwrap();
        output(&log, 10).ok().unwrap();
        output(&log, 10).ok().unwrap();
        assert_eq!(log.read_after(first - 1), Err(first + 1));
        assert_eq!(log.read_after(first).unwrap().0.len(), 2);
    }

    #[test]
    fn the_shared_budget_is_never_exceeded_by_output() {
        let limit = 700;
        let budget = Arc::new(ByteBudget::new(limit));
        let a = JobLog::new("a".into(), 0, 100, 1 << 20, budget.clone());
        let b = JobLog::new("b".into(), 0, 100, 1 << 20, budget.clone());
        b.set_primary(true);
        assert!(output(&a, 200).is_ok());
        assert!(output(&b, 200).is_ok());
        // Its own unread output is in the way: wait for the consumer.
        assert_eq!(output(&b, 200), Err(LogFull::ConsumerBehind));
        // Caught up, it gives up its own events but never forces past the
        // limit: when other jobs hold the budget it gets `BudgetFull`, and its
        // writer keeps retrying instead of writing past the limit.
        let (events, _) = b.read_after(0).unwrap();
        b.mark_consumed(events[0].sequence);
        assert!(output(&b, 200).is_ok());
        assert!(budget.used() <= limit);
        let mut caught_up = Vec::new();
        for name in ["c", "d", "e", "f"] {
            let log = JobLog::new(name.into(), 0, 100, 1 << 20, budget.clone());
            log.set_primary(true);
            assert_eq!(output(&log, 200), Err(LogFull::BudgetFull));
            assert!(budget.used() <= limit);
            caught_up.push(log);
        }
        // Without a primary consumer the first try reports the full budget;
        // only the last try drops the event, recording a cursor gap.
        let g = JobLog::new("g".into(), 100, 100, 1 << 20, budget.clone());
        assert_eq!(output_try(&g, 200, false), Err(LogFull::BudgetFull));
        let dropped = output_try(&g, 200, true).unwrap();
        assert_eq!(g.read_after(100), Err(dropped + 1));
        assert!(g.read_after(0).unwrap().0.is_empty());
        assert!(budget.used() <= limit);
        // Lifecycle events are the only allowance past the limit.
        let before = budget.used();
        g.append(None, EventKind::Accepted, 0);
        assert_eq!(budget.used(), before + EVENT_OVERHEAD as u64);
        drop((a, b, g, caught_up));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn consumer_progress_is_sent_only_to_a_waiting_writer() {
        let log = log(4, 1 << 20, 1 << 20);
        log.set_primary(true);
        let before = *log.consumed.borrow();
        // Nobody waits for this consumer: nothing to send.
        log.mark_consumed(5);
        assert_eq!(*log.consumed.borrow(), before);
        let mut writer = log.subscribe_consumed();
        log.mark_consumed(6);
        assert!(writer.has_changed().unwrap());
        assert_eq!(*writer.borrow_and_update(), 6);
    }

    #[test]
    fn a_finished_log_keeps_only_unread_events_and_its_outcome() {
        let budget = Arc::new(ByteBudget::new(1 << 20));
        let finish = |log: &JobLog| {
            log.append(
                None,
                EventKind::Finished {
                    state: super::super::api::JobState::Succeeded,
                    outputs: Vec::new(),
                    error: None,
                },
                0,
            )
        };
        // Without a primary consumer only Finished stays.
        let log = JobLog::new("a".into(), 100, 100, 1 << 20, budget.clone());
        log.append(None, EventKind::Accepted, 0);
        let first = output(&log, 1_000).unwrap();
        output(&log, 1_000).unwrap();
        log.compact_finished();
        assert_eq!(log.read_after(0).unwrap().0.len(), 3, "not finished yet");
        let finished = finish(&log);
        log.compact_finished();
        let (events, done) = log.read_after(0).unwrap();
        assert!(done);
        assert_eq!(
            events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![finished]
        );
        assert_eq!(log.read_after(first), Err(finished));
        assert_eq!(budget.used(), EVENT_OVERHEAD as u64);

        // A primary consumer keeps what it has not read, until it lets go.
        let log = JobLog::new("b".into(), 100, 100, 1 << 20, budget.clone());
        log.set_primary(true);
        let read = output(&log, 1_000).unwrap();
        let unread = output(&log, 1_000).unwrap();
        log.mark_consumed(read);
        let finished = finish(&log);
        log.compact_finished();
        let kept: Vec<u64> = log
            .read_after(read)
            .unwrap()
            .0
            .iter()
            .map(|e| e.sequence)
            .collect();
        assert_eq!(kept, vec![unread, finished]);
        log.set_primary(false);
        log.compact_finished();
        assert_eq!(log.read_after(0).unwrap().0.len(), 1);
    }

    #[test]
    fn a_followed_log_is_compacted_by_its_last_follower() {
        let budget = Arc::new(ByteBudget::new(1 << 20));
        let log = JobLog::new("f".into(), 100, 100, 1 << 20, budget);
        let first = log.subscribe();
        let second = log.subscribe();
        log.append(None, EventKind::Accepted, 0);
        output(&log, 100).unwrap();
        log.append(
            None,
            EventKind::Finished {
                state: super::super::api::JobState::Succeeded,
                outputs: Vec::new(),
                error: None,
            },
            0,
        );
        log.finish_ephemeral();
        assert_eq!(
            log.read_after(0).unwrap().0.len(),
            3,
            "followers still reading"
        );
        drop(first);
        log.compact_if_unfollowed();
        assert_eq!(log.read_after(0).unwrap().0.len(), 3, "one follower left");
        drop(second);
        log.compact_if_unfollowed();
        assert_eq!(log.read_after(0).unwrap().0.len(), 1);
    }

    #[test]
    fn budgeted_bytes_hold_their_permit_until_the_last_clone() {
        let budget = Arc::new(ByteBudget::new(100));
        let mut permit = Permit::new(budget.clone());
        assert!(permit.grow(60));
        assert!(!permit.grow(60));
        let bytes = budgeted_bytes(Bytes::from(vec![1u8; 60]), permit);
        let clone = bytes.slice(10..20);
        drop(bytes);
        assert_eq!(budget.used(), 60);
        drop(clone);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn finished_outputs_do_not_count_against_event_budget() {
        let budget = Arc::new(ByteBudget::new(1_000));
        let log = JobLog::new("j".into(), 0, 100, 1_000, budget.clone());
        log.append(
            None,
            EventKind::Finished {
                state: super::super::api::JobState::Succeeded,
                outputs: vec![super::super::api::Object::inline(vec![0u8; 1 << 20])],
                error: None,
            },
            0,
        );
        assert_eq!(budget.used(), EVENT_OVERHEAD as u64);
    }
}
