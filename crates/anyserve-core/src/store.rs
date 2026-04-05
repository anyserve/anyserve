use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio::sync::{Mutex, watch};
use uuid::Uuid;

use crate::model::{
    AttemptRecord, AttemptState, Attributes, EventKind, JobEvent, JobRecord, JobState,
    LeaseAssignment, LeaseRecord, ObjectRef, StreamRecord, StreamState, WorkerRecord,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseDispatchMode {
    KernelOrderedCandidates,
    StoreNative,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateTransition<T> {
    pub value: T,
    pub streams_to_finalize: Vec<StreamRecord>,
    pub job_event_updates: Vec<String>,
    pub job_stream_updates: Vec<String>,
}

impl<T> StateTransition<T> {
    pub fn new(value: T, streams_to_finalize: Vec<StreamRecord>) -> Self {
        Self {
            value,
            streams_to_finalize,
            job_event_updates: Vec::new(),
            job_stream_updates: Vec::new(),
        }
    }

    pub fn without_streams(value: T) -> Self {
        Self::new(value, Vec::new())
    }

    pub fn with_job_event_updates(mut self, job_ids: impl IntoIterator<Item = String>) -> Self {
        self.job_event_updates.extend(job_ids);
        self
    }

    pub fn with_job_stream_updates(mut self, job_ids: impl IntoIterator<Item = String>) -> Self {
        self.job_stream_updates.extend(job_ids);
        self
    }
}

#[async_trait]
pub trait StateStore: Send + Sync {
    fn lease_dispatch_mode(&self) -> LeaseDispatchMode {
        LeaseDispatchMode::KernelOrderedCandidates
    }

    async fn create_job(&self, job: JobRecord) -> Result<()>;
    async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>>;
    async fn list_jobs(&self) -> Result<Vec<JobRecord>>;
    async fn list_pending_jobs(&self) -> Result<Vec<JobRecord>>;
    async fn update_job(&self, job: JobRecord) -> Result<()>;
    async fn try_assign_job(
        &self,
        worker_id: &str,
        ordered_candidates: &[JobRecord],
        default_lease_ttl_secs: u64,
        now_ms: u64,
    ) -> Result<Option<LeaseAssignment>>;
    async fn report_event_transition(
        &self,
        worker_id: &str,
        lease_id: &str,
        kind: EventKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<()>;
    async fn cancel_job_transition(
        &self,
        job_id: &str,
        now_ms: u64,
    ) -> Result<StateTransition<JobRecord>>;
    async fn complete_lease_transition(
        &self,
        worker_id: &str,
        lease_id: &str,
        outputs: Vec<ObjectRef>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<StateTransition<()>>;
    async fn fail_lease_transition(
        &self,
        worker_id: &str,
        lease_id: &str,
        reason: String,
        retryable: bool,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<StateTransition<()>>;
    async fn reap_expired_leases_transition(&self, now_ms: u64) -> Result<StateTransition<()>>;

    async fn append_job_event(
        &self,
        job_id: &str,
        kind: EventKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<JobEvent>;
    async fn job_events_after(&self, job_id: &str, after_sequence: u64) -> Result<Vec<JobEvent>>;
    async fn subscribe_job_events(&self, job_id: &str) -> watch::Receiver<u64>;

    async fn create_attempt(&self, attempt: AttemptRecord) -> Result<()>;
    async fn get_attempt(&self, attempt_id: &str) -> Result<Option<AttemptRecord>>;
    async fn get_attempt_for_lease(&self, lease_id: &str) -> Result<Option<AttemptRecord>>;
    async fn list_attempts_for_job(&self, job_id: &str) -> Result<Vec<AttemptRecord>>;
    async fn update_attempt(&self, attempt: AttemptRecord) -> Result<()>;

    async fn upsert_worker(&self, worker: WorkerRecord) -> Result<()>;
    async fn get_worker(&self, worker_id: &str) -> Result<Option<WorkerRecord>>;
    async fn list_workers(&self) -> Result<Vec<WorkerRecord>>;

    async fn create_lease(&self, lease: LeaseRecord) -> Result<()>;
    async fn update_lease(&self, lease: LeaseRecord) -> Result<()>;
    async fn get_lease(&self, lease_id: &str) -> Result<Option<LeaseRecord>>;
    async fn list_leases(&self) -> Result<Vec<LeaseRecord>>;
    async fn delete_lease(&self, lease_id: &str) -> Result<Option<LeaseRecord>>;

    async fn create_stream(&self, stream: StreamRecord) -> Result<()>;
    async fn get_stream(&self, stream_id: &str) -> Result<Option<StreamRecord>>;
    async fn list_streams_for_job(&self, job_id: &str) -> Result<Vec<StreamRecord>>;
    async fn update_stream(&self, stream: StreamRecord) -> Result<()>;
    async fn subscribe_job_streams(&self, job_id: &str) -> watch::Receiver<u64>;
}

#[derive(Default)]
struct MemoryState {
    jobs: BTreeMap<String, JobRecord>,
    events: BTreeMap<String, Vec<JobEvent>>,
    attempts: BTreeMap<String, AttemptRecord>,
    workers: BTreeMap<String, WorkerRecord>,
    leases: BTreeMap<String, LeaseRecord>,
    streams: BTreeMap<String, StreamRecord>,
}

#[derive(Default)]
struct MemoryStateWatchers {
    job_events: BTreeMap<String, watch::Sender<u64>>,
    job_streams: BTreeMap<String, watch::Sender<u64>>,
}

pub struct MemoryStateStore {
    inner: Mutex<MemoryState>,
    watchers: Mutex<MemoryStateWatchers>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self {
            inner: Mutex::new(MemoryState::default()),
            watchers: Mutex::new(MemoryStateWatchers::default()),
        }
    }
}

impl MemoryStateStore {
    async fn notify_job_events(&self, job_ids: &BTreeSet<String>) {
        if job_ids.is_empty() {
            return;
        }
        let mut watchers = self.watchers.lock().await;
        for job_id in job_ids {
            bump_version(
                watchers
                    .job_events
                    .entry(job_id.clone())
                    .or_insert_with(version_sender),
            );
        }
    }

    async fn notify_job_streams(&self, job_ids: &BTreeSet<String>) {
        if job_ids.is_empty() {
            return;
        }
        let mut watchers = self.watchers.lock().await;
        for job_id in job_ids {
            bump_version(
                watchers
                    .job_streams
                    .entry(job_id.clone())
                    .or_insert_with(version_sender),
            );
        }
    }
}

fn latest_attempt_id_for_lease(inner: &MemoryState, lease_id: &str) -> Option<String> {
    inner
        .attempts
        .values()
        .filter(|attempt| attempt.lease_id == lease_id)
        .max_by_key(|attempt| (attempt.created_at_ms, attempt.attempt_id.clone()))
        .map(|attempt| attempt.attempt_id.clone())
}

fn append_job_event_locked(
    inner: &mut MemoryState,
    job_id: &str,
    kind: EventKind,
    payload: Vec<u8>,
    metadata: Attributes,
    now_ms: u64,
) -> Result<JobEvent> {
    if !inner.jobs.contains_key(job_id) {
        bail!("job '{}' does not exist", job_id);
    }

    let events = inner.events.entry(job_id.to_string()).or_default();
    let event = JobEvent {
        job_id: job_id.to_string(),
        sequence: events.len() as u64 + 1,
        kind,
        payload,
        metadata,
        created_at_ms: now_ms,
    };
    events.push(event.clone());
    Ok(event)
}

fn close_job_streams_locked(
    inner: &mut MemoryState,
    job_id: &str,
    now_ms: u64,
) -> Vec<StreamRecord> {
    inner
        .streams
        .values_mut()
        .filter(|stream| stream.job_id == job_id && !stream.state.is_terminal())
        .map(|stream| {
            stream.state = StreamState::Closed;
            stream.closed_at_ms = Some(now_ms);
            stream.clone()
        })
        .collect()
}

fn close_lease_streams_locked(
    inner: &mut MemoryState,
    job_id: &str,
    lease_id: &str,
    now_ms: u64,
) -> Vec<StreamRecord> {
    inner
        .streams
        .values_mut()
        .filter(|stream| {
            stream.job_id == job_id
                && stream.lease_id.as_deref() == Some(lease_id)
                && !stream.state.is_terminal()
        })
        .map(|stream| {
            stream.state = StreamState::Closed;
            stream.closed_at_ms = Some(now_ms);
            stream.clone()
        })
        .collect()
}

#[async_trait]
impl StateStore for MemoryStateStore {
    async fn create_job(&self, job: JobRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.jobs.contains_key(&job.job_id) {
            bail!("job '{}' already exists", job.job_id);
        }
        inner.jobs.insert(job.job_id.clone(), job);
        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>> {
        let inner = self.inner.lock().await;
        Ok(inner.jobs.get(job_id).cloned())
    }

    async fn list_jobs(&self) -> Result<Vec<JobRecord>> {
        let inner = self.inner.lock().await;
        Ok(inner.jobs.values().cloned().collect())
    }

    async fn list_pending_jobs(&self) -> Result<Vec<JobRecord>> {
        let inner = self.inner.lock().await;
        Ok(inner
            .jobs
            .values()
            .filter(|job| job.state == JobState::Pending)
            .cloned()
            .collect())
    }

    async fn update_job(&self, job: JobRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if !inner.jobs.contains_key(&job.job_id) {
            bail!("job '{}' does not exist", job.job_id);
        }
        inner.jobs.insert(job.job_id.clone(), job);
        Ok(())
    }

    async fn try_assign_job(
        &self,
        worker_id: &str,
        ordered_candidates: &[JobRecord],
        default_lease_ttl_secs: u64,
        now_ms: u64,
    ) -> Result<Option<LeaseAssignment>> {
        let mut notify_job_ids = BTreeSet::new();
        let mut assigned = None;

        {
            let mut inner = self.inner.lock().await;
            let worker = inner
                .workers
                .get(worker_id)
                .cloned()
                .ok_or_else(|| anyhow!("worker '{}' does not exist", worker_id))?;
            if worker.expires_at_ms <= now_ms {
                bail!("worker '{}' heartbeat expired", worker_id);
            }

            let active_leases = inner
                .leases
                .values()
                .filter(|lease| lease.worker_id == worker_id)
                .count() as u32;
            if active_leases >= worker.spec.max_active_leases {
                return Ok(None);
            }

            for candidate in ordered_candidates {
                let Some(current) = inner.jobs.get(&candidate.job_id).cloned() else {
                    continue;
                };
                if current.state != JobState::Pending {
                    continue;
                }

                let lease_id = Uuid::new_v4().to_string();
                let attempt_id = Uuid::new_v4().to_string();
                let lease_ttl_secs = if current.spec.policy.lease_ttl_secs == 0 {
                    default_lease_ttl_secs
                } else {
                    current.spec.policy.lease_ttl_secs
                };
                let lease = LeaseRecord {
                    lease_id: lease_id.clone(),
                    job_id: current.job_id.clone(),
                    worker_id: worker_id.to_string(),
                    issued_at_ms: now_ms,
                    expires_at_ms: now_ms + lease_ttl_secs * 1000,
                };
                let attempt = AttemptRecord {
                    attempt_id: attempt_id.clone(),
                    job_id: current.job_id.clone(),
                    worker_id: worker_id.to_string(),
                    lease_id: lease_id.clone(),
                    state: AttemptState::Leased,
                    created_at_ms: now_ms,
                    started_at_ms: None,
                    finished_at_ms: None,
                    last_error: None,
                    metadata: Attributes::new(),
                };
                let mut job = current;
                job.state = JobState::Leased;
                job.lease_id = Some(lease_id.clone());
                job.current_attempt_id = Some(attempt_id.clone());
                job.updated_at_ms = now_ms;
                job.version += 1;

                inner.jobs.insert(job.job_id.clone(), job.clone());
                inner.leases.insert(lease_id.clone(), lease.clone());
                inner.attempts.insert(attempt_id.clone(), attempt.clone());
                append_job_event_locked(
                    &mut inner,
                    &job.job_id,
                    EventKind::LeaseGranted,
                    Vec::new(),
                    BTreeMap::from([
                        ("lease_id".to_string(), lease_id),
                        ("attempt_id".to_string(), attempt_id),
                    ]),
                    now_ms,
                )?;
                notify_job_ids.insert(job.job_id.clone());
                assigned = Some(LeaseAssignment {
                    lease,
                    attempt,
                    job,
                });
                break;
            }
        }

        self.notify_job_events(&notify_job_ids).await;
        Ok(assigned)
    }

    async fn report_event_transition(
        &self,
        worker_id: &str,
        lease_id: &str,
        kind: EventKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<()> {
        let mut notify_job_ids = BTreeSet::new();

        {
            let mut inner = self.inner.lock().await;
            let lease = inner
                .leases
                .get(lease_id)
                .cloned()
                .ok_or_else(|| anyhow!("lease '{}' does not exist", lease_id))?;
            if lease.worker_id != worker_id {
                bail!(
                    "lease '{}' does not belong to worker '{}'",
                    lease_id,
                    worker_id
                );
            }

            let job = inner
                .jobs
                .get_mut(&lease.job_id)
                .ok_or_else(|| anyhow!("job '{}' does not exist", lease.job_id))?;
            if matches!(
                kind,
                EventKind::Started | EventKind::Progress | EventKind::OutputReady
            ) && job.state == JobState::Leased
            {
                job.state = JobState::Running;
                job.updated_at_ms = now_ms;
                job.version += 1;
            }

            let attempt_id = latest_attempt_id_for_lease(&inner, lease_id)
                .ok_or_else(|| anyhow!("no attempt found for lease '{}'", lease_id))?;
            let attempt = inner
                .attempts
                .get_mut(&attempt_id)
                .ok_or_else(|| anyhow!("attempt '{}' does not exist", attempt_id))?;
            if matches!(
                kind,
                EventKind::Started | EventKind::Progress | EventKind::OutputReady
            ) && attempt.state == AttemptState::Leased
            {
                attempt.state = AttemptState::Running;
                attempt.started_at_ms = Some(now_ms);
            }

            append_job_event_locked(&mut inner, &lease.job_id, kind, payload, metadata, now_ms)?;
            notify_job_ids.insert(lease.job_id);
        }

        self.notify_job_events(&notify_job_ids).await;
        Ok(())
    }

    async fn cancel_job_transition(
        &self,
        job_id: &str,
        now_ms: u64,
    ) -> Result<StateTransition<JobRecord>> {
        let mut notify_job_events = BTreeSet::new();
        let mut notify_job_streams = BTreeSet::new();
        let result = {
            let mut inner = self.inner.lock().await;
            let mut job = inner
                .jobs
                .get(job_id)
                .cloned()
                .ok_or_else(|| anyhow!("job '{}' does not exist", job_id))?;
            if job.state.is_terminal() {
                StateTransition::without_streams(job)
            } else {
                if let Some(lease_id) = job.lease_id.clone() {
                    inner.leases.remove(&lease_id);
                }

                if let Some(attempt_id) = job.current_attempt_id.clone()
                    && let Some(attempt) = inner.attempts.get_mut(&attempt_id)
                {
                    attempt.state = AttemptState::Cancelled;
                    attempt.finished_at_ms = Some(now_ms);
                }

                job.state = JobState::Cancelled;
                job.lease_id = None;
                job.updated_at_ms = now_ms;
                job.version += 1;
                inner.jobs.insert(job.job_id.clone(), job.clone());

                let streams_to_finalize = close_job_streams_locked(&mut inner, &job.job_id, now_ms);
                append_job_event_locked(
                    &mut inner,
                    &job.job_id,
                    EventKind::Cancelled,
                    Vec::new(),
                    Attributes::new(),
                    now_ms,
                )?;
                notify_job_events.insert(job.job_id.clone());
                if !streams_to_finalize.is_empty() {
                    notify_job_streams.insert(job.job_id.clone());
                }

                StateTransition::new(job.clone(), streams_to_finalize)
                    .with_job_event_updates([job.job_id.clone()])
                    .with_job_stream_updates([job.job_id.clone()])
            }
        };

        self.notify_job_events(&notify_job_events).await;
        self.notify_job_streams(&notify_job_streams).await;
        Ok(result)
    }

    async fn complete_lease_transition(
        &self,
        worker_id: &str,
        lease_id: &str,
        outputs: Vec<ObjectRef>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<StateTransition<()>> {
        let mut notify_job_events = BTreeSet::new();
        let mut notify_job_streams = BTreeSet::new();
        let result = {
            let mut inner = self.inner.lock().await;
            let lease = inner
                .leases
                .get(lease_id)
                .cloned()
                .ok_or_else(|| anyhow!("lease '{}' does not exist", lease_id))?;
            if lease.worker_id != worker_id {
                bail!(
                    "lease '{}' does not belong to worker '{}'",
                    lease_id,
                    worker_id
                );
            }

            let mut job = inner
                .jobs
                .get(&lease.job_id)
                .cloned()
                .ok_or_else(|| anyhow!("job '{}' does not exist", lease.job_id))?;
            let attempt_id = latest_attempt_id_for_lease(&inner, lease_id)
                .ok_or_else(|| anyhow!("no attempt found for lease '{}'", lease_id))?;
            let mut attempt = inner
                .attempts
                .get(&attempt_id)
                .cloned()
                .ok_or_else(|| anyhow!("attempt '{}' does not exist", attempt_id))?;

            if inner.leases.remove(lease_id).is_none() {
                bail!("lease '{}' no longer exists", lease_id);
            }

            job.state = JobState::Succeeded;
            job.outputs = outputs;
            job.lease_id = None;
            job.updated_at_ms = now_ms;
            job.version += 1;
            job.last_error = None;
            inner.jobs.insert(job.job_id.clone(), job.clone());

            attempt.state = AttemptState::Succeeded;
            attempt.finished_at_ms = Some(now_ms);
            attempt.last_error = None;
            inner.attempts.insert(attempt.attempt_id.clone(), attempt);

            let streams_to_finalize =
                close_lease_streams_locked(&mut inner, &job.job_id, lease_id, now_ms);
            append_job_event_locked(
                &mut inner,
                &job.job_id,
                EventKind::Succeeded,
                Vec::new(),
                metadata,
                now_ms,
            )?;
            notify_job_events.insert(job.job_id.clone());
            if !streams_to_finalize.is_empty() {
                notify_job_streams.insert(job.job_id.clone());
            }

            StateTransition::new((), streams_to_finalize)
                .with_job_event_updates([job.job_id.clone()])
                .with_job_stream_updates([job.job_id.clone()])
        };

        self.notify_job_events(&notify_job_events).await;
        self.notify_job_streams(&notify_job_streams).await;
        Ok(result)
    }

    async fn fail_lease_transition(
        &self,
        worker_id: &str,
        lease_id: &str,
        reason: String,
        retryable: bool,
        mut metadata: Attributes,
        now_ms: u64,
    ) -> Result<StateTransition<()>> {
        let mut notify_job_events = BTreeSet::new();
        let mut notify_job_streams = BTreeSet::new();
        let result = {
            let mut inner = self.inner.lock().await;
            let lease = inner
                .leases
                .get(lease_id)
                .cloned()
                .ok_or_else(|| anyhow!("lease '{}' does not exist", lease_id))?;
            if lease.worker_id != worker_id {
                bail!(
                    "lease '{}' does not belong to worker '{}'",
                    lease_id,
                    worker_id
                );
            }

            let mut job = inner
                .jobs
                .get(&lease.job_id)
                .cloned()
                .ok_or_else(|| anyhow!("job '{}' does not exist", lease.job_id))?;
            let attempt_id = latest_attempt_id_for_lease(&inner, lease_id)
                .ok_or_else(|| anyhow!("no attempt found for lease '{}'", lease_id))?;
            let mut attempt = inner
                .attempts
                .get(&attempt_id)
                .cloned()
                .ok_or_else(|| anyhow!("attempt '{}' does not exist", attempt_id))?;

            if inner.leases.remove(lease_id).is_none() {
                bail!("lease '{}' no longer exists", lease_id);
            }

            let streams_to_finalize =
                close_lease_streams_locked(&mut inner, &job.job_id, lease_id, now_ms);

            metadata.insert("reason".to_string(), reason.clone());
            append_job_event_locked(
                &mut inner,
                &job.job_id,
                EventKind::Failed,
                reason.as_bytes().to_vec(),
                metadata.clone(),
                now_ms,
            )?;

            attempt.state = AttemptState::Failed;
            attempt.finished_at_ms = Some(now_ms);
            attempt.last_error = Some(reason.clone());
            inner.attempts.insert(attempt.attempt_id.clone(), attempt);

            job.last_error = Some(reason);
            job.updated_at_ms = now_ms;
            job.version += 1;
            job.lease_id = None;

            if retryable {
                job.state = JobState::Pending;
                job.current_attempt_id = None;
                inner.jobs.insert(job.job_id.clone(), job.clone());
                append_job_event_locked(
                    &mut inner,
                    &job.job_id,
                    EventKind::Requeued,
                    Vec::new(),
                    metadata,
                    now_ms,
                )?;
            } else {
                job.state = JobState::Failed;
                inner.jobs.insert(job.job_id.clone(), job.clone());
            }

            notify_job_events.insert(job.job_id.clone());
            if !streams_to_finalize.is_empty() {
                notify_job_streams.insert(job.job_id.clone());
            }

            StateTransition::new((), streams_to_finalize)
                .with_job_event_updates([job.job_id.clone()])
                .with_job_stream_updates([job.job_id.clone()])
        };

        self.notify_job_events(&notify_job_events).await;
        self.notify_job_streams(&notify_job_streams).await;
        Ok(result)
    }

    async fn reap_expired_leases_transition(&self, now_ms: u64) -> Result<StateTransition<()>> {
        let mut notify_job_events = BTreeSet::new();
        let mut notify_job_streams = BTreeSet::new();
        let result = {
            let mut inner = self.inner.lock().await;
            let expired_leases: Vec<LeaseRecord> = inner
                .leases
                .values()
                .filter(|lease| lease.expires_at_ms <= now_ms)
                .cloned()
                .collect();
            if expired_leases.is_empty() {
                return Ok(StateTransition::without_streams(()));
            }
            let mut streams_to_finalize = Vec::new();

            for lease in expired_leases {
                if inner.leases.remove(&lease.lease_id).is_none() {
                    continue;
                }

                let closed_streams =
                    close_lease_streams_locked(&mut inner, &lease.job_id, &lease.lease_id, now_ms);
                if !closed_streams.is_empty() {
                    notify_job_streams.insert(lease.job_id.clone());
                    streams_to_finalize.extend(closed_streams);
                }

                if let Some(attempt_id) = latest_attempt_id_for_lease(&inner, &lease.lease_id)
                    && let Some(mut attempt) = inner.attempts.get(&attempt_id).cloned()
                    && !attempt.state.is_terminal()
                {
                    attempt.state = AttemptState::Expired;
                    attempt.finished_at_ms = Some(now_ms);
                    inner.attempts.insert(attempt.attempt_id.clone(), attempt);
                }

                if let Some(mut job) = inner.jobs.get(&lease.job_id).cloned()
                    && !job.state.is_terminal()
                {
                    job.state = JobState::Pending;
                    job.lease_id = None;
                    job.current_attempt_id = None;
                    job.updated_at_ms = now_ms;
                    job.version += 1;
                    inner.jobs.insert(job.job_id.clone(), job.clone());
                    append_job_event_locked(
                        &mut inner,
                        &job.job_id,
                        EventKind::LeaseExpired,
                        Vec::new(),
                        BTreeMap::from([("lease_id".to_string(), lease.lease_id.clone())]),
                        now_ms,
                    )?;
                    append_job_event_locked(
                        &mut inner,
                        &job.job_id,
                        EventKind::Requeued,
                        Vec::new(),
                        Attributes::new(),
                        now_ms,
                    )?;
                    notify_job_events.insert(job.job_id.clone());
                }
            }

            StateTransition::new((), streams_to_finalize)
                .with_job_event_updates(notify_job_events.iter().cloned())
                .with_job_stream_updates(notify_job_streams.iter().cloned())
        };

        self.notify_job_events(&notify_job_events).await;
        self.notify_job_streams(&notify_job_streams).await;
        Ok(result)
    }

    async fn append_job_event(
        &self,
        job_id: &str,
        kind: EventKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<JobEvent> {
        let event = {
            let mut inner = self.inner.lock().await;
            append_job_event_locked(&mut inner, job_id, kind, payload, metadata, now_ms)?
        };
        self.notify_job_events(&BTreeSet::from([job_id.to_string()]))
            .await;
        Ok(event)
    }

    async fn job_events_after(&self, job_id: &str, after_sequence: u64) -> Result<Vec<JobEvent>> {
        let inner = self.inner.lock().await;
        Ok(inner
            .events
            .get(job_id)
            .into_iter()
            .flatten()
            .filter(|event| event.sequence > after_sequence)
            .cloned()
            .collect())
    }

    async fn subscribe_job_events(&self, job_id: &str) -> watch::Receiver<u64> {
        let mut watchers = self.watchers.lock().await;
        watchers
            .job_events
            .entry(job_id.to_string())
            .or_insert_with(version_sender)
            .subscribe()
    }

    async fn create_attempt(&self, attempt: AttemptRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if !inner.jobs.contains_key(&attempt.job_id) {
            bail!("job '{}' does not exist", attempt.job_id);
        }
        if inner.attempts.contains_key(&attempt.attempt_id) {
            bail!("attempt '{}' already exists", attempt.attempt_id);
        }
        inner.attempts.insert(attempt.attempt_id.clone(), attempt);
        Ok(())
    }

    async fn get_attempt(&self, attempt_id: &str) -> Result<Option<AttemptRecord>> {
        let inner = self.inner.lock().await;
        Ok(inner.attempts.get(attempt_id).cloned())
    }

    async fn get_attempt_for_lease(&self, lease_id: &str) -> Result<Option<AttemptRecord>> {
        let inner = self.inner.lock().await;
        Ok(latest_attempt_id_for_lease(&inner, lease_id)
            .and_then(|attempt_id| inner.attempts.get(&attempt_id).cloned()))
    }

    async fn list_attempts_for_job(&self, job_id: &str) -> Result<Vec<AttemptRecord>> {
        let inner = self.inner.lock().await;
        let mut attempts: Vec<AttemptRecord> = inner
            .attempts
            .values()
            .filter(|attempt| attempt.job_id == job_id)
            .cloned()
            .collect();
        attempts.sort_by_key(|attempt| (attempt.created_at_ms, attempt.attempt_id.clone()));
        Ok(attempts)
    }

    async fn update_attempt(&self, attempt: AttemptRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if !inner.attempts.contains_key(&attempt.attempt_id) {
            bail!("attempt '{}' does not exist", attempt.attempt_id);
        }
        inner.attempts.insert(attempt.attempt_id.clone(), attempt);
        Ok(())
    }

    async fn upsert_worker(&self, worker: WorkerRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.workers.insert(worker.worker_id.clone(), worker);
        Ok(())
    }

    async fn get_worker(&self, worker_id: &str) -> Result<Option<WorkerRecord>> {
        let inner = self.inner.lock().await;
        Ok(inner.workers.get(worker_id).cloned())
    }

    async fn list_workers(&self) -> Result<Vec<WorkerRecord>> {
        let inner = self.inner.lock().await;
        Ok(inner.workers.values().cloned().collect())
    }

    async fn create_lease(&self, lease: LeaseRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if !inner.jobs.contains_key(&lease.job_id) {
            return Err(anyhow!("job '{}' does not exist", lease.job_id));
        }
        if !inner.workers.contains_key(&lease.worker_id) {
            return Err(anyhow!("worker '{}' does not exist", lease.worker_id));
        }
        if inner.leases.contains_key(&lease.lease_id) {
            bail!("lease '{}' already exists", lease.lease_id);
        }
        if inner
            .leases
            .values()
            .any(|existing| existing.job_id == lease.job_id)
        {
            bail!("job '{}' already has an active lease", lease.job_id);
        }
        inner.leases.insert(lease.lease_id.clone(), lease);
        Ok(())
    }

    async fn update_lease(&self, lease: LeaseRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if !inner.leases.contains_key(&lease.lease_id) {
            bail!("lease '{}' does not exist", lease.lease_id);
        }
        inner.leases.insert(lease.lease_id.clone(), lease);
        Ok(())
    }

    async fn get_lease(&self, lease_id: &str) -> Result<Option<LeaseRecord>> {
        let inner = self.inner.lock().await;
        Ok(inner.leases.get(lease_id).cloned())
    }

    async fn list_leases(&self) -> Result<Vec<LeaseRecord>> {
        let inner = self.inner.lock().await;
        Ok(inner.leases.values().cloned().collect())
    }

    async fn delete_lease(&self, lease_id: &str) -> Result<Option<LeaseRecord>> {
        let mut inner = self.inner.lock().await;
        Ok(inner.leases.remove(lease_id))
    }

    async fn create_stream(&self, stream: StreamRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.streams.contains_key(&stream.stream_id) {
            bail!("stream '{}' already exists", stream.stream_id);
        }
        inner
            .streams
            .insert(stream.stream_id.clone(), stream.clone());
        drop(inner);
        self.notify_job_streams(&BTreeSet::from([stream.job_id.clone()]))
            .await;
        Ok(())
    }

    async fn get_stream(&self, stream_id: &str) -> Result<Option<StreamRecord>> {
        let inner = self.inner.lock().await;
        Ok(inner.streams.get(stream_id).cloned())
    }

    async fn list_streams_for_job(&self, job_id: &str) -> Result<Vec<StreamRecord>> {
        let inner = self.inner.lock().await;
        let mut streams: Vec<StreamRecord> = inner
            .streams
            .values()
            .filter(|stream| stream.job_id == job_id)
            .cloned()
            .collect();
        streams.sort_by_key(|stream| (stream.created_at_ms, stream.stream_id.clone()));
        Ok(streams)
    }

    async fn update_stream(&self, stream: StreamRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if !inner.streams.contains_key(&stream.stream_id) {
            bail!("stream '{}' does not exist", stream.stream_id);
        }
        inner
            .streams
            .insert(stream.stream_id.clone(), stream.clone());
        drop(inner);
        self.notify_job_streams(&BTreeSet::from([stream.job_id.clone()]))
            .await;
        Ok(())
    }

    async fn subscribe_job_streams(&self, job_id: &str) -> watch::Receiver<u64> {
        let mut watchers = self.watchers.lock().await;
        watchers
            .job_streams
            .entry(job_id.to_string())
            .or_insert_with(version_sender)
            .subscribe()
    }
}

fn version_sender() -> watch::Sender<u64> {
    let (sender, _) = watch::channel(0);
    sender
}

fn bump_version(sender: &watch::Sender<u64>) {
    let next = sender.borrow().saturating_add(1);
    sender.send_replace(next);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{MemoryStateStore, StateStore};
    use crate::model::{Attributes, EventKind, JobRecord, JobState, StreamRecord};

    #[tokio::test]
    async fn appending_job_event_notifies_subscribers() {
        let store = MemoryStateStore::new();
        store
            .create_job(JobRecord {
                job_id: "job-1".to_string(),
                ..JobRecord::default()
            })
            .await
            .unwrap();
        let mut updates = store.subscribe_job_events("job-1").await;

        store
            .append_job_event(
                "job-1",
                EventKind::Accepted,
                Vec::new(),
                Attributes::new(),
                1,
            )
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_millis(50), updates.changed())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn creating_stream_notifies_job_stream_watchers() {
        let store = MemoryStateStore::new();
        store
            .create_job(JobRecord {
                job_id: "job-1".to_string(),
                state: JobState::Pending,
                ..JobRecord::default()
            })
            .await
            .unwrap();
        let mut updates = store.subscribe_job_streams("job-1").await;

        store
            .create_stream(StreamRecord {
                stream_id: "stream-1".to_string(),
                job_id: "job-1".to_string(),
                ..StreamRecord::default()
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_millis(50), updates.changed())
            .await
            .unwrap()
            .unwrap();
    }
}
