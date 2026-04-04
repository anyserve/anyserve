use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::model::{
    AttemptRecord, AttemptState, Attributes, EventKind, Frame, FrameKind, JobRecord, JobSpec,
    JobState, LeaseAssignment, LeaseRecord, ObjectRef, StreamDirection, StreamRecord, StreamScope,
    StreamState, WorkerRecord, WorkerSpec, WorkerStatus,
};
use crate::scheduler::Scheduler;
use crate::store::{StateStore, StreamStore};

pub struct Kernel {
    state_store: Arc<dyn StateStore>,
    stream_store: Arc<dyn StreamStore>,
    scheduler: Arc<dyn Scheduler>,
    dispatch_lock: Mutex<()>,
    heartbeat_ttl_secs: u64,
    default_lease_ttl_secs: u64,
}

#[derive(Clone, Debug)]
pub struct OpenStreamCommand {
    pub job_id: String,
    pub attempt_id: Option<String>,
    pub worker_id: Option<String>,
    pub lease_id: Option<String>,
    pub stream_name: String,
    pub scope: StreamScope,
    pub direction: StreamDirection,
    pub metadata: Attributes,
}

impl Kernel {
    pub fn new(
        state_store: Arc<dyn StateStore>,
        stream_store: Arc<dyn StreamStore>,
        scheduler: Arc<dyn Scheduler>,
        heartbeat_ttl_secs: u64,
        default_lease_ttl_secs: u64,
    ) -> Self {
        Self {
            state_store,
            stream_store,
            scheduler,
            dispatch_lock: Mutex::new(()),
            heartbeat_ttl_secs,
            default_lease_ttl_secs,
        }
    }

    pub fn heartbeat_ttl_secs(&self) -> u64 {
        self.heartbeat_ttl_secs
    }

    pub async fn submit_job(
        &self,
        requested_job_id: Option<String>,
        spec: JobSpec,
    ) -> Result<JobRecord> {
        validate_job_spec(&spec)?;

        let now_ms = now_ms();
        let job = JobRecord {
            job_id: requested_job_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            state: JobState::Pending,
            spec,
            outputs: Vec::new(),
            lease_id: None,
            version: 1,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            last_error: None,
            current_attempt_id: None,
        };
        self.state_store.create_job(job.clone()).await?;
        self.state_store
            .append_job_event(
                &job.job_id,
                EventKind::Accepted,
                Vec::new(),
                Attributes::new(),
                now_ms,
            )
            .await?;
        Ok(job)
    }

    pub async fn watch_job(
        &self,
        job_id: &str,
        after_sequence: u64,
    ) -> Result<Vec<crate::model::JobEvent>> {
        if self.state_store.get_job(job_id).await?.is_none() {
            bail!("job '{}' does not exist", job_id);
        }
        self.state_store
            .job_events_after(job_id, after_sequence)
            .await
    }

    pub async fn get_job(&self, job_id: &str) -> Result<JobRecord> {
        self.state_store
            .get_job(job_id)
            .await?
            .ok_or_else(|| anyhow!("job '{}' does not exist", job_id))
    }

    pub async fn list_jobs(&self) -> Result<Vec<JobRecord>> {
        let mut jobs = self.state_store.list_jobs().await?;
        jobs.sort_by(|left, right| {
            right
                .updated_at_ms
                .cmp(&left.updated_at_ms)
                .then_with(|| right.created_at_ms.cmp(&left.created_at_ms))
                .then_with(|| left.job_id.cmp(&right.job_id))
        });
        Ok(jobs)
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<JobRecord> {
        let _guard = self.dispatch_lock.lock().await;
        let mut job = self.get_job(job_id).await?;
        if job.state.is_terminal() {
            return Ok(job);
        }

        if let Some(lease_id) = job.lease_id.clone() {
            self.state_store.delete_lease(&lease_id).await?;
        }

        if let Some(attempt_id) = job.current_attempt_id.clone()
            && let Some(mut attempt) = self.state_store.get_attempt(&attempt_id).await?
        {
            attempt.state = AttemptState::Cancelled;
            attempt.finished_at_ms = Some(now_ms());
            self.state_store.update_attempt(attempt).await?;
        }

        let now_ms = now_ms();
        job.state = JobState::Cancelled;
        job.lease_id = None;
        job.updated_at_ms = now_ms;
        job.version += 1;
        self.state_store.update_job(job.clone()).await?;
        self.close_streams_for_job(&job.job_id, now_ms).await?;
        self.state_store
            .append_job_event(
                job_id,
                EventKind::Cancelled,
                Vec::new(),
                Attributes::new(),
                now_ms,
            )
            .await?;
        Ok(job)
    }

    pub async fn get_attempt(&self, attempt_id: &str) -> Result<AttemptRecord> {
        self.state_store
            .get_attempt(attempt_id)
            .await?
            .ok_or_else(|| anyhow!("attempt '{}' does not exist", attempt_id))
    }

    pub async fn list_attempts(&self, job_id: &str) -> Result<Vec<AttemptRecord>> {
        if self.state_store.get_job(job_id).await?.is_none() {
            bail!("job '{}' does not exist", job_id);
        }
        self.state_store.list_attempts_for_job(job_id).await
    }

    pub async fn register_worker(
        &self,
        requested_worker_id: Option<String>,
        mut spec: WorkerSpec,
    ) -> Result<WorkerRecord> {
        validate_worker_spec(&spec)?;

        let now_ms = now_ms();
        let worker_id = requested_worker_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        if spec.max_active_leases == 0 {
            spec.max_active_leases = 1;
        }
        let worker = WorkerRecord {
            worker_id,
            status: WorkerStatus {
                available_capacity: spec.total_capacity.clone(),
                active_leases: 0,
                metadata: BTreeMap::new(),
                last_seen_at_ms: now_ms,
            },
            spec,
            registered_at_ms: now_ms,
            expires_at_ms: now_ms + self.heartbeat_ttl_secs * 1000,
        };
        self.state_store.upsert_worker(worker.clone()).await?;
        Ok(worker)
    }

    pub async fn heartbeat_worker(
        &self,
        worker_id: &str,
        available_capacity: BTreeMap<String, i64>,
        active_leases: u32,
        metadata: Attributes,
    ) -> Result<WorkerRecord> {
        let mut worker = self
            .state_store
            .get_worker(worker_id)
            .await?
            .ok_or_else(|| anyhow!("worker '{}' does not exist", worker_id))?;
        let now_ms = now_ms();
        worker.status.available_capacity = available_capacity;
        worker.status.active_leases = active_leases;
        worker.status.metadata = metadata;
        worker.status.last_seen_at_ms = now_ms;
        worker.expires_at_ms = now_ms + self.heartbeat_ttl_secs * 1000;
        self.state_store.upsert_worker(worker.clone()).await?;
        Ok(worker)
    }

    pub async fn poll_lease(&self, worker_id: &str) -> Result<Option<LeaseAssignment>> {
        let _guard = self.dispatch_lock.lock().await;
        let now_ms = now_ms();
        self.reap_expired_leases(now_ms).await?;

        let worker = self
            .state_store
            .get_worker(worker_id)
            .await?
            .ok_or_else(|| anyhow!("worker '{}' does not exist", worker_id))?;
        if worker.expires_at_ms <= now_ms {
            bail!("worker '{}' heartbeat expired", worker_id);
        }

        let active_leases = self
            .state_store
            .list_leases()
            .await?
            .into_iter()
            .filter(|lease| lease.worker_id == worker_id)
            .count() as u32;
        if active_leases >= worker.spec.max_active_leases {
            return Ok(None);
        }

        let jobs = self.state_store.list_jobs().await?;
        let Some(mut job) = self.scheduler.select_job_for_worker(&worker, &jobs) else {
            return Ok(None);
        };

        let lease_id = Uuid::new_v4().to_string();
        let attempt_id = Uuid::new_v4().to_string();
        let lease_ttl_secs = if job.spec.policy.lease_ttl_secs == 0 {
            self.default_lease_ttl_secs
        } else {
            job.spec.policy.lease_ttl_secs
        };
        let lease = LeaseRecord {
            lease_id: lease_id.clone(),
            job_id: job.job_id.clone(),
            worker_id: worker_id.to_string(),
            issued_at_ms: now_ms,
            expires_at_ms: now_ms + lease_ttl_secs * 1000,
        };
        let attempt = AttemptRecord {
            attempt_id: attempt_id.clone(),
            job_id: job.job_id.clone(),
            worker_id: worker_id.to_string(),
            lease_id: lease_id.clone(),
            state: AttemptState::Leased,
            created_at_ms: now_ms,
            started_at_ms: None,
            finished_at_ms: None,
            last_error: None,
            metadata: Attributes::new(),
        };

        job.state = JobState::Leased;
        job.lease_id = Some(lease_id.clone());
        job.current_attempt_id = Some(attempt_id.clone());
        job.updated_at_ms = now_ms;
        job.version += 1;

        self.state_store.put_lease(lease.clone()).await?;
        self.state_store.create_attempt(attempt.clone()).await?;
        self.state_store.update_job(job.clone()).await?;
        self.state_store
            .append_job_event(
                &job.job_id,
                EventKind::LeaseGranted,
                Vec::new(),
                BTreeMap::from([
                    ("lease_id".to_string(), lease_id),
                    ("attempt_id".to_string(), attempt_id),
                ]),
                now_ms,
            )
            .await?;

        Ok(Some(LeaseAssignment {
            lease,
            attempt,
            job,
        }))
    }

    pub async fn renew_lease(&self, worker_id: &str, lease_id: &str) -> Result<LeaseRecord> {
        let mut lease = self.require_lease(worker_id, lease_id).await?;
        let job = self.get_job(&lease.job_id).await?;
        let ttl_secs = if job.spec.policy.lease_ttl_secs == 0 {
            self.default_lease_ttl_secs
        } else {
            job.spec.policy.lease_ttl_secs
        };
        lease.expires_at_ms = now_ms() + ttl_secs * 1000;
        self.state_store.put_lease(lease.clone()).await?;
        Ok(lease)
    }

    pub async fn report_event(
        &self,
        worker_id: &str,
        lease_id: &str,
        kind: EventKind,
        payload: Vec<u8>,
        metadata: Attributes,
    ) -> Result<()> {
        let lease = self.require_lease(worker_id, lease_id).await?;
        let mut job = self.get_job(&lease.job_id).await?;
        let mut attempt = self.require_attempt_for_lease(lease_id).await?;
        let now_ms = now_ms();

        if matches!(
            kind,
            EventKind::Started | EventKind::Progress | EventKind::OutputReady
        ) && job.state == JobState::Leased
        {
            job.state = JobState::Running;
            job.updated_at_ms = now_ms;
            job.version += 1;
            self.state_store.update_job(job).await?;
        }

        if matches!(
            kind,
            EventKind::Started | EventKind::Progress | EventKind::OutputReady
        ) && attempt.state == AttemptState::Leased
        {
            attempt.state = AttemptState::Running;
            attempt.started_at_ms = Some(now_ms);
            self.state_store.update_attempt(attempt).await?;
        }

        self.state_store
            .append_job_event(&lease.job_id, kind, payload, metadata, now_ms)
            .await?;
        Ok(())
    }

    pub async fn complete_lease(
        &self,
        worker_id: &str,
        lease_id: &str,
        outputs: Vec<ObjectRef>,
        metadata: Attributes,
    ) -> Result<()> {
        let _guard = self.dispatch_lock.lock().await;
        let lease = self.require_lease(worker_id, lease_id).await?;
        let mut job = self.get_job(&lease.job_id).await?;
        let mut attempt = self.require_attempt_for_lease(lease_id).await?;
        let now_ms = now_ms();

        self.state_store.delete_lease(lease_id).await?;
        job.state = JobState::Succeeded;
        job.outputs = outputs;
        job.lease_id = None;
        job.updated_at_ms = now_ms;
        job.version += 1;
        job.last_error = None;
        self.state_store.update_job(job.clone()).await?;

        attempt.state = AttemptState::Succeeded;
        attempt.finished_at_ms = Some(now_ms);
        attempt.last_error = None;
        self.state_store.update_attempt(attempt).await?;
        self.close_streams_for_lease(&job.job_id, lease_id, now_ms)
            .await?;

        self.state_store
            .append_job_event(
                &job.job_id,
                EventKind::Succeeded,
                Vec::new(),
                metadata,
                now_ms,
            )
            .await?;
        Ok(())
    }

    pub async fn fail_lease(
        &self,
        worker_id: &str,
        lease_id: &str,
        reason: String,
        retryable: bool,
        mut metadata: Attributes,
    ) -> Result<()> {
        let _guard = self.dispatch_lock.lock().await;
        let lease = self.require_lease(worker_id, lease_id).await?;
        let mut job = self.get_job(&lease.job_id).await?;
        let mut attempt = self.require_attempt_for_lease(lease_id).await?;
        let now_ms = now_ms();

        self.state_store.delete_lease(lease_id).await?;
        self.close_streams_for_lease(&job.job_id, lease_id, now_ms)
            .await?;

        metadata.insert("reason".to_string(), reason.clone());
        self.state_store
            .append_job_event(
                &job.job_id,
                EventKind::Failed,
                reason.as_bytes().to_vec(),
                metadata.clone(),
                now_ms,
            )
            .await?;

        attempt.state = AttemptState::Failed;
        attempt.finished_at_ms = Some(now_ms);
        attempt.last_error = Some(reason.clone());
        self.state_store.update_attempt(attempt).await?;

        job.last_error = Some(reason);
        job.updated_at_ms = now_ms;
        job.version += 1;
        job.lease_id = None;

        if retryable {
            job.state = JobState::Pending;
            job.current_attempt_id = None;
            self.state_store.update_job(job.clone()).await?;
            self.state_store
                .append_job_event(
                    &job.job_id,
                    EventKind::Requeued,
                    Vec::new(),
                    metadata,
                    now_ms,
                )
                .await?;
        } else {
            job.state = JobState::Failed;
            self.state_store.update_job(job).await?;
        }

        Ok(())
    }

    pub async fn open_stream(&self, command: OpenStreamCommand) -> Result<StreamRecord> {
        let OpenStreamCommand {
            job_id,
            attempt_id,
            worker_id,
            lease_id,
            stream_name,
            scope,
            direction,
            metadata,
        } = command;
        if stream_name.trim().is_empty() {
            bail!("stream_name is required");
        }
        self.get_job(&job_id).await?;

        let resolved_lease = match lease_id.as_deref() {
            Some(lease_id) => Some(
                self.require_lease_for_job(&job_id, worker_id.as_deref(), lease_id)
                    .await?,
            ),
            None => None,
        };

        let resolved_attempt = match attempt_id.as_deref() {
            Some(attempt_id) => Some(
                self.require_attempt_for_job(&job_id, worker_id.as_deref(), attempt_id)
                    .await?,
            ),
            None => match resolved_lease.as_ref() {
                Some(lease) => {
                    self.state_store
                        .get_attempt_for_lease(&lease.lease_id)
                        .await?
                }
                None => None,
            },
        };

        match scope {
            StreamScope::Job => {}
            StreamScope::Attempt => {
                if resolved_attempt.is_none() {
                    bail!("attempt-scoped streams require attempt_id");
                }
            }
            StreamScope::Lease => {
                if resolved_lease.is_none() {
                    bail!("lease-scoped streams require lease_id");
                }
            }
        }

        let now_ms = now_ms();
        let stream = StreamRecord {
            stream_id: Uuid::new_v4().to_string(),
            job_id,
            attempt_id: resolved_attempt
                .as_ref()
                .map(|attempt| attempt.attempt_id.clone()),
            lease_id: resolved_lease.as_ref().map(|lease| lease.lease_id.clone()),
            stream_name,
            scope,
            direction,
            state: StreamState::Open,
            metadata,
            created_at_ms: now_ms,
            closed_at_ms: None,
            last_sequence: 0,
        };
        self.stream_store.create_stream(stream.clone()).await?;
        Ok(stream)
    }

    pub async fn get_stream(&self, stream_id: &str) -> Result<StreamRecord> {
        self.stream_store
            .get_stream(stream_id)
            .await?
            .ok_or_else(|| anyhow!("stream '{}' does not exist", stream_id))
    }

    pub async fn list_streams(&self, job_id: &str) -> Result<Vec<StreamRecord>> {
        self.get_job(job_id).await?;
        self.stream_store.list_streams_for_job(job_id).await
    }

    pub async fn close_stream(
        &self,
        stream_id: &str,
        worker_id: Option<String>,
        lease_id: Option<String>,
        metadata: Attributes,
    ) -> Result<StreamRecord> {
        let mut stream = self.get_stream(stream_id).await?;
        if stream.state.is_terminal() {
            return Ok(stream);
        }

        self.validate_stream_write(&stream, worker_id.as_deref(), lease_id.as_deref())
            .await?;

        let now_ms = now_ms();
        stream.state = StreamState::Closed;
        stream.closed_at_ms = Some(now_ms);
        stream.metadata.extend(metadata);
        self.stream_store.update_stream(stream.clone()).await?;
        Ok(stream)
    }

    pub async fn push_frame(
        &self,
        stream_id: &str,
        worker_id: Option<String>,
        lease_id: Option<String>,
        kind: FrameKind,
        payload: Vec<u8>,
        metadata: Attributes,
    ) -> Result<Frame> {
        let mut stream = self.get_stream(stream_id).await?;
        self.validate_stream_write(&stream, worker_id.as_deref(), lease_id.as_deref())
            .await?;
        let now_ms = now_ms();
        let frame = self
            .stream_store
            .append_frame(stream_id, kind, payload, metadata, now_ms)
            .await?;

        match kind {
            FrameKind::Close => {
                stream.state = StreamState::Closed;
                stream.closed_at_ms = Some(now_ms);
                self.stream_store.update_stream(stream).await?;
            }
            FrameKind::Error => {
                stream.state = StreamState::Error;
                stream.closed_at_ms = Some(now_ms);
                self.stream_store.update_stream(stream).await?;
            }
            _ => {}
        }

        Ok(frame)
    }

    pub async fn pull_frames(&self, stream_id: &str, after_sequence: u64) -> Result<Vec<Frame>> {
        self.get_stream(stream_id).await?;
        self.stream_store
            .frames_after(stream_id, after_sequence)
            .await
    }

    async fn require_lease(&self, worker_id: &str, lease_id: &str) -> Result<LeaseRecord> {
        let lease = self
            .state_store
            .get_lease(lease_id)
            .await?
            .ok_or_else(|| anyhow!("lease '{}' does not exist", lease_id))?;
        if lease.worker_id != worker_id {
            bail!(
                "lease '{}' does not belong to worker '{}'",
                lease_id,
                worker_id
            );
        }
        Ok(lease)
    }

    async fn require_lease_for_job(
        &self,
        job_id: &str,
        worker_id: Option<&str>,
        lease_id: &str,
    ) -> Result<LeaseRecord> {
        let lease = self
            .state_store
            .get_lease(lease_id)
            .await?
            .ok_or_else(|| anyhow!("lease '{}' does not exist", lease_id))?;
        if lease.job_id != job_id {
            bail!("lease '{}' does not belong to job '{}'", lease_id, job_id);
        }
        if let Some(worker_id) = worker_id
            && lease.worker_id != worker_id
        {
            bail!(
                "lease '{}' does not belong to worker '{}'",
                lease_id,
                worker_id
            );
        }
        Ok(lease)
    }

    async fn require_attempt_for_lease(&self, lease_id: &str) -> Result<AttemptRecord> {
        self.state_store
            .get_attempt_for_lease(lease_id)
            .await?
            .ok_or_else(|| anyhow!("no attempt found for lease '{}'", lease_id))
    }

    async fn require_attempt_for_job(
        &self,
        job_id: &str,
        worker_id: Option<&str>,
        attempt_id: &str,
    ) -> Result<AttemptRecord> {
        let attempt = self
            .state_store
            .get_attempt(attempt_id)
            .await?
            .ok_or_else(|| anyhow!("attempt '{}' does not exist", attempt_id))?;
        if attempt.job_id != job_id {
            bail!(
                "attempt '{}' does not belong to job '{}'",
                attempt_id,
                job_id
            );
        }
        if let Some(worker_id) = worker_id
            && attempt.worker_id != worker_id
        {
            bail!(
                "attempt '{}' does not belong to worker '{}'",
                attempt_id,
                worker_id
            );
        }
        Ok(attempt)
    }

    async fn validate_stream_write(
        &self,
        stream: &StreamRecord,
        worker_id: Option<&str>,
        lease_id: Option<&str>,
    ) -> Result<()> {
        if stream.state.is_terminal() {
            bail!("stream '{}' is already closed", stream.stream_id);
        }

        match (stream.direction, worker_id) {
            (StreamDirection::ClientToWorker, Some(_)) => {
                bail!(
                    "workers cannot write to client-to-worker stream '{}'",
                    stream.stream_id
                );
            }
            (StreamDirection::WorkerToClient | StreamDirection::Internal, None) => {
                bail!(
                    "clients cannot write to worker-owned stream '{}'",
                    stream.stream_id
                );
            }
            _ => {}
        }

        if let Some(worker_id) = worker_id {
            let lease_id = lease_id.ok_or_else(|| {
                anyhow!(
                    "worker writes to stream '{}' require lease_id",
                    stream.stream_id
                )
            })?;
            let lease = self.require_lease(worker_id, lease_id).await?;
            if lease.job_id != stream.job_id {
                bail!(
                    "lease '{}' does not belong to stream job '{}'",
                    lease_id,
                    stream.job_id
                );
            }
            if let Some(stream_lease_id) = stream.lease_id.as_deref()
                && stream_lease_id != lease_id
            {
                bail!(
                    "stream '{}' is bound to a different lease",
                    stream.stream_id
                );
            }
            if let Some(stream_attempt_id) = stream.attempt_id.as_deref() {
                let attempt = self.get_attempt(stream_attempt_id).await?;
                if attempt.lease_id != lease_id {
                    bail!(
                        "stream '{}' is bound to a different attempt",
                        stream.stream_id
                    );
                }
            }
        } else if lease_id.is_some() {
            bail!("lease_id is only valid for worker stream operations");
        }

        Ok(())
    }

    async fn close_streams_for_job(&self, job_id: &str, now_ms: u64) -> Result<()> {
        for mut stream in self.stream_store.list_streams_for_job(job_id).await? {
            if stream.state.is_terminal() {
                continue;
            }
            stream.state = StreamState::Closed;
            stream.closed_at_ms = Some(now_ms);
            self.stream_store.update_stream(stream).await?;
        }
        Ok(())
    }

    async fn close_streams_for_lease(
        &self,
        job_id: &str,
        lease_id: &str,
        now_ms: u64,
    ) -> Result<()> {
        for mut stream in self.stream_store.list_streams_for_job(job_id).await? {
            if stream.state.is_terminal() {
                continue;
            }
            if stream.lease_id.as_deref() != Some(lease_id) {
                continue;
            }
            stream.state = StreamState::Closed;
            stream.closed_at_ms = Some(now_ms);
            self.stream_store.update_stream(stream).await?;
        }
        Ok(())
    }

    async fn reap_expired_leases(&self, now_ms: u64) -> Result<()> {
        for lease in self.state_store.list_leases().await? {
            if lease.expires_at_ms > now_ms {
                continue;
            }

            self.state_store.delete_lease(&lease.lease_id).await?;
            self.close_streams_for_lease(&lease.job_id, &lease.lease_id, now_ms)
                .await?;

            if let Some(mut attempt) = self
                .state_store
                .get_attempt_for_lease(&lease.lease_id)
                .await?
                && !attempt.state.is_terminal()
            {
                attempt.state = AttemptState::Expired;
                attempt.finished_at_ms = Some(now_ms);
                self.state_store.update_attempt(attempt).await?;
            }

            if let Some(mut job) = self.state_store.get_job(&lease.job_id).await? {
                if job.state.is_terminal() {
                    continue;
                }

                job.state = JobState::Pending;
                job.lease_id = None;
                job.current_attempt_id = None;
                job.updated_at_ms = now_ms;
                job.version += 1;
                self.state_store.update_job(job.clone()).await?;
                self.state_store
                    .append_job_event(
                        &job.job_id,
                        EventKind::LeaseExpired,
                        Vec::new(),
                        BTreeMap::from([("lease_id".to_string(), lease.lease_id.clone())]),
                        now_ms,
                    )
                    .await?;
                self.state_store
                    .append_job_event(
                        &job.job_id,
                        EventKind::Requeued,
                        Vec::new(),
                        Attributes::new(),
                        now_ms,
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

fn validate_job_spec(spec: &JobSpec) -> Result<()> {
    if spec.interface_name.trim().is_empty() {
        bail!("job interface_name is required");
    }
    Ok(())
}

fn validate_worker_spec(spec: &WorkerSpec) -> Result<()> {
    if spec.interfaces.is_empty() {
        bail!("worker must expose at least one interface");
    }
    Ok(())
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::Duration;

    use super::Kernel;
    use crate::model::{
        Attributes, EventKind, ExecutionPolicy, FrameKind, JobSpec, JobState, StreamDirection,
        StreamScope, WorkerSpec,
    };
    use crate::scheduler::BasicScheduler;
    use crate::store::{MemoryStateStore, MemoryStreamStore};

    #[tokio::test]
    async fn failed_retryable_lease_is_requeued() {
        let kernel = Kernel::new(
            Arc::new(MemoryStateStore::new()),
            Arc::new(MemoryStreamStore::new()),
            Arc::new(BasicScheduler),
            30,
            30,
        );

        let job = kernel
            .submit_job(
                Some("job-1".to_string()),
                JobSpec {
                    interface_name: "demo.execute.v1".to_string(),
                    policy: ExecutionPolicy::default(),
                    ..JobSpec::default()
                },
            )
            .await
            .unwrap();
        let worker = kernel
            .register_worker(
                Some("worker-1".to_string()),
                WorkerSpec {
                    interfaces: BTreeSet::from(["demo.execute.v1".to_string()]),
                    total_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                    max_active_leases: 1,
                    ..WorkerSpec::default()
                },
            )
            .await
            .unwrap();

        let lease = kernel
            .poll_lease(&worker.worker_id)
            .await
            .unwrap()
            .unwrap()
            .lease;

        kernel
            .fail_lease(
                &worker.worker_id,
                &lease.lease_id,
                "retry".to_string(),
                true,
                Attributes::new(),
            )
            .await
            .unwrap();

        let job = kernel.get_job(&job.job_id).await.unwrap();
        let events = kernel.watch_job(&job.job_id, 0).await.unwrap();

        assert_eq!(job.state, JobState::Pending);
        assert!(events.iter().any(|event| event.kind == EventKind::Requeued));
    }

    #[tokio::test]
    async fn list_jobs_returns_most_recently_updated_first() {
        let kernel = Kernel::new(
            Arc::new(MemoryStateStore::new()),
            Arc::new(MemoryStreamStore::new()),
            Arc::new(BasicScheduler),
            30,
            30,
        );

        let first = kernel
            .submit_job(
                Some("job-1".to_string()),
                JobSpec {
                    interface_name: "demo.execute.v1".to_string(),
                    ..JobSpec::default()
                },
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let second = kernel
            .submit_job(
                Some("job-2".to_string()),
                JobSpec {
                    interface_name: "demo.execute.v1".to_string(),
                    ..JobSpec::default()
                },
            )
            .await
            .unwrap();

        let jobs = kernel.list_jobs().await.unwrap();
        assert_eq!(
            jobs.iter().map(|job| job.job_id.as_str()).collect::<Vec<_>>(),
            vec![second.job_id.as_str(), first.job_id.as_str()]
        );
    }

    #[tokio::test]
    async fn cancelling_leased_job_cancels_attempt_and_closes_streams() {
        let kernel = Kernel::new(
            Arc::new(MemoryStateStore::new()),
            Arc::new(MemoryStreamStore::new()),
            Arc::new(BasicScheduler),
            30,
            30,
        );

        let job = kernel
            .submit_job(
                Some("job-cancel".to_string()),
                JobSpec {
                    interface_name: "demo.execute.v1".to_string(),
                    ..JobSpec::default()
                },
            )
            .await
            .unwrap();
        let worker = kernel
            .register_worker(
                Some("worker-1".to_string()),
                WorkerSpec {
                    interfaces: BTreeSet::from(["demo.execute.v1".to_string()]),
                    total_capacity: BTreeMap::from([("slot".to_string(), 1)]),
                    max_active_leases: 1,
                    ..WorkerSpec::default()
                },
            )
            .await
            .unwrap();
        let assignment = kernel
            .poll_lease(&worker.worker_id)
            .await
            .unwrap()
            .unwrap();
        let stream = kernel
            .open_stream(super::OpenStreamCommand {
                job_id: job.job_id.clone(),
                attempt_id: Some(assignment.attempt.attempt_id.clone()),
                worker_id: Some(worker.worker_id.clone()),
                lease_id: Some(assignment.lease.lease_id.clone()),
                stream_name: "output.default".to_string(),
                scope: StreamScope::Lease,
                direction: StreamDirection::WorkerToClient,
                metadata: Attributes::new(),
            })
            .await
            .unwrap();

        let job = kernel.cancel_job(&job.job_id).await.unwrap();
        let attempt = kernel
            .get_attempt(&assignment.attempt.attempt_id)
            .await
            .unwrap();
        let stream = kernel.get_stream(&stream.stream_id).await.unwrap();

        assert_eq!(job.state, JobState::Cancelled);
        assert!(job.lease_id.is_none());
        assert_eq!(attempt.state, crate::model::AttemptState::Cancelled);
        assert!(stream.state.is_terminal());
    }

    #[tokio::test]
    async fn stream_frames_can_be_written_and_read() {
        let kernel = Kernel::new(
            Arc::new(MemoryStateStore::new()),
            Arc::new(MemoryStreamStore::new()),
            Arc::new(BasicScheduler),
            30,
            30,
        );
        kernel
            .submit_job(
                Some("job-stream".to_string()),
                JobSpec {
                    interface_name: "demo.execute.v1".to_string(),
                    ..JobSpec::default()
                },
            )
            .await
            .unwrap();

        let stream = kernel
            .open_stream(super::OpenStreamCommand {
                job_id: "job-stream".to_string(),
                attempt_id: None,
                worker_id: None,
                lease_id: None,
                stream_name: "input.default".to_string(),
                scope: StreamScope::Job,
                direction: StreamDirection::ClientToWorker,
                metadata: Attributes::new(),
            })
            .await
            .unwrap();

        kernel
            .push_frame(
                &stream.stream_id,
                None,
                None,
                FrameKind::Data,
                b"hello".to_vec(),
                Attributes::new(),
            )
            .await
            .unwrap();
        kernel
            .push_frame(
                &stream.stream_id,
                None,
                None,
                FrameKind::Close,
                Vec::new(),
                Attributes::new(),
            )
            .await
            .unwrap();

        let frames = kernel.pull_frames(&stream.stream_id, 0).await.unwrap();
        let closed = kernel.get_stream(&stream.stream_id).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].payload, b"hello".to_vec());
        assert!(closed.state.is_terminal());
    }
}
