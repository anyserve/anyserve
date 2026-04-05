use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use tokio::sync::{Mutex, watch};
use uuid::Uuid;

use crate::frame::FramePlane;
use crate::model::{
    AttemptRecord, Attributes, EventKind, Frame, FrameKind, JobEvent, JobRecord, JobSpec, JobState,
    LeaseAssignment, LeaseRecord, ObjectRef, StreamDirection, StreamRecord, StreamScope,
    StreamState, WorkerRecord, WorkerSpec, WorkerStatus,
};
use crate::notify::{ClusterNotifier, ClusterSubscription};
use crate::scheduler::Scheduler;
use crate::store::{
    JobStateCounts, JobSummaryPage, LeaseDispatchMode, StateStore, StateTransition,
};

pub struct Kernel {
    state_store: Arc<dyn StateStore>,
    frame_plane: Arc<dyn FramePlane>,
    notifier: Arc<dyn ClusterNotifier>,
    scheduler: Arc<dyn Scheduler>,
    stream_cache: Mutex<BTreeMap<String, StreamRecord>>,
    heartbeat_ttl_secs: u64,
    default_lease_ttl_secs: u64,
    watch_poll_interval_ms: u64,
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
        frame_plane: Arc<dyn FramePlane>,
        notifier: Arc<dyn ClusterNotifier>,
        scheduler: Arc<dyn Scheduler>,
        heartbeat_ttl_secs: u64,
        default_lease_ttl_secs: u64,
        watch_poll_interval_ms: u64,
    ) -> Self {
        Self {
            state_store,
            frame_plane,
            notifier,
            scheduler,
            stream_cache: Mutex::new(BTreeMap::new()),
            heartbeat_ttl_secs,
            default_lease_ttl_secs,
            watch_poll_interval_ms,
        }
    }

    pub fn heartbeat_ttl_secs(&self) -> u64 {
        self.heartbeat_ttl_secs
    }

    pub fn watch_poll_interval_ms(&self) -> u64 {
        self.watch_poll_interval_ms
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
        self.publish_job_events_best_effort(&job.job_id).await;
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

    pub async fn subscribe_job_events(&self, job_id: &str) -> Result<watch::Receiver<u64>> {
        self.get_job(job_id).await?;
        Ok(self.state_store.subscribe_job_events(job_id).await)
    }

    pub async fn subscribe_cluster_job_events(&self, job_id: &str) -> Result<ClusterSubscription> {
        self.get_job(job_id).await?;
        Ok(self
            .notifier
            .subscribe_job_events(job_id)
            .await
            .unwrap_or_else(|_| ClusterSubscription::noop()))
    }

    pub async fn subscribe_cluster_job_streams(&self, job_id: &str) -> Result<ClusterSubscription> {
        self.get_job(job_id).await?;
        Ok(self
            .notifier
            .subscribe_job_streams(job_id)
            .await
            .unwrap_or_else(|_| ClusterSubscription::noop()))
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

    pub async fn job_state_counts(&self) -> Result<JobStateCounts> {
        self.state_store.job_state_counts().await
    }

    pub async fn list_job_summary_page(
        &self,
        states: &[JobState],
        limit: usize,
        offset: usize,
    ) -> Result<JobSummaryPage> {
        self.state_store
            .list_job_summary_page(states, limit, offset)
            .await
    }

    pub async fn list_workers(&self) -> Result<Vec<WorkerRecord>> {
        let mut workers = self.state_store.list_workers().await?;
        workers.sort_by(|left, right| {
            right
                .status
                .last_seen_at_ms
                .cmp(&left.status.last_seen_at_ms)
                .then_with(|| right.registered_at_ms.cmp(&left.registered_at_ms))
                .then_with(|| left.worker_id.cmp(&right.worker_id))
        });
        Ok(workers)
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<JobRecord> {
        let now_ms = now_ms();
        let transition = self
            .state_store
            .cancel_job_transition(job_id, now_ms)
            .await?;
        self.finalize_terminal_streams(transition.streams_to_finalize.clone())
            .await?;
        self.publish_transition_best_effort(&transition).await;
        Ok(transition.value)
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

    pub async fn list_job_events(&self, job_id: &str) -> Result<Vec<JobEvent>> {
        self.get_job(job_id).await?;
        self.state_store.job_events_after(job_id, 0).await
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
        let now_ms = now_ms();
        let reaped = self
            .state_store
            .reap_expired_leases_transition(now_ms)
            .await?;
        self.finalize_terminal_streams(reaped.streams_to_finalize.clone())
            .await?;
        self.publish_transition_best_effort(&reaped).await;

        let ordered_candidates = if self.state_store.lease_dispatch_mode()
            == LeaseDispatchMode::KernelOrderedCandidates
        {
            let worker = self
                .state_store
                .get_worker(worker_id)
                .await?
                .ok_or_else(|| anyhow!("worker '{}' does not exist", worker_id))?;
            if worker.expires_at_ms <= now_ms {
                bail!("worker '{}' heartbeat expired", worker_id);
            }

            let jobs = self.state_store.list_pending_jobs().await?;
            let ordered_candidates = self
                .scheduler
                .ordered_jobs_for_worker(&worker, jobs.as_slice());
            if ordered_candidates.is_empty() {
                return Ok(None);
            }
            ordered_candidates
        } else {
            Vec::new()
        };

        let assignment = self
            .state_store
            .try_assign_job(
                worker_id,
                ordered_candidates.as_slice(),
                self.default_lease_ttl_secs,
                now_ms,
            )
            .await?;
        if let Some(assignment) = assignment.as_ref() {
            self.publish_job_events_best_effort(&assignment.job.job_id)
                .await;
        }
        Ok(assignment)
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
        self.state_store.update_lease(lease.clone()).await?;
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
        let now_ms = now_ms();
        self.state_store
            .report_event_transition(worker_id, lease_id, kind, payload, metadata, now_ms)
            .await?;
        self.publish_job_events_best_effort(&lease.job_id).await;
        Ok(())
    }

    pub async fn complete_lease(
        &self,
        worker_id: &str,
        lease_id: &str,
        outputs: Vec<ObjectRef>,
        metadata: Attributes,
    ) -> Result<()> {
        self.require_lease(worker_id, lease_id).await?;
        let now_ms = now_ms();
        let transition = self
            .state_store
            .complete_lease_transition(worker_id, lease_id, outputs, metadata, now_ms)
            .await?;
        self.finalize_terminal_streams(transition.streams_to_finalize.clone())
            .await?;
        self.publish_transition_best_effort(&transition).await;
        Ok(())
    }

    pub async fn fail_lease(
        &self,
        worker_id: &str,
        lease_id: &str,
        reason: String,
        retryable: bool,
        metadata: Attributes,
    ) -> Result<()> {
        self.require_lease(worker_id, lease_id).await?;
        let now_ms = now_ms();
        let transition = self
            .state_store
            .fail_lease_transition(worker_id, lease_id, reason, retryable, metadata, now_ms)
            .await?;
        self.finalize_terminal_streams(transition.streams_to_finalize.clone())
            .await?;
        self.publish_transition_best_effort(&transition).await;
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
        self.state_store.create_stream(stream.clone()).await?;
        self.frame_plane.stream_created(&stream).await?;
        self.cache_stream(&stream).await;
        self.publish_job_streams_best_effort(&stream.job_id).await;
        Ok(stream)
    }

    pub async fn get_stream(&self, stream_id: &str) -> Result<StreamRecord> {
        let mut stream = self.load_stream_record(stream_id).await?;
        if let Some(last_sequence) = self.frame_plane.latest_sequence(stream_id).await? {
            stream.last_sequence = last_sequence;
        }
        Ok(stream)
    }

    pub async fn get_stream_metadata(&self, stream_id: &str) -> Result<StreamRecord> {
        self.load_stream_record(stream_id).await
    }

    pub async fn list_streams(&self, job_id: &str) -> Result<Vec<StreamRecord>> {
        self.get_job(job_id).await?;
        let mut streams = self.state_store.list_streams_for_job(job_id).await?;
        for stream in &mut streams {
            if let Some(last_sequence) = self.frame_plane.latest_sequence(&stream.stream_id).await?
            {
                stream.last_sequence = last_sequence;
            }
        }
        self.cache_streams(&streams).await;
        Ok(streams)
    }

    pub async fn subscribe_job_streams(&self, job_id: &str) -> Result<watch::Receiver<u64>> {
        self.get_job(job_id).await?;
        Ok(self.state_store.subscribe_job_streams(job_id).await)
    }

    pub async fn close_stream(
        &self,
        stream_id: &str,
        worker_id: Option<String>,
        lease_id: Option<String>,
        metadata: Attributes,
    ) -> Result<StreamRecord> {
        let mut stream = self.load_stream_record(stream_id).await?;
        if stream.state.is_terminal() {
            return Ok(stream);
        }

        self.validate_stream_write(&stream, worker_id.as_deref(), lease_id.as_deref())
            .await?;

        let now_ms = now_ms();
        stream.state = StreamState::Closed;
        stream.closed_at_ms = Some(now_ms);
        stream.last_sequence = self
            .frame_plane
            .latest_sequence(stream_id)
            .await?
            .unwrap_or(stream.last_sequence);
        stream.metadata.extend(metadata);
        self.state_store.update_stream(stream.clone()).await?;
        self.frame_plane.stream_updated(&stream).await?;
        self.cache_stream(&stream).await;
        self.persist_default_input_stream(&stream).await?;
        self.publish_job_streams_best_effort(&stream.job_id).await;
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
        let mut stream = self.load_stream_record(stream_id).await?;
        self.validate_stream_write(&stream, worker_id.as_deref(), lease_id.as_deref())
            .await?;
        let now_ms = now_ms();
        let frame = self
            .frame_plane
            .append_frame(&stream, kind, payload, metadata, now_ms)
            .await?;

        match kind {
            FrameKind::Close => {
                stream.state = StreamState::Closed;
                stream.closed_at_ms = Some(now_ms);
                stream.last_sequence = frame.sequence;
                self.state_store.update_stream(stream.clone()).await?;
                self.frame_plane.stream_updated(&stream).await?;
                self.cache_stream(&stream).await;
                self.persist_default_input_stream(&stream).await?;
                self.publish_job_streams_best_effort(&stream.job_id).await;
            }
            FrameKind::Error => {
                stream.state = StreamState::Error;
                stream.closed_at_ms = Some(now_ms);
                stream.last_sequence = frame.sequence;
                self.state_store.update_stream(stream.clone()).await?;
                self.frame_plane.stream_updated(&stream).await?;
                self.cache_stream(&stream).await;
                self.publish_job_streams_best_effort(&stream.job_id).await;
            }
            _ => {}
        }

        Ok(frame)
    }

    pub async fn pull_frames(&self, stream_id: &str, after_sequence: u64) -> Result<Vec<Frame>> {
        self.load_stream_record(stream_id).await?;
        self.frame_plane
            .frames_after(stream_id, after_sequence)
            .await
    }

    pub async fn wait_for_frames(
        &self,
        stream_id: &str,
        after_sequence: u64,
        timeout: Duration,
    ) -> Result<Vec<Frame>> {
        self.load_stream_record(stream_id).await?;
        self.frame_plane
            .wait_for_frames(stream_id, after_sequence, timeout)
            .await
    }

    pub async fn subscribe_stream_updates(&self, stream_id: &str) -> Result<watch::Receiver<u64>> {
        self.load_stream_record(stream_id).await?;
        Ok(self.frame_plane.subscribe_stream_updates(stream_id).await)
    }

    pub async fn persist_job_inputs(&self, job_id: &str, inputs: Vec<ObjectRef>) -> Result<()> {
        self.state_store
            .persist_job_inputs(job_id, inputs, now_ms())
            .await
    }

    async fn load_stream_record(&self, stream_id: &str) -> Result<StreamRecord> {
        if let Some(stream) = self.stream_cache.lock().await.get(stream_id).cloned() {
            return Ok(stream);
        }

        let stream = self
            .state_store
            .get_stream(stream_id)
            .await?
            .ok_or_else(|| anyhow!("stream '{}' does not exist", stream_id))?;
        self.cache_stream(&stream).await;
        Ok(stream)
    }

    async fn cache_stream(&self, stream: &StreamRecord) {
        self.stream_cache
            .lock()
            .await
            .insert(stream.stream_id.clone(), stream.clone());
    }

    async fn cache_streams(&self, streams: &[StreamRecord]) {
        let mut cache = self.stream_cache.lock().await;
        for stream in streams {
            cache.insert(stream.stream_id.clone(), stream.clone());
        }
    }

    async fn persist_default_input_stream(&self, stream: &StreamRecord) -> Result<()> {
        if stream.stream_name != "input.default"
            || stream.direction != StreamDirection::ClientToWorker
            || !stream.state.is_terminal()
        {
            return Ok(());
        }

        let frames = self.frame_plane.frames_after(&stream.stream_id, 0).await?;
        if frames.is_empty() {
            return Ok(());
        }

        let mut payload = Vec::new();
        let mut metadata = stream.metadata.clone();
        for frame in frames {
            if frame.kind == FrameKind::Data {
                if metadata.is_empty() && !frame.metadata.is_empty() {
                    metadata = frame.metadata.clone();
                }
                payload.extend(frame.payload);
            }
        }

        if payload.is_empty() {
            return Ok(());
        }

        self.persist_job_inputs(
            &stream.job_id,
            vec![ObjectRef::Inline {
                content: payload,
                metadata,
            }],
        )
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

    async fn finalize_terminal_streams(&self, streams: Vec<StreamRecord>) -> Result<()> {
        for mut stream in streams {
            if let Some(last_sequence) = self.frame_plane.latest_sequence(&stream.stream_id).await?
            {
                if last_sequence > stream.last_sequence {
                    stream.last_sequence = last_sequence;
                    self.state_store.update_stream(stream.clone()).await?;
                }
            }
            self.frame_plane.stream_updated(&stream).await?;
            self.cache_stream(&stream).await;
            self.publish_job_streams_best_effort(&stream.job_id).await;
        }
        Ok(())
    }

    async fn publish_job_events_best_effort(&self, job_id: &str) {
        let _ = self.notifier.publish_job_events(job_id).await;
    }

    async fn publish_job_streams_best_effort(&self, job_id: &str) {
        let _ = self.notifier.publish_job_streams(job_id).await;
    }

    async fn publish_transition_best_effort<T>(&self, transition: &StateTransition<T>) {
        for job_id in &transition.job_event_updates {
            self.publish_job_events_best_effort(job_id).await;
        }
        for job_id in &transition.job_stream_updates {
            self.publish_job_streams_best_effort(job_id).await;
        }
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
    use crate::frame::MemoryFramePlane;
    use crate::model::{
        Attributes, EventKind, ExecutionPolicy, FrameKind, JobSpec, JobState, StreamDirection,
        StreamScope, WorkerSpec,
    };
    use crate::notify::NoopClusterNotifier;
    use crate::scheduler::BasicScheduler;
    use crate::store::MemoryStateStore;

    #[tokio::test]
    async fn failed_retryable_lease_is_requeued() {
        let kernel = Kernel::new(
            Arc::new(MemoryStateStore::new()),
            Arc::new(MemoryFramePlane::new()),
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            250,
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
            Arc::new(MemoryFramePlane::new()),
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            250,
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
            jobs.iter()
                .map(|job| job.job_id.as_str())
                .collect::<Vec<_>>(),
            vec![second.job_id.as_str(), first.job_id.as_str()]
        );
    }

    #[tokio::test]
    async fn cancelling_leased_job_cancels_attempt_and_closes_streams() {
        let kernel = Kernel::new(
            Arc::new(MemoryStateStore::new()),
            Arc::new(MemoryFramePlane::new()),
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            250,
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
        let assignment = kernel.poll_lease(&worker.worker_id).await.unwrap().unwrap();
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
            Arc::new(MemoryFramePlane::new()),
            Arc::new(NoopClusterNotifier),
            Arc::new(BasicScheduler),
            30,
            30,
            250,
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
