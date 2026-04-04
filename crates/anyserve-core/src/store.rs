use std::collections::BTreeMap;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio::sync::{Mutex, watch};

use crate::model::{
    AttemptRecord, Attributes, EventKind, Frame, FrameKind, JobEvent, JobRecord, LeaseRecord,
    StreamRecord, WorkerRecord,
};

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn create_job(&self, job: JobRecord) -> Result<()>;
    async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>>;
    async fn list_jobs(&self) -> Result<Vec<JobRecord>>;
    async fn update_job(&self, job: JobRecord) -> Result<()>;

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

    async fn put_lease(&self, lease: LeaseRecord) -> Result<()>;
    async fn get_lease(&self, lease_id: &str) -> Result<Option<LeaseRecord>>;
    async fn list_leases(&self) -> Result<Vec<LeaseRecord>>;
    async fn delete_lease(&self, lease_id: &str) -> Result<Option<LeaseRecord>>;
}

#[async_trait]
pub trait StreamStore: Send + Sync {
    async fn create_stream(&self, stream: StreamRecord) -> Result<()>;
    async fn get_stream(&self, stream_id: &str) -> Result<Option<StreamRecord>>;
    async fn list_streams_for_job(&self, job_id: &str) -> Result<Vec<StreamRecord>>;
    async fn update_stream(&self, stream: StreamRecord) -> Result<()>;
    async fn append_frame(
        &self,
        stream_id: &str,
        kind: FrameKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<Frame>;
    async fn frames_after(&self, stream_id: &str, after_sequence: u64) -> Result<Vec<Frame>>;
    async fn subscribe_job_streams(&self, job_id: &str) -> watch::Receiver<u64>;
    async fn subscribe_stream_updates(&self, stream_id: &str) -> watch::Receiver<u64>;
}

#[derive(Default)]
struct MemoryState {
    jobs: BTreeMap<String, JobRecord>,
    events: BTreeMap<String, Vec<JobEvent>>,
    attempts: BTreeMap<String, AttemptRecord>,
    workers: BTreeMap<String, WorkerRecord>,
    leases: BTreeMap<String, LeaseRecord>,
}

#[derive(Default)]
struct MemoryStateWatchers {
    job_events: BTreeMap<String, watch::Sender<u64>>,
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
    async fn notify_job_events(&self, job_id: &str) {
        let mut watchers = self.watchers.lock().await;
        bump_version(
            watchers
                .job_events
                .entry(job_id.to_string())
                .or_insert_with(version_sender),
        );
    }
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

    async fn update_job(&self, job: JobRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if !inner.jobs.contains_key(&job.job_id) {
            bail!("job '{}' does not exist", job.job_id);
        }
        inner.jobs.insert(job.job_id.clone(), job);
        Ok(())
    }

    async fn append_job_event(
        &self,
        job_id: &str,
        kind: EventKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<JobEvent> {
        let mut inner = self.inner.lock().await;
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
        drop(inner);
        self.notify_job_events(job_id).await;
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
        Ok(inner
            .attempts
            .values()
            .filter(|attempt| attempt.lease_id == lease_id)
            .cloned()
            .max_by_key(|attempt| attempt.created_at_ms))
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

    async fn put_lease(&self, lease: LeaseRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if !inner.jobs.contains_key(&lease.job_id) {
            return Err(anyhow!("job '{}' does not exist", lease.job_id));
        }
        if !inner.workers.contains_key(&lease.worker_id) {
            return Err(anyhow!("worker '{}' does not exist", lease.worker_id));
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
}

#[derive(Default)]
struct MemoryStreams {
    streams: BTreeMap<String, StreamRecord>,
    frames: BTreeMap<String, Vec<Frame>>,
}

#[derive(Default)]
struct MemoryStreamWatchers {
    job_streams: BTreeMap<String, watch::Sender<u64>>,
    stream_updates: BTreeMap<String, watch::Sender<u64>>,
}

pub struct MemoryStreamStore {
    inner: Mutex<MemoryStreams>,
    watchers: Mutex<MemoryStreamWatchers>,
}

impl MemoryStreamStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for MemoryStreamStore {
    fn default() -> Self {
        Self {
            inner: Mutex::new(MemoryStreams::default()),
            watchers: Mutex::new(MemoryStreamWatchers::default()),
        }
    }
}

impl MemoryStreamStore {
    async fn notify_job_streams(&self, job_id: &str) {
        let mut watchers = self.watchers.lock().await;
        bump_version(
            watchers
                .job_streams
                .entry(job_id.to_string())
                .or_insert_with(version_sender),
        );
    }

    async fn notify_stream_updates(&self, stream_id: &str) {
        let mut watchers = self.watchers.lock().await;
        bump_version(
            watchers
                .stream_updates
                .entry(stream_id.to_string())
                .or_insert_with(version_sender),
        );
    }
}

#[async_trait]
impl StreamStore for MemoryStreamStore {
    async fn create_stream(&self, stream: StreamRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.streams.contains_key(&stream.stream_id) {
            bail!("stream '{}' already exists", stream.stream_id);
        }
        let job_id = stream.job_id.clone();
        inner.streams.insert(stream.stream_id.clone(), stream);
        drop(inner);
        self.notify_job_streams(&job_id).await;
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
        let job_id = stream.job_id.clone();
        let stream_id = stream.stream_id.clone();
        inner.streams.insert(stream.stream_id.clone(), stream);
        drop(inner);
        self.notify_job_streams(&job_id).await;
        self.notify_stream_updates(&stream_id).await;
        Ok(())
    }

    async fn append_frame(
        &self,
        stream_id: &str,
        kind: FrameKind,
        payload: Vec<u8>,
        metadata: Attributes,
        now_ms: u64,
    ) -> Result<Frame> {
        let mut inner = self.inner.lock().await;
        let (frame, job_id) = {
            let stream = inner
                .streams
                .get_mut(stream_id)
                .ok_or_else(|| anyhow!("stream '{}' does not exist", stream_id))?;
            if stream.state.is_terminal() {
                bail!("stream '{}' is already closed", stream_id);
            }

            let sequence = stream.last_sequence + 1;
            stream.last_sequence = sequence;
            (
                Frame {
                    stream_id: stream_id.to_string(),
                    sequence,
                    kind,
                    payload,
                    metadata,
                    created_at_ms: now_ms,
                },
                stream.job_id.clone(),
            )
        };
        inner
            .frames
            .entry(stream_id.to_string())
            .or_default()
            .push(frame.clone());
        drop(inner);
        self.notify_job_streams(&job_id).await;
        self.notify_stream_updates(stream_id).await;
        Ok(frame)
    }

    async fn frames_after(&self, stream_id: &str, after_sequence: u64) -> Result<Vec<Frame>> {
        let inner = self.inner.lock().await;
        if !inner.streams.contains_key(stream_id) {
            bail!("stream '{}' does not exist", stream_id);
        }
        Ok(inner
            .frames
            .get(stream_id)
            .into_iter()
            .flatten()
            .filter(|frame| frame.sequence > after_sequence)
            .cloned()
            .collect())
    }

    async fn subscribe_job_streams(&self, job_id: &str) -> watch::Receiver<u64> {
        let mut watchers = self.watchers.lock().await;
        watchers
            .job_streams
            .entry(job_id.to_string())
            .or_insert_with(version_sender)
            .subscribe()
    }

    async fn subscribe_stream_updates(&self, stream_id: &str) -> watch::Receiver<u64> {
        let mut watchers = self.watchers.lock().await;
        watchers
            .stream_updates
            .entry(stream_id.to_string())
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

    use super::{MemoryStateStore, MemoryStreamStore, StateStore, StreamStore};
    use crate::model::{
        Attributes, EventKind, FrameKind, JobRecord, StreamDirection, StreamRecord, StreamScope,
    };

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
    async fn appends_and_reads_frames_in_order() {
        let store = MemoryStreamStore::new();
        store
            .create_stream(StreamRecord {
                stream_id: "stream-1".to_string(),
                job_id: "job-1".to_string(),
                stream_name: "input.default".to_string(),
                scope: StreamScope::Job,
                direction: StreamDirection::ClientToWorker,
                ..StreamRecord::default()
            })
            .await
            .unwrap();

        let first = store
            .append_frame(
                "stream-1",
                FrameKind::Data,
                b"hello".to_vec(),
                Attributes::new(),
                1,
            )
            .await
            .unwrap();
        let second = store
            .append_frame(
                "stream-1",
                FrameKind::Data,
                b"world".to_vec(),
                Attributes::new(),
                2,
            )
            .await
            .unwrap();

        let frames = store.frames_after("stream-1", 0).await.unwrap();

        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].payload, b"hello".to_vec());
        assert_eq!(frames[1].payload, b"world".to_vec());
    }

    #[tokio::test]
    async fn appending_frame_notifies_stream_subscribers() {
        let store = MemoryStreamStore::new();
        store
            .create_stream(StreamRecord {
                stream_id: "stream-1".to_string(),
                job_id: "job-1".to_string(),
                stream_name: "input.default".to_string(),
                scope: StreamScope::Job,
                direction: StreamDirection::ClientToWorker,
                ..StreamRecord::default()
            })
            .await
            .unwrap();
        let mut updates = store.subscribe_stream_updates("stream-1").await;

        store
            .append_frame(
                "stream-1",
                FrameKind::Data,
                b"hello".to_vec(),
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
}
