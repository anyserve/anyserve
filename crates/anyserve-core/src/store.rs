use std::collections::BTreeMap;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio::sync::Mutex;

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
pub struct MemoryStateStore {
    inner: Mutex<MemoryState>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
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
pub struct MemoryStreamStore {
    inner: Mutex<MemoryStreams>,
}

impl MemoryStreamStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StreamStore for MemoryStreamStore {
    async fn create_stream(&self, stream: StreamRecord) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.streams.contains_key(&stream.stream_id) {
            bail!("stream '{}' already exists", stream.stream_id);
        }
        inner.streams.insert(stream.stream_id.clone(), stream);
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
        inner.streams.insert(stream.stream_id.clone(), stream);
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
        let stream = inner
            .streams
            .get_mut(stream_id)
            .ok_or_else(|| anyhow!("stream '{}' does not exist", stream_id))?;
        if stream.state.is_terminal() {
            bail!("stream '{}' is already closed", stream_id);
        }

        let sequence = stream.last_sequence + 1;
        let frame = Frame {
            stream_id: stream_id.to_string(),
            sequence,
            kind,
            payload,
            metadata,
            created_at_ms: now_ms,
        };
        stream.last_sequence = sequence;
        inner
            .frames
            .entry(stream_id.to_string())
            .or_default()
            .push(frame.clone());
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
}

#[cfg(test)]
mod tests {
    use super::{MemoryStreamStore, StreamStore};
    use crate::model::{Attributes, FrameKind, StreamDirection, StreamRecord, StreamScope};

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
}
