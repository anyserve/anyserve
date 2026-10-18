//! Streaming input of an ephemeral execution, retained whole so that every
//! attempt (including retries) reads it from the start.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::watch;

use super::api::Metadata;
use super::events::EventBudget;
use crate::error::RuntimeError;

/// Accounted per chunk in addition to its data and metadata, so empty chunks
/// cannot accumulate for free.
const CHUNK_OVERHEAD: u64 = 64;

pub(crate) struct InputLog {
    inner: Mutex<InputInner>,
    changed: watch::Sender<u64>,
    /// Per-job limit on accounted bytes.
    limit: u64,
    /// Shared with every other streaming input and released on drop.
    budget: Arc<EventBudget>,
}

#[derive(Default)]
struct InputInner {
    chunks: Vec<(Bytes, Metadata)>,
    accounted: u64,
    finished: bool,
    aborted: bool,
}

impl InputLog {
    pub(crate) fn new(limit: u64, budget: Arc<EventBudget>) -> Self {
        Self {
            inner: Mutex::new(InputInner::default()),
            changed: watch::channel(0).0,
            limit,
            budget,
        }
    }

    pub(crate) fn push(&self, data: Bytes, metadata: Metadata) -> Result<(), RuntimeError> {
        let cost = CHUNK_OVERHEAD
            + data.len() as u64
            + metadata
                .iter()
                .map(|(key, value)| (key.len() + value.len()) as u64)
                .sum::<u64>();
        let mut inner = self.inner.lock().expect("input log poisoned");
        if inner.finished || inner.aborted {
            return Err(RuntimeError::invalid("input stream is already closed"));
        }
        if inner.accounted + cost > self.limit {
            return Err(RuntimeError::exhausted(format!(
                "streaming input exceeds {} bytes",
                self.limit
            )));
        }
        if !self.budget.try_reserve(cost as usize) {
            return Err(RuntimeError::exhausted(
                "streaming input budget is exhausted",
            ));
        }
        inner.accounted += cost;
        inner.chunks.push((data, metadata));
        let count = inner.chunks.len() as u64;
        drop(inner);
        self.changed.send_replace(count);
        Ok(())
    }

    pub(crate) fn finish(&self) {
        let mut inner = self.inner.lock().expect("input log poisoned");
        inner.finished = true;
        drop(inner);
        self.changed.send_modify(|value| *value += 1);
    }

    /// Stops readers waiting for more input; used when the job ends.
    pub(crate) fn abort(&self) {
        let mut inner = self.inner.lock().expect("input log poisoned");
        inner.aborted = true;
        drop(inner);
        self.changed.send_modify(|value| *value += 1);
    }

    /// Chunks from `index` on, and whether the input is complete.
    pub(crate) fn read_from(&self, index: usize) -> (Vec<(Bytes, Metadata)>, InputStatus) {
        let inner = self.inner.lock().expect("input log poisoned");
        let chunks = inner.chunks.get(index..).unwrap_or_default().to_vec();
        let status = if inner.aborted {
            InputStatus::Aborted
        } else if inner.finished {
            InputStatus::Finished
        } else {
            InputStatus::Open
        };
        (chunks, status)
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
}

impl Drop for InputLog {
    fn drop(&mut self) {
        let accounted = self
            .inner
            .get_mut()
            .map(|inner| inner.accounted)
            .unwrap_or(0);
        self.budget.release(accounted as usize);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InputStatus {
    Open,
    Finished,
    Aborted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_chunks_and_metadata_are_accounted() {
        let budget = Arc::new(EventBudget::new(1 << 20));
        let log = InputLog::new(CHUNK_OVERHEAD * 3, budget.clone());
        for _ in 0..3 {
            log.push(Bytes::new(), Metadata::new()).unwrap();
        }
        assert!(log.push(Bytes::new(), Metadata::new()).is_err());
        drop(log);
        assert_eq!(budget.used(), 0);
    }
}
