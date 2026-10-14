use std::io;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};

use anyserve_core::kernel::Kernel;
use anyserve_core::notify::ClusterSubscription;

const STREAM_TIMEOUT: &str = "request timed out waiting for worker stream";

/// Local and cluster-wide change notifications for one job's streams and events.
pub(super) struct JobWatchers {
    streams: watch::Receiver<u64>,
    cluster_streams: ClusterSubscription,
    events: watch::Receiver<u64>,
    cluster_events: ClusterSubscription,
}

impl JobWatchers {
    pub(super) async fn subscribe(kernel: &Kernel, job_id: &str) -> io::Result<Self> {
        let failed = |what: &str, error: anyhow::Error| {
            io::Error::other(format!("subscribe {what}: {error}"))
        };
        Ok(Self {
            streams: kernel
                .subscribe_job_streams(job_id)
                .await
                .map_err(|error| failed("job streams", error))?,
            cluster_streams: kernel
                .subscribe_cluster_job_streams(job_id)
                .await
                .map_err(|error| failed("cluster job streams", error))?,
            events: kernel
                .subscribe_job_events(job_id)
                .await
                .map_err(|error| failed("job events", error))?,
            cluster_events: kernel
                .subscribe_cluster_job_events(job_id)
                .await
                .map_err(|error| failed("cluster job events", error))?,
        })
    }

    /// Waits until the job's streams or events change, or `poll_interval` elapses.
    pub(super) async fn job_or_stream_change(
        &mut self,
        deadline: Instant,
        poll_interval: Duration,
    ) -> io::Result<()> {
        let poll_deadline = (Instant::now() + poll_interval).min(deadline);
        tokio::select! {
            _ = sleep_until(deadline) => Err(io::Error::new(io::ErrorKind::TimedOut, STREAM_TIMEOUT)),
            _ = sleep_until(poll_deadline) => Ok(()),
            result = self.streams.changed() => {
                result.map_err(|_| io::Error::other("job stream watcher closed"))
            }
            result = self.cluster_streams.changed() => {
                result.map_err(|error| io::Error::other(format!("cluster job stream watcher closed: {error}")))
            }
            result = self.events.changed() => {
                result.map_err(|_| io::Error::other("job event watcher closed"))
            }
            result = self.cluster_events.changed() => {
                result.map_err(|error| io::Error::other(format!("cluster job event watcher closed: {error}")))
            }
        }
    }

    /// Waits for new frames on `stream_id` or a job event change.
    pub(super) async fn stream_or_job_change(
        &mut self,
        kernel: &Kernel,
        stream_id: &str,
        after_sequence: u64,
        deadline: Instant,
        poll_interval: Duration,
    ) -> io::Result<()> {
        tokio::select! {
            _ = sleep_until(deadline) => Err(io::Error::new(io::ErrorKind::TimedOut, STREAM_TIMEOUT)),
            result = kernel.wait_for_frames(stream_id, after_sequence, poll_interval) => {
                result
                    .map(|_| ())
                    .map_err(|error| io::Error::other(format!("wait for frames: {error}")))
            }
            result = self.events.changed() => {
                result.map_err(|_| io::Error::other("job event watcher closed"))
            }
            result = self.cluster_events.changed() => {
                result.map_err(|error| io::Error::other(format!("cluster job event watcher closed: {error}")))
            }
        }
    }
}
