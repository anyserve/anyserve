use std::io;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};

use anyserve_core::kernel::Kernel;
use anyserve_core::notify::ClusterSubscription;

pub(super) async fn wait_for_job_or_stream_change(
    deadline: Instant,
    poll_interval: Duration,
    local_job_stream_updates: &mut watch::Receiver<u64>,
    cluster_job_stream_updates: &mut ClusterSubscription,
    local_job_event_updates: &mut watch::Receiver<u64>,
    cluster_job_event_updates: &mut ClusterSubscription,
    timeout_message: &'static str,
) -> io::Result<()> {
    let poll_deadline = (Instant::now() + poll_interval).min(deadline);
    tokio::select! {
        _ = sleep_until(deadline) => Err(io::Error::new(io::ErrorKind::TimedOut, timeout_message)),
        _ = sleep_until(poll_deadline) => Ok(()),
        result = local_job_stream_updates.changed() => {
            result.map_err(|_| io::Error::other("job stream watcher closed"))
        }
        result = cluster_job_stream_updates.changed() => {
            result.map_err(|error| io::Error::other(format!("cluster job stream watcher closed: {error}")))
        }
        result = local_job_event_updates.changed() => {
            result.map_err(|_| io::Error::other("job event watcher closed"))
        }
        result = cluster_job_event_updates.changed() => {
            result.map_err(|error| io::Error::other(format!("cluster job event watcher closed: {error}")))
        }
    }
}

pub(super) async fn wait_for_stream_or_job_change(
    kernel: &Kernel,
    stream_id: &str,
    after_sequence: u64,
    deadline: Instant,
    poll_interval: Duration,
    local_job_event_updates: &mut watch::Receiver<u64>,
    cluster_job_event_updates: &mut ClusterSubscription,
    timeout_message: &'static str,
) -> io::Result<()> {
    tokio::select! {
        _ = sleep_until(deadline) => Err(io::Error::new(io::ErrorKind::TimedOut, timeout_message)),
        result = kernel.wait_for_frames(stream_id, after_sequence, poll_interval) => {
            result
                .map(|_| ())
                .map_err(|error| io::Error::other(format!("wait for frames: {error}")))
        }
        result = local_job_event_updates.changed() => {
            result.map_err(|_| io::Error::other("job event watcher closed"))
        }
        result = cluster_job_event_updates.changed() => {
            result.map_err(|error| io::Error::other(format!("cluster job event watcher closed: {error}")))
        }
    }
}

pub(super) async fn wait_for_job_event_change(
    poll_interval: Duration,
    local_updates: &mut watch::Receiver<u64>,
    cluster_updates: &mut ClusterSubscription,
) {
    tokio::select! {
        result = local_updates.changed() => {
            if result.is_err() {
                sleep_until(Instant::now() + poll_interval).await;
            }
        }
        result = cluster_updates.changed() => {
            if result.is_err() {
                sleep_until(Instant::now() + poll_interval).await;
            }
        }
        _ = sleep_until(Instant::now() + poll_interval) => {}
    }
}
