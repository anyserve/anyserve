use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use anyserve_client::{
    AttemptState, JobRecord, JobState, StreamDirection, StreamScope, StreamState,
};

use crate::job_commands::args::JobStateFilter;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct JobCounts {
    pending: usize,
    leased: usize,
    running: usize,
    succeeded: usize,
    failed: usize,
    cancelled: usize,
}

pub(super) fn count_jobs(jobs: &[JobRecord]) -> JobCounts {
    let mut counts = JobCounts::default();
    for job in jobs {
        match job.state() {
            JobState::Pending => counts.pending += 1,
            JobState::Leased => counts.leased += 1,
            JobState::Running => counts.running += 1,
            JobState::Succeeded => counts.succeeded += 1,
            JobState::Failed => counts.failed += 1,
            JobState::Cancelled => counts.cancelled += 1,
            JobState::Unspecified => {}
        }
    }
    counts
}

pub(super) fn format_state_filters(states: &[JobStateFilter]) -> String {
    states
        .iter()
        .map(|state| job_state_filter_name(*state))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_job_counts(counts: JobCounts) -> String {
    format!(
        "pending {} | leased {} | running {} | succeeded {} | failed {} | cancelled {}",
        counts.pending,
        counts.leased,
        counts.running,
        counts.succeeded,
        counts.failed,
        counts.cancelled
    )
}

pub(super) fn sorted_map(map: &HashMap<String, String>) -> BTreeMap<String, String> {
    map.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

pub(super) fn optional_string(value: &str) -> Option<String> {
    normalize_text(value)
}

pub(super) fn optional_timestamp(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

pub(super) fn interface_name(job: &JobRecord) -> &str {
    job.spec
        .as_ref()
        .map(|spec| spec.interface_name.as_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("-")
}

pub(super) fn job_metadata(job: &JobRecord) -> &HashMap<String, String> {
    static EMPTY: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
    job.spec
        .as_ref()
        .map(|spec| &spec.metadata)
        .unwrap_or_else(|| EMPTY.get_or_init(HashMap::new))
}

pub(super) fn format_string_map(map: &HashMap<String, String>) -> String {
    if map.is_empty() {
        return "-".to_string();
    }

    let mut entries = map
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    entries.sort();
    entries.join(", ")
}

pub(super) fn optional_text(value: &str) -> &str {
    if value.trim().is_empty() { "-" } else { value }
}

pub(super) fn ellipsize(value: &str, limit: usize) -> String {
    let length = value.chars().count();
    if length <= limit {
        return value.to_string();
    }
    if limit <= 3 {
        return value.chars().take(limit).collect();
    }

    let prefix = value.chars().take(limit - 3).collect::<String>();
    format!("{prefix}...")
}

pub(super) fn format_age(timestamp_ms: u64) -> String {
    if timestamp_ms == 0 {
        return "-".to_string();
    }

    let now_ms = now_ms();
    if timestamp_ms >= now_ms {
        return "just now".to_string();
    }

    let delta_ms = now_ms - timestamp_ms;
    format!("{} ago", human_duration(delta_ms))
}

pub(super) fn format_timestamp(timestamp_ms: u64) -> String {
    if timestamp_ms == 0 {
        "-".to_string()
    } else {
        format!("{timestamp_ms} ({})", format_age(timestamp_ms))
    }
}

pub(super) fn format_optional_timestamp(timestamp_ms: u64) -> String {
    if timestamp_ms == 0 {
        "-".to_string()
    } else {
        format_age(timestamp_ms)
    }
}

pub(super) fn human_duration(duration_ms: u64) -> String {
    const SECOND: u64 = 1_000;
    const MINUTE: u64 = 60 * SECOND;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;

    if duration_ms >= DAY {
        format!("{}d", duration_ms / DAY)
    } else if duration_ms >= HOUR {
        format!("{}h", duration_ms / HOUR)
    } else if duration_ms >= MINUTE {
        format!("{}m", duration_ms / MINUTE)
    } else if duration_ms >= SECOND {
        format!("{}s", duration_ms / SECOND)
    } else {
        format!("{duration_ms}ms")
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

pub(super) fn job_state_name(state: JobState) -> &'static str {
    match state {
        JobState::Pending => "pending",
        JobState::Leased => "leased",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
        JobState::Unspecified => "unspecified",
    }
}

fn job_state_filter_name(state: JobStateFilter) -> &'static str {
    match state {
        JobStateFilter::Pending => "pending",
        JobStateFilter::Leased => "leased",
        JobStateFilter::Running => "running",
        JobStateFilter::Succeeded => "succeeded",
        JobStateFilter::Failed => "failed",
        JobStateFilter::Cancelled => "cancelled",
    }
}

pub(super) fn attempt_state_name(state: AttemptState) -> &'static str {
    match state {
        AttemptState::Created => "created",
        AttemptState::Leased => "leased",
        AttemptState::Running => "running",
        AttemptState::Succeeded => "succeeded",
        AttemptState::Failed => "failed",
        AttemptState::Expired => "expired",
        AttemptState::Cancelled => "cancelled",
        AttemptState::Unspecified => "unspecified",
    }
}

pub(super) fn stream_scope_name(scope: StreamScope) -> &'static str {
    match scope {
        StreamScope::Job => "job",
        StreamScope::Attempt => "attempt",
        StreamScope::Lease => "lease",
        StreamScope::Unspecified => "unspecified",
    }
}

pub(super) fn stream_direction_name(direction: StreamDirection) -> &'static str {
    match direction {
        StreamDirection::ClientToWorker => "client_to_worker",
        StreamDirection::WorkerToClient => "worker_to_client",
        StreamDirection::Bidirectional => "bidirectional",
        StreamDirection::Internal => "internal",
        StreamDirection::Unspecified => "unspecified",
    }
}

pub(super) fn stream_state_name(state: StreamState) -> &'static str {
    match state {
        StreamState::Open => "open",
        StreamState::Closing => "closing",
        StreamState::Closed => "closed",
        StreamState::Error => "error",
        StreamState::Unspecified => "unspecified",
    }
}

fn normalize_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{ellipsize, human_duration};

    #[test]
    fn ellipsize_truncates_long_values() {
        assert_eq!(ellipsize("1234567890abcdef", 8), "12345...");
    }

    #[test]
    fn human_duration_uses_compact_units() {
        assert_eq!(human_duration(999), "999ms");
        assert_eq!(human_duration(2_000), "2s");
        assert_eq!(human_duration(120_000), "2m");
    }
}
