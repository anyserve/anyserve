use super::api::{
    AttemptSummary, InlinePreviewSummary, JobEventSummary, JobSummary, ObjectRefSummary,
    WorkerSummary,
};
use anyserve_core::runtime::api::{
    EventKind, ExecutionEvent, JobState, JobView, Metadata, Object, WorkerView,
};
use std::collections::BTreeMap;

pub(crate) const INLINE_PREVIEW_LIMIT_BYTES: usize = 4 * 1024;

pub(crate) fn state_name(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
    }
}
pub(crate) fn aggregate_capacity<'a>(
    capacities: impl Iterator<Item = &'a BTreeMap<String, i64>>,
) -> BTreeMap<String, i64> {
    let mut total = BTreeMap::new();
    for capacity in capacities {
        for (name, value) in capacity {
            *total.entry(name.clone()).or_default() += value;
        }
    }
    total
}
pub(crate) fn job_summary(job: &JobView) -> JobSummary {
    JobSummary {
        job_id: job.job_id.clone(),
        state: state_name(job.state),
        interface_name: job.interface.clone(),
        source: metadata_value(&job.metadata, "source"),
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        duration_ms: job
            .state
            .is_terminal()
            .then(|| job.updated_at_ms.saturating_sub(job.created_at_ms)),
        current_attempt_id: job.current_attempt_id.clone(),
        latest_worker_id: job.current_worker_id.clone(),
        attempt_count: job.attempt_count,
        last_error: job.error.as_ref().map(|error| error.message.clone()),
        metadata: job.metadata.clone(),
    }
}
pub(crate) fn worker_summary(worker: &WorkerView) -> WorkerSummary {
    WorkerSummary {
        worker_id: worker.worker_id.clone(),
        healthy: worker.connected && !worker.draining,
        interfaces: worker.interfaces.clone(),
        attributes: worker.attributes.clone(),
        total_capacity: worker.total_capacity.clone(),
        max_concurrency: worker.max_concurrency,
        active_leases: worker.active_leases,
        free_slots: worker.free_slots,
        utilization: worker.load.utilization,
        connected: worker.connected,
        draining: worker.draining,
        last_seen_at_ms: worker.last_seen_ms,
    }
}
pub(crate) fn attempts_from_events(events: &[ExecutionEvent]) -> Vec<AttemptSummary> {
    let mut attempts: Vec<AttemptSummary> = Vec::new();
    for event in events {
        match &event.kind {
            EventKind::Assigned { worker_id, .. } => {
                if let Some(attempt_id) = &event.attempt_id {
                    attempts.push(AttemptSummary {
                        attempt_id: attempt_id.clone(),
                        state: "assigned",
                        worker_id: worker_id.clone(),
                        created_at_ms: event.at_ms,
                        finished_at_ms: None,
                        duration_ms: None,
                        last_error: None,
                    });
                }
            }
            EventKind::AttemptRestarted {
                previous_attempt_id,
                reason,
            } => {
                if let Some(attempt) = attempts
                    .iter_mut()
                    .rev()
                    .find(|attempt| &attempt.attempt_id == previous_attempt_id)
                {
                    finish_attempt(
                        attempt,
                        "restarted",
                        event.at_ms,
                        Some(reason.message.clone()),
                    );
                }
            }
            EventKind::Finished { state, error, .. } => {
                if let Some(attempt) = attempts
                    .iter_mut()
                    .rev()
                    .find(|attempt| Some(&attempt.attempt_id) == event.attempt_id.as_ref())
                {
                    finish_attempt(
                        attempt,
                        state_name(*state),
                        event.at_ms,
                        error.as_ref().map(|error| error.message.clone()),
                    );
                }
            }
            _ => {}
        }
    }
    attempts
}
fn finish_attempt(
    attempt: &mut AttemptSummary,
    state: &'static str,
    at_ms: u64,
    error: Option<String>,
) {
    attempt.state = state;
    attempt.finished_at_ms = Some(at_ms);
    attempt.duration_ms = Some(at_ms.saturating_sub(attempt.created_at_ms));
    attempt.last_error = error;
}
pub(crate) fn event_summary(event: &ExecutionEvent) -> JobEventSummary {
    let (kind, payload_size_bytes) = match &event.kind {
        EventKind::Accepted => ("accepted", 0),
        EventKind::Assigned { .. } => ("assigned", 0),
        EventKind::Output { data, progress, .. } => {
            (if *progress { "progress" } else { "output" }, data.len())
        }
        EventKind::AttemptRestarted { .. } => ("attempt_restarted", 0),
        EventKind::Finished { .. } => ("finished", 0),
    };
    JobEventSummary {
        sequence: event.sequence,
        kind,
        created_at_ms: event.at_ms,
        payload_size_bytes,
    }
}
pub(crate) fn object_ref_summary(value: &Object) -> ObjectRefSummary {
    match value {
        Object::Inline { data, metadata } => ObjectRefSummary {
            kind: "inline",
            size_bytes: Some(data.len()),
            uri: None,
            content_type: metadata_value(metadata, "content_type"),
            preview: inline_preview(data),
            truncated: data.len() > INLINE_PREVIEW_LIMIT_BYTES,
            metadata: metadata.clone(),
        },
        Object::Uri { uri, metadata } => ObjectRefSummary {
            kind: "uri",
            size_bytes: None,
            uri: Some(uri.clone()),
            content_type: metadata_value(metadata, "content_type"),
            preview: None,
            truncated: false,
            metadata: metadata.clone(),
        },
    }
}
pub(crate) fn output_preview(
    events: &[ExecutionEvent],
    outputs: &[Object],
) -> Option<InlinePreviewSummary> {
    let mut size = 0usize;
    let mut preview = Vec::new();
    let mut content_type = None;
    let frames: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.kind {
            EventKind::Output {
                data,
                metadata,
                progress: false,
            } => Some((data.as_ref(), metadata)),
            _ => None,
        })
        .collect();
    let chunks: Vec<_> = if frames.is_empty() {
        outputs
            .iter()
            .filter_map(|object| match object {
                Object::Inline { data, metadata } => Some((data.as_ref(), metadata)),
                _ => None,
            })
            .collect()
    } else {
        frames
    };
    for (data, metadata) in chunks {
        size = size.saturating_add(data.len());
        if content_type.is_none() {
            content_type = metadata_value(metadata, "content_type");
        }
        let remaining = INLINE_PREVIEW_LIMIT_BYTES.saturating_sub(preview.len());
        preview.extend_from_slice(&data[..data.len().min(remaining)]);
    }
    (size > 0).then(|| InlinePreviewSummary {
        size_bytes: size,
        content_type,
        preview: inline_preview(&preview),
        truncated: size > INLINE_PREVIEW_LIMIT_BYTES,
    })
}
pub(crate) fn inline_preview(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    let sample = &bytes[..bytes.len().min(512)];
    let binary = sample
        .iter()
        .filter(|byte| matches!(byte, 0..=8 | 11..=12 | 14..=31))
        .count();
    if binary * 10 > sample.len() {
        return None;
    }
    Some(
        String::from_utf8_lossy(&bytes[..bytes.len().min(INLINE_PREVIEW_LIMIT_BYTES)])
            .trim()
            .to_string(),
    )
}
pub(crate) fn metadata_value(metadata: &Metadata, key: &str) -> Option<String> {
    metadata
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
}
