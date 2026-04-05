use std::collections::BTreeMap;

use anyhow::Result;

use anyserve_core::kernel::Kernel;
use anyserve_core::model::{
    AttemptRecord, FrameKind, JobEvent, JobRecord, ObjectRef, StreamRecord, WorkerRecord,
};
use anyserve_core::store::JobListSummary as StoreJobListSummary;

use super::api::{
    AttemptSummary, InlinePreviewSummary, JobEventSummary, JobSummary, ObjectRefSummary,
    StreamSummary, WorkerSummary,
};

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

pub(crate) fn job_summary(job: &JobRecord, attempts: &[AttemptRecord]) -> JobSummary {
    JobSummary {
        job_id: job.job_id.clone(),
        state: job.state,
        interface_name: job.spec.interface_name.clone(),
        source: metadata_value(&job.spec.metadata, "source"),
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        duration_ms: job_duration_ms(job),
        current_attempt_id: non_empty(&job.current_attempt_id),
        latest_worker_id: latest_worker_id(attempts),
        attempt_count: attempts.len(),
        last_error: non_empty(&job.last_error),
        metadata: job.spec.metadata.clone(),
    }
}

pub(crate) fn job_summary_from_store(job: &StoreJobListSummary) -> JobSummary {
    JobSummary {
        job_id: job.job_id.clone(),
        state: job.state,
        interface_name: job.interface_name.clone(),
        source: job.source.clone(),
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        duration_ms: job
            .state
            .is_terminal()
            .then(|| job.updated_at_ms.saturating_sub(job.created_at_ms)),
        current_attempt_id: non_empty(&job.current_attempt_id),
        latest_worker_id: job.latest_worker_id.clone(),
        attempt_count: job.attempt_count,
        last_error: non_empty(&job.last_error),
        metadata: job.metadata.clone(),
    }
}

pub(crate) fn worker_summary(worker: &WorkerRecord, now_ms: u64) -> WorkerSummary {
    WorkerSummary {
        worker_id: worker.worker_id.clone(),
        healthy: worker.expires_at_ms > now_ms,
        interfaces: worker.spec.interfaces.iter().cloned().collect(),
        attributes: worker.spec.attributes.clone(),
        total_capacity: worker.spec.total_capacity.clone(),
        available_capacity: worker.status.available_capacity.clone(),
        max_active_leases: worker.spec.max_active_leases,
        active_leases: worker.status.active_leases,
        registered_at_ms: worker.registered_at_ms,
        last_seen_at_ms: worker.status.last_seen_at_ms,
        expires_at_ms: worker.expires_at_ms,
        spec_metadata: worker.spec.metadata.clone(),
        status_metadata: worker.status.metadata.clone(),
    }
}

pub(crate) fn attempt_summary(attempt: &AttemptRecord) -> AttemptSummary {
    AttemptSummary {
        attempt_id: attempt.attempt_id.clone(),
        state: attempt.state,
        worker_id: non_empty_string(&attempt.worker_id),
        lease_id: non_empty_string(&attempt.lease_id),
        created_at_ms: attempt.created_at_ms,
        started_at_ms: attempt.started_at_ms,
        finished_at_ms: attempt.finished_at_ms,
        duration_ms: attempt_duration_ms(attempt),
        last_error: non_empty(&attempt.last_error),
        metadata: attempt.metadata.clone(),
    }
}

pub(crate) fn stream_summary(stream: &StreamRecord) -> StreamSummary {
    StreamSummary {
        stream_id: stream.stream_id.clone(),
        stream_name: stream.stream_name.clone(),
        scope: stream.scope,
        direction: stream.direction,
        state: stream.state,
        attempt_id: non_empty(&stream.attempt_id),
        lease_id: non_empty(&stream.lease_id),
        created_at_ms: stream.created_at_ms,
        closed_at_ms: stream.closed_at_ms,
        last_sequence: stream.last_sequence,
        metadata: stream.metadata.clone(),
    }
}

pub(crate) fn event_summary(event: &JobEvent) -> JobEventSummary {
    JobEventSummary {
        sequence: event.sequence,
        kind: event.kind,
        created_at_ms: event.created_at_ms,
        payload_size_bytes: event.payload.len(),
        metadata: event.metadata.clone(),
    }
}

pub(crate) fn object_ref_summary(value: &ObjectRef) -> ObjectRefSummary {
    match value {
        ObjectRef::Inline { content, metadata } => ObjectRefSummary {
            kind: "inline",
            size_bytes: Some(content.len()),
            uri: None,
            content_type: metadata_value(metadata, "content_type"),
            preview: inline_preview(content),
            truncated: content.len() > INLINE_PREVIEW_LIMIT_BYTES,
            metadata: metadata.clone(),
        },
        ObjectRef::Uri { uri, metadata } => ObjectRefSummary {
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

pub(crate) async fn read_output_preview(
    kernel: &Kernel,
    streams: &[StreamRecord],
) -> Result<Option<InlinePreviewSummary>> {
    let Some(output_stream) = streams
        .iter()
        .find(|stream| stream.stream_name == "output.default")
    else {
        return Ok(None);
    };

    let frames = kernel.pull_frames(&output_stream.stream_id, 0).await?;
    let mut payload = Vec::new();
    let mut content_type = metadata_value(&output_stream.metadata, "content_type");

    for frame in frames {
        match frame.kind {
            FrameKind::Data => {
                if content_type.is_none() {
                    content_type = metadata_value(&frame.metadata, "content_type");
                }
                payload.extend(frame.payload);
            }
            FrameKind::Error => {
                if payload.is_empty() {
                    payload.extend(frame.payload);
                }
            }
            _ => {}
        }
    }

    if payload.is_empty() {
        return Ok(None);
    }

    Ok(Some(InlinePreviewSummary {
        size_bytes: payload.len(),
        content_type,
        preview: inline_preview(&payload),
        truncated: payload.len() > INLINE_PREVIEW_LIMIT_BYTES,
    }))
}

pub(crate) fn inline_preview(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() || !is_probably_text(bytes) {
        return None;
    }

    let preview = &bytes[..bytes.len().min(INLINE_PREVIEW_LIMIT_BYTES)];
    Some(String::from_utf8_lossy(preview).trim().to_string())
}

pub(crate) fn is_probably_text(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }

    let sample = &bytes[..bytes.len().min(512)];
    let binary_like = sample
        .iter()
        .filter(|byte| matches!(byte, 0..=8 | 11..=12 | 14..=31))
        .count();

    binary_like * 10 <= sample.len()
}

pub(crate) fn metadata_value(metadata: &BTreeMap<String, String>, key: &str) -> Option<String> {
    metadata.get(key).and_then(|value| non_empty_string(value))
}

pub(crate) fn latest_worker_id(attempts: &[AttemptRecord]) -> Option<String> {
    attempts
        .iter()
        .max_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.attempt_id.cmp(&right.attempt_id))
        })
        .and_then(|attempt| non_empty_string(&attempt.worker_id))
}

pub(crate) fn attempt_duration_ms(attempt: &AttemptRecord) -> Option<u64> {
    attempt.finished_at_ms.map(|finished| {
        finished.saturating_sub(attempt.started_at_ms.unwrap_or(attempt.created_at_ms))
    })
}

pub(crate) fn job_duration_ms(job: &JobRecord) -> Option<u64> {
    job.state
        .is_terminal()
        .then(|| job.updated_at_ms.saturating_sub(job.created_at_ms))
}

pub(crate) fn non_empty(value: &Option<String>) -> Option<String> {
    value.as_deref().and_then(non_empty_string)
}

pub(crate) fn non_empty_string(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.to_string())
}

const INLINE_PREVIEW_LIMIT_BYTES: usize = 4 * 1024;
