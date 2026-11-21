use super::api::{
    AttemptSummary, HealthSummary, InlinePreviewSummary, InterfacePolicySummary, JobEventSummary,
    JobSummary, JournalHealthSummary, JournalStatsSummary, ObjectRefSummary, PoolTierSummary,
    WorkerLoadSummary, WorkerRollup, WorkerSummary,
};
use anyserve_core::runtime::api::{
    Durability, EventKind, ExecutionEvent, Health, InterfacePolicy, JobState, JobView,
    JournalHealth, JournalStats, Metadata, Object, ObjectPreview, WorkerView,
};
use std::collections::BTreeMap;
use std::time::Duration;

pub(crate) use anyserve_core::runtime::api::INSPECTION_PREVIEW_BYTES as INLINE_PREVIEW_LIMIT_BYTES;

pub(crate) fn state_name(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
    }
}

pub(crate) fn durability_name(durability: Durability) -> &'static str {
    match durability {
        Durability::Durable => "durable",
        Durability::Ephemeral => "ephemeral",
    }
}

pub(crate) fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn health_summary(health: Health) -> HealthSummary {
    let (status, message) = match health.journal {
        JournalHealth::Disabled => ("disabled", None),
        JournalHealth::Healthy => ("healthy", None),
        JournalHealth::Unavailable(msg) => ("unavailable", Some(msg)),
    };
    HealthSummary {
        live: health.live,
        ready: health.ready,
        journal: JournalHealthSummary { status, message },
    }
}

pub(crate) fn journal_stats_summary(stats: JournalStats) -> JournalStatsSummary {
    JournalStatsSummary {
        pending_ops: stats.pending_ops,
        committed_batches: stats.committed_batches,
        committed_ops: stats.committed_ops,
        last_batch_size: stats.last_batch_size,
        last_commit_micros: stats.last_commit_micros,
    }
}

pub(crate) fn interface_policy_summary(policy: &InterfacePolicy) -> InterfacePolicySummary {
    InterfacePolicySummary {
        max_queue: policy.max_queue,
        queue_timeout_ms: policy.queue_timeout.map(duration_ms),
        max_in_flight_per_key: policy.max_in_flight_per_key,
        pool_tiers: policy
            .pool_tiers
            .iter()
            .map(|tier| PoolTierSummary {
                pool: tier.pool.clone(),
                spillover_after_ms: duration_ms(tier.spillover_after),
                reconnect_grace_ms: tier.reconnect_grace.map(duration_ms),
            })
            .collect(),
    }
}

pub(crate) fn worker_rollup(workers: &[WorkerView]) -> WorkerRollup {
    let mut healthy = 0usize;
    let mut active_leases = 0u64;
    let mut total_capacity: BTreeMap<String, i64> = BTreeMap::new();
    let mut last_seen_at_ms: Option<u64> = None;

    for worker in workers {
        active_leases += u64::from(worker.active_leases);
        last_seen_at_ms =
            Some(last_seen_at_ms.map_or(worker.last_seen_ms, |max| max.max(worker.last_seen_ms)));
        if worker.connected && !worker.draining {
            healthy += 1;
            for (name, value) in &worker.total_capacity {
                *total_capacity.entry(name.clone()).or_default() += value;
            }
        }
    }

    WorkerRollup {
        total: workers.len(),
        healthy,
        unavailable: workers.len() - healthy,
        active_leases,
        total_capacity,
        last_seen_at_ms,
    }
}

pub(crate) fn job_summary(job: JobView) -> JobSummary {
    let duration_ms = job
        .state
        .is_terminal()
        .then(|| job.updated_at_ms.saturating_sub(job.created_at_ms));
    let source = metadata_value(&job.metadata, "source");
    let (last_error, error_code, error_retryable) = match job.error {
        Some(error) => (Some(error.message), Some(error.code), Some(error.retryable)),
        None => (None, None, None),
    };
    JobSummary {
        job_id: job.job_id,
        namespace: job.namespace,
        state: state_name(job.state),
        durability: durability_name(job.durability),
        interface_name: job.interface,
        source,
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        duration_ms,
        current_attempt_id: job.current_attempt_id,
        latest_worker_id: job.current_worker_id,
        attempt_count: job.attempt_count,
        last_error,
        error_code,
        error_retryable,
        metadata: job.metadata,
    }
}

pub(crate) fn worker_summary(worker: WorkerView) -> WorkerSummary {
    let healthy = worker.connected && !worker.draining;
    let utilization = worker.load.utilization;
    WorkerSummary {
        session_id: worker.session_id,
        worker_id: worker.worker_id,
        namespace: worker.namespace,
        pool: worker.pool,
        healthy,
        interfaces: worker.interfaces,
        attributes: worker.attributes,
        total_capacity: worker.total_capacity,
        max_concurrency: worker.max_concurrency,
        active_leases: worker.active_leases,
        free_slots: worker.free_slots,
        utilization,
        load: WorkerLoadSummary {
            queued: worker.load.queued,
            running: worker.load.running,
            utilization: worker.load.utilization,
            custom: worker.load.custom,
        },
        connected: worker.connected,
        draining: worker.draining,
        connected_at_ms: worker.connected_at_ms,
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

pub(crate) fn object_preview_summary(value: &ObjectPreview) -> ObjectRefSummary {
    let mut summary = object_ref_summary(&value.prefix);
    summary.size_bytes = value.size_bytes;
    summary.truncated = value
        .size_bytes
        .is_some_and(|size| size > INLINE_PREVIEW_LIMIT_BYTES);
    summary
}

pub(crate) fn output_preview(
    events: &[ExecutionEvent],
    outputs: &[ObjectPreview],
) -> Option<InlinePreviewSummary> {
    let mut size = 0usize;
    let mut preview = Vec::new();
    let mut content_type = None;
    let mut append = |data: &[u8], metadata: &Metadata, original_size: usize| {
        size = size.saturating_add(original_size);
        if content_type.is_none() {
            content_type = metadata_value(metadata, "content_type");
        }
        let remaining = INLINE_PREVIEW_LIMIT_BYTES.saturating_sub(preview.len());
        if remaining > 0 && !data.is_empty() {
            preview.extend_from_slice(&data[..data.len().min(remaining)]);
        }
    };
    let mut has_frames = false;
    for event in events {
        if let EventKind::Output {
            data,
            metadata,
            progress: false,
        } = &event.kind
        {
            has_frames = true;
            append(data, metadata, data.len());
        }
    }
    if !has_frames {
        for output in outputs {
            if let Object::Inline { data, metadata } = &output.prefix {
                append(data, metadata, output.size_bytes.unwrap_or(data.len()));
            }
        }
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
