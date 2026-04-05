use std::collections::BTreeMap;
use std::io::{self, Write};

use anyhow::{Context, Result};
use anyserve_client::{AttemptRecord, JobRecord, StreamRecord};
use serde::Serialize;

use super::shared::{
    attempt_state_name, job_metadata, job_state_name, optional_string, optional_timestamp,
    sorted_map, stream_direction_name, stream_scope_name, stream_state_name,
};

#[derive(Serialize)]
struct JobJsonRecord {
    job_id: String,
    state: String,
    interface_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_attempt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_id: Option<String>,
    version: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    outputs_count: usize,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct AttemptJsonRecord {
    attempt_id: String,
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_id: Option<String>,
    created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    started_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    finished_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct StreamJsonRecord {
    stream_id: String,
    stream_name: String,
    scope: String,
    direction: String,
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_id: Option<String>,
    created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_at_ms: Option<u64>,
    last_sequence: u64,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct JobInspectJsonRecord {
    job: JobJsonRecord,
    attempts: Vec<AttemptJsonRecord>,
    streams: Vec<StreamJsonRecord>,
}

#[derive(Serialize)]
pub(crate) struct CancelJsonResult {
    pub(crate) job_id: String,
    pub(crate) ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) updated_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

pub(super) fn write_job_list_json(jobs: &[JobRecord]) -> Result<()> {
    let payload = jobs.iter().map(job_json_record).collect::<Vec<_>>();
    write_json(&payload)
}

pub(super) fn write_job_inspect_json(
    job: &JobRecord,
    attempts: &[AttemptRecord],
    streams: &[StreamRecord],
) -> Result<()> {
    let payload = JobInspectJsonRecord {
        job: job_json_record(job),
        attempts: attempts.iter().map(attempt_json_record).collect(),
        streams: streams.iter().map(stream_json_record).collect(),
    };
    write_json(&payload)
}

pub(crate) fn cancel_result_from_job(job: JobRecord) -> CancelJsonResult {
    let state = job_state_name(job.state()).to_string();
    let updated_at_ms = job.updated_at_ms;
    CancelJsonResult {
        job_id: job.job_id,
        ok: true,
        state: Some(state),
        updated_at_ms: Some(updated_at_ms),
        error: None,
    }
}

pub(crate) fn cancel_result_from_error(job_id: String, error: String) -> CancelJsonResult {
    CancelJsonResult {
        job_id,
        ok: false,
        state: None,
        updated_at_ms: None,
        error: Some(error),
    }
}

pub(super) fn write_cancel_result_json(result: &CancelJsonResult) -> Result<()> {
    write_json_line(result)
}

fn job_json_record(job: &JobRecord) -> JobJsonRecord {
    JobJsonRecord {
        job_id: job.job_id.clone(),
        state: job_state_name(job.state()).to_string(),
        interface_name: super::shared::interface_name(job).to_string(),
        current_attempt_id: optional_string(&job.current_attempt_id),
        lease_id: optional_string(&job.lease_id),
        version: job.version,
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        last_error: optional_string(&job.last_error),
        outputs_count: job.outputs.len(),
        metadata: sorted_map(job_metadata(job)),
    }
}

fn attempt_json_record(attempt: &AttemptRecord) -> AttemptJsonRecord {
    AttemptJsonRecord {
        attempt_id: attempt.attempt_id.clone(),
        state: attempt_state_name(attempt.state()).to_string(),
        worker_id: optional_string(&attempt.worker_id),
        lease_id: optional_string(&attempt.lease_id),
        created_at_ms: attempt.created_at_ms,
        started_at_ms: optional_timestamp(attempt.started_at_ms),
        finished_at_ms: optional_timestamp(attempt.finished_at_ms),
        last_error: optional_string(&attempt.last_error),
        metadata: sorted_map(&attempt.metadata),
    }
}

fn stream_json_record(stream: &StreamRecord) -> StreamJsonRecord {
    StreamJsonRecord {
        stream_id: stream.stream_id.clone(),
        stream_name: stream.stream_name.clone(),
        scope: stream_scope_name(stream.scope()).to_string(),
        direction: stream_direction_name(stream.direction()).to_string(),
        state: stream_state_name(stream.state()).to_string(),
        attempt_id: optional_string(&stream.attempt_id),
        lease_id: optional_string(&stream.lease_id),
        created_at_ms: stream.created_at_ms,
        closed_at_ms: optional_timestamp(stream.closed_at_ms),
        last_sequence: stream.last_sequence,
        metadata: sorted_map(&stream.metadata),
    }
}

fn write_json<T: Serialize>(value: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer(&mut handle, value).context("serialize json output")?;
    handle.write_all(b"\n").context("flush json newline")
}

fn write_json_line<T: Serialize>(value: &T) -> Result<()> {
    write_json(value)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyserve_client::JobRecord;
    use anyserve_proto::controlplane::JobSpec;
    use serde_json::json;

    use super::{CancelJsonResult, job_json_record};

    #[test]
    fn job_json_record_serializes_machine_readable_shape() {
        let job = JobRecord {
            job_id: "job-1".to_string(),
            state: anyserve_client::JobState::Pending as i32,
            spec: Some(JobSpec {
                interface_name: "demo.echo.v1".to_string(),
                metadata: HashMap::from([("team".to_string(), "ops".to_string())]),
                ..JobSpec::default()
            }),
            version: 3,
            created_at_ms: 100,
            updated_at_ms: 200,
            ..JobRecord::default()
        };

        let value = serde_json::to_value(job_json_record(&job)).unwrap();
        assert_eq!(
            value,
            json!({
                "job_id": "job-1",
                "state": "pending",
                "interface_name": "demo.echo.v1",
                "version": 3,
                "created_at_ms": 100,
                "updated_at_ms": 200,
                "outputs_count": 0,
                "metadata": {
                    "team": "ops"
                }
            })
        );
    }

    #[test]
    fn cancel_json_result_omits_optional_fields_when_missing() {
        let result = CancelJsonResult {
            job_id: "job-1".to_string(),
            ok: false,
            state: None,
            updated_at_ms: None,
            error: Some("boom".to_string()),
        };

        let value = serde_json::to_value(result).unwrap();
        assert_eq!(
            value,
            json!({ "job_id": "job-1", "ok": false, "error": "boom" })
        );
    }
}
