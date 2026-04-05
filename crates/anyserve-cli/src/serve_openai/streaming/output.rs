use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyserve_core::kernel::Kernel;
use anyserve_core::model::{Frame, FrameKind, JobRecord, JobState};

use super::super::state::{AppState, OpenAIError};
use super::OUTPUT_STREAM;
use super::waiters::wait_for_job_event_change;

pub(super) async fn wait_for_output_bytes(
    state: &AppState,
    job_id: String,
) -> Result<Vec<u8>, OpenAIError> {
    match tokio::time::timeout(
        Duration::from_secs(state.config.request_timeout_secs),
        collect_output_bytes(Arc::clone(&state.kernel), job_id.clone()),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            cancel_job_best_effort(Arc::clone(&state.kernel), job_id).await;
            Err(OpenAIError::gateway_timeout(
                "request timed out waiting for worker",
            ))
        }
    }
}

pub(super) async fn cancel_job_best_effort(kernel: Arc<Kernel>, job_id: String) {
    let _ = kernel.cancel_job(&job_id).await;
}

pub(super) async fn cancel_submitted_job(
    kernel: Arc<Kernel>,
    job_id: String,
    error: String,
) -> OpenAIError {
    match kernel.cancel_job(&job_id).await {
        Ok(_) => OpenAIError::internal(error),
        Err(cleanup_error) => OpenAIError::internal(format!(
            "{error}; cleanup cancel job '{job_id}': {cleanup_error}"
        )),
    }
}

pub(super) fn read_output_from_job(job: &JobRecord) -> Result<Option<Vec<u8>>, OpenAIError> {
    for output in &job.outputs {
        match output {
            anyserve_core::model::ObjectRef::Inline { content, .. } if !content.is_empty() => {
                return Ok(Some(content.clone()));
            }
            anyserve_core::model::ObjectRef::Inline { .. } => continue,
            anyserve_core::model::ObjectRef::Uri { uri, .. } => {
                return Err(OpenAIError::bad_gateway(format!(
                    "worker returned unsupported uri output: {uri}"
                )));
            }
        }
    }

    Ok(None)
}

pub(super) fn extract_data_chunks(
    frames: Vec<Frame>,
    after_sequence: &mut u64,
) -> io::Result<Vec<Vec<u8>>> {
    let mut chunks = Vec::new();
    for frame in frames {
        *after_sequence = frame.sequence;
        match frame.kind {
            FrameKind::Data => chunks.push(frame.payload),
            FrameKind::Error => {
                return Err(io::Error::other(
                    String::from_utf8_lossy(&frame.payload).into_owned(),
                ));
            }
            _ => {}
        }
    }
    Ok(chunks)
}

async fn collect_output_bytes(kernel: Arc<Kernel>, job_id: String) -> Result<Vec<u8>, OpenAIError> {
    let mut local_updates = kernel
        .subscribe_job_events(&job_id)
        .await
        .map_err(|error| OpenAIError::internal(format!("subscribe job events: {error}")))?;
    let mut cluster_updates = kernel
        .subscribe_cluster_job_events(&job_id)
        .await
        .map_err(|error| OpenAIError::internal(format!("subscribe cluster job events: {error}")))?;
    let poll_interval = Duration::from_millis(kernel.watch_poll_interval_ms());

    loop {
        let job = kernel
            .get_job(&job_id)
            .await
            .map_err(|error| OpenAIError::internal(format!("get job: {error}")))?;

        match job.state {
            JobState::Succeeded => {
                if let Some(output) = read_output_from_job(&job)? {
                    return Ok(output);
                }
                return read_output_stream(kernel, &job.job_id).await;
            }
            JobState::Failed => {
                return Err(OpenAIError::bad_gateway(
                    job.last_error
                        .unwrap_or_else(|| "worker reported failure".to_string()),
                ));
            }
            JobState::Cancelled => return Err(OpenAIError::bad_gateway("job was cancelled")),
            JobState::Pending | JobState::Leased | JobState::Running => {
                wait_for_job_event_change(poll_interval, &mut local_updates, &mut cluster_updates)
                    .await;
            }
        }
    }
}

async fn read_output_stream(kernel: Arc<Kernel>, job_id: &str) -> Result<Vec<u8>, OpenAIError> {
    let streams = kernel
        .list_streams(job_id)
        .await
        .map_err(|error| OpenAIError::internal(format!("list output streams: {error}")))?;
    let Some(output_stream) = streams
        .iter()
        .find(|stream| stream.stream_name == OUTPUT_STREAM)
    else {
        return Err(OpenAIError::bad_gateway(
            "worker completed without output.default stream",
        ));
    };

    let frames = kernel
        .pull_frames(&output_stream.stream_id, 0)
        .await
        .map_err(|error| OpenAIError::internal(format!("pull output frames: {error}")))?;

    let mut payload = Vec::new();
    for frame in frames {
        match frame.kind {
            FrameKind::Data => payload.extend(frame.payload),
            FrameKind::Error => {
                let message = String::from_utf8_lossy(&frame.payload).into_owned();
                return Err(OpenAIError::bad_gateway(if message.is_empty() {
                    "worker returned error frame".to_string()
                } else {
                    message
                }));
            }
            _ => {}
        }
    }

    if payload.is_empty() {
        return Err(OpenAIError::bad_gateway(
            "worker completed without response payload",
        ));
    }

    Ok(payload)
}
