use axum::response::Response;
use std::sync::Arc;

use anyserve_core::kernel::Kernel;
#[cfg(test)]
use anyserve_core::model::JobRecord;

use super::state::{AppState, OpenAIError};

mod output;
mod response;
mod waiters;

const OUTPUT_STREAM: &str = "output.default";

pub(super) async fn wait_for_output_bytes(
    state: &AppState,
    job_id: String,
) -> Result<Vec<u8>, OpenAIError> {
    output::wait_for_output_bytes(state, job_id).await
}

pub(super) async fn cancel_submitted_job(
    kernel: Arc<Kernel>,
    job_id: String,
    error: String,
) -> OpenAIError {
    output::cancel_submitted_job(kernel, job_id, error).await
}

#[cfg(test)]
pub(super) fn read_output_from_job(job: &JobRecord) -> Result<Option<Vec<u8>>, OpenAIError> {
    output::read_output_from_job(job)
}

pub(super) fn streaming_response(state: AppState, job_id: String) -> Response {
    response::streaming_response(state, job_id)
}
