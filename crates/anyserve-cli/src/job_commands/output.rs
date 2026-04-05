use anyhow::Result;
use anyserve_client::{AttemptRecord, JobRecord, StreamRecord};

use super::args::{JobStateFilter, OutputFormat};
mod json;
mod shared;
mod text;

use json::write_job_list_json as write_list_json_impl;
pub(super) use json::{CancelJsonResult, cancel_result_from_error, cancel_result_from_job};
use json::{write_cancel_result_json, write_job_inspect_json as write_inspect_json_impl};
use text::print_job_inspect_text as print_inspect_text_impl;
use text::print_job_list_text as print_list_text_impl;
use text::{print_single_cancel_text as print_single_cancel_text_impl, write_cancel_result_text};

pub(super) fn write_job_list_json(jobs: &[JobRecord]) -> Result<()> {
    write_list_json_impl(jobs)
}

pub(super) fn print_job_list_text(endpoint: &str, jobs: &[JobRecord], states: &[JobStateFilter]) {
    print_list_text_impl(endpoint, jobs, states)
}

pub(super) fn write_job_inspect_json(
    job: &JobRecord,
    attempts: &[AttemptRecord],
    streams: &[StreamRecord],
) -> Result<()> {
    write_inspect_json_impl(job, attempts, streams)
}

pub(super) fn print_job_inspect_text(
    endpoint: &str,
    job: &JobRecord,
    attempts: &[AttemptRecord],
    streams: &[StreamRecord],
) {
    print_inspect_text_impl(endpoint, job, attempts, streams)
}

pub(super) fn print_single_cancel_text(endpoint: &str, job: &JobRecord) {
    print_single_cancel_text_impl(endpoint, job)
}

pub(super) fn write_cancel_result(output: OutputFormat, result: &CancelJsonResult) -> Result<()> {
    match output {
        OutputFormat::Json => write_cancel_result_json(result),
        OutputFormat::Text => write_cancel_result_text(result),
    }
}
