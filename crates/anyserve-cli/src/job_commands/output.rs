use anyhow::Result;

use super::args::OutputFormat;
mod json;
mod shared;
mod text;

use json::write_cancel_result_json;
pub(super) use json::{
    CancelJsonResult, cancel_result_from_error, cancel_result_from_job, write_job_inspect_json,
    write_job_list_json,
};
use text::write_cancel_result_text;
pub(super) use text::{print_job_inspect_text, print_job_list_text, print_single_cancel_text};

pub(super) fn write_cancel_result(output: OutputFormat, result: &CancelJsonResult) -> Result<()> {
    match output {
        OutputFormat::Json => write_cancel_result_json(result),
        OutputFormat::Text => write_cancel_result_text(result),
    }
}
