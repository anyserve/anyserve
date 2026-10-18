mod output;
mod response;
mod waiters;

const OUTPUT_STREAM: &str = "output.default";

#[cfg(test)]
pub(super) use output::read_output_from_job;
pub(super) use output::wait_for_output_bytes;
pub(super) use response::streaming_response;
