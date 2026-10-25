/// Shared transport limit for inline job inputs, outputs, and stream frames.
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Execution protocol: a client API and separate worker control/data streams.
#[allow(
    clippy::large_enum_variant,
    clippy::result_large_err,
    clippy::double_must_use,
    reason = "prost and tonic generate protocol layouts and async service signatures"
)]
pub mod v2 {
    tonic::include_proto!("anyserve.v2");
}
