/// Shared transport limit for inline job inputs, outputs, and stream frames.
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Execution protocol: a client API and separate worker control/data streams.
#[allow(
    clippy::large_enum_variant,
    reason = "prost-generated oneofs retain their protocol layout"
)]
pub mod v2 {
    tonic::include_proto!("anyserve.v2");
}
