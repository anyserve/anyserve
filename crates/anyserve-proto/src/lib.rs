/// Shared transport limit for inline job inputs, outputs, and stream frames.
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

pub mod controlplane {
    tonic::include_proto!("controlplane");
}
