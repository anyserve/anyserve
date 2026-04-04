use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let proto_dir = manifest_dir.join("protos");
    let proto = proto_dir.join("grpc_service.proto");
    println!("cargo:rerun-if-changed={}", proto.display());

    tonic_build::configure().compile_protos(&[proto], &[proto_dir])?;

    Ok(())
}
