use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("protos");
    let proto = proto_dir.join("execution.proto");
    println!("cargo:rerun-if-changed={}", proto.display());
    tonic_build::configure()
        .bytes([".anyserve.v2"])
        .compile_protos(&[proto], &[proto_dir])?;
    Ok(())
}
