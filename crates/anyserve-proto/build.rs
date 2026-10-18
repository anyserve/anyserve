use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("protos");
    let protos = [proto_dir.join("execution.proto")];
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    tonic_build::configure()
        .bytes([".anyserve.v2"])
        .compile_protos(&protos, &[proto_dir])?;
    Ok(())
}
