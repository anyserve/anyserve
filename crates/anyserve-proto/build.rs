fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../api/anyserve/protos/grpc_service.proto";
    println!("cargo:rerun-if-changed={proto}");

    tonic_build::configure().compile_protos(&[proto], &["../../api/anyserve/protos"])?;

    Ok(())
}
