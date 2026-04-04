# Architecture

The workspace is split into a few focused crates:

- `clients/python`: Python extension module and thin Python package wrapper
- `crates/anyserve-proto`: protobuf and gRPC bindings generated from `api/anyserve/protos/grpc_service.proto`
- `crates/anyserve-core`: shared config, Redis metadata store, and gRPC service implementation
- `crates/anyserve-cli`: the `anyserve` binary with `init`, `serve`, and `queue` commands
- `clients/rust`: a small Rust client that mirrors the old producer/consumer demo flow

The runtime request path is:

1. A caller sends `Infer`.
2. The server stores request content and metadata in Redis.
3. A worker calls `FetchInfer` and receives the oldest queued request that matches its filters.
4. The worker streams progress and terminal messages through `SendResponse`.
5. The caller receives those responses from the original `Infer` stream.

Redis stores:

- system format metadata
- queue definitions
- request payloads
- request metadata
- streamed response chunks
- request-to-queue mappings
