# anyserve

Anyserve is now a Rust workspace managed with `mise`.

The repository keeps the original protobuf contract in [`api/anyserve/protos/grpc_service.proto`](api/anyserve/protos/grpc_service.proto) and uses Rust for both the server runtime and the exported Python SDK:

- `clients/python`: Python SDK exported from Rust with `PyO3` and `maturin`
- `crates/anyserve-proto`: generated protobuf and tonic bindings
- `crates/anyserve-core`: Redis-backed metadata store and gRPC service logic
- `crates/anyserve-cli`: the `anyserve` binary
- `clients/rust`: a Rust demo client
- `docs`: an `mdBook` documentation tree

## Prerequisites

- `mise`
- `protoc`
- Redis
- Python 3.9+

## Setup

```bash
mise trust
mise install
mise run build
```

## Commands

```bash
# initialize metadata
cargo run -p anyserve -- init redis://127.0.0.1:6379 myserve

# start HTTP + gRPC
cargo run -p anyserve -- serve redis://127.0.0.1:6379

# manage queues
cargo run -p anyserve -- queue list redis://127.0.0.1:6379
cargo run -p anyserve -- queue create redis://127.0.0.1:6379 priority --index @model,@priority
cargo run -p anyserve -- queue stats redis://127.0.0.1:6379 default

# demo producer / worker
cargo run -p anyserve-client -- --mode consume
cargo run -p anyserve-client -- --mode produce

# build/install the Python SDK from Rust
mise run python-sdk
mise run python-sdk-dev
```

## Mise Tasks

```bash
mise run setup
mise run build
mise run check
mise run test
mise run fmt
mise run clippy
mise run python-sdk
mise run python-sdk-dev
```
