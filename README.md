# anyserve

Anyserve is a zero-dependency control plane for generic distributed execution.

It does not assume LLM, image, or any other workload category at the core layer.

## Core Model

A job describes:

- an `interface_name`
- a set of `required_attributes`
- a set of `preferred_attributes`
- a set of `required_capacity`

Workers advertise supply through the same generic model, and the control plane issues leases, tracks attempts, exposes generic streams, and requeues expired work.

## Module Diagram

```text
                 +-----------------------------+
                 |       SDK / CLI / API       |
                 |   Rust / Python / gRPC      |
                 +-------------+---------------+
                               |
                               v
                 +-----------------------------+
                 |         Transport           |
                 |  Client API   Worker API    |
                 +-------------+---------------+
                               |
                               v
        +--------------------------------------------------+
        |                    Kernel                        |
        |--------------------------------------------------|
        | Job Manager      Worker Registry   Lease Manager |
        | Attempt Log      Stream Router     Recovery Loop |
        | Event Stream     State Machine                     |
        +------------------+-------------------------------+
                           |
                           v
        +--------------------------------------------------+
        |                     Ports                        |
        |--------------------------------------------------|
        | StateStore   StreamStore   Scheduler   ObjectStore |
        +--------+----------+------------+------------+------+
                 |          |            |            |
                 v          v            v            v
              +------+   +------+    +------+     +--------+
              |memory|   |memory|    |basic |     |inline  |
              +------+   +------+    +------+     +--------+
```

## Current Capabilities

- pure gRPC runtime with gRPC health on the same port
- in-memory state store
- in-memory stream store
- basic demand/supply scheduler
- worker registration and heartbeats
- lease issuance, renewal, completion, and failure
- attempt tracking per lease assignment
- event streaming per job
- generic stream/frame data plane for client and worker IO
- Rust demo client and Rust-backed Python SDK

## Workspace

- `crates/anyserve-proto`: protobuf and tonic bindings
- `crates/anyserve-core`: domain model, in-memory state store, scheduler, kernel, and gRPC service
- `crates/anyserve-cli`: the `anyserve` binary
- `clients/rust`: demo submitter / worker client
- `clients/python`: Rust-exported Python SDK via `PyO3` and `maturin`
- `docs`: mdBook documentation

Static docs can be deployed from this repository to GitHub Pages. The current mdBook config assumes the project-page base path `/anyserve/`.

## Prerequisites

- `mise`
- `protoc`
- Python 3.9+ for the Python SDK

## Setup

```bash
mise trust
mise install
mise run build
```

## Local Run

Start the control plane:

```bash
mise exec -- cargo run -p anyserve -- serve
```

The runtime exposes only gRPC. Liveness and readiness use the standard gRPC health service on the same port.

Examples use endpoint strings like `http://127.0.0.1:50052`. That is a gRPC channel URI used by `tonic`, not a REST or JSON HTTP API.

Start a demo worker:

```bash
mise exec -- cargo run -p anyserve-client -- --mode worker
```

Submit a demo job and watch its events:

```bash
mise exec -- cargo run -p anyserve-client -- --mode submit
```

The demo path now uses both control-plane and data-plane APIs:

- it submits a `Job`
- opens `input.default`
- pushes input `Frame`s
- waits for worker events
- pulls `output.default` frames after completion

## Testing

Run the workspace tests:

```bash
mise run test
mise run e2e
mise run docs-build
```

Run linting:

```bash
mise run clippy
```

## Python SDK

Build the wheel:

```bash
mise run python-sdk
```

Then use the SDK:

```python
from anyserve import AnyserveClient, FRAME_DATA

client = AnyserveClient("http://127.0.0.1:50052")
# gRPC channel URI for the SDK transport, not a REST endpoint.

# This assumes a compatible worker is already running,
# for example: `mise exec -- cargo run -p anyserve-client -- --mode worker`

job = client.submit_job(
    interface_name="demo.echo.v1",
    required_attributes={"runtime": "demo"},
    required_capacity={"slot": 1},
)

stream = client.open_stream(job["job_id"], "input.default")
client.push_frames(
    stream["stream_id"],
    [(FRAME_DATA, b"hello from python", {})],
)
client.close_stream(stream["stream_id"])

events = client.watch_job(job["job_id"])
```

## Mise Tasks

```bash
mise run setup
mise run fmt
mise run build
mise run check
mise run test
mise run e2e
mise run clippy
mise run python-sdk
mise run python-sdk-smoke
mise run docs-build
mise run python-sdk-dev
```
