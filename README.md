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
- first-class Rust client crate, Rust demo apps, and Rust-backed Python bindings

## Workspace

- `crates/anyserve-client`: first-class Rust client for the control-plane gRPC API
- `crates/anyserve-proto`: protobuf and tonic bindings
- `crates/anyserve-core`: domain model, in-memory state store, scheduler, kernel, and gRPC service
- `crates/anyserve-cli`: the `anyserve` binary
- `examples/rust`: demo submitter / worker apps that exercise the Rust client crate
- `clients/python`: Rust-exported Python bindings via `PyO3` and `maturin`
- `docs`: mdBook documentation

Static docs can be deployed from this repository to GitHub Pages. The current mdBook config assumes the project-page base path `/anyserve/`.

## Prerequisites

- `mise`
- `protoc`
- Python 3.9+ for the Python bindings

## Install

Install the latest prebuilt binary from GitHub Releases:

```bash
curl -fsSL https://raw.githubusercontent.com/anyserve/anyserve/main/scripts/install.sh | sh
```

Install on Windows with PowerShell:

```powershell
irm https://raw.githubusercontent.com/anyserve/anyserve/main/scripts/install.ps1 | iex
```

Install a specific version or choose a custom install directory:

```bash
curl -fsSL https://raw.githubusercontent.com/anyserve/anyserve/main/scripts/install.sh | sh -s -- --version v0.2.0
curl -fsSL https://raw.githubusercontent.com/anyserve/anyserve/main/scripts/install.sh | sh -s -- --dir /usr/local/bin
```

```powershell
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/anyserve/anyserve/main/scripts/install.ps1))) -Version v0.2.0
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/anyserve/anyserve/main/scripts/install.ps1))) -InstallDir "$env:LOCALAPPDATA\Programs\AnyServe\bin"
```

Prebuilt binaries are currently published for:

- macOS `arm64`
- macOS `x86_64`
- Linux `x86_64`
- Windows `x86_64`

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
mise exec -- cargo run -p anyserve-demo -- --mode worker
```

Submit a demo job and watch its events:

```bash
mise exec -- cargo run -p anyserve-demo -- --mode submit
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
mise run python-sdk-e2e
mise run docs-build
```

Run linting:

```bash
mise run clippy
```

## Python Bindings

Build the wheel:

```bash
mise run python-sdk
```

You can build function-style workers on top of the low-level bindings:

```python
from anyserve import serve, worker


@worker(
    interface="demo.echo.v1",
    attributes={"runtime": "python"},
    capacity={"slot": 1},
    codec="bytes",
)
def echo(payload: bytes) -> bytes:
    return payload


serve(echo, endpoint="http://127.0.0.1:50052")
```

Then use the bindings:

```python
from anyserve import AnyserveClient, FRAME_DATA

client = AnyserveClient("http://127.0.0.1:50052")
# gRPC channel URI for the bindings transport, not a REST endpoint.
# The Python facade is backed by the Rust anyserve-client transport layer.

# This assumes a compatible worker is already running,
# for example: `mise exec -- cargo run -p anyserve-demo -- --mode worker`

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

for event in client.watch_job(job["job_id"]):
    print(event["kind"], event["metadata"])
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
mise run python-sdk-e2e
mise run docs-build
mise run python-sdk-dev
```
