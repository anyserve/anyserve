# anyserve

Serve Models Anywhere, Anytime, on Any Platform.

Anyserve lets you serve models through your own workers, wherever they run.
Use one production endpoint and one execution model across chat, embeddings,
vision, speech, and streaming workloads.

The point is simple: stop building separate systems for each inference type.
Deploy Anyserve once, then reuse the same worker model, scheduling, streams,
and recovery flow everywhere.

The clearest way to understand it is simple:

- Anyserve owns the public endpoint, auth, and routing layer
- you own the workers, models, and inference backends
- requests hit Anyserve first, then get scheduled onto your worker pool
- new inference workloads plug into the same runtime instead of needing
  separate infrastructure

Underneath that product surface, Anyserve is a workload-neutral control plane for
distributed execution. It does not hard-code LLM, image, or any single workload
category into the core runtime.

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
                 |  Native / Python / gRPC     |
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
- first-class native client transport, sample apps, and Python bindings

## Repository Layout

- `crates/anyserve-client`: native client transport for the control-plane gRPC API
- `crates/anyserve-proto`: protobuf and tonic bindings
- `crates/anyserve-core`: domain model, in-memory state store, scheduler, kernel, and gRPC service
- `crates/anyserve-cli`: the `anyserve` binary
- `examples/rust`: sample submitter / worker apps that exercise the native client transport
- `examples/python`: sample submitter / worker scripts that exercise the Python SDK
- `clients/python`: Python bindings built on the native client transport
- `docs`: mdBook documentation

Static docs can be deployed from this repository to GitHub Pages. The current mdBook config assumes the project-page base path `/anyserve/`.

## Prerequisites

- `mise`
- `protoc`
- Python 3.12+ for the Python bindings

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
curl -fsSL https://raw.githubusercontent.com/anyserve/anyserve/main/scripts/install.sh | sh -s -- --version v0.4.0
curl -fsSL https://raw.githubusercontent.com/anyserve/anyserve/main/scripts/install.sh | sh -s -- --dir /usr/local/bin
```

```powershell
& ([scriptblock]::Create((irm https://raw.githubusercontent.com/anyserve/anyserve/main/scripts/install.ps1))) -Version v0.4.0
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

Start the built-in OpenAI-compatible example stack from the checked-in config:

```bash
mise exec -- cargo run -p anyserve -- serve --config examples/ollama/anyserve.toml
mise exec -- cargo run -p anyserve -- worker --config examples/ollama/worker.toml
```

That example starts:

- gRPC on `127.0.0.1:50052`
- the built-in OpenAI-compatible gateway on `127.0.0.1:8080`

The built-in gateway exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /v1/models`
- `POST /v1/chat/completions`
- `POST /v1/embeddings`

Examples use endpoint strings like `http://127.0.0.1:50052`. That is a gRPC channel URI used by `tonic`, not a REST or JSON HTTP API.

Treat the checked-in example docs as the canonical walkthrough:

- [examples/ollama/README.md](examples/ollama/README.md): Ollama-backed built-in OpenAI gateway + built-in LLM worker
- [examples/google-colab/README.md](examples/google-colab/README.md): Google Colab examples for `Qwen/Qwen3-0.6B`, both all-in-one and remote-worker
- [docs/src/ollama.md](docs/src/ollama.md): Ollama-specific notes for the same example
- [docs/src/testing.md](docs/src/testing.md): how the example fits into the test layers

The worker expects an OpenAI-compatible upstream at the configured `base_url`, for example Ollama, SGLang, or vLLM.

Start the sample worker:

```bash
mise exec -- cargo run -p anyserve-demo -- --mode worker
```

Submit a sample job and watch its events:

```bash
mise exec -- cargo run -p anyserve-demo -- --mode submit
```

Or use the Python examples after installing the bindings:

```bash
# clean venv from local source
python -m venv .venv
. .venv/bin/activate
pip install ./clients/python

# or use the active mise Python
# mise run python-sdk-dev

python examples/python/worker.py
python examples/python/submit.py

# with the active mise Python instead
# mise exec -- python examples/python/worker.py
# mise exec -- python examples/python/submit.py
```

The sample path now uses both control-plane and data-plane APIs:

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
# The Python facade uses the same high-performance client transport.

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
