# Anyserve

Anyserve is a backend-independent execution and scheduling system. Clients send
opaque inputs through the native execution protocol. One control-plane Runtime
matches jobs to outbound workers by capability and available capacity, and owns
admission, flow control, cancellation, retries and optional durable recovery.

Workers own their application code and backends. An ONNX engine, a remote
inference service or an ordinary function can implement a worker without adding
backend-specific semantics to the control plane.

## Core contract

- **Invoke** is ephemeral and supports complete or live duplex input/output.
  Dropping the invocation cancels unfinished work.
- **Submit** accepts complete input and defaults to durable execution. The
  optional SQLite journal commits input before acceptance and terminal results
  before completion acknowledgement. External side effects need application
  idempotency because unfinished work can execute again after a restart.
- **Placement** matches interfaces, required/preferred attributes, resource
  quantities, concurrency and worker credits. Workers establish outbound
  Session/Attempt connections and need no inbound endpoint.
- **Bounded delivery** provides output events, per-attempt byte windows, replay
  cursors and three memory budgets. Exposed streamed output cannot silently
  switch attempts. `max_attempts` includes the first attempt.
- **Operations** provides client/worker token roles, TLS for gRPC, a management
  API, console, health probes, Runtime metrics and logs.

```text
Rust / Python client -> native Client gRPC -> Execution Runtime
                                                |       |
                                 optional SQLite       Worker.Session / Attempt
                                     journal                  |
                                                     application worker
                                                              |
                                                       application backend
```

The supported control plane is one process. Its journal has one writer and
survives a short restart when stored on a persistent volume. Adding workers
scales execution capacity. Active control-plane replicas sharing a database
are outside this architecture.

## Run from this checkout

Rust and `protoc` are required to build the binary. Run from the repository root:

```bash
cargo build -p anyserve
./target/debug/anyserve serve --config examples/server.toml
```

The example enables native gRPC on `127.0.0.1:50052` and the optional management
API on `127.0.0.1:3001`. Its journal is `.anyserve/journal.sqlite` relative to the
server's working directory. `serve --ephemeral` disables durable execution;
`serve --journal /persistent/journal.sqlite` selects a persistent path.

In another terminal, install the SDK with Python 3.12+ and start a native worker:

```bash
python -m venv .venv
. .venv/bin/activate
pip install ./clients/python
python examples/python/worker.py
```

From a terminal with the same Python environment:

```bash
python examples/python/submit.py
```

The [Python examples](examples/python/README.md) are an echo worker and a
submitter; workers for Triton Inference Server, vLLM, ComfyUI and live video
live in [anyserve.universe](https://github.com/anyserve/anyserve.universe).
The [Rust example](examples/rust) uses the same protocol. Applications supply backend-specific worker code; the CLI starts the
control plane and inspects/cancels jobs.

Configure separate client and worker tokens before remote access. The job CLI
and native clients read `ANYSERVE_TOKEN`; application workers use a worker-role
token. `job ls`, `job inspect <id>` and `job cancel <id>` use the native API.

## Management

When `[console]` is enabled, its listener exposes the console API plus
`/healthz`, `/readyz` and `/metrics`. Metrics describe the shared Runtime,
interfaces, workers, byte budgets and journal. Job state gauges count entries
currently retained in memory, not cumulative completions. With authentication
enabled, `/metrics` and console reads require a client token; health probes
remain accessible without one.

The core exposes the native `anyserve.v2` Client and Worker services only.
Applications connect their own workers with the SDK and keep backend protocols,
model loading and batching in worker code. Use matching binary, SDK and protocol
versions; see [deployment and recovery](docs/src/deployment.md).

## Repository and documentation

- `crates/anyserve-core`: execution state machine, journal, authentication and gRPC services.
- `crates/anyserve-proto`: the native Client and Worker protocol.
- `crates/anyserve-client`: native clients and bounded worker transport.
- `crates/anyserve-cli`: control-plane startup, job CLI and management API.
- `clients/python`: Python bindings and the application worker API.
- `console`: views of interfaces, jobs, workers and events, and a Setup page that generates `server.toml`.
- `bench/load`: native execution workloads and separate CPU profiling.
- `deploy/helm/anyserve`: Helm chart for one control plane, its journal volume and an optional worker fleet.

Start with the [reading guide](docs/src/index.md), [architecture](docs/src/architecture.md),
[configuration](docs/src/configuration.md) and [testing](docs/src/testing.md).
