# Anyserve

Anyserve is a backend-independent execution and scheduling system. Clients send
opaque inputs through the native execution protocol. The control plane
matches jobs to outbound workers by capability and available capacity, and owns
admission, flow control, cancellation, retries and optional durable recovery.

Workers own their application code and backends. An ONNX engine, a remote
inference service or an ordinary function can implement a worker without adding
backend-specific semantics to the control plane.

## Core contract

- **Invoke** is ephemeral and supports complete or live duplex input/output.
  Dropping the invocation cancels unfinished work.
- **Submit** accepts complete input and defaults to durable execution. The
  optional SQLite or PostgreSQL backend commits input before acceptance and terminal results
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
Clients -- Client gRPC --> control-plane replica(s) <-- Worker gRPC -- workers
                                    |                                  |
                       SQLite (one process) or                 application backend
                       PostgreSQL (shared queue)
```

PostgreSQL supports multiple active control-plane replicas sharing a durable
queue. Each replica claims work for its connected workers; database-clock node
leases and per-job fencing epochs protect takeover and terminal commits.
Submit, GetJob, Watch and Cancel can reach any replica. SQLite remains the
single-process option. See [deployment](docs/deployment.md#postgresql-active-active)
for configuration, load balancing and failure recovery.

## Run from this checkout

Rust and `protoc` are required to build the binary. Run from the repository root:

```bash
cargo build --locked -p anyserve
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

The Python examples are an [echo worker](examples/python/worker.py) and a
[submitter](examples/python/submit.py); workers for Triton Inference Server,
vLLM, ComfyUI and live video
live in [anyserve.universe](https://github.com/anyserve/anyserve.universe).
The [Rust example](examples/rust) uses the same protocol. Applications supply
backend-specific worker code; the CLI starts the control plane and
inspects/cancels jobs. Python clients and workers use `ANYSERVE_ENDPOINT`,
defaulting to `http://127.0.0.1:50052`.

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

For the browser console, run `npm --prefix console ci` and
`npm --prefix console run dev`, then open the URL printed by Vite. The dev server
proxies `/api` to `127.0.0.1:3001`. The management listener serves the API;
browser assets are built and hosted separately. See
[Console deployment](docs/deployment.md#browser-console) for production hosting.
The console keeps its client token in the current browser tab's session storage.

Explore uses DuckDB-WASM to query a bounded, manually loaded metadata dataset.
It excludes request/result bodies and media. Use matching binary, SDK and protocol
versions; see [configuration](docs/configuration.md#console-explore) for query limits.

## Repository and documentation

- `crates/anyserve-core`: execution state machine, journal, authentication and gRPC services.
- `crates/anyserve-proto`: the native Client and Worker protocol.
- `crates/anyserve-client`: native clients and bounded worker transport.
- `crates/anyserve-cli`: control-plane startup, job CLI and management API.
- `clients/python`: Python bindings and the application worker API.
- `console`: views of interfaces, jobs, workers and retained execution events.
- `bench/load`: native execution workloads and separate CPU profiling.
- `deploy/helm/anyserve`: Helm chart for PostgreSQL replicas or one SQLite control plane, and an optional worker fleet.

References cover [configuration](docs/configuration.md), the
[Python SDK](docs/python-sdk.md), [deployment and recovery](docs/deployment.md)
and [development checks and benchmarks](docs/contributing.md).
