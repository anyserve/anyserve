# Anyserve

Serve Models Anywhere, Anytime, on Any Platform.

Anyserve routes inference requests to workers running on your own hosts or
clusters. One runtime handles opaque inputs, capability matching, resource
reservations, attempts, cancellation and output delivery. Workers own the models
and inference engines.

The control plane is **one process**. Workers establish outbound gRPC sessions;
they need no inbound endpoint. A short control-plane restart is supported.
Multiple control-plane replicas sharing a database are outside this architecture.

## Execution and persistence

- **Invoke** is ephemeral and supports live, duplex input/output. Disconnecting
  cancels the invocation. HTTP invoke and OpenAI gateways use this path.
- **Submit** defaults to durable. Complete input is committed to an optional
  SQLite journal before acceptance. Jobs and final results survive restart;
  unfinished work may run again within its attempt budget. External side effects
  require application idempotency.
- Live output events stay in bounded memory. A recovered durable job exposes its
  accepted/final state, rather than replaying its former live output history.
- Explicit submission IDs provide idempotency for the same canonical job spec.
  `max_attempts` includes the first attempt; zero selects the server default.
  After streamed output is exposed, retries cannot silently replace the attempt.

The journal uses SQLite WAL with FULL synchronization by default and a bounded
writer queue. PostgreSQL, Redis and a message broker are unnecessary for this
single-instance control plane. Inference batching, model memory management and
cluster orchestration belong to the worker engine or deployment platform.

```text
Python / Rust SDK / HTTP / OpenAI / Console
                     |
              Execution Runtime
       jobs · placement · attempts · deadlines
           /                         \
 optional SQLite journal      Worker.Session (control)
 durable input and result     Worker.Attempt (data)
                                      |
                               inference backend
```

## Run from this checkout

Build the binary with Rust and a `protoc` compiler on PATH:

```bash
cargo build -p anyserve
./target/debug/anyserve serve
```

The default listener is `127.0.0.1:50052`; the journal is
`.anyserve/journal.sqlite`. Use `serve --ephemeral` to disable durable jobs or
`serve --journal /path/to/journal.sqlite` to select a persistent volume. Preserve
that file and its SQLite sidecar files together. Configure client and worker
bearer tokens before exposing listeners outside a trusted network.

For the existing OpenAI-compatible worker and gateway:

```bash
./target/debug/anyserve serve --config examples/ollama/anyserve.toml
./target/debug/anyserve worker --config examples/ollama/worker.toml
```

Follow [the Ollama walkthrough](examples/ollama/README.md) for the backend and
request commands. [The Python examples](examples/python/README.md) use the same
execution protocol after `pip install ./clients/python`. The Rust example is in
[examples/rust](examples/rust).

Release installers remain available in [scripts](scripts). Released binaries and
SDKs must be used with their matching protocol version; the current source is
0.5 and does not retain a 0.4 compatibility service.

## Configuration and migration

The runtime always owns live state in memory. `[runtime]` controls admission,
timeouts and memory budgets; `[journal]` controls durable recovery. There are no
pluggable storage/frame backend combinations. Old `[server.storage]` and
`[server.frames]` configuration is rejected with a migration error.

The 0.4 protocol and databases are not upgraded in place. Deploy a matching 0.5
binary and SDK with a fresh SQLite journal after draining older jobs. Keep the
previous release and its original data for rollback. See
[deployment and migration](docs/src/deployment.md) and the
[configuration reference](docs/src/configuration.md).

For `anyserve job ls`, `job inspect` and `job cancel`, set `ANYSERVE_TOKEN` to
one client bearer token configured in `[auth]`. The CLI has no token flag; this
environment variable is also the native SDK default. Worker tokens use the
worker role and belong in the worker's `token` / `token_env` configuration.

## Repository

- `crates/anyserve-core`: memory runtime, optional journal and execution services.
- `crates/anyserve-proto`: the Client and Worker execution protocol.
- `crates/anyserve-client`: native client and bounded worker transport.
- `crates/anyserve-cli`: server, HTTP/OpenAI adapters, console API and HTTP worker.
- `clients/python`: Python bindings and function worker API.
- `console`: web UI reading the same Runtime as the gateways.
- `examples`: existing Rust/Python, Ollama and deployment walkthroughs.

## Documentation

Start with the [reading guide](docs/src/index.md). The
[execution architecture](docs/src/architecture.md) describes the implemented
runtime, and [deployment and recovery](docs/src/deployment.md) covers topology,
persistence, readiness and migration. Use [configuration](docs/src/configuration.md),
[edge placement](docs/src/edge.md) and [testing](docs/src/testing.md) for operation
and validation.

The [load benchmark results](bench/load/RESULTS.md) record the 2026-10-19 baseline
and two optimization rounds. The [0.5 acceptance record](docs/src/redesign.md)
and [reference reviews](docs/src/reference-architectures.md) are dated design
history. The [hosted gateway document](docs/src/create-gateway.md) is a product
proposal, including provisioning and workspace flows that are not implemented
in this checkout.
