# Anyserve

Backend-independent execution and scheduling control plane. Clients send opaque payloads over gRPC or WebSocket; the control plane matches jobs to outbound workers by capability and capacity, handling admission, flow control, cancellation, retries, and optional durable recovery—while workers own all application and backend logic.

```text
Clients ── gRPC / WSS ──▶ Control plane (leader or Cloudflare DO) ◀── gRPC / WSS ── Outbound workers
                                │                                                         │
                  SQLite · PostgreSQL · DO SQLite + R2                           application backend
```

## Capabilities

- **Execution**: `Invoke` (ephemeral, optional full-duplex streaming; drop cancels) and `Submit` (durable by default, crash recovery, `job_id` idempotency, bounded at-least-once retries; exposed streamed output never silently switches attempts).
- **Placement & backpressure**: Interface matching, `namespace` isolation, `pool` tiers with spillover, `affinity_key` sticky routing, priority + `fairness_key` queueing, `concurrency_key` serialization, required/preferred attributes, resource capacity, worker credits, and bounded memory budgets.
- **Outbound SDKs**: Python 3.12+ and Rust SDKs over gRPC (`http(s)://`) or WebSocket (`ws(s)://`)—workers dial out and need no inbound port. Ready-made inference connectors (vLLM, Ollama, Triton, YOLO, VLA) live in [anyserve.universe](https://github.com/anyserve/anyserve.universe).
- **Operations**: Client/worker/scoped-tenant bearer tokens, gRPC TLS, `/healthz` · `/readyz` · `/metrics`, CLI (`job ls|inspect|cancel`), and a React Console with DuckDB-WASM Explore.

## Quick start

Requires Rust ([rust-toolchain.toml](rust-toolchain.toml)), `protoc`, and Python 3.12+. From the repository root:

```bash
# Terminal 1 — control plane (gRPC :50052, HTTP API :3001, SQLite .anyserve/journal.sqlite)
cargo build --locked -p anyserve
./target/debug/anyserve serve --config examples/server.toml
```

```bash
# Terminal 2 — outbound Python echo worker
python -m venv .venv && . .venv/bin/activate
pip install ./clients/python
python examples/python/worker.py
```

```bash
# Terminal 3 — submit a durable job (same venv)
python examples/python/submit.py
```

- Pass `--ephemeral` to disable persistence, or `--journal PATH` for a custom SQLite path.
- Set `ANYSERVE_ENDPOINT` (default `http://127.0.0.1:50052`; supports `http(s)://`, `ws(s)://`, or comma-separated leader lists) and role-specific `ANYSERVE_TOKEN` for remote access.
- Rust equivalent: [examples/rust](examples/rust). Browser Console (proxies `/api` to `:3001`): `npm --prefix console ci && npm --prefix console run dev`.

## Deployment modes

| Mode | Storage & HA | Entry point |
|---|---|---|
| Single node / Edge | Local SQLite journal (single process) | `anyserve serve`, Docker, [Helm](deploy/helm/anyserve) |
| HA cluster | Shared PostgreSQL journal + single-leader lease with automatic standby failover | `backend = "postgres"`, [Helm](deploy/helm/anyserve) |
| Cloudflare Workers Free | Native Hibernatable DO + DO SQLite + R2 (`wss://`, no Docker/Containers) | [wrangler.jsonc](deploy/cloudflare/wrangler.jsonc) |
| Cloudflare Containers | Rust binary in Cloudflare Containers + DO SQLite + R2 (`wss://`, Workers Paid) | [wrangler.containers.jsonc](deploy/cloudflare/wrangler.containers.jsonc) |

## Repository

- `crates/anyserve-core` — scheduler state machine, journal backends, auth, gRPC and HTTP/WS services
- `crates/anyserve-proto` — native Client/Worker protobuf protocol
- `crates/anyserve-client` — Rust clients and bounded worker transport
- `crates/anyserve-cli` — `anyserve` binary (`serve`, `job` CLI, management API)
- `clients/python` — Python SDK (`anyserve-runtime`)
- `console` — React Console and DuckDB-WASM Explore
- `deploy/helm/anyserve` — Kubernetes Helm chart (SQLite or PostgreSQL HA + optional workers)
- `deploy/cloudflare` — Cloudflare Workers Free native DO and Containers deployments
- `bench/load` — load benchmarks and console profiling

## Documentation

- [Configuration & CLI](docs/configuration.md) — server TOML, auth/TLS, limits, Explore, CLI flags
- [Python SDK](docs/python-sdk.md) — sync/async clients, duplex streaming, `ExecutionOptions`, `@worker`
- [Deployment & Recovery](docs/deployment.md) — PostgreSQL HA, Docker, Helm, Console hosting, Cloudflare
- [Contributing & Benchmarks](docs/contributing.md) — local checks, PostgreSQL tests, load benchmarks, releases
