# AnyServe

Call the GPU under your desk from anywhere. AnyServe is a backend-independent execution and scheduling control plane: clients and workers both dial **out** over HTTP/2 gRPC or WebSocket (`wss://`) with zero open inbound ports, while the control plane handles capability/capacity placement, streaming backpressure, cancellation, retries, and optional durable recovery—leaving all model and application logic inside workers.

```text
Clients ── gRPC / WSS ──▶ Control plane (single leader or Cloudflare DO) ◀── gRPC / WSS ── Outbound workers
                                │                                                                │
                  SQLite · PostgreSQL · DO SQLite + R2                                  local GPU / model / app
```

## Measured baselines

All figures below are reproducible from this repository on an idle host (16-vCPU Intel Xeon Platinum 8259CL @ 2.50 GHz, Linux 6.14, `rustc 1.99.1`):

| Metric | Measured result | Conditions & how to reproduce |
|---|---|---|
| **Localhost echo round-trip (`client → control plane → worker → client`, C=16)** | **1.59 ms** p50 (`1.587–1.590 ms`) · **3.07 ms** p99 (`3.057–3.075 ms`) · **9,638 req/s** (`9,628–9,662`) | Ephemeral `Invoke` (`S1-c16`, no fsync), 256 B payload, 8 gRPC channels, 4 workers (Tokio threads: control default 16, worker 2/proc, client 4), 20 s × 3 repeats (`./bench/load/run.sh --cases S1-c16,S3-c16 --repeat 3`; median & range) |
| **Durable echo round-trip (localhost, C=16, SQLite `FULL` sync)** | **25.64 ms** p50 (`25.422–25.949 ms`) · **50.11 ms** p99 (`49.893–50.394 ms`) · **566 req/s** (`558–573`) | Durable `Submit` (`S3-c16`, SQLite `PRAGMA synchronous = FULL` fsync per commit), same host & 3 repeats (`2.1%` max p50 drift) |
| **Worker-kill reschedule latency (`worker.terminate()`)** | **1.5 s** (`1,493–1,496 ms`, 3/3 runs) with `1 s` pool tier · **15.5 s** (`15,494 ms`) with default `15 s` grace | Real Headless Chromium 153 killing primary browser worker during prefill; immediate WebSocket Close (`1001 Away`) detected (`0` heartbeat timeouts), then rescheduled after `reconnect_grace_ms` |
| **Single-binary release footprint** | **25.3 MiB** binary · **14.1 MiB** idle RSS (SQLite) / **11.2 MiB** (`--ephemeral`) | `cargo build --release --locked -p anyserve`; with `--features embed-console` (React Console + DuckDB-WASM): **34.4 MiB** binary · **14.6 MiB** idle RSS |

## Connect any local HTTP model in 9 lines

AnyServe core does not host models or impose an LLM schema—workers wrap whatever local process you already run. Below is the core of [examples/python/local_model_worker.py](examples/python/local_model_worker.py) (stdlib HTTP + `anyserve-runtime`, verified end-to-end against a real `anyserve` server in `clients/python/tests/sdk_e2e.py`):

```python
import os, urllib.request
from anyserve import serve, worker

# Point MODEL_URL at any local HTTP server (e.g. FastAPI, vLLM, Triton HTTP, or Ollama).
MODEL_URL = os.environ.get("MODEL_URL", "http://127.0.0.1:8000/predict")

@worker(interface=os.environ.get("ANYSERVE_INTERFACE", "demo.local_model.v1"), max_active_leases=4)
def predict(payload: bytes) -> bytes:
    req = urllib.request.Request(MODEL_URL, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()

serve(predict)
```

Production connectors for vLLM, Ollama, Triton, YOLO, and VLA live outside core in [anyserve.universe](https://github.com/anyserve/anyserve.universe).

## How AnyServe compares

| Tool | Primary abstraction | Inbound ports on worker | Model hosting / sharding | Queueing, backpressure & recovery | When to pick it |
|---|---|---|---|---|---|
| **AnyServe** | Backend-agnostic job & stream control plane (`gRPC` / `wss://`) | None (workers dial out) | None — workers own backends | Capability/capacity routing, credit flow control, cancellation, bounded retries, optional SQLite/PG/DO durability | Exposing heterogeneous local/edge/cloud workers behind NAT with scheduling and backpressure |
| **Tailscale** | WireGuard L3 mesh VPN (`P2P` / DERP) | None | None | Network layer only (no job queue, placement, or durable replay) | Direct private network reachability to arbitrary TCP/HTTP services when callers can join the tailnet |
| **Ray Serve** | Distributed Python serving framework on a Ray cluster | Cluster network to Ray head/nodes | Python deployments, autoscaling & batching | Cluster-native replica routing and request batching | Managed multi-node Python clusters where nodes share a cluster network |
| **Modal** | Managed serverless cloud GPU platform | None (managed cloud) | Builds & scales containers on rented cloud GPUs | Managed serverless queue and autoscaling | Elastic rented cloud GPUs with zero self-hosted infrastructure |
| **Mesh LLM** | Distributed LLM inference mesh | P2P mesh | **Shards LLM weights across consumer machines and exposes an OpenAI-compatible API** (AnyServe does neither) | LLM-specific request handling across sharded peers | Splitting a single large LLM across multiple consumer machines behind an OpenAI API |

## Live Demo

Try the hosted instance without running your own server:

- **Web Console**: [https://anyserve.arcosx.org](https://anyserve.arcosx.org)
- **GitHub Login Token Portal**: [https://anyserve.arcosx.org/tokens](https://anyserve.arcosx.org/tokens)

Sign in with GitHub to mint scoped `client` and `worker` tokens in your own isolated `gh-<id>` namespace, then connect a local worker over native HTTP/2 gRPC (`https://anyserve.arcosx.org`) or WebSocket (`wss://anyserve.arcosx.org`):

```bash
pip install anyserve-runtime

# 1. Connect an outbound worker from your machine (no inbound ports needed)
ANYSERVE_ENDPOINT=https://anyserve.arcosx.org ANYSERVE_TOKEN=<worker-token> \
  python examples/python/worker.py
# Or forward a local HTTP model server:
# MODEL_URL=http://127.0.0.1:8000/predict ANYSERVE_ENDPOINT=... ANYSERVE_TOKEN=... \
#   python examples/python/local_model_worker.py

# 2. Invoke it from another terminal and inspect the live Execution Waterfall in Console
ANYSERVE_ENDPOINT=https://anyserve.arcosx.org ANYSERVE_TOKEN=<client-token> \
  anyserve invoke demo.echo.v1 --input "hello AnyServe"
```

> To try the embedded **Browser Worker Playground** (`/playground` — Dedicated Web Worker echo, live `worker.terminate()` failover, cross-tab WebRTC P2P vs. WS Relay comparison, and optional `browser.llm.v1` SmolLM2-135M verified on WASM CPU fallback in Headless Chromium: `4.5–6.1 s` load, `717 ms` TTFT, `73 words` in `6.7 s` ≈ `10.9 words/s`; WebGPU path untested in headless), build locally with `--features embed-console` (see [Quick start](#quick-start)) or run the latest container image.

## Capabilities

- **Execution**: `Invoke` (ephemeral, optional full-duplex streaming; drop cancels) and `Submit` (durable by default, crash recovery, `job_id` idempotency, bounded at-least-once retries; exposed streamed output never silently switches attempts).
- **Placement & backpressure**: Interface matching, `namespace` isolation, `pool` tiers with spillover and per-pool `reconnect_grace_ms`, `affinity_key` sticky routing, priority + `fairness_key` queueing, `concurrency_key` serialization, required/preferred attributes, resource capacity, worker credits, per-tenant/prefix quotas (Rust control plane), and bounded memory budgets.
- **Outbound SDKs & optional data-plane bypasses**: Python 3.10+ and Rust SDKs over gRPC (`http(s)://`) or WebSocket (`ws(s)://`)—workers dial out and need no inbound port. Ready-made inference connectors (vLLM, Ollama, Triton, YOLO, VLA) live in [anyserve.universe](https://github.com/anyserve/anyserve.universe). Optional data-plane bypasses include:
  - *Browser WebRTC P2P (`RTCDataChannel`)*: ephemeral `Invoke` only, STUN-only with automatic 3.5 s fallback to WS Relay; reduces control-plane relayed payload to `0 B` (**client-counted**), though on localhost/same-machine Chromium's userspace SCTP/DTLS (~4–5 MB/s) is slower than TCP WS Relay (~33–53 MB/s).
  - *Experimental Rust direct transfer (`anyserve+iroh://`)*: opt-in `--features direct` in `anyserve-client` (zero `iroh` dependencies in default builds) for static large objects only; see [Limitations](#limitations--when-not-to-use).
- **Operations**: Client/worker/scoped/signed bearer tokens, gRPC TLS, `/healthz` · `/readyz` · `/metrics`, CLI (`job`, `token`, `invoke`, `doctor`), and a React Console with W3C Trace Waterfall (breaking down AnyServe admit/queue/relay time vs. handler execution time) and DuckDB-WASM Explore (embedded by default in the official Docker image; optional in local builds via `--features embed-console`).

## Quick start

Requires Rust ([rust-toolchain.toml](rust-toolchain.toml)), `protoc`, and Python 3.10+. From the repository root:

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

- **Zero-TOML authenticated relay / Quick Tunnel**: start with `anyserve serve --console-listen 127.0.0.1:3001 --auto-token` (generates & persists `as_c_*`/`as_w_*` tokens and prints a copy-paste connection card). Add `cloudflared tunnel --url http://127.0.0.1:3001` for an instant public `wss://` relay with zero server or domain ([Zero-VPS Quick Tunnel](docs/deployment.md#zero-vps-quick-tunnel)).
- Pass `--ephemeral` to disable persistence, or `--journal PATH` for a custom SQLite path. Set `ANYSERVE_ENDPOINT` (default `http://127.0.0.1:50052`; supports `http(s)://`, `ws(s)://`, or comma-separated leader lists) and role-specific `ANYSERVE_TOKEN` for remote access.
- Rust equivalent: [examples/rust](examples/rust). The official Docker image embeds the React Console UI out of the box (`--features embed-console`); for local frontend dev, run `npm --prefix console ci && npm --prefix console run dev`.

## Deployment modes

| Mode | What you get | Entry point |
|---|---|---|
| Local / Zero-VPS Quick Tunnel | Local SQLite + `--auto-token`; optional `cloudflared tunnel --url http://127.0.0.1:3001` for instant public `wss://` | `anyserve serve --console-listen 127.0.0.1:3001 --auto-token`, [Quick Tunnel](docs/deployment.md#zero-vps-quick-tunnel) |
| VPS (Docker Compose + Caddy) | Single-domain auto-HTTPS serving native gRPC (`https://`), `wss://`, embedded Console, and optional [GitHub Login token portal](plugins/anyserve-auth-github/README.md) | [deploy/vps/README.md](deploy/vps/README.md) |
| Fly.io | Single persistent NVMe Machine with native HTTP/2 gRPC (`:50052`), `wss://`, and embedded Console | [deploy/fly/README.md](deploy/fly/README.md) |
| Cloudflare Workers Free | `$0` serverless Hibernatable DO + DO SQLite + R2 (`wss://` + Console, no Docker; **differences vs. Rust server**: WebSocket/HTTP only — no native HTTP/2 gRPC, no `tenant_quotas`) | [wrangler.jsonc](deploy/cloudflare/wrangler.jsonc), [docs/deployment.md](docs/deployment.md#cloudflare-deployment-deploycloudflare) |
| Cloudflare Containers | Rust binary in Cloudflare Containers + DO SQLite + R2 (`wss://`, Workers Paid) | [wrangler.containers.jsonc](deploy/cloudflare/wrangler.containers.jsonc) |
| Single node / Edge & HA cluster | Local SQLite or PostgreSQL single-leader HA with automatic standby failover | Docker (embedded Console), [Helm](deploy/helm/anyserve), [docs/deployment.md](docs/deployment.md) |

## Limitations / When not to use

- **Single-leader control plane**: High availability uses active-standby failover backed by PostgreSQL leader leases, not multi-master active-active writes. If you need multi-region active-active write sharding across millions of QPS, use a different architecture.
- **At-least-once delivery**: Retries happen automatically only *before* output is exposed to the caller; once streamed bytes reach the caller, AnyServe fails fast with `output_interrupted` rather than splicing two attempts. End-to-end idempotency relies on caller-provided `job_id` and idempotent worker handlers.
- **Not a model host or LLM gateway**: AnyServe does not download, quantize, or shard model weights across nodes and does not enforce OpenAI-specific schemas in core. Workers own all backend integration.
- **Cloudflare Workers Free differences**: The native TypeScript Durable Object port speaks WebSocket/HTTP only (no native HTTP/2 gRPC) and does not implement per-tenant or prefix quotas (`tenant_quotas`).
- **Experimental `anyserve+iroh://` direct data plane**: Opt-in via `--features direct` in the **Rust SDK only** (not in Python SDK v1 or default server builds), supports **static large objects only** (no live streaming URIs), and is **cross-WAN untested** (verified hermetically on loopback only).
- **Browser WebRTC P2P trade-off**: On localhost / same-machine tests in Headless Chromium 153, WebRTC DataChannel P2P (~4–5 MB/s) is slower than TCP WebSocket relay (~33–53 MB/s). Its value is `0 B` client-counted relayed payload and direct peer transfer, not higher localhost throughput.
- **Browser LLM verification boundary**: The Playground's optional `browser.llm.v1` (SmolLM2-135M) was verified on CPU **WASM** fallback in Headless Chromium (`4.5–6.1 s` load, `717 ms` TTFT, `~10.9 words/s`); the hardware **WebGPU** path has not been verified in headless tests.
- **Guest sandbox disabled by default**: The stateless guest sandbox in `plugins/anyserve-auth-github` is opt-in and **off by default** (`ANYSERVE_AUTH_GUEST_ENABLED=false`).

## Repository

- `crates/anyserve-core` — scheduler state machine, journal backends, auth, gRPC and HTTP/WS services
- `crates/anyserve-proto` — native Client/Worker protobuf protocol
- `crates/anyserve-client` — Rust clients and bounded worker transport
- `crates/anyserve-cli` — `anyserve` binary (`serve`, `job`, `token`, `invoke`, `doctor` CLI, management API)
- `clients/python` — Python SDK (`anyserve-runtime`)
- `console` — React Console and DuckDB-WASM Explore
- `deploy/vps`, `deploy/fly` — 1-minute Docker Compose + Caddy and Fly.io relay templates
- `plugins/anyserve-auth-github` — optional GitHub Login OAuth portal plugin (HMAC tokens + revocation)
- `deploy/helm/anyserve` — Kubernetes Helm chart (SQLite or PostgreSQL HA + optional workers)
- `deploy/cloudflare` — Cloudflare Workers Free native DO and Containers deployments
- `bench/load` — load benchmarks and console profiling

## Documentation

- [Configuration & CLI](docs/configuration.md) — server TOML, zero-TOML flags & env vars, auth/TLS, limits, Explore
- [Python SDK](docs/python-sdk.md) — sync/async clients, duplex streaming, `ExecutionOptions`, `@worker`
- [Deployment & Recovery](docs/deployment.md) — Quick Tunnel, VPS, Fly.io, Docker, PostgreSQL HA, Helm, Cloudflare
- [Contributing & Benchmarks](docs/contributing.md) — local checks, PostgreSQL tests, load benchmarks, releases
