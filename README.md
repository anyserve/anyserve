# AnyServe

Call the GPU under your desk from anywhere. AnyServe is a backend-independent execution and scheduling control plane: clients and workers both dial **out** over HTTP/2 gRPC or WebSocket (`wss://`) with zero open inbound ports, while the control plane handles capability/capacity placement, streaming backpressure, cancellation, retries, and optional durable recovery—leaving all model and application logic inside workers.

**[Documentation](https://anyserve.github.io/anyserve/)** · **[Live Demo](https://anyserve.arcosx.org)** · **[Releases](https://github.com/anyserve/anyserve/releases)**

```text
Clients ── gRPC / WSS ──▶ Control plane (single leader or Cloudflare DO) ◀── gRPC / WSS ── Outbound workers
                                │                                                                │
                  SQLite · PostgreSQL · DO SQLite + R2                                  local GPU / model / app
```

## Install

```bash
# macOS & Linux (x86_64 / arm64) — installs ~/.local/bin/anyserve with SHA-256 verification
curl -fsSL https://anyserve.github.io/anyserve/install.sh | sh

# Windows (PowerShell x86_64)
irm https://anyserve.github.io/anyserve/install.ps1 | iex

# Python SDK (3.10+)
pip install anyserve-runtime
```

## Quick start

```bash
# Terminal 1 — control plane + embedded Web Console (:3001) + auto-generated tokens
anyserve serve --console-listen 127.0.0.1:3001 --auto-token
```

```bash
# Terminal 2 — outbound Python echo worker (paste the worker token printed above)
ANYSERVE_TOKEN="<worker-token>" python examples/python/worker.py
```

```bash
# Terminal 3 — invoke a job (paste the client token printed above)
ANYSERVE_TOKEN="<client-token>" anyserve invoke demo.echo.v1 --input "hello AnyServe"
```

- Open the embedded Web Console at `http://127.0.0.1:3001` (including **Settings** at `/settings` to tune scheduling, retention, and pool tiers live).
- Expose `:3001` over `wss://` with zero server or domain using `cloudflared tunnel --url http://127.0.0.1:3001` ([Quick Tunnel](docs/deployment.md#zero-vps-quick-tunnel)).
- Pass `--ephemeral` to disable SQLite persistence, or `--config examples/server.toml` for a declarative server file. Building from source requires Rust ([rust-toolchain.toml](rust-toolchain.toml)) and `protoc`: `cargo build --locked -p anyserve`.

## Connect any local HTTP model in 9 lines

AnyServe core does not host models or impose an LLM schema—workers wrap whatever local process you already run ([examples/python/local_model_worker.py](examples/python/local_model_worker.py)):

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

Production connectors for vLLM, Ollama, Triton, YOLO, and VLA live in [anyserve.universe](https://github.com/anyserve/anyserve.universe).

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

# 2. Invoke it from another terminal and inspect the live Execution Waterfall in Console
ANYSERVE_ENDPOINT=https://anyserve.arcosx.org ANYSERVE_TOKEN=<client-token> \
  anyserve invoke demo.echo.v1 --input "hello AnyServe"
```

## Capabilities

- **Execution**: `Invoke` (ephemeral, optional full-duplex streaming; drop cancels) and `Submit` (durable by default, crash recovery, `job_id` idempotency, bounded at-least-once retries).
- **Placement & backpressure**: Interface matching, `namespace` isolation, `pool` tiers with spillover and per-pool `reconnect_grace_ms`, `affinity_key` sticky routing, priority + `fairness_key` queueing, `concurrency_key` serialization, required/preferred attributes, resource capacity, worker credits, per-tenant/prefix quotas (Rust control plane), and bounded memory budgets.
- **Outbound SDKs**: Python 3.10+ and Rust SDKs over gRPC (`http(s)://`) or WebSocket (`ws(s)://`)—workers dial out and need no inbound port. Optional data-plane bypasses include browser WebRTC P2P (`RTCDataChannel` in Playground) and experimental Rust `anyserve+iroh://` (`--features direct`).
- **Operations & Console**: Client/worker/scoped/signed bearer tokens, gRPC TLS, `/healthz` · `/readyz` · `/metrics`, CLI (`serve`, `invoke`, `job`, `token`, `doctor`), and an embedded React Console with W3C Trace Waterfall, DuckDB-WASM Explore, and live runtime **Settings** (`/settings`).

## Deployment modes

| Mode | What you get | Entry point |
|---|---|---|
| Local / Zero-VPS Quick Tunnel | Local SQLite + `--auto-token`; optional `cloudflared tunnel` for instant `wss://` | [Quick Tunnel](docs/deployment.md#zero-vps-quick-tunnel) |
| VPS (Docker Compose + Caddy) | Single-domain auto-HTTPS serving native gRPC, `wss://`, embedded Console, and optional [GitHub Login portal](plugins/anyserve-auth-github/README.md) | [deploy/vps/README.md](deploy/vps/README.md) |
| Fly.io | Persistent NVMe Machine with native HTTP/2 gRPC (`:50052`), `wss://`, and embedded Console | [deploy/fly/README.md](deploy/fly/README.md) |
| Cloudflare Workers Free | `$0` Hibernatable DO + DO SQLite + R2 (`wss://` + Console, no Docker; WebSocket/HTTP only, no `tenant_quotas`) | [wrangler.jsonc](deploy/cloudflare/wrangler.jsonc), [Cloudflare guide](docs/deployment.md#cloudflare-deployment-deploycloudflare) |
| Cloudflare Containers | Rust binary in Cloudflare Containers + DO SQLite + R2 (Workers Paid) | [wrangler.containers.jsonc](deploy/cloudflare/wrangler.containers.jsonc) |
| Docker & Kubernetes | Single container with embedded Console, or Helm chart (SQLite / PostgreSQL HA) | [Helm](deploy/helm/anyserve), [docs/deployment.md](docs/deployment.md) |

## Limitations

- **Single-leader control plane**: High availability uses active-standby failover backed by PostgreSQL leader leases, not multi-master active-active writes.
- **At-least-once delivery**: Retries run only *before* output is exposed; mid-stream drops fail fast with `output_interrupted`. Rely on caller-provided `job_id` and idempotent handlers.
- **Not a model host or LLM gateway**: Core never hosts/shards weights or enforces OpenAI schemas; workers own backend integration.
- **Cloudflare Workers Free differences**: WebSocket/HTTP only (no native HTTP/2 gRPC) and no `tenant_quotas`.
- **Optional direct transfers**: Rust `anyserve+iroh://` (`--features direct`) supports static large objects only and is not cross-WAN verified; browser WebRTC P2P is STUN-only with automatic WS Relay fallback.
- **Guest sandbox disabled by default**: Opt-in in `plugins/anyserve-auth-github` (`ANYSERVE_AUTH_GUEST_ENABLED=false`).

## Repository

- `crates/anyserve-core` — scheduler state machine, journal backends, auth, gRPC and HTTP/WS services
- `crates/anyserve-proto` — native Client/Worker protobuf protocol
- `crates/anyserve-client` — Rust clients and bounded worker transport
- `crates/anyserve-cli` — `anyserve` binary (`serve`, `job`, `token`, `invoke`, `doctor` CLI, management API)
- `clients/python` — Python SDK (`anyserve-runtime`)
- `console` — React Console, Settings, Playground, and DuckDB-WASM Explore
- `website` — Astro Starlight documentation site ([anyserve.github.io/anyserve](https://anyserve.github.io/anyserve/))
- `deploy/vps`, `deploy/fly`, `deploy/helm/anyserve`, `deploy/cloudflare` — deployment templates
- `plugins/anyserve-auth-github` — optional GitHub Login OAuth portal plugin

## Documentation

- **[Docs Website](https://anyserve.github.io/anyserve/)** — concise guides, CLI reference, and install instructions
- [Configuration & CLI](docs/configuration.md) — server TOML, zero-TOML flags & env vars, auth/TLS, limits
- [Python SDK](docs/python-sdk.md) — sync/async clients, duplex streaming, `ExecutionOptions`, `@worker`
- [Deployment & Recovery](docs/deployment.md) — Quick Tunnel, VPS, Fly.io, Docker, PostgreSQL HA, Helm, Cloudflare
- [Contributing](docs/contributing.md) — local checks, PostgreSQL tests, releases
