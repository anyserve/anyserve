# Deployment and Recovery

An execution domain runs one active in-memory scheduler backed either by a
PostgreSQL database/schema (allowing warm standby control-plane replicas for
automatic failover) or by a local SQLite journal in a single process. Add
workers for execution capacity. Separate database schemas isolate domains.
Redis is not required.

## Zero-VPS Quick Tunnel

Run an authenticated local relay with no config file and expose `:3001` publicly via `wss://`:

```bash
anyserve serve --console-listen 127.0.0.1:3001 --auto-token
cloudflared tunnel --url http://127.0.0.1:3001
```

`--auto-token` generates `as_c_*` / `as_w_*` tokens (`0600` `<journal_dir>/tokens.env` across restarts, memory-only with `--ephemeral`) and prints a copy-paste startup card; point workers/clients at `wss://<random>.trycloudflare.com` (pass `--no-grpc` if only `:3001` is needed). For private native gRPC over Tailscale, bind `--grpc-host 0.0.0.0 --console-listen 0.0.0.0:3001 --auto-token` and connect to `http://<tailscale-ip>:50052`.

## Quick VPS Relay (`deploy/vps`)

`deploy/vps` provides a 1-minute single-domain Compose stack with Caddy automatic HTTPS: gRPC (`/anyserve.v2.*`, `/grpc.health.*`, `/grpc.reflection.*`) proxies to `h2c://anyserve:50052`, `/metrics` is blocked (`403`), and Console/WSS proxy to `:3001`.

For public multi-user deployments where anyone signs in with **GitHub Login** to mint and revoke isolated `client`/`worker` tokens (`gh-<id>` namespace) and inspect their own jobs in the Console (or drain workers in their namespace via `POST /api/workers/:session_id/drain`), enable the optional [anyserve-auth-github](../plugins/anyserve-auth-github/README.md) Compose profile (`ANYSERVE_CADDYFILE=./Caddyfile.auth-github ANYSERVE_CONSOLE_AUTH_PORTAL_URL=https://relay.example.com/tokens docker compose --profile auth-github up -d`). The public demo at [https://anyserve.arcosx.org](https://anyserve.arcosx.org) ([token portal](https://anyserve.arcosx.org/tokens)) runs this stack with automated `main`-branch rollouts via [deploy-vps.yml](../.github/workflows/deploy-vps.yml). See [deploy/vps/README.md](../deploy/vps/README.md).

> **SQLite vs. PostgreSQL on a single VPS:** AnyServe schedules entirely in memory and uses its journal only for durable recovery and archive reads. On a single VPS, local **SQLite WAL** (single-writer batch commits + `max_bytes` eviction + scheduled backups) delivers lower latency and much lower RAM overhead than co-locating PostgreSQL on the same box. Switch to an external **PostgreSQL** instance when you run **2+ control-plane replicas** for zero-downtime upgrades and automatic leader-standby failover.

## Fly.io (`deploy/fly`)

`deploy/fly` runs one persistent Fly Machine (`512mb`, NVMe `/data` volume) built from `Dockerfile` with native HTTP/2 gRPC on `https://<app>.fly.dev:50052` and WSS/Console on `https://<app>.fly.dev`, configured purely via Fly secrets (`ANYSERVE_CLIENT_TOKENS`, `ANYSERVE_WORKER_TOKENS`, `ANYSERVE_PUBLIC_ENDPOINT`). See [deploy/fly/README.md](../deploy/fly/README.md).

## PostgreSQL high availability

Replace the SQLite `[journal]` section in the server file with:

```toml
[journal]
backend = "postgres"
url_env = "ANYSERVE_DATABASE_URL"
schema = "anyserve"
lease_duration_ms = 5000
poll_interval_ms = 100
max_connections = 8
```

Provide the PostgreSQL URL through `ANYSERVE_DATABASE_URL`, preferably from a
secret manager. All control-plane instances point to the same writable primary
database and schema. PostgreSQL's own HA replication, synchronous commit policy,
backups and connection pooling remain database responsibilities. Schema v3
(`scheduler_lease`, `jobs`, `objects`) is a clean break from older releases:
drain existing work and recreate the schema before upgrading.

Exactly one instance holds the single-row `scheduler_lease` (`generation`
fencing token + database `clock_timestamp()` expiry) and runs the scheduler.
Every journal write batch validates `holder_id`, `generation`, and `expires_at`
under `FOR SHARE` in a single implicit-transaction SQL round-trip, with bounded
session/statement/lock timeouts so a hung leader never blocks standby takeover.
Standby instances stay live (`/healthz` returns `204`, `/readyz` returns `503`,
gRPC returns `UNAVAILABLE`) and poll `scheduler_lease` every `poll_interval_ms`.

- **Graceful shutdown (`SIGTERM`/`SIGINT`)** flushes queued commits and expires
  `scheduler_lease` immediately so a standby takes over within roughly one poll
  interval.
- **Crash or network partition** recovers once `lease_duration_ms` elapses on
  the database clock. If a stalled leader wakes after losing its lease, its next
  write or renewal is fenced and the process exits so the supervisor restarts it
  cleanly as a standby.
- **Routing:** In Kubernetes, a standard Service routes exclusively to the
  `/readyz`-passing leader. Without a readiness-aware load balancer, pass a
  comma-separated endpoint list (`ANYSERVE_ENDPOINT="http://cp1:50052,http://cp2:50052"`);
  Rust and Python clients/workers probe sequentially on `UNAVAILABLE` and stick
  to the active leader.
- **Retries across failover:** An attempt already started when a leader crashes
  counts toward `attempt_count`; keep `max_attempts >= 2` (default `3`) and
  idempotent handlers so recovered durable jobs execute on the new leader.

For Kubernetes with PostgreSQL, create a Secret containing `ANYSERVE_DATABASE_URL`
and install the chart (which does not install PostgreSQL itself):

```bash
kubectl create secret generic anyserve-pg --from-env-file=/secure/anyserve-pg.env
helm install anyserve-pg deploy/helm/anyserve \
  --set journal.backend=postgres --set journal.postgres.existingSecret=anyserve-pg \
  --set replicaCount=2 --set auth.existingSecret=anyserve-tokens
```

The PG release creates no journal PVC. Keep an existing SQLite release and its
volume until its work has drained.

## Edge sites

At factory, warehouse, or robot edge sites, run a single `anyserve serve`
process with a local SQLite journal under `systemd` or K3s (`replicaCount=1`).
Local hardware failure is handled by supervisor restart (seconds) or by
configuring `pool_tiers` with `spillover_after_ms` to spill queued work to a
site-rack or cloud worker pool rather than operating multi-node PG HA on the edge.

## Containers

The official `Dockerfile` runs as non-root `anyserve` (`10001:10001`) and embeds the React Console UI out of the box (`--features embed-console`), so a single container serves gRPC, HTTP/WS, and the Web Console. Release images are published to `ghcr.io/anyserve/anyserve:vVERSION` (Linux amd64). If mounting an existing root-owned `/data` host directory or volume, fix ownership once with `chown -R 10001:10001 /data`.

To build and run a local demo with the embedded Console on `http://127.0.0.1:3001`:

```bash
docker build -t anyserve:local .
docker network create anyserve
docker volume create anyserve-data
docker run -d --name anyserve --network anyserve --restart unless-stopped \
  -p 127.0.0.1:50052:50052 -p 127.0.0.1:3001:3001 \
  -v anyserve-data:/data anyserve:local \
  serve --grpc-host 0.0.0.0 --console-listen 0.0.0.0:3001 --journal /data/journal.sqlite
```

Run the example echo worker on the same network:

```bash
docker build -f examples/python/Dockerfile -t anyserve-worker:local .
docker run -d --name anyserve-worker --network anyserve --restart unless-stopped \
  -e ANYSERVE_ENDPOINT=http://anyserve:50052 anyserve-worker:local
```

For an authenticated container without writing a TOML file, pass token
environment variables (or `--auto-token`) alongside `--console-listen 0.0.0.0:3001`:

```bash
docker run -d --name anyserve --restart unless-stopped \
  -p 127.0.0.1:50052:50052 -p 127.0.0.1:3001:3001 \
  -v anyserve-data:/data \
  -e ANYSERVE_CLIENT_TOKENS="<client-token>" \
  -e ANYSERVE_WORKER_TOKENS="<worker-token>" \
  -e ANYSERVE_PUBLIC_ENDPOINT="https://relay.example.com" \
  anyserve:local \
  serve --grpc-host 0.0.0.0 --console-listen 0.0.0.0:3001 --journal /data/journal.sqlite
```

Both builds accept `--build-arg DEBIAN_MIRROR=https://<mirror-host>` when a
different Debian package mirror is needed. Stop the demo with
`docker stop anyserve-worker anyserve`; the named volume retains its journal.

For custom runtime limits, scoped tenant tokens, or gRPC TLS, mount a
[server file](configuration.md) (or use the single-domain Caddy setup in
[deploy/vps/README.md](../deploy/vps/README.md)). Persist the journal directory,
including SQLite sidecar files. Use matching server, SDK and protocol releases.

## Kubernetes

The chart deploys a StatefulSet, a Service and an optional application worker
Deployment. SQLite uses one replica and a journal PVC; PostgreSQL allows
`replicaCount > 1` without local journal volumes. Health probes use the management listener.

```bash
helm install anyserve deploy/helm/anyserve \
  --set auth.existingSecret=anyserve-tokens
```

The existing Secret is mounted via `envFrom` and can supply any supported auth
env vars (`ANYSERVE_CLIENT_TOKENS`, `ANYSERVE_WORKER_TOKENS`,
`ANYSERVE_SCOPED_TOKENS`, `ANYSERVE_TOKEN_SIGNING_KEY`,
`ANYSERVE_AUTH_REVOCATION_URL`), plus `ANYSERVE_WORKER_TOKEN` when the worker
fleet is enabled. Alternatively, supply `auth.clientTokens` and
`auth.workerTokens` as lists in a private values file; the chart generates the
Secret. The StatefulSet sets `podSecurityContext.fsGroup=10001` and runs the
container as non-root `10001:10001` (`anyserve`) so the SQLite PVC is writable
out of the box.

To validate the local images above in an isolated kind cluster:

```bash
kind create cluster --name anyserve --kubeconfig /tmp/anyserve-kind.kubeconfig
kind load docker-image --name anyserve anyserve:local anyserve-worker:local
helm install anyserve deploy/helm/anyserve \
  --kubeconfig /tmp/anyserve-kind.kubeconfig --namespace anyserve --create-namespace \
  --set image.repository=anyserve --set image.tag=local --set image.pullPolicy=Never \
  --set worker.enabled=true --set worker.image=anyserve-worker:local --wait
```

| Values | Default / purpose |
|---|---|
| `image.repository`, `image.tag`, `image.pullPolicy` | GHCR repository, chart `appVersion` tag, `IfNotPresent` |
| `auth.existingSecret`, `auth.clientTokens`, `auth.workerTokens` | Empty; all empty disables auth |
| `console.publicEndpoint`, `console.authPortalUrl` | Optional `[console].public_endpoint` / `auth_portal_url` |
| `journal.enabled`, `journal.size`, `journal.storageClass`, `journal.maxBytes` | `true`, `1Gi`, default storage class, optional SQLite byte cap |
| `journal.backend`, `replicaCount` | `sqlite`, `1`; `replicaCount > 1` runs leader + standby on `postgres` |
| `journal.postgres.existingSecret`, `journal.postgres.secretKey` | Existing database URL Secret, `ANYSERVE_DATABASE_URL` |
| `journal.postgres.schema`, `leaseDurationMs`, `pollIntervalMs`, `maxConnections` | `anyserve`, `5000`, `100`, `8` |
| `runtime`, `runtime.tenantQuotas` | Scalar `[runtime]` keys and nested `[runtime.tenant_quotas]` |
| `podSecurityContext`, `securityContext` | Non-root UID/GID `10001`, `fsGroup: 10001`, `drop: ["ALL"]` |
| `service.type`, `service.grpcPort`, `service.httpPort` | `ClusterIP`, `50052`, `3001` |
| `resources`, `nodeSelector`, `tolerations`, `affinity` | Control-plane pod scheduling and resources; unset |
| `worker.enabled`, `worker.replicas`, `worker.image` | `false`, `1`, required image when enabled |
| `worker.command`, `worker.args`, `worker.env` | Application startup and extra environment; empty lists |
| `worker.resources`, `worker.nodeSelector`, `worker.tolerations` | Worker resource and placement settings; unset |

The worker image contains your handler and the SDK. The chart injects
`ANYSERVE_ENDPOINT=http://anyserve:50052` (using the actual Service name/port)
and a worker-role `ANYSERVE_TOKEN`; Python workers can call `serve(handler)`.
Application clients can run in their own pods, using that Service endpoint and
a client token from the Secret.

## Browser Console

The official container image embeds the React Console UI out of the box
(`--features embed-console`) whenever the management listener is enabled
(`--console-listen` or `[console]`). Default `cargo build` binaries expose only
the management API; build with `--features embed-console` for a single binary,
or build and host the frontend separately:

```bash
npm --prefix console ci
npm --prefix console run build
```

Serve `console/dist` from a static web server with an `index.html` fallback for
`/queue`, `/workers`, `/history` and `/explore`. Reverse-proxy `/api` to the
management listener on port 3001; this keeps browser API requests on the same
origin. For single-binary deployments without a separate web server, build with
`cargo build --locked -p anyserve --features embed-console` (after building
`console/dist`) and start `anyserve serve --console-listen 127.0.0.1:3001`.
For a separate API origin, set `VITE_CONSOLE_API_BASE_URL` at build time
and include the frontend origin in `[console].allowed_origins`.

Sign in at the Console prompt with either an operator token from
`[auth].client_tokens` (global cluster view) or a scoped/signed tenant `client`
token (strictly restricted to that tenant's namespace, with operator-only fields
redacted; cross-namespace requests receive `403 Forbidden`). Enable compression
and immutable caching for hashed assets, including the Explore WASM and Worker.
See [Explore limits](configuration.md#console-explore); a partial metadata load
does not represent the whole task history.

## Workers across hosts and clusters

Workers need outbound network access to the control plane's gRPC listener and
their backend. Advertise location and model attributes, then express routing
preferences on calls:

```python
from anyserve import AnyserveClient, ExecutionOptions, serve, worker

@worker(interface="policy.evaluate.v1",
        attributes={"region": "near", "model": "v1"},
        capacity={"slot": 4}, max_active_leases=4)
def evaluate(observation: bytes) -> bytes:
    return observation  # Replace with the application's policy.

# Worker process: serve(evaluate), with worker endpoint/token environment.
# Client process: use the same endpoint and a client-role token.
client = AnyserveClient()
action = client.call("policy.evaluate.v1", b"observation", options=ExecutionOptions(
    required_attributes={"model": "v1"}, preferred_attributes={"region": "near"},
    required_capacity={"slot": 1}, spillover_after_ms=20,
    queue_timeout_ms=100, total_timeout_ms=1000, max_attempts=3,
))
```

Required constraints always apply. After the hold interval, workers missing
some preferences become eligible. Python workers also advertise `worker_id`;
preferring it can improve locality but does not preserve worker-local state.
Reconstruct sessions or model caches at the application layer after failover.

## Restart and backup

- **SQLite:** stop incoming work and send SIGTERM/SIGINT. Wait for process exit
  before copying the database with its SQLite sidecars or starting its replacement.
  Start one matching binary with the same journal, then check known job IDs,
  readiness and worker connectivity before restoring traffic.
- **PostgreSQL:** roll control-plane instances one at a time, checking `/readyz`
  on the active leader after each replacement. Graceful exit relinquishes the
  scheduler lease immediately; a crash requires `lease_duration_ms` before
  standby takeover. Workers reconnect automatically. Back up and restore
  PostgreSQL using the database deployment's own procedures.

Shutdown stops admission and sends Drain; it does not guarantee every handler
finishes before exit. Unfinished durable jobs can execute again within their
remaining attempt budget. Downtime consumes wall-clock deadlines, and takeover
still needs an eligible worker. Invoke, ephemeral jobs and worker-local state
do not recover through the journal.

| State | Survives with a persistent journal? |
|---|---|
| Committed durable input, attempt count, final result | Yes, until journal retention expires |
| Invoke and ephemeral Submit | No |
| Live progress and intermediate lifecycle events | No (held in memory; terminal outcome replays from journal) |
| Worker-local state | No |

## Operations

The optional management listener serves `/healthz` (liveness), `/readyz`
(Runtime admission/journal readiness), `/api/info` (public auth/portal
metadata), `/metrics`, and `/api/*` (`/api/overview`, `/api/jobs`,
`/api/workers`, `/api/analytics/jobs`). When auth is enabled:

- `/healthz`, `/readyz`, and `/api/info` are unauthenticated.
- `/metrics` strictly requires an operator token from `[auth].client_tokens` (`403 Forbidden` for tenant or worker tokens).
- `/api/*` accepts both operator tokens (global cluster view) and scoped/signed tenant `client` tokens (restricted to the token's namespace with operator-only fields redacted; worker tokens receive `403 Forbidden`).

Terminate management HTTP TLS at your ingress or proxy.

| Signal | Next check |
|---|---|
| `/readyz` returns 503 | Instance is on standby, shutting down, or journal writes are unavailable |
| Ready but calls queue | Matching interfaces, worker connectivity, free slots and capacity |
| SQLite journal lock failure | Previous process still owns that path |
| Durable admission fails | Journal capacity and write availability; recover a frozen instance by restart, then reconcile uncertain Submit with the same job ID/specification |
| GetJob works but ListJobs omits a job | In-memory retention expired; read the durable result by ID |
| PG leader exits after lease loss | Check database connectivity and restart policy; standby takes over once lease expires |
| Cursor expired | Event history was evicted or belongs to a prior generation; read current state by ID |

`/metrics` exposes Runtime gauges (retained jobs by state, interfaces, pools,
workers, byte budgets, journal) alongside cumulative lifecycle and auth
metrics: `anyserve_jobs_submitted_total`, `anyserve_jobs_completed_total`,
`anyserve_attempts_started_total`, `anyserve_auth_rejections_total`,
`anyserve_auth_revocation_sync_ok`, and
`anyserve_auth_revocation_last_sync_age_seconds`. Per-request latency
histograms and distributed trace export require application instrumentation.

## Cloudflare Deployment (`deploy/cloudflare`)

`deploy/cloudflare` ships two isolated Cloudflare deployment configurations sharing one `https://` / `wss://` protocol and React Console frontend (`env.ASSETS`, SPA fallback):

| Mode | Config file | Cloudflare plan | Worker name / DO class |
|---|---|---|---|
| **Workers Free (default)** | [wrangler.jsonc](../deploy/cloudflare/wrangler.jsonc) | **Workers Free + R2 Free** (`$0` within reference envelope; no Docker or Containers) | `anyserve-cloudflare-native` / `AnyserveNativeDO` (`native-v1`) |
| **Containers** | [wrangler.containers.jsonc](../deploy/cloudflare/wrangler.containers.jsonc) | **Workers Paid** (Cloudflare Containers + DO SQLite + R2) | `anyserve-cloudflare` / `AnyserveControlPlaneDO` (`v1`) |

Separate Worker names and Durable Object migration histories mean the two modes never share SQLite state. Moving an existing Container deployment onto the Native Durable Object (or vice versa) is an application-level export/import cutover (drain in-flight jobs and switch `ANYSERVE_ENDPOINT`), not an in-place DO class migration.

### Mode A: Cloudflare Workers Free (Native Durable Object + R2 — Default)

[wrangler.jsonc](../deploy/cloudflare/wrangler.jsonc) runs the full AnyServe v2 execution control plane directly inside a single Hibernatable WebSocket Durable Object (`AnyserveNativeDO`) backed by DO SQLite and R2, with **no `containers` stanza** and no Docker dependency. Unmodified Rust and Python SDKs connect over `wss://`.

The Free config's `main` is `src/native-worker.ts`, a dedicated entrypoint that exports only `AnyserveNativeDO` and a thin routing `fetch`, so the bundle excludes `@cloudflare/containers`, `JournalGateway` and the journal Durable Object. [wrangler.containers.jsonc](../deploy/cloudflare/wrangler.containers.jsonc) keeps `src/index.ts`, which re-uses the same routing and additionally exports `AnyserveControlPlaneDO`, `ContainerProxy` and `JournalGateway`.

#### Prerequisites

- A Cloudflare account on **Workers Free** (or Paid) with **R2 enabled** on the account before first deploy.
- Node.js 24+ and Wrangler authenticated (`npx wrangler login` or `CLOUDFLARE_API_TOKEN`).

#### 1. Create the R2 bucket and install dependencies

Create the `anyserve-journal` R2 bucket before first deploy (or let Wrangler provision it once R2 is enabled on the account):

```bash
npm --prefix console ci
npm --prefix deploy/cloudflare ci
npx --prefix deploy/cloudflare wrangler r2 bucket create anyserve-journal
```

#### 2. Configure tokens and public endpoint

Empty token sets leave authentication in anonymous mode; always configure client and worker bearer tokens before exposing a public endpoint:

```bash
npx --prefix deploy/cloudflare wrangler secret put ANYSERVE_CLIENT_TOKENS \
  --config deploy/cloudflare/wrangler.jsonc
npx --prefix deploy/cloudflare wrangler secret put ANYSERVE_WORKER_TOKENS \
  --config deploy/cloudflare/wrangler.jsonc
```

Optional environment variables / secrets:

- `ANYSERVE_SCOPED_TOKENS`: JSON array of `{ "token", "role", "namespace", "subject", "forced_pool" }` entries for multi-tenant isolation.
- `ANYSERVE_TOKEN_SIGNING_KEY`: comma-separated HMAC signing secret(s) (newest first for key rotation) verifying `asv1.` stateless tokens.
- `ANYSERVE_AUTH_REVOCATION_URL`: optional revocation snapshot URL polled alongside `ANYSERVE_TOKEN_SIGNING_KEY`.
- `ANYSERVE_PUBLIC_ENDPOINT`: explicit `wss://...` endpoint advertised in `/api/overview` (defaults to `wss://<request host>`).
- `ANYSERVE_CONSOLE_POLL_MS`: Console polling interval in milliseconds (`10000` default in Free mode; `0` disables automatic polling in favor of manual `Refresh`).

#### 3. Deploy with one command

`wrangler.jsonc` includes a `build.command` that builds `console/dist` (compressing the DuckDB-WASM bundle to `.wasm.bin` under the `25 MiB` Workers Static Assets limit) automatically before uploading:

```bash
npx --prefix deploy/cloudflare wrangler deploy \
  --config deploy/cloudflare/wrangler.jsonc
```

Connect unmodified Rust and Python workers and clients over `wss://`:

```bash
export ANYSERVE_ENDPOINT="wss://<your-worker>.workers.dev"
export ANYSERVE_TOKEN="<worker-or-client-token>"
```

#### Visible differences and `$0` reference operating envelope (Workers Free)

- **Terminal retention (`24 h`)**: completed durable jobs are retained for `24 h` by default (vs `7 days` in Container mode).
- **`Heartbeat.Load.custom` (`64` entries / `8 KiB`, memory-only)**: custom load metrics are bounded to `64` entries and `8 KiB`, held in memory only (never stored in WebSocket attachments or SQLite), and appear empty immediately after a hibernation wake until the worker's next heartbeat.
- **Heartbeat and polling intervals**: worker heartbeats default to `30 s` interval / `90 s` timeout (vs `5 s` / `15 s` on local/container Rust servers) with zero SQLite writes per heartbeat; `/api/overview` advertises `recommended_poll_ms: 10000` (`10 s`) so an open Console tab stays well inside Free request budgets.
- **`32 MiB` WebSocket frame ceiling**: incoming WebSocket frames larger than `32 MiB` (`33,554,432` bytes) encoded are closed at the platform boundary with WebSocket status `1009`; below `32 MiB`, per-message limits (`8 MiB` per inline object, `24 MiB` total inline inputs/params, `64 KiB` streaming chunks) return structured application errors.
- **`$0` reference operating envelope**: `<= 25` workers online `24/7` at Free defaults, `<= 3,000` reference jobs/day (`1` attempt, `<= 24` small inline inputs packed into one SQLite bundle row totalling `<= 16 KiB`, `<= 1` single-chunk output `<= 16 KiB`, `<= 4` events, `<= 1` watcher, `24 h` retention), and one foreground Console tab at `10 s` polling. Beyond the six-dimension Free quota guardrails (`90,000`/day soft cap on DO requests and SQLite `rowsWritten` reserving headroom for in-flight completions, `4.5M`/day `rowsRead`, `850 MiB` DB + reserved storage, and R2 Class A/B soft caps), new work sheds cleanly with `RESOURCE_EXHAUSTED` (`HTTP 429`).

---

### Mode B: Cloudflare Containers + DO SQLite + R2 (Workers Paid)

[wrangler.containers.jsonc](../deploy/cloudflare/wrangler.containers.jsonc) packages the Rust `anyserve serve` binary inside Cloudflare Containers backed by Durable Object SQLite and R2:

- **Static frontend (`env.ASSETS`)**: serves the React Console from
  `console/dist` with SPA fallback.
- **Control plane (`AnyserveControlPlaneDO`)**: runs `anyserve serve` inside a
  `lite` container (`256 MiB` memory) with `[server].grpc_enabled = false` on
  port `3001`, exposing `/api/*`, `/rpc/v2/*`, `/ws/v2/*`, `/healthz`,
  `/readyz`, and `/metrics`. Active WebSocket connections (`activeWsCount`)
  prevent premature container sleep while workers are connected; once all
  workers disconnect and traffic goes idle, the container sleeps automatically.
- **Durable journal (`JournalStorage` + `JournalGateway`)**: `AnyserveControlPlaneDO`
  intercepts outbound HTTP to `http://journal.internal` via `JournalGateway`
  and persists metadata and inline payloads (`<= 1 MiB`) in DO SQLite while
  spilling payloads `> 1 MiB` to R2 (`jobs/<job_id>/<role>/<idx>`) outside
  `transactionSync`.

#### Prerequisites (Containers mode)

- A Cloudflare account with **Workers Paid** and R2 enabled.
- Node.js 24+ and Docker running locally (Wrangler builds
  [the container Dockerfile](../deploy/cloudflare/Dockerfile) during deploy).
- Wrangler authenticated (`npx wrangler login` or `CLOUDFLARE_API_TOKEN`).

#### 1. Create the R2 bucket and install deploy dependencies

```bash
npm --prefix deploy/cloudflare ci
npx --prefix deploy/cloudflare wrangler r2 bucket create anyserve-journal
```

Verify the bucket name (`anyserve-journal` by default) in
[wrangler.containers.jsonc](../deploy/cloudflare/wrangler.containers.jsonc).

#### 2. Configure endpoints and secrets

The endpoint advertised to workers (`/api/overview` and the Console's
"Connect worker" dialog) is resolved in this order:

1. A non-empty `ANYSERVE_PUBLIC_ENDPOINT` Worker variable/secret (for example
   `wss://anyserve.example.com` for a custom domain). It is passed to the
   container as an environment variable, which **overrides** the TOML value.
2. Otherwise, the Worker passes `wss://<request host>` of the request that
   starts the container, so a fresh deployment advertises its own
   `*.workers.dev` host without extra configuration.
3. `public_endpoint` in [server.toml](../deploy/cloudflare/server.toml) (the
   placeholder `wss://anyserve.example.com`) only applies when the container
   runs without either of the above, for example outside the Worker.

Set client and worker bearer tokens before exposing the deployment:

```bash
npx --prefix deploy/cloudflare wrangler secret put ANYSERVE_CLIENT_TOKENS \
  --config deploy/cloudflare/wrangler.containers.jsonc
npx --prefix deploy/cloudflare wrangler secret put ANYSERVE_WORKER_TOKENS \
  --config deploy/cloudflare/wrangler.containers.jsonc
```

Optionally set `JOURNAL_INTERNAL_SECRET` to authenticate internal
`http://journal.internal` requests between the container and `JournalGateway`:

```bash
npx --prefix deploy/cloudflare wrangler secret put JOURNAL_INTERNAL_SECRET \
  --config deploy/cloudflare/wrangler.containers.jsonc
```

`AnyserveControlPlaneDO` forwards `ANYSERVE_CLIENT_TOKENS`,
`ANYSERVE_WORKER_TOKENS`, `ANYSERVE_SCOPED_TOKENS`, `ANYSERVE_TOKEN_SIGNING_KEY`,
`ANYSERVE_AUTH_REVOCATION_URL`, `ANYSERVE_PUBLIC_ENDPOINT`, and
`JOURNAL_INTERNAL_SECRET` (as `ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN`) into the
container environment at startup.

#### 3. Build Console assets (`.wasm.bin`)

Cloudflare Workers Static Assets enforces a `25 MiB` per-file limit, while
the uncompressed DuckDB-WASM exception-handling binary is `~34 MiB`. Building
`console` compresses `duckdb-eh-*.wasm` to `.wasm.bin` (`~7.7 MiB`) and removes
the raw `.wasm` file; the browser decompresses it on demand in Explore via
`DecompressionStream("gzip")`:

```bash
npm --prefix console ci
npm --prefix console run build
```

To host the DuckDB-WASM binary externally (for example on an R2 custom domain)
and bypass client-side decompression, set `VITE_DUCKDB_WASM_URL` when building:

```bash
VITE_DUCKDB_WASM_URL="https://assets.example.com/duckdb-eh.wasm" \
  npm --prefix console run build
```

Keep `duckdb-browser-eh.worker.js` served from the same Worker origin (default)
so browsers permit `new Worker(...)`.

#### 4. Deploy Containers mode to Cloudflare

```bash
npx --prefix deploy/cloudflare wrangler deploy \
  --config deploy/cloudflare/wrangler.containers.jsonc
```

After deployment, connect remote workers and clients using the `wss://`
endpoint and role-specific tokens:

```bash
export ANYSERVE_ENDPOINT="wss://anyserve.example.com"
export ANYSERVE_TOKEN="<worker-or-client-token>"
```

### Local Node 24 Cloudflare gateways

For local development and cross-language SDK E2E verification without Wrangler or Docker:

- [node-native-gateway.ts](../deploy/cloudflare/src/node-native-gateway.ts) runs `AnyserveNativeDO` / `NativeEngine` over Node 24 `node:sqlite`, an in-memory R2 bucket, and HTTP + RFC 6455 WebSockets (`ws://`).
- [node-dev-gateway.ts](../deploy/cloudflare/src/node-dev-gateway.ts) runs the Container mode Contract C `/v1/*` HTTP journal gateway.

```bash
npm --prefix deploy/cloudflare test
npm --prefix deploy/cloudflare run typecheck
node --experimental-strip-types deploy/cloudflare/src/node-native-gateway.ts
node --experimental-strip-types deploy/cloudflare/src/node-dev-gateway.ts
```
