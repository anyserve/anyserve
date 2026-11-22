# Deployment and Recovery

An execution domain runs one active in-memory scheduler backed either by a
PostgreSQL database/schema (allowing warm standby control-plane replicas for
automatic failover) or by a local SQLite journal in a single process. Add
workers for execution capacity. Separate database schemas isolate domains.
Redis is not required.

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

Release images are `ghcr.io/anyserve/anyserve:vVERSION` (Linux amd64).
To build and run this checkout:

```bash
docker build -t anyserve:local .
docker network create anyserve
docker volume create anyserve-data
docker run -d --name anyserve --network anyserve --restart unless-stopped \
  -p 127.0.0.1:50052:50052 -v anyserve-data:/data anyserve:local \
  serve --grpc-host 0.0.0.0 --journal /data/journal.sqlite
```

Run the example echo worker on the same network:

```bash
docker build -f examples/python/Dockerfile -t anyserve-worker:local .
docker run -d --name anyserve-worker --network anyserve --restart unless-stopped \
  -e ANYSERVE_ENDPOINT=http://anyserve:50052 anyserve-worker:local
```

Both builds accept `--build-arg DEBIAN_MIRROR=https://<mirror-host>` when a
different Debian package mirror is needed. Stop the demo with
`docker stop anyserve-worker anyserve`; the named volume retains its journal.

For remote access, mount a [server file](configuration.md) with separate client
and worker credentials and TLS settings. Persist the journal directory,
including SQLite sidecar files. Use matching server, SDK and protocol releases.

## Kubernetes

The chart deploys a StatefulSet, a Service and an optional application worker
Deployment. SQLite uses one replica and a journal PVC; PostgreSQL allows
`replicaCount > 1` without local journal volumes. Health probes use the management listener.

```bash
helm install anyserve deploy/helm/anyserve \
  --set auth.existingSecret=anyserve-tokens
```

The existing Secret must contain `ANYSERVE_CLIENT_TOKENS` and
`ANYSERVE_WORKER_TOKENS` (comma-separated token lists), plus
`ANYSERVE_WORKER_TOKEN` (one worker token when the worker fleet is enabled).
Alternatively, supply `auth.clientTokens` and `auth.workerTokens` as lists in a
private values file; the chart generates the Secret.

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
| `auth.existingSecret`, `auth.clientTokens`, `auth.workerTokens` | Empty; both token lists empty disables auth |
| `journal.enabled`, `journal.size`, `journal.storageClass` | `true`, `1Gi`, cluster default storage class |
| `journal.backend`, `replicaCount` | `sqlite`, `1`; `replicaCount > 1` runs leader + standby on `postgres` |
| `journal.postgres.existingSecret`, `journal.postgres.secretKey` | Existing database URL Secret, `ANYSERVE_DATABASE_URL` |
| `journal.postgres.schema`, `leaseDurationMs`, `pollIntervalMs`, `maxConnections` | `anyserve`, `5000`, `100`, `8` |
| `runtime` | Scalar `[runtime]` overrides; empty map |
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

The binary and container expose the management API, not browser assets. Build
the frontend separately:

```bash
npm --prefix console ci
npm --prefix console run build
```

Serve `console/dist` from a static web server with an `index.html` fallback for
`/queue`, `/workers`, `/history` and `/explore`. Reverse-proxy `/api` to the
management listener on port 3001; this keeps browser API requests on the same
origin. For a separate API origin, set `VITE_CONSOLE_API_BASE_URL` at build time
and include the frontend origin in `[console].allowed_origins`.

Use an operator token from `[auth].client_tokens` at the Console sign-in prompt
(scoped tenant tokens receive `403 Forbidden`). Enable compression and
immutable caching for hashed assets, including the Explore WASM and Worker.
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
(Runtime admission/journal readiness), `/metrics`, and `/api/overview`,
`/api/jobs`, `/api/workers`. Probes are public; management reads and metrics
require an operator token from `[auth].client_tokens` when auth is enabled (`403 Forbidden`
for scoped tenant tokens or worker tokens). TLS for this HTTP listener belongs
at the deployment's ingress or proxy.

| Signal | Next check |
|---|---|
| `/readyz` returns 503 | Instance is on standby, shutting down, or journal writes are unavailable |
| Ready but calls queue | Matching interfaces, worker connectivity, free slots and capacity |
| SQLite journal lock failure | Previous process still owns that path |
| Durable admission fails | Journal capacity and write availability; recover a frozen instance by restart, then reconcile uncertain Submit with the same job ID/specification |
| GetJob works but ListJobs omits a job | In-memory retention expired; read the durable result by ID |
| PG leader exits after lease loss | Check database connectivity and restart policy; standby takes over once lease expires |
| Cursor expired | Event history was evicted or belongs to a prior generation; read current state by ID |

Metrics cover Runtime jobs, interfaces, workers, byte budgets and journal.
Job-state gauges count retained jobs, not cumulative completions. Per-request
latency histograms and distributed trace export require application instrumentation.

## Cloudflare Containers + DO SQLite + R2

`deploy/cloudflare` packages AnyServe for fully managed deployment on Cloudflare
Workers Static Assets, Cloudflare Containers, Durable Object SQLite, and
Cloudflare R2 with a single public `https://` / `wss://` endpoint:

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

### Prerequisites

- A Cloudflare account with **Workers Paid**. Only Cloudflare Containers
  requires it; Durable Object SQLite and R2 also have free tiers.
- Node.js 24+ and Docker running locally (Wrangler builds
  [the container Dockerfile](../deploy/cloudflare/Dockerfile) during deploy).
- Wrangler authenticated (`npx wrangler login` or `CLOUDFLARE_API_TOKEN`).

### 1. Create the R2 bucket and install deploy dependencies

```bash
npm --prefix deploy/cloudflare ci
npx --prefix deploy/cloudflare wrangler r2 bucket create anyserve-journal
```

Verify the bucket name (`anyserve-journal` by default) in
[wrangler.jsonc](../deploy/cloudflare/wrangler.jsonc).

### 2. Configure endpoints and secrets

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
  --config deploy/cloudflare/wrangler.jsonc
npx --prefix deploy/cloudflare wrangler secret put ANYSERVE_WORKER_TOKENS \
  --config deploy/cloudflare/wrangler.jsonc
```

Optionally set `JOURNAL_INTERNAL_SECRET` to authenticate internal
`http://journal.internal` requests between the container and `JournalGateway`:

```bash
npx --prefix deploy/cloudflare wrangler secret put JOURNAL_INTERNAL_SECRET \
  --config deploy/cloudflare/wrangler.jsonc
```

`AnyserveControlPlaneDO` forwards `ANYSERVE_CLIENT_TOKENS`,
`ANYSERVE_WORKER_TOKENS`, `ANYSERVE_PUBLIC_ENDPOINT`, and
`JOURNAL_INTERNAL_SECRET` (as `ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN`) into the
container environment at startup.

### 3. Build Console assets (`.wasm.bin`)

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

### 4. Deploy to Cloudflare

```bash
npx --prefix deploy/cloudflare wrangler deploy \
  --config deploy/cloudflare/wrangler.jsonc
```

After deployment, connect remote workers and clients using the `wss://`
endpoint and role-specific tokens:

```bash
export ANYSERVE_ENDPOINT="wss://anyserve.example.com"
export ANYSERVE_TOKEN="<worker-or-client-token>"
```

### Local Node 24 journal gateway testing

For local development or E2E verification without Wrangler/Docker,
[node-dev-gateway.ts](../deploy/cloudflare/src/node-dev-gateway.ts) runs the
Contract C `/v1/*` HTTP gateway backed by Node 24 `node:sqlite` and an
in-memory R2 store:

```bash
npm --prefix deploy/cloudflare test
npm --prefix deploy/cloudflare run typecheck
node --experimental-strip-types deploy/cloudflare/src/node-dev-gateway.ts
```
