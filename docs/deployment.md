# Deployment and Recovery

An execution domain is either a PostgreSQL database/schema shared by multiple
active control-plane replicas, or one process with a SQLite journal. Add workers
for execution capacity and PG control-plane replicas for concurrent admission
and scheduling. Separate database schemas isolate domains. Redis is not required.

## PostgreSQL active-active

Replace the SQLite `[journal]` section in the server file with:

```toml
[journal]
backend = "postgres"
url_env = "ANYSERVE_DATABASE_URL"
schema = "anyserve"
lease_duration_ms = 15000
poll_interval_ms = 100
max_connections = 16
```

Provide the PostgreSQL URL through that environment variable, preferably from
a secret manager. All replicas use the same database and schema. The role needs
schema/table creation privileges for bootstrap; DDL is serialized only during
startup. The execution path has no leader or process-wide exclusive database
lock. Use a writable primary endpoint. PostgreSQL's own replication, failover,
backups and connection capacity remain database deployment responsibilities.

Task admission uses a unique job ID and atomic fingerprint comparison. Schedulers
claim matching work with `FOR UPDATE SKIP LOCKED`; ownership changes increment a
job epoch. A database-clock node lease, owner ID and epoch guard every attempt
start, retry and terminal transaction. After a crash, another replica can claim
the unfinished work once the lease expires (15 seconds by default), subject to
worker availability and the remaining attempt budget. Graceful shutdown expires
membership immediately. A replica that loses membership fences its local work
and the CLI exits; Kubernetes or a Docker restart policy gives it a fresh identity.
Keep all replicas on the same version and runtime policies.

Durable Submit, GetJob, Watch, ListJobs, Cancel and Console job queries work across
replicas. Lifecycle and bounded progress events share database cursors; retries
and receipt replay survive ownership changes. Final outputs are stored once,
and Console reads only bounded prefixes. Use URI objects for large media; no
automatic object-storage upload or SQLite-to-PG data migration is performed.
Execution remains at least once: handlers need idempotency for external effects.
Fencing prevents an expired worker result from replacing the committed outcome;
it cannot undo effects already performed by that worker.

Worker Session and Attempt streams need the same backend. Use the matching SDK,
which shares one HTTP/2 connection in PG mode, and a TCP/L4 load balancer such as
a Kubernetes ClusterIP Service. An L7 proxy that independently balances RPCs
must provide equivalent affinity. Upgrade existing worker images with the server.
Ephemeral Invoke/Submit and worker sessions remain local to their serving replica;
use durable Submit for failover and shared scheduling. In PG mode, ephemeral jobs
use generated `ephemeral:` IDs; caller-chosen IDs are reserved for durable Submit
and may not use that prefix. This keeps local and shared job identities disjoint.
Long-lived worker connections can concentrate on one replica after rolling updates.
Durable Submit works even on a replica with no local worker; Invoke requires a
matching local worker. Rebalance worker connections when using Invoke across replicas.
Metrics and worker lists describe the serving replica; aggregate metrics across
pods. Memory and admission
limits bound each replica's loaded/in-flight work, not the entire PG backlog.

For Helm, create a Secret from a private file containing `ANYSERVE_DATABASE_URL`,
then install a separate PG release (the chart does not install PostgreSQL):

```bash
kubectl create secret generic anyserve-pg --from-env-file=/secure/anyserve-pg.env
helm install anyserve-pg deploy/helm/anyserve \
  --set journal.backend=postgres --set journal.postgres.existingSecret=anyserve-pg \
  --set replicaCount=3 --set auth.existingSecret=anyserve-tokens
```

The PG release creates no journal PVC. Keep an existing SQLite release and its
volume until its work has drained and any required data migration is complete.

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
| `journal.backend`, `replicaCount` | `sqlite`, `1`; multiple replicas require `postgres` |
| `journal.postgres.existingSecret`, `journal.postgres.secretKey` | Existing database URL Secret, `ANYSERVE_DATABASE_URL` |
| `journal.postgres.schema`, `leaseDurationMs`, `pollIntervalMs`, `maxConnections` | `anyserve`, `15000`, `100`, `16`; one extra heartbeat connection per replica |
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
a client token from the Secret. SQLite must remain at one replica.

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

1. Stop incoming work at the deployment boundary and send SIGTERM/SIGINT. The
   server stops admission, drains sessions and waits for journal operations.
2. Wait for exit before replacement. For an offline backup, copy the database
   and any SQLite sidecars together while the server is stopped.
3. Start one matching binary with the same journal. Unfinished durable jobs
   recover within their committed attempt budget. Downtime consumes wall-clock
   deadlines; recovery time depends on journal size and storage, with no fixed
   outage guarantee.
4. Read known job IDs and verify readiness and matching workers before restoring
   traffic. External side effects need idempotency because recovered work can
   execute again.

| State | Survives with a persistent journal? |
|---|---|
| Committed durable input, attempt count, final result | Yes, until journal retention expires |
| Invoke and ephemeral Submit | No |
| Retained durable progress and lifecycle events | PostgreSQL: yes; SQLite: no |
| Worker-local state | No |

## Operations

The optional management listener serves `/healthz` (liveness), `/readyz`
(Runtime admission/journal readiness), `/metrics`, and `/api/overview`,
`/api/jobs`, `/api/workers`. Probes are public; management reads and metrics
require a client token when auth is enabled. TLS for this HTTP listener belongs
at the deployment's ingress or proxy.

| Signal | Next check |
|---|---|
| `/readyz` returns 503 | Shutdown or journal write availability |
| Ready but calls queue | Matching interfaces, worker connectivity, free slots and capacity |
| Journal lock failure | Previous process still owns that path |
| Durable admission fails | Journal capacity, write errors or an uncertain commit; reconcile uncertain commits by restarting |
| GetJob works but ListJobs omits a job | In-memory retention expired; durable result remains in SQLite |
| Cursor expired | Event history was evicted or belongs to a prior generation; read current state by ID |

Metrics cover Runtime jobs, interfaces, workers, byte budgets and journal.
Job-state gauges count retained jobs, not cumulative completions. Per-request
latency histograms and distributed trace export require application instrumentation.
