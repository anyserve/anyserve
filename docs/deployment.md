# Deployment and Recovery

An execution domain is one control-plane process and its journal. Connect more
workers to add execution capacity. Run separate domains per site, cluster or
tenant when each needs its own jobs and endpoint. A client connects to one
domain; a worker connects to one domain.

The Runtime keeps scheduling state in memory and SQLite records durable
recovery state. It requires no Redis or PostgreSQL. Multiple active control
planes behind a load balancer do not share jobs, and cannot share a journal.

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

The chart deploys a one-replica StatefulSet, a journal PVC, a Service and optional
application worker Deployment. Health probes use the management listener. A
rolling update waits for the previous control plane to exit before replacement.

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
a client token from the Secret. Keep the StatefulSet at one replica.

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
| Live frames, prior event history, worker-local state | No |

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
