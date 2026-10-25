# Deployment and Recovery

Start with [usage](usage.md) for a local walkthrough and [configuration](configuration.md)
for field defaults. The [architecture](architecture.md) explains job and commit
semantics.

## Supported topology

Run one control-plane process and connect as many worker processes as the
configured admission/session limits allow. Workers may run on the same host,
other hosts or separate clusters. They establish outbound gRPC connections to
the control plane and own their backend calls.

A control plane and its journal form one execution domain: jobs, sessions and
recovery are scoped to it. Add workers to scale execution capacity. Scale out
by running more domains, one per site, cluster or tenant, each with its own
journal and endpoint; a client talks to one domain and a worker connects to
one. No shared database is involved. An active control plane keeps its whole
state in memory under one lock and recovers durable jobs from the journal after
a short restart, so a shared store would add a round trip to every transition
without adding a guarantee the journal does not already give. Several control
planes behind one load balancer do not form a single domain.

Use a persistent filesystem for the SQLite journal when durable jobs must
survive process replacement. The journal has an exclusive process lock; the
replacement starts after the previous owner has exited. GPU scheduling and
worker autoscaling belong to the deployment platform. Cross-cluster locality and
spillover are described in [workers across clusters](edge.md).

## Kubernetes

The Helm chart in `deploy/helm/anyserve` deploys one execution domain:

- a one-replica StatefulSet running `anyserve serve`, with the journal on a
  PersistentVolumeClaim, HTTP liveness and readiness probes on the management
  listener, and a termination grace period for draining workers;
- a Service exposing gRPC and the management API;
- a Secret with client and worker tokens, read through `client_tokens_env` and
  `worker_tokens_env`, or an existing Secret of your own;
- an optional worker Deployment for an application image that receives
  `ANYSERVE_ENDPOINT` and `ANYSERVE_TOKEN`.

Application clients inside the cluster use the Service name as their endpoint
and a client token from the same Secret. Keep the StatefulSet at one replica; a
rolling update replaces the pod after the old one has exited, which is the
restart procedure below.

```bash
helm install anyserve deploy/helm/anyserve \
  --set auth.clientTokens[0]=CLIENT_TOKEN --set auth.workerTokens[0]=WORKER_TOKEN
```

Container images are published to `ghcr.io/anyserve/anyserve` by the release
workflow; the chart defaults to the tag matching its `appVersion`.

## Listeners and credentials

| Surface | Default address | Enablement |
|---|---|---|
| Client/Worker gRPC and gRPC health | `127.0.0.1:50052` | Started by `anyserve serve` |
| Management/console API, probes and metrics | `127.0.0.1:3001` | Configure `[console]` |

CLI flags override file settings. Configure client and worker bearer tokens
before exposing the service outside a trusted network. Client credentials cover
SDK/CLI and management access; worker credentials cover worker
connections. They are separate roles, not per-workspace tenant isolation.
`ANYSERVE_TOKEN` is the job CLI/native client default. Worker `token_env` selects
its own token variable. See [usage](usage.md) for the token flow.

gRPC supports configured TLS certificates. When HTTP listeners need HTTPS, use
the deployment's TLS termination. Workers need network access to the configured
control-plane listener and their backends. Outbound sessions remove the need
for an inbound worker endpoint; they do not bypass network policy.

## Persistence modes

```bash
anyserve serve                                      # .anyserve/journal.sqlite
anyserve serve --journal /persistent/journal.sqlite  # explicit persistent path
anyserve serve --ephemeral                          # memory-only execution
```

These are alternative startup commands. The default journal path is relative to
the server's working directory. The CLI enables it by default; an embedded Rust
Runtime enables it only when `RuntimeConfig.journal` is set.

| State | Memory-only | With journal |
|---|---|---|
| Invoke and ephemeral Submit | Lost on restart | Lost on restart |
| Durable Submit | Rejected | Complete input committed before acceptance |
| Unfinished durable execution | Unavailable | Recovered within its committed attempt budget |
| Final durable state/results | Unavailable | Readable by ID until journal retention expires |
| Live frames, assignment history and backend-local state | Not recovered | Not recovered |

FULL synchronization is the journal default. Disabling it changes the durability
guarantee under power loss. Database retention and in-memory retention are
independent; memory eviction can leave a durable result readable from SQLite.

## Restart and recovery

1. Stop new incoming work at the deployment boundary and send SIGTERM/SIGINT to
   the server. Graceful shutdown stops admission, drains worker sessions and
   waits for the journal. Ephemeral calls can end during shutdown.
2. Wait for that process to exit before replacing it. Keep the journal and its
   SQLite sidecar files on the persistent volume. For an offline backup, stop
   the server and copy the database and any remaining sidecars as one set;
   independently copying a live database and WAL is not a consistent snapshot.
3. Start one matching binary against that journal. Startup advances the
   generation and recovers unfinished durable jobs. Workers reconnect; old
   unfinished leases are fenced, and eligible work runs as a new attempt.
4. Look up known durable IDs with GetJob/Watch. A cold read reconstructs current
   accepted/final state; it does not restore earlier frames or assignments.
5. Check management readiness and matching worker availability before restoring
   ingress. A process answering gRPC health is not sufficient evidence that a
   particular inference interface can serve traffic.

The winning lease may retransmit an already committed result after a lost
acknowledgement, without executing the model again. Other recovered work can run
again, so applications must make external side effects idempotent. Worker-local
conversation state, model state and KV caches require application/backend
reconstruction.

## Failure signals

| Signal | Meaning and next check |
|---|---|
| Management `/readyz` fails | Check Runtime journal write availability and shutdown status |
| All matching workers busy | They can still satisfy readiness; inspect free credits, queue age and admission limits for available capacity |
| Journal lock failure on startup | Another process holds this path; verify the previous owner has exited |
| Durable admission is unavailable | Check journal write failure, capacity and commit status; definite rollback can recover after a writable probe |
| Commit outcome unknown | The job identity remains frozen and durable work stops until restart reconciles it |
| GetJob works but ListJobs omits an ID | ListJobs covers entries retained in memory; cold durable reads use the journal |
| CursorExpired | The requested event history was evicted or predates this generation; inspect current state/final result by ID |

Runtime readiness requires a healthy or disabled journal and no shutdown in
progress. It does not require available workers. Check the management worker
view for connectivity, advertised interfaces and capacity, and let application
workers enforce backend health. Journal failure can make readiness false while
unrelated ephemeral work continues.
