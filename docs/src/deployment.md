# Deployment and Recovery

This page applies to the current 0.5.0 execution architecture. Start with
[usage](usage.md) for a local walkthrough and [configuration](configuration.md)
for field defaults. The [architecture](architecture.md) explains job and commit
semantics.

## Supported topology

Run one control-plane process and connect as many worker processes as the
configured admission/session limits allow. Workers may run on the same host,
other hosts or separate clusters. They establish outbound gRPC connections to
the control plane and call their backends locally or over HTTP.

Use a persistent filesystem for the SQLite journal when durable jobs must
survive process replacement. The journal has an exclusive process lock. A
second active control plane cannot share it. Worker replicas scale backend
capacity; placing several control planes behind a load balancer does not provide
shared jobs, sessions or recovery. Any independent control plane owns a separate
journal and execution domain.

For Kubernetes, keep the control-plane replica count at one and ensure the old
process has exited before its replacement opens the journal. GPU scheduling and
worker autoscaling belong to the deployment platform. Cross-cluster locality and
spillover are described in [edge inference](edge.md).

## Listeners and credentials

| Surface | Default address | Enablement |
|---|---|---|
| Client/Worker gRPC and gRPC health | `127.0.0.1:50052` | Started by `anyserve serve` |
| OpenAI HTTP | `127.0.0.1:8080` | Configure `[openai]` |
| Generic HTTP invoke | `127.0.0.1:8090` | Configure `[invoke]` |
| Console API | `127.0.0.1:3001` | Configure `[console]` |

CLI flags override file settings. Configure client and worker bearer tokens
before exposing the service outside a trusted network. Client credentials cover
SDK/CLI, HTTP/OpenAI and console access; worker credentials cover worker
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
5. Check gateway readiness and matching worker availability before restoring
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
| Gateway `/readyz` fails | Check Runtime journal/shutdown status and a connected, non-draining worker matching the configured required demand |
| All matching workers busy | They can still satisfy readiness; inspect free credits, queue age and admission limits for available capacity |
| Journal lock failure on startup | Another process holds this path; verify the previous owner has exited |
| Durable admission is unavailable | Check journal write failure, capacity and commit status; definite rollback can recover after a writable probe |
| Commit outcome unknown | The job identity remains frozen and durable work stops until restart reconciles it |
| GetJob works but ListJobs omits an ID | ListJobs covers entries retained in memory; cold durable reads use the journal |
| CursorExpired | The requested event history was evicted or predates this generation; inspect current state/final result by ID |

Runtime readiness requires a healthy or disabled journal. HTTP/OpenAI gateway
readiness also checks the configured required worker demand; zero free credit
alone does not mean an interface is unready. Journal failure can make readiness
false while unrelated ephemeral work continues.

## Migration from 0.4

Use matching 0.5 binary, Rust/Python SDKs and protocol. Package version 0.5.0
uses the `anyserve.v2` wire namespace. The old control-plane protocol, generic
named streams and worker polling RPCs are removed.

Drain the old deployment, preserve its original data for rollback and start the
new release with a fresh journal. Replace `[server.storage]` and
`[server.frames]` with `[runtime]` and optional `[journal]`; obsolete keys are
rejected. There is no in-place upgrade of the 0.4 SQL schema. Rollback uses the
previous binary/SDK with its corresponding original data.

Hosted endpoint provisioning, user workspaces, managed pools and their dashboard
flows remain in the [hosted gateway product proposal](create-gateway.md).
