# Edge Inference

One central control plane, workers in many clusters. Workers dial out
(streaming sessions), so edge clusters need no inbound connectivity, no service mesh,
and no VPN to the center.

```text
caller ──HTTP──> center (invoke / OpenAI gateway, gRPC :50052)
                    ▲            ▲
          session │            │ session        (outbound only)
              edge-a worker   edge-b worker
                    │            │
              local vLLM /    local Triton /
              policy server   any HTTP service
```

## Connection model

Each worker holds a bidirectional Session for assignments, heartbeats and credits,
and opens an Attempt data stream for each execution. Control and payload use
separate connections. Small inputs travel in Assign; larger inputs arrive over
Attempt. This protocol makes no fixed one-RTT promise: opening the attempt and
acknowledging input/results add exchanges, and durable commits add storage work.

## Center

```toml
[auth]                       # bearer tokens; worker and client roles are separate
client_tokens_env = "ANYSERVE_CLIENT_TOKENS"
worker_tokens_env = "ANYSERVE_WORKER_TOKENS"

[invoke]                     # POST /v1/invoke/{interface}
listen = "0.0.0.0:8090"
default_timeout_ms = 5000
forward_headers = ["x-tenant-id"]

[invoke.policy]
spillover_after_ms = 20      # default locality hold
```

## Edge worker

```toml
kind = "http"                # or "llm" for OpenAI-compatible backends
base_url = "http://holdem-svc:8080"
max_active_leases = 32       # concurrent attempts on one Session
health_url = "http://holdem-svc:8080/healthz"   # unhealthy => stop leasing
token_env = "ANYSERVE_WORKER_TOKEN"

[routes]
"gameplay.holdem.act.v1" = "/strategy_act"

[attributes]
region = "ap-south-1"
accelerator = "l4"
```

`anyserve --endpoint https://center:50052 worker --config edge.toml`

For CLI job listing, inspection and cancellation, set `ANYSERVE_TOKEN` to a
**client** bearer token, then use `anyserve --endpoint https://center:50052 job
ls` (or `job inspect <id>` / `job cancel <id>`). This variable is distinct from
the server's token-list variables and from `ANYSERVE_WORKER_TOKEN` selected by
the worker config above. The job CLI has no token flag.

## Placement headers

| Header | Meaning |
|---|---|
| `x-anyserve-require: k=v,...` | hard constraint |
| `x-anyserve-prefer: k=v,...` | soft preference |
| `x-anyserve-spillover-ms` | hold for workers matching **all** preferences this long, then any eligible worker |
| `x-anyserve-queue-timeout-ms` | fail with 503 `queue_timeout` if nobody leases it in time |
| `x-anyserve-timeout-ms` | total wait; job is cancelled on timeout (504) |
| `x-anyserve-priority` | higher first |

Responses carry `x-anyserve-job-id`, `x-anyserve-worker-id`, `x-anyserve-attempts`.
Upstream status and body pass through unchanged. The worker marks transport and
configured upstream errors retryable. The runtime applies the shared attempt
budget (default 3); an exposed streamed response cannot switch attempts.

## Patterns

**Nearest cluster with failover** — `prefer: region=<caller region>`,
`spillover-ms` ≈ a few ms over the local p99 queue wait.

**RL rollouts** — actors call one interface; pin a policy version with
`require: model=<version>` while workers for several versions run side by side.
Adding capacity in any cluster is starting a worker; no routing change.

**Embodied / stateful sessions** — every worker advertises `worker_id`. After
the first step, send `prefer: worker_id=<x-anyserve-worker-id>` with a small
spillover: the episode stays on the worker holding its state (history, KV cache)
and may spill over if that worker is unavailable. Worker-local model state does
not migrate automatically; the application must reconstruct it when needed. Use
`queue-timeout-ms` ≤ the control period so stale actions are dropped, not
delivered late.
