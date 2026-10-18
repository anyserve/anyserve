# Reference Architectures

This note records what the Anyserve redesign borrows from four mature systems
and what it deliberately leaves out. It is a research input to
[the execution architecture](redesign.md), not a specification.

Every source link is pinned to the commit that was read on 2026-10-18, so line
anchors stay valid:

| Repository | Commit |
|---|---|
| `ray-project/ray` | `6f01d617120885918b783086eedf712ebdb5f441` |
| `triton-inference-server/common` | `82f8bc7efd8f7d175122eb225ee0fb5c9eb3581d` |
| `triton-inference-server/core` | `00e4e60351ae088ac971c7fac10e55666005f760` |
| `triton-inference-server/server` | `57b3eba6b5ac9b493747bec0eb8c69f0a0cee7a4` |
| `kserve/kserve` | `5d738906d22e003166d09ae28733366420e30d8d` |
| `kserve/open-inference-protocol` | `d49cc23f89d709d87b210ef9449e273ae243984e` |
| `knative/serving` | `b6c3209a138642d6e8954b208db793835ffe49fb` |
| `kubernetes-sigs/gateway-api-inference-extension` | `db75ec81b80f8728835b56cdb445e8ff1fbfd430` |
| `temporalio/temporal` | `d9341690d2b10c191c88e1634b07e8cb87bff36b` |
| `temporalio/api` | `1a8f67faead00aceb8ad90a3dd16b04e73415648` |

## Ray Serve

Ray Serve routes requests from a handle or proxy directly to replica actors. A
controller reconciles deployments off the request path.

Sources:

- [Serve architecture](https://docs.ray.io/en/latest/serve/architecture.html)
- [Router-side queue bound and `BackPressureError`](https://github.com/ray-project/ray/blob/6f01d617120885918b783086eedf712ebdb5f441/python/ray/serve/_private/router.py#L226-L256)
- [Replica admission: `_is_replica_quiescing`, `_can_accept_request`](https://github.com/ray-project/ray/blob/6f01d617120885918b783086eedf712ebdb5f441/python/ray/serve/_private/replica.py#L1421-L1440)
  and [`handle_request_with_rejection`](https://github.com/ray-project/ray/blob/6f01d617120885918b783086eedf712ebdb5f441/python/ray/serve/_private/replica.py#L1759-L1795)
- [`RequestRouter`: queue-length cache, probe deadlines, retry backoff](https://github.com/ray-project/ray/blob/6f01d617120885918b783086eedf712ebdb5f441/python/ray/serve/_private/request_router/request_router.py#L464-L650)
- [`LocalityMixin`](https://github.com/ray-project/ray/blob/6f01d617120885918b783086eedf712ebdb5f441/python/ray/serve/_private/request_router/request_router.py#L59)
  and [`MultiplexMixin`](https://github.com/ray-project/ray/blob/6f01d617120885918b783086eedf712ebdb5f441/python/ray/serve/_private/request_router/request_router.py#L193)
- [`PowerOfTwoChoicesRequestRouter`](https://github.com/ray-project/ray/blob/6f01d617120885918b783086eedf712ebdb5f441/python/ray/serve/_private/request_router/pow_2_router.py#L27-L60)
- [`LongPollClient` / `LongPollHost`](https://github.com/ray-project/ray/blob/6f01d617120885918b783086eedf712ebdb5f441/python/ray/serve/_private/long_poll.py#L111-L318)

Adopt:

- **Two-level admission.** The caller-side queue is bounded and fails fast with
  an explicit backpressure error instead of waiting without limit. The replica
  enforces its own `max_ongoing_requests` and rejects work when it is full or
  quiescing. Anyserve applies this as follows: each interface has a bounded
  admission queue that rejects with `RESOURCE_EXHAUSTED`/429. A worker may
  reject an assignment, and the runtime re-dispatches it without counting an
  attempt. A draining worker rejects all new work.
- **Load-aware choice without a global sort.** The router keeps cached
  per-replica queue lengths, probes them with a deadline, and picks with power
  of two choices, backing off between retries. Anyserve uses the load each
  worker reports to pick among eligible sessions in the same way.
- **Affinity as ranking, not filtering.** Locality and multiplexed-model
  affinity rank candidates but do not exclude them. This matches Anyserve's
  `preferred_attributes` plus spillover.
- **Membership pushed off the request path.** Routers learn replica sets
  through long-poll from the controller. Anyserve keeps worker registration and
  configuration out of the per-request path in the same way.

Do not adopt:

- **Push from router to replica over a cluster RPC fabric.** Anyserve workers
  sit behind NAT or in other clusters and must dial out. The direction is
  inverted: the worker opens a session, and the server pushes assignments over
  it.
- **The Ray runtime itself** (GCS, actors, object store) and **deployment,
  replica-lifecycle and autoscaling ownership.** Anyserve does not start or
  scale workers.
- **Transparent retries after a response has started.** Rejection-and-retry is
  safe only before execution begins. Once output has reached the caller,
  Anyserve fails instead of switching attempts.

## Triton Inference Server

Triton executes models in-process through backends. Per-model schedulers and
batchers sit in front of model instances.

Sources:

- [`ModelRateLimiter`](https://github.com/triton-inference-server/common/blob/82f8bc7efd8f7d175122eb225ee0fb5c9eb3581d/protobuf/model_config.proto#L86)
  and [Rate limiter guide](https://github.com/triton-inference-server/server/blob/57b3eba6b5ac9b493747bec0eb8c69f0a0cee7a4/docs/user_guide/rate_limiter.md)
- [`ModelQueuePolicy`: `timeout_action`, `default_timeout_microseconds`, `allow_timeout_override`, `max_queue_size`](https://github.com/triton-inference-server/common/blob/82f8bc7efd8f7d175122eb225ee0fb5c9eb3581d/protobuf/model_config.proto#L1055-L1113)
- [`ModelDynamicBatching` including `priority_levels`](https://github.com/triton-inference-server/common/blob/82f8bc7efd8f7d175122eb225ee0fb5c9eb3581d/protobuf/model_config.proto#L1122-L1164)
- [`TRITONSERVER_RESPONSE_COMPLETE_FINAL`](https://github.com/triton-inference-server/core/blob/00e4e60351ae088ac971c7fac10e55666005f760/include/triton/core/tritonserver.h#L989)
  and [Decoupled models](https://github.com/triton-inference-server/server/blob/57b3eba6b5ac9b493747bec0eb8c69f0a0cee7a4/docs/user_guide/decoupled_models.md)
- [`TRITONSERVER_InferenceRequestCancel` / `IsCancelled`](https://github.com/triton-inference-server/core/blob/00e4e60351ae088ac971c7fac10e55666005f760/include/triton/core/tritonserver.h#L1183-L1197)
  and [Request cancellation](https://github.com/triton-inference-server/server/blob/57b3eba6b5ac9b493747bec0eb8c69f0a0cee7a4/docs/user_guide/request_cancellation.md)

Adopt:

- **Per-endpoint queue policy.** This covers a maximum queue size, a default
  queue timeout with a reject action, a bounded per-request timeout override,
  and a fixed number of priority levels. Anyserve maps it one-to-one onto a
  per-interface queue policy.
- **Named, counted resources held for the duration of execution.** Triton's
  rate limiter is the same model as Anyserve's `required_capacity`.
  Reservations are taken at assignment and released exactly once when the
  attempt ends.
- **Explicit end-of-response marker.** A decoupled model sends zero or more
  responses and signals completion only with the `FINAL` flag. Anyserve
  streams carry zero or more data frames and exactly one terminal frame
  (`end`, `error` or `cancelled`). A closed connection never counts as
  completion.
- **Cooperative cancellation.** Cancellation is a flag that the backend
  observes and acts on. Anyserve propagates cancel to the worker over its
  session. The attempt stays cancelling until the worker acknowledges or the
  lease ends.

Do not adopt:

- **Dynamic and sequence batching in the middleware.** Engines such as vLLM and
  SGLang batch continuously. Batching in Anyserve would add queueing delay and
  couple the runtime to tensor shapes.
- **The backend shared-library ABI, the model repository and the tensor-typed
  request model.** Anyserve payloads stay opaque bytes or object references.
- **Ensembles and pipelines inside the server.**

## KServe, with Knative and the Gateway API Inference Extension

KServe reconciles `InferenceService` resources into deployments. Request
traffic flows through a separate data plane. In serverless mode that is the
Knative queue-proxy, and newer LLM paths use an endpoint picker.

Sources:

- [`InferenceService` controller](https://github.com/kserve/kserve/blob/5d738906d22e003166d09ae28733366420e30d8d/pkg/controller/v1beta1/inferenceservice/controller.go)
  and [predictor component reconciler](https://github.com/kserve/kserve/blob/5d738906d22e003166d09ae28733366420e30d8d/pkg/controller/v1beta1/inferenceservice/components/predictor.go)
- [Open Inference Protocol REST spec: live, ready and model-ready health](https://github.com/kserve/open-inference-protocol/blob/d49cc23f89d709d87b210ef9449e273ae243984e/specification/protocol/inference_rest.md)
- [Knative queue-proxy `Breaker`: concurrency limit, bounded pending queue, `ErrRequestQueueFull`](https://github.com/knative/serving/blob/b6c3209a138642d6e8954b208db793835ffe49fb/pkg/queue/breaker.go#L28-L140)
- [Model server metrics contract: queued, running, KV-cache utilisation, LoRA](https://github.com/kubernetes-sigs/gateway-api-inference-extension/blob/db75ec81b80f8728835b56cdb445e8ff1fbfd430/docs/proposals/003-model-server-protocol/README.md)
  and [endpoint picker protocol](https://github.com/kubernetes-sigs/gateway-api-inference-extension/blob/db75ec81b80f8728835b56cdb445e8ff1fbfd430/docs/proposals/004-endpoint-picker-protocol/README.md)

Adopt:

- **Reconciliation never sits on the request path.** The KServe controller
  converges deployment state and is absent from inference traffic. Anyserve
  keeps registration, configuration and maintenance sweeps off the
  per-request path.
- **Layered health.**
  - Server live means the process is up.
  - Server ready means it can take traffic. For Anyserve this includes a
    healthy journal when durable work is enabled.
  - Model ready means a specific model is servable. For Anyserve this is
    interface ready: at least one eligible worker session for the interface.
- **Local admission at the executor.** The queue-proxy breaker allows a fixed
  concurrency plus a bounded pending queue and rejects beyond that. The
  Anyserve worker SDK grants credits equal to its free slots and keeps no
  unbounded local queue.
- **Engine-neutral load signals.** The inference extension standardises the
  metrics a model server exposes and maps vLLM, SGLang, TensorRT-LLM and
  Triton names onto them. Anyserve worker heartbeats carry the same fields as
  optional load data, so routing plugins can score workers without
  engine-specific code.

Do not adopt:

- **Kubernetes CRDs, Knative or Istio as requirements**, the scale-to-zero
  activator, storage initialisers, or `InferenceGraph`. Workers run anywhere
  and Anyserve does not own their deployment.
- **The OIP tensor schema as the core job model.** Anyserve can expose an OIP
  adapter at the edge, but jobs stay workload-neutral.

## Temporal

Temporal persists workflow state in a history service and dispatches tasks
through a matching service to workers that long-poll task queues. Only its
principles for durable submission, recovery and idempotency are relevant here.

Sources:

- [History service: state transition and transfer task written together](https://github.com/temporalio/temporal/blob/d9341690d2b10c191c88e1634b07e8cb87bff36b/docs/architecture/history-service.md)
  and [Matching service](https://github.com/temporalio/temporal/blob/d9341690d2b10c191c88e1634b07e8cb87bff36b/docs/architecture/matching-service.md)
- Sync match before spooling:
  [`syncMatchTaskId`](https://github.com/temporalio/temporal/blob/d9341690d2b10c191c88e1634b07e8cb87bff36b/service/matching/physical_task_queue_manager.go#L44),
  [`SpoolTask`](https://github.com/temporalio/temporal/blob/d9341690d2b10c191c88e1634b07e8cb87bff36b/service/matching/physical_task_queue_manager.go#L392),
  [`TrySyncMatch`](https://github.com/temporalio/temporal/blob/d9341690d2b10c191c88e1634b07e8cb87bff36b/service/matching/physical_task_queue_manager.go#L629)
- Shard ownership fencing through `RangeID`:
  [`GetRangeID`](https://github.com/temporalio/temporal/blob/d9341690d2b10c191c88e1634b07e8cb87bff36b/service/history/shard/context_impl.go#L219),
  [conditional write](https://github.com/temporalio/temporal/blob/d9341690d2b10c191c88e1634b07e8cb87bff36b/service/history/shard/context_impl.go#L585),
  [`renewRangeLocked`](https://github.com/temporalio/temporal/blob/d9341690d2b10c191c88e1634b07e8cb87bff36b/service/history/shard/context_impl.go#L1164)
- [`StartWorkflowExecutionRequest`: `request_id`, `workflow_id_conflict_policy`](https://github.com/temporalio/api/blob/1a8f67faead00aceb8ad90a3dd16b04e73415648/temporal/api/workflowservice/v1/request_response.proto#L148-L174)
  and [`WorkflowIdReusePolicy` / `WorkflowIdConflictPolicy`](https://github.com/temporalio/api/blob/1a8f67faead00aceb8ad90a3dd16b04e73415648/temporal/api/enums/v1/workflow.proto#L16-L46)
- [`PollActivityTaskQueueResponse`: `heartbeat_details`, `schedule_to_close`, `start_to_close` and `heartbeat` timeouts](https://github.com/temporalio/api/blob/1a8f67faead00aceb8ad90a3dd16b04e73415648/temporal/api/workflowservice/v1/request_response.proto#L558-L602)
- [`RecordActivityTaskHeartbeatResponse.cancel_requested`](https://github.com/temporalio/api/blob/1a8f67faead00aceb8ad90a3dd16b04e73415648/temporal/api/workflowservice/v1/request_response.proto#L644-L647)
  and [`last_heartbeat_details` on failure](https://github.com/temporalio/api/blob/1a8f67faead00aceb8ad90a3dd16b04e73415648/temporal/api/workflowservice/v1/request_response.proto#L735-L746)

Adopt:

- **A state change and the work it creates commit together.** For Anyserve:
  - a durable submit commits the job, its inputs and the `accepted` event in
    one journal transaction;
  - an attempt start commits before the assignment reaches a worker, so the
    attempt budget survives restarts.
- **Sync match first.** When a poller is waiting, Temporal hands the task over
  directly and spools it only otherwise. Anyserve hands ephemeral jobs straight
  to a waiting session without touching disk. Durable jobs are recorded first,
  then matched the same way.
- **Explicit ID conflict policy.** A caller-supplied ID plus a policy (`FAIL`
  or `USE_EXISTING`) makes submission idempotent and turns mismatches into
  errors instead of silent duplicates. Anyserve compares a canonical spec hash
  under the same policy, valid within the retention window.
- **A timeout taxonomy.** Anyserve uses the same split:
  - queue timeout for scheduled-to-start;
  - execution timeout for start-to-close;
  - total timeout for schedule-to-close;
  - heartbeat timeout for liveness.
- **Cancellation delivered through the heartbeat channel.** Workers learn of
  cancellation from heartbeat responses. In Anyserve, cancel rides the worker
  session; the session is the heartbeat.
- **Heartbeat details and application recovery.** Temporal supports resumable
  application progress. Anyserve keeps recovery at whole-attempt granularity. Application-owned
  idempotency is required to avoid repeating external side effects.
- **Ownership fencing.** Every shard write is conditional on the current
  `RangeID`. Anyserve persists a monotonically increasing generation at
  startup. Leases carry it, and stale generations are rejected. The single
  exception is an idempotent duplicate completion that matches the recorded
  winning lease and result digest.

Do not adopt:

- **Deterministic workflow replay and event-sourced history.** Anyserve stores
  a recovery snapshot per job, not an ever-growing event history.
- **The multi-service topology** (frontend, history, matching), history
  sharding, task-queue partition forwarding, and Cassandra or Elasticsearch
  dependencies. Anyserve runs a single control-plane process.

## Summary

| Concern | Borrowed from | Anyserve rule |
|---|---|---|
| Admission | Ray router, Triton queue policy, Knative breaker | Bounded per-interface queues with priority levels and queue timeouts; reject on overflow |
| Executor capacity | Ray replica rejection, Knative breaker, Triton rate limiter | Worker credits and counted resources; a worker may reject an assignment; reservations are released exactly once |
| Worker choice | Ray power of two choices, inference extension metrics | Filter on interface, required attributes and capacity, then rank by preference and reported load |
| Streams | Triton decoupled `FINAL` | Data frames plus exactly one terminal frame; never splice attempts after output is exposed |
| Cancellation | Triton cancel flag, Temporal heartbeat `cancel_requested` | Cancel is a signal over the worker session; the worker aborts its upstream call |
| Durable submit | Temporal transfer task, ID conflict policy | One transaction for job, inputs and `accepted`; caller ID plus conflict policy plus spec hash |
| Retries | Temporal timeouts and heartbeat details | Durable attempt start before dispatch; queue, execution, total and heartbeat timeouts; whole-attempt restart |
| Restart safety | Temporal `RangeID` | Persisted generation fences stale leases |
| Health | Open Inference Protocol | Process live, runtime ready, interface ready |
