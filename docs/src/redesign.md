# Anyserve execution architecture

This design is the implementation contract for the next Anyserve runtime. It
targets a single control-plane process with workers connecting outbound from
different hosts or clusters. Both live inference and recoverable asynchronous
execution are first-class workloads.

## Product boundary

Anyserve owns authenticated ingress, admission, capability matching, resource
reservations, execution attempts, cancellation, streaming, and recovery. Workers
own model loading and backend execution. Token batching, KV-cache management,
model-specific parsing, GPU placement, and Kubernetes reconciliation remain in
the inference engine or the user's deployment platform.

HTTP/OpenAI adapters and native/Python clients use the same execution protocol.
The runtime must not contain model-specific assumptions. A worker advertises
interfaces, attributes and resource capacity and receives bounded leases over
an outbound connection.

## Separation of responsibilities

1. **Execution runtime:** one in-memory state machine and indexed scheduling
   state; it owns jobs, workers, reservations, attempts, deadlines, and live
   stream ownership. All scheduling and transition rules have one implementation.
2. **Journal:** optional SQLite persistence for durable jobs, isolated behind a
   bounded commit queue. It stores recovery snapshots and binary objects, rather
   than duplicating the execution state machine in SQL.
3. **Stream transport:** bounded in-process replay and live delivery, with
   explicit end/error markers, attempt identities, cursor-expiry errors and
   resource limits. No database reads or writes belong in the frame path.
4. **Adapters:** authentication, HTTP/OpenAI translation, gRPC, CLI and SDKs.
   Request parsing and backend-specific behavior remain outside the runtime.

No external database, message broker, Kubernetes installation or GPU is required
to start the control plane. Multi-worker placement does not imply multi-writer
control-plane state. PostgreSQL and Redis are not part of this runtime's
coordination model. The preceding release remains the rollback reference for
multi-instance deployments.

## Execution policies

Durability belongs to an individual job, independently of whether a caller waits
synchronously, streams the answer, or polls asynchronously.

- **Ephemeral:** online state and payloads live in bounded memory. A control-plane
  restart ends the attempt. OpenAI and synchronous invoke adapters select this
  policy by default.
- **Durable:** the accepted input, attempts and terminal result survive restart.
  Input must be complete before durable acceptance. Execution is at least once;
  external effects require application idempotency.

The 0.4 compatibility service and named streams are removed. Submit carries
complete input; Invoke owns optional live input chunks and response events.
`ExecutionPolicy.max_attempts` is flat: zero/absent selects the server default;
worker `ErrorInfo.retryable` and the runtime's backoff determine retries.

## Commit protocol and cancellation

A durable job has three persistence boundaries:

1. Acceptance atomically saves the job, complete input, idempotency fingerprint
   and Accepted event before acknowledging acceptance or making it schedulable.
2. Attempt start saves the attempt identity and increments its retry budget
   before the worker receives a lease. Renewals and heartbeats remain in memory.
3. Terminal commit atomically saves final state, result, attempt outcome, event
   and completion receipt before acknowledging completion and releasing the
   reservation. Completion, non-retryable failure, exhausted retries, expiry,
   cancellation and queue timeout share this path.

An admitted commit is owned by the runtime, independently of the requesting
RPC's lifetime. Dropping a caller must not strand a prepared operation. No
in-memory state lock is held while awaiting SQLite. A bounded writer groups
pending changes into short transactions using WAL and FULL synchronization.

Definite rollback releases the corresponding preparation. An indeterminate
commit quarantines its identity until restart reconciles persisted state. Persistence failure rejects new durable work; ordinary waiting for
commits must not stop ephemeral dispatch. A recovery restart may terminate all
live connections.

## Recovery and idempotency

Each control-plane start has a new generation. Old active leases cannot complete
new attempts. Unfinished durable jobs recover according to their committed
attempt budget; the initial implementation retries from a new attempt rather
than taking over an old live lease.

A repeated job identity with the same canonical specification returns the
existing job. A different specification conflicts. Generic business metadata
and input metadata participate in identity; transport tracing is separate.
Completion retries with the same lease and result return the already committed
outcome, including when an acknowledgement was lost before restart. A different
result or stale attempt is rejected.

Retries and expiry consume one shared budget. Once an attempt's answer has been
exposed to a live caller, that response never silently switches to a different
attempt. Internal progress from an asynchronous job does not prohibit a retry.
Output events carry attempt identities; abnormal termination is an error.

## Bounded resources

Admission bounds pending jobs and bytes, worker sessions, event logs and commit
backlog. Stream storage is bounded by both frame count and bytes. Slow or expired
replay cursors receive explicit errors, never silent data loss. Flow control and
cancellation propagate across the worker link to the backend request.

Scheduling consumes metadata rather than cloning input payloads. Binary input
and output use shared immutable buffers in memory and BLOB storage on disk;
JSON stores small structured metadata only. Protocol message budgets include
metadata and framing, and are enforced before acceptance. Object-store upload
and garbage collection are not prerequisites for bounded inline execution.

## Configuration and budget contract

The [configuration table](configuration.md) lists every current Runtime, Limits
and JournalConfig field, defaults and observable overflow behavior. Global
budgets are input, result and event: inputs live until terminal state, result
permits live until the last reference drops, and events live until log eviction.
Shared Bytes can be counted by more than one budget; their sum is not an RSS
limit. There is no separate retained-byte budget or 75% maintenance eviction.
Resource pressure causes demand-driven eviction and one reservation retry.
Under the final CTO D5 ruling, streamed frames still blocked by the global
event budget retry reservation every 250 ms and on consumer progress, bounded by
the existing consumer stall timeout. Expiry returns RESOURCE_EXHAUSTED from the
Attempt without directly failing the job; the built-in worker reports retryable
failure, and an exposed attempt ends as output_interrupted. Result uploads still
return RESOURCE_EXHAUSTED immediately when their one retry does not fit.
Lifecycle events retain the fixed-overhead exception.

The [architecture](architecture.md) specifies recovery, SDK delivery retry
limits and the deployment/migration boundary. The source-backed reviews of
NVIDIA Dynamo, SGLang, vLLM Semantic Router, LiteLLM, Ray Serve, Triton, KServe and
Temporal inform the design; those projects are not runtime dependencies.

## 验收记录

The following table records verification results for this implementation. A
command that has not completed is recorded as pending, never as a pass. There
are no feature-delivery checkboxes.

| Obligation | Verification command | Actual result |
|---|---|---|
| Formatting | `cargo fmt --all -- --check` | PASS, exit 0 |
| Workspace lint | `cargo clippy --workspace --all-targets -- -D warnings` | PASS, exit 0; run with --offline |
| Runtime, recovery, adapters, auth and SDK | `cargo test --workspace` | PASS, exit 0: 49 CLI + 5 SDK + 13 core unit + 34 runtime integration tests; 101 total, 0 failures. Run with --offline |
| Current Python wheel | `maturin build --manifest-path clients/python/Cargo.toml` followed by wheel installation and both commands in testing.md | PASS, rebuilt and installed 0.5.0 wheel for CPython 3.12; 15 unit + 8 process tests |
| Python unit tests | `python -m unittest discover -s clients/python/tests -p 'test_*.py' -v` | PASS, 15 tests, 0 failures |
| Python real-process recovery and duplex | `python clients/python/tests/sdk_e2e.py -v` | PASS, 8 tests, 0 failures; current binary and rebuilt wheel |
| Console types and production assets | `cd console && npm ci && npm run build` | PASS, both exit 0; TypeScript checks and Vite production build |
| Removed protocol/domain names | `bash scripts/check-legacy-names.sh` | PASS, 8 journal-internal column/SQL matches, 0 matches elsewhere; --untracked includes new source files |
| Global event pressure and lifecycle exception | `cargo test --workspace --offline` (named global event budget tests and event-budget unit tests) | PASS in workspace tests: global_event_budget_evicts_terminal_jobs_before_waiting and a_full_global_event_budget_is_never_forced. Covers one eviction pass, no forced payload, waiting at most consumer_stall_timeout, then RESOURCE_EXHAUSTED without directly failing the job. Lifecycle events retain their 96 B accounting exception. |
| Dependency cleanup | `cargo tree -e no-dev` and direct manifest inspection | PASS under the revised CTO criterion: 0 redis/postgres tree matches, no direct workspace async-trait dependency; existing axum/tonic transitive async-trait retained |
| Repository configurations, pre-output HTTP status, readiness, worker recovery | `cargo test --workspace --offline` | PASS: strict schemas for all examples/bench TOML, first-output status/JSON errors, readiness/metrics, resumed frame sequence, panic and reject-credit cleanup, non-buffered result retry, and idle-read timeout tests |
| Zero cursor after recovery; strict nonzero cursors | `cargo test --workspace --offline` and the rebuilt wheel's Python tests above | PASS: events/watch(0) begin at the earliest retained event; recovered watch(0) reaches Finished; expired nonzero cursors still return CursorExpired. Python result performs one get followed by watch(0), without polling; SIGKILL recovery passes. |
| Source and interface review | Review pinned primary sources in the two reference-review documents and the final execution.proto | Source-backed review and CTO review-1 findings implemented; final CTO recheck pending |
| GPU/model performance and release comparisons | No current-protocol release comparison run | Not performed; no throughput/latency improvement claim |

The final gates, including CTO review fixes, were run on 2026-10-18. Run the commands above from the repository root, using the same
checkout to build the binary and install the Python wheel.
The current wheel passes cancellation, duplex input, role errors, GIL-safe
connection close, both workers surviving graceful restart, and SIGKILL recovery.
`result()` reads the job once, then follows `watch(0)` from the earliest retained
event until Finished; expired nonzero cursors still report the gap.
GPU demos and release performance comparisons were not run.
