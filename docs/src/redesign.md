# 0.5 Redesign Acceptance (2026-10-18)

**Status: historical acceptance record.** These checks were recorded for the
initial 0.5 runtime redesign at `22f0853` on 2026-10-18. Their counts and pending
review status belong to that revision, not to the current checkout. This page
is not a second implementation specification.

Read [execution architecture](architecture.md) for the current behavior,
[deployment and recovery](deployment.md) for operating procedures, and
[configuration](configuration.md) for defaults. The
[2026-10-19 load results](benchmark-results.md) record the later optimization
rounds. [Testing](testing.md) gives the commands to validate another revision.

## Decisions retained by the redesign

| Area | Decision |
|---|---|
| Execution ownership | One in-memory Runtime owns admission, jobs, reservations, attempts and cancellation |
| Recovery | Optional SQLite journal; complete durable inputs before acceptance; execution is at least once |
| Worker connections | Outbound Session for control and Attempt for data, with independent bounds |
| Identity | Explicit submission IDs, canonical spec fingerprints and server-computed terminal receipts |
| Memory | Input, result and event budgets with separate lifecycles; shared buffers mean their sum is not an RSS limit |
| Product boundary | Models, batching, KV caches, GPU placement and cluster reconciliation belong to backends/deployment |
| Migration | The 0.4 compatibility service, generic named streams and storage/frame backend combinations were removed |

The [runtime reference review](reference-architectures.md) and
[routing reference review](inference-routing-research.md) preserve the original
research. Detailed current commit boundaries, retention and stream completion
are maintained in the architecture page so subsequent implementation changes
have one reference.

## Recorded verification

The following table preserves verification results reported for `22f0853`.
Pending review and unperformed checks retain their status from that date.

| Obligation | Verification command | Recorded result |
|---|---|---|
| Formatting | `cargo fmt --all -- --check` | PASS, exit 0 |
| Workspace lint | `cargo clippy --workspace --all-targets -- -D warnings` | PASS, exit 0; run with --offline |
| Runtime, recovery, adapters, auth and SDK | `cargo test --workspace` | PASS, exit 0: 49 CLI + 5 SDK + 13 core unit + 34 runtime integration tests; 101 total, 0 failures. Run with --offline |
| Rebuilt Python wheel | `maturin build --manifest-path clients/python/Cargo.toml` followed by wheel installation and both commands in testing.md | PASS, rebuilt and installed 0.5.0 wheel for CPython 3.12; 15 unit + 8 process tests |
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

The final gates, including CTO review fixes, were run on 2026-10-18. Run the
commands above from the repository root, using the same checkout to build the
binary and install the Python wheel.
The wheel built for that acceptance run passed cancellation, duplex input, role
errors, GIL-safe connection close, both workers surviving graceful restart, and
SIGKILL recovery.
`result()` reads the job once, then follows `watch(0)` from the earliest retained
event until Finished; expired nonzero cursors still report the gap.
GPU demos and release performance comparisons were not run.
