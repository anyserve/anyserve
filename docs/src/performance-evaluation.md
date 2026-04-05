# Current Performance Evaluation

This document records the current performance profile of the runtime modes supported by the repository as of April 5, 2026.

## Scope

The measurements below cover the gRPC control plane only.

The following surfaces are not included in this evaluation:

- The OpenAI-compatible HTTP gateway
- The console API
- Cross-machine or cross-region network effects

The goal is to capture the relative cost of each runtime assembly under the current implementation.

## Test Setup

All scenarios were executed against the current local build of `target/debug/anyserve` on a single machine.

Benchmark driver:

- `crates/anyserve-bench`

Common benchmark parameters:

- Warmup: `1s`
- Measurement window: `5s`
- Concurrency: `2`
- Workers: `2`
- Payload size: `1024 bytes`
- Frames per stream: `16`

## Runtime Modes

The following runtime assemblies were measured:

### `memory + memory`

- State store: in-memory
- Frame plane: in-memory
- Intended use: development baseline

### `sqlite + memory`

- State store: SQLite
- Frame plane: in-memory
- Intended use: single-node persistence

### `postgres + memory`

- State store: PostgreSQL
- Frame plane: in-memory
- Intended use: single control-plane instance with durable metadata and in-memory hot stream handling

### `postgres + redis`

- State store: PostgreSQL
- Frame plane: Redis
- Intended use: multi-instance deployment

## Scenarios

### `lease-cycle`

End-to-end job lifecycle:

- submit job
- poll lease
- complete lease

### `watch-latency`

Event propagation latency:

- report job event
- observe the event via `watch_job`

### `stream-follow`

Streaming data path:

- open stream
- push frames
- consume via `pull_frames(follow = true)`

## Results

| Mode | Lease Cycle | Watch Latency | Stream Follow |
|---|---:|---:|---:|
| `memory + memory` | `960.0 ops/s`, `p50 1.94 ms` | `1941.4 ops/s`, `p50 1.00 ms` | `1843.0 ops/s`, `p50 1.08 ms` |
| `sqlite + memory` | `335.0 ops/s`, `p50 4.96 ms` | `2175.0 ops/s`, `p50 0.88 ms` | `1538.8 ops/s`, `p50 1.27 ms` |
| `postgres + memory` | `65.0 ops/s`, `p50 17.58 ms` | `717.6 ops/s`, `p50 2.68 ms` | `642.8 ops/s`, `p50 2.98 ms` |
| `postgres + redis` | `67.2 ops/s`, `p50 18.83 ms` | `640.0 ops/s`, `p50 3.04 ms` | `278.6 ops/s`, `p50 6.88 ms` |

## Relative to `memory + memory`

| Mode | Lease Cycle | Watch Latency | Stream Follow |
|---|---:|---:|---:|
| `sqlite + memory` | `34.9%` throughput, `2.6x` p50 | `112.0%` throughput, `0.9x` p50 | `83.5%` throughput, `1.2x` p50 |
| `postgres + memory` | `6.8%` throughput, `9.1x` p50 | `37.0%` throughput, `2.7x` p50 | `34.9%` throughput, `2.8x` p50 |
| `postgres + redis` | `7.0%` throughput, `9.7x` p50 | `33.0%` throughput, `3.0x` p50 | `15.1%` throughput, `6.4x` p50 |

## Assessment

### General observations

- `memory + memory` remains the throughput and latency baseline.
- `sqlite + memory` preserves most of the stream-path performance while introducing a measurable control-plane cost.
- `postgres + memory` and `postgres + redis` are very close on `lease-cycle`, which indicates that the dominant cost for that scenario is still the PostgreSQL control-plane path rather than the frame backend.
- `postgres + memory` is materially faster than `postgres + redis` on `stream-follow`, which confirms that keeping frames in memory remains valuable for single-node deployments.

### Single-node conclusion

For a single-node deployment, the current data supports the following interpretation:

- `sqlite + memory` is the strongest single-node option when low operational complexity and strong local performance are the primary goals.
- `postgres + memory` is viable when durable PostgreSQL metadata is required on a single node, but it should not be treated as a near-memory-performance mode.
- The cost of PostgreSQL metadata persistence is visible in all three benchmark scenarios and is dominant in `lease-cycle`.

### Multi-instance conclusion

For a multi-instance deployment, `postgres + redis` remains the correct architecture, but the current implementation pays a visible cost for shared durable state and shared stream infrastructure.

The largest gap relative to in-memory operation remains:

- lease assignment and completion on the PostgreSQL control-plane path
- shared frame transport and follow latency on the Redis-backed stream path

## Current Bottlenecks

The benchmark shape points to the following bottlenecks in the current codebase:

- PostgreSQL control-plane transactions dominate `lease-cycle`.
- Shared stream transport dominates `postgres + redis` stream latency.
- The gap between `postgres + memory` and `postgres + redis` on `stream-follow` is primarily attributable to the frame backend rather than the control-plane store.

## Interpretation Boundaries

These results should be read as a comparative local snapshot, not as a production capacity claim.

They do not include:

- gateway JSON serialization overhead
- browser or console polling behavior
- cloud networking, TLS termination, or service mesh effects
- larger concurrency regimes or mixed workloads

## Summary

The current version shows a clear separation between the single-node and shared-state modes:

- `memory + memory` is the fastest baseline
- `sqlite + memory` is the most performance-efficient persistent single-node mode
- `postgres + memory` provides durable PostgreSQL metadata on a single node, with a significant control-plane cost
- `postgres + redis` remains the multi-instance mode, with the largest penalty on streaming performance
