# Architecture

Anyserve is a control plane kernel with a small fixed core:

- `Job`
- `Worker`
- `Lease`
- `Attempt`
- `Stream`
- `Frame`
- `ObjectRef`
- `JobEvent`

The kernel does not encode workload types like LLM or image processing. It only matches:

- `interface_name`
- `required_attributes`
- `preferred_attributes`
- `required_capacity`

against worker supply.

## Module Diagram

```text
                 +-----------------------------+
                 |       SDK / CLI / API       |
                 |   Rust / Python / gRPC      |
                 +-------------+---------------+
                               |
                               v
                 +-----------------------------+
                 |         Transport           |
                 |  Client API   Worker API    |
                 +-------------+---------------+
                               |
                               v
        +--------------------------------------------------+
        |                    Kernel                        |
        |--------------------------------------------------|
        | Job Manager      Worker Registry   Lease Manager |
        | Attempt Log      Stream Router     Recovery Loop |
        | Event Stream     State Machine                     |
        +------------------+-------------------------------+
                           |
                           v
        +--------------------------------------------------+
        |                     Ports                        |
        |--------------------------------------------------|
        | StateStore   StreamStore   Scheduler   ObjectStore |
        +--------+----------+------------+------------+------+
                 |          |            |            |
                 v          v            v            v
              +------+   +------+    +------+     +--------+
              |memory|   |memory|    |basic |     |inline  |
              +------+   +------+    +------+     +--------+
```

## Core Flow

1. A client calls `SubmitJob`.
2. The kernel stores the job and appends an `accepted` event.
3. A worker registers itself and maintains heartbeats.
4. The worker calls `PollLease`.
5. The scheduler picks the oldest compatible pending job for that worker.
6. The kernel issues a lease and emits `lease_granted`.
7. The client or worker can open generic streams and exchange frames.
8. The worker reports progress through `ReportEvent`.
9. The worker finishes with `CompleteLease` or `FailLease`.
10. If a lease expires, the recovery loop requeues the job.

## Ports

- `StateStore`
  Default implementation: `MemoryStateStore`
- `StreamStore`
  Default implementation: `MemoryStreamStore`
- `Scheduler`
  Default implementation: `BasicScheduler`
- `ObjectStore`
  Default implementation: inline object references

That is enough to keep v1 small, bootable, and easy to evolve.

## Stable Core vs Future Plugins

Stable core:

- `Job`
- `Worker`
- `Lease`
- `Attempt`
- `Stream`
- `Frame`
- `ObjectRef`
- `JobEvent`
- kernel state machine
- lease lifecycle

Likely future plugin points:

- persistent state stores
- richer schedulers
- external object stores
- provider adapters
