# Configuration

This reference describes the Runtime, Limits and JournalConfig fields. Defaults
come from `crates/anyserve-core/src/runtime/api.rs`; the tables also mark limits
that only an embedded Rust Runtime can set. Read [execution architecture](architecture.md)
for the lifecycle behind these limits and [deployment and recovery](deployment.md)
for listener and persistence choices.

The CLI maps `[runtime]` keys `max_attempts` and `max_queue` to
`default_max_attempts` and `default_max_queue`. Time keys use `_ms` for
heartbeat interval/timeout, consumer stall, cancel grace and per-interface queue
timeout; terminal retention uses `_secs`. Its remaining supported keys have the
same names: `max_pending_jobs`, `max_pending_bytes`, `max_event_bytes`,
`max_result_bytes`, `max_sessions`, `max_terminal_jobs`, `event_log_max_events`
and `event_log_max_bytes`, plus `[runtime.interfaces.<name>]` overrides.
Priority levels, the attempt ceiling, retry backoff and Limits use the compiled
defaults in the CLI; they remain fields of the embedded API.

The CLI enables `[journal]` by default. It accepts `enabled`, `path`, `full_sync`,
`queue_capacity`, `max_batch` and `retention_secs`; database `max_bytes` is an
embedded API field. `serve --ephemeral` disables the journal and
`serve --journal PATH` selects it. CLI flags override file settings. Unknown
configuration keys are rejected.

## Native services and management

`[server]` configures native Client/Worker gRPC. The optional `[console]` section
configures the management API, including `/healthz`, `/readyz` and `/metrics`.
Its fields are `enabled` (true when the section exists), `listen` (default
`127.0.0.1:3001`) and `allowed_origins` (default empty). See
`examples/server.toml` and [usage](usage.md) for startup and authentication.

## The three byte budgets

The budgets overlap on shared `Bytes`: one buffer can be referenced as input,
as a result and as an Output event at the same time, and Finished is charged to
the event budget at its fixed overhead only while its result data stays on the
result budget. Their sum is therefore not an RSS limit.

| Budget | Field | Default | Counts | Lifetime | When exceeded |
|---|---|---|---|---|---|
| input | `max_pending_bytes` | 2 GiB | static inputs, params and streaming input (plus 64 B and metadata per chunk) | acceptance until the job ends | Submit/Invoke return `RESOURCE_EXHAUSTED`; a streaming `send` returns `RESOURCE_EXHAUSTED` |
| result | `max_result_bytes` | 2 GiB | result data uploaded by workers | upload until the last reference drops (journal commits, retained terminal results, Finished events) | the oldest terminal jobs are evicted once and the reservation retried; if it still does not fit, the Attempt stream ends with `RESOURCE_EXHAUSTED` and the worker resends only its result after backing off |
| event | `max_event_bytes` | 1 GiB | event logs of all jobs (plus 96 B per event) | append until eviction, compaction of a finished ephemeral job (only Finished stays, once its current readers are done) or job eviction | terminal jobs are evicted once before the append; a streamed consumer that is behind back-pressures the worker and the job fails with `consumer_stalled` after `consumer_stall_timeout`; global pressure waits at most `consumer_stall_timeout`, then the Attempt stream ends with `RESOURCE_EXHAUSTED`; a job without a primary consumer drops its oldest events and readers see `CursorExpired{oldest}` |

Output payloads are never reserved beyond the event budget. Under global
pressure the writer retries every 250 ms and on consumer progress; a timeout
ends only the Attempt stream, and the server does not fail the job itself. A
worker may report that transport failure as a retryable failure; an attempt
whose output was already exposed ends with `output_interrupted`. The result
budget retries once after eviction and refuses immediately afterwards.

Lifecycle events (Accepted, Assigned, AttemptRestarted, Finished) are always
written and charged 96 B each, even beyond the event budget. Their total is
bounded by `(max_pending_jobs + max_terminal_jobs) × (2 × max_attempts_limit + 1) × 96 B`,
about 433 MB with the defaults and below the default 1 GiB event budget. The
event budget is neither an exact count of all metadata nor a hard RSS limit.

## RuntimeConfig

| Field | Default | Purpose | When exceeded or triggered |
|---|---|---|---|
| `heartbeat_interval` | 5 s | Heartbeat period sent to workers in Welcome | — |
| `heartbeat_timeout` | 15 s | Silence limit of a control session, and the reconnect grace after it closes | the session closes; leases not reclaimed within the grace become `worker_lost` and retry |
| `default_max_attempts` | 3 | `max_attempts` for jobs that do not set one | the job fails with `attempts_exhausted` |
| `max_attempts_limit` | 20 | Ceiling for a job's own `max_attempts` | larger values are clamped |
| `default_initial_backoff` | 200 ms | Wait before the first retry, growing exponentially | — |
| `default_max_backoff` | 10 s | Ceiling of the retry backoff | — |
| `priority_levels` | 4 | Number of priority levels | `priority` is clamped to `levels - 1` |
| `default_max_queue` | 10 000 | Queue limit per interface (Queued plus Accepting) | `RESOURCE_EXHAUSTED` (queue full) |
| `interfaces.<name>.max_queue` | none | Queue limit override for one interface | as above |
| `interfaces.<name>.queue_timeout` | none | Default queue timeout for one interface | the job fails with `queue_timeout` |
| `max_pending_jobs` | 100 000 | Unfinished jobs across all interfaces | `RESOURCE_EXHAUSTED` |
| `max_pending_bytes` | 2 GiB | Input budget, see above | see above |
| `consumer_stall_timeout` | 30 s | How long a streamed consumer may stop reading | the job fails with `consumer_stalled`; the worker receives Cancel |
| `cancel_grace` | 15 s | Wait for a worker's cancel acknowledgement; capacity stays reserved meanwhile | a Fence is sent and the capacity released |
| `terminal_retention` | 10 min | How long terminal jobs stay in memory | evicted; durable jobs remain readable from the journal, ephemeral ones become `NotFound` |
| `max_terminal_jobs` | 10 000 | Terminal jobs kept in memory | the oldest terminal job is evicted |
| `event_log_max_events` | 8 192 | Events per job log | as the event budget |
| `event_log_max_bytes` | 8 MiB | Bytes per job log | as the event budget |
| `max_event_bytes` | 1 GiB | Event budget, see above | see above |
| `max_result_bytes` | 2 GiB | Result budget, see above | see above |
| `max_sessions` | 4 096 | Worker sessions, including those in grace | a new Hello receives `RESOURCE_EXHAUSTED` |
| `interfaces` | empty | Per-interface policies (the two rows above) | interfaces without an entry use the global defaults |
| `limits` | `Limits::default()` | Protocol and payload limits, next table | see Limits |
| `journal` | `None` (embedded Runtime) | Enables the durable journal, last table | durable Submit is rejected without it; the CLI enables it by default |

## Limits

The first four are sent to workers in Welcome.

| Field | Default | Purpose | When exceeded |
|---|---|---|---|
| `max_control_message_bytes` | 256 KiB | Size of one control message | Assign is built to stay within it |
| `max_chunk_bytes` | 64 KiB | Size of one InputData or Frame chunk | an oversized Frame ends the Attempt stream with `INVALID_ARGUMENT` |
| `attempt_window_bytes` | 4 MiB | Unacknowledged bytes per attempt (enforced by the SDK) | the SDK waits for Ack |
| `assign_inline_limit` | 64 KiB | Inputs, params and metadata up to this size travel inside Assign | `inputs_via_attempt=true`; the data stream carries them |
| `max_inline_object_bytes` | 16 MiB | Size of one input or result object | input: `INVALID_ARGUMENT`; result: the job fails with `output_too_large` |
| `max_inline_total_bytes` | 32 MiB | Total input of a job (with params) or total result of an attempt (with URIs, metadata and 64 B per object) | as above |
| `max_metadata_bytes` | 32 KiB | Job metadata, attributes and interface name; worker attributes, metadata and interfaces | `INVALID_ARGUMENT`; oversized result metadata is `output_too_large` |
| `max_objects` | 1 024 | Input objects per job, result objects per attempt, interfaces and held leases per worker | input or Hello: `INVALID_ARGUMENT`; result: `output_too_large` |
| `max_identifier_bytes` | 256 | Length of job_id, interface, worker_id and resume_session_id | `INVALID_ARGUMENT` |

## JournalConfig

Setting `journal` enables durable execution.

| Field | Default | Purpose | When exceeded or failing |
|---|---|---|---|
| `path` | CLI: `.anyserve/journal.sqlite` | SQLite file; one process per path | a second process fails to start |
| `full_sync` | true | `synchronous=FULL`; false selects `NORMAL`, which weakens durability under power loss | — |
| `queue_capacity` | 4 096 | Operations waiting to be committed | durable operations return `RESOURCE_EXHAUSTED` |
| `max_batch` | 256 | Operations merged into one group commit | — |
| `retention` | 7 days | How long terminal durable jobs stay in the journal, purged in batches of 500 | `get_job` returns `NotFound` afterwards |
| `max_bytes` | none | Database size cap | writes roll back, durable work is refused and readiness turns false; ephemeral work continues; recovers once writable again |
