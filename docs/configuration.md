# Configuration

`anyserve serve --config server.toml` reads TOML. Flags override file settings;
unknown keys are rejected. With no file, gRPC listens on `127.0.0.1:50052`, the
journal is enabled and management HTTP is disabled.

## Server file

This example lists all server-file keys. Runtime and journal values are their
defaults; the TLS block is commented because it requires your certificate files.
Paths are relative to the server's working directory.

```toml
[server]
grpc_host = "127.0.0.1"
grpc_port = 50052

# [server.tls]
# enabled = true
# cert_file = "/etc/anyserve/server.crt"
# key_file = "/etc/anyserve/server.key"

[auth]
client_tokens = []
worker_tokens = []
# Read comma-separated tokens from named environment variables:
# client_tokens_env = "ANYSERVE_CLIENT_TOKENS"
# worker_tokens_env = "ANYSERVE_WORKER_TOKENS"

# Omit this section to disable management HTTP.
[console]
enabled = true
listen = "127.0.0.1:3001"
allowed_origins = []

[journal]
enabled = true
path = ".anyserve/journal.sqlite"
full_sync = true
queue_capacity = 4096
max_batch = 256
retention_secs = 604800

[runtime]
heartbeat_interval_ms = 5000
heartbeat_timeout_ms = 15000
max_attempts = 3
max_queue = 10000
max_pending_jobs = 100000
max_pending_bytes = 2147483648
max_event_bytes = 1073741824
max_result_bytes = 2147483648
max_sessions = 4096
consumer_stall_timeout_ms = 30000
cancel_grace_ms = 15000
terminal_retention_secs = 600
max_terminal_jobs = 10000
event_log_max_events = 8192
event_log_max_bytes = 8388608

# Optional per-interface overrides; quote names containing dots.
# [runtime.interfaces."demo.echo.v1"]
# max_queue = 100
# queue_timeout_ms = 1000
```

Token lists and their environment sources are combined. An environment key names
the variable to read; it does not contain the token itself. With both role lists
empty, authentication is disabled. Once enabled, clients need client-role tokens
and workers need worker-role tokens. TLS protects gRPC; terminate management HTTP
TLS at your ingress or proxy. CORS origins control browser access independently
of authentication.

TLS clients use an `https://` endpoint. The supplied Rust/Python clients trust
their built-in public CA roots; they do not expose a custom CA option. Use a
certificate trusted by those roots, or supply a custom Rust transport for a
private CA.

## Limits and retention

`max_queue` limits queued/accepting jobs per interface; `max_pending_jobs` limits
unfinished jobs globally. Admission beyond these limits returns
`RESOURCE_EXHAUSTED`. Heartbeat timeout closes a silent session, then gives its
leases another heartbeat-timeout interval to reconnect. Cancellation keeps a
worker reservation until acknowledgement or `cancel_grace_ms` expires.

| Byte budget | Default | What it retains / pressure behavior |
|---|---|---|
| `max_pending_bytes` | 2 GiB | Unfinished input and params; new input is refused when full |
| `max_result_bytes` | 2 GiB | Uploaded, retained and journal-read inline result data; evicts terminal jobs once before refusing an upload |
| `max_event_bytes` | 1 GiB | Events across jobs; slow consumers back-pressure workers, and old history can expire |

Buffers can be shared across budgets; these values do not add up to an RSS cap.
Reading an evicted durable result through GetJob or Watch reserves its inline payload bytes
before loading the payload. Reads that do not fit return `RESOURCE_EXHAUSTED`
and can be retried after capacity is released. State-only GetJob reads do not
reserve result bytes. Clones keep the reservation until their last reference
is dropped.
Each job also has `event_log_max_events` and `event_log_max_bytes` limits.
`consumer_stall_timeout_ms` bounds delivery stalls. Lifecycle events can exceed
the event byte budget; admission, attempt and retained-job limits bound them.

`terminal_retention_secs` and `max_terminal_jobs` limit terminal jobs in memory.
Durable results remain readable by ID until journal retention expires; ListJobs
only lists jobs still in memory. `full_sync=false` selects SQLite NORMAL sync,
which weakens durability under power loss. Journal queue capacity bounds pending
operations; `max_batch` controls group commits.

For evicted durable jobs, Console previews read at most 4 KiB per inline output
directly from the journal, preserving the original size without reserving
full-result capacity. GetJob/Watch continue to return complete results and
account for their bytes. Console polling pauses in hidden browser tabs;
inactive panes and completed job details do not poll.
Opening an older journal creates a large-output metadata index once; its build time
depends on the retained data size.

Compiled protocol limits are 16 MiB per inline object, 32 MiB per job's input or
attempt result (including associated metadata), 32 KiB metadata, 1,024 objects,
256-byte identifiers, 64 KiB chunks and a 4 MiB unacknowledged attempt window.
The gRPC message cap is 64 MiB including protobuf overhead. URI objects are opaque
references; applications fetch them.

Console's **Explore** page loads task metadata into an in-memory DuckDB-WASM
database in a dedicated Worker. Choose completed durable history (including
evicted jobs) or current retained jobs, and a creation-time window of at most
31 days. The authenticated `/api/analytics/jobs` endpoint uses creation-time
and job-id cursors, with at most 250 rows per page. A new partial history index
is built on first journal open; its startup cost depends on retained history.
Pages are independent observations, so job states can change during a load.

Explore loads data manually, with a default of 10,000 jobs and upper bounds of
100,000 jobs and 32 MiB of serialized metadata. It marks partial datasets;
aggregations cover only the loaded data. `jobs` contains summary columns and
`job_metadata(job_id, key, value)` supports tag filters and joins. Durations
include queue time. Request/result bodies and media are excluded. Query results
are capped at 1,000 rows / 100 columns / 4 MiB, large cells are shortened, and tables render
50 rows per page. CSV exports contain the displayed, filtered results.

SQL runs locally with a 256 MB DuckDB memory limit and a 15-second deadline.
Cancellation or timeout terminates the Worker and requires reloading the data.
External access and extension installation are disabled; queries are prepared
as a single SELECT relation. Leaving Explore releases its Worker and dataset.
The WASM and Worker assets are served with the Console (no runtime CDN). Only
Explore loads them; the EH WASM asset is about 36 MB before HTTP compression.
Use a browser with WebAssembly exception handling. Serve the production assets
with compression and immutable caching for hashed filenames; no cross-origin
isolation headers are required by this single-threaded bundle.

## CLI

Global flags: `--endpoint URL` (job commands; default
`http://127.0.0.1:50052`), `--log-level LEVEL` (default `info`), `-v/--verbose`
(`debug`), `-h/--help` and `-V/--version`. `RUST_LOG` takes precedence over the
logging flags. Job commands read the client token from `ANYSERVE_TOKEN`.

| Command | Flags / arguments |
|---|---|
| `serve` | `--config PATH`, `--grpc-host HOST`, `--grpc-port PORT`, `--grpc-tls-enabled[=true\|false]`, `--grpc-cert-file PATH`, `--grpc-key-file PATH`, `--journal PATH`, `--ephemeral` |
| `job ls` (alias `list`) | Repeatable `--state queued\|running\|succeeded\|failed\|cancelled`, `--limit N` (100), `--page-token TOKEN` |
| `job inspect` | `JOB_ID` |
| `job cancel` | One or more `JOB_ID` arguments, or `--stdin` for IDs from standard input |

All job commands accept `-o/--output text|json` (default `text`). `--journal` and
`--ephemeral` are mutually exclusive. No token flag is provided. Use each
command's `--help` for accepted values.

## Embedded Rust Runtime

The [Rust API](../crates/anyserve-core/src/runtime/api.rs) also exposes priority
levels, retry backoff, the attempt ceiling, protocol `Limits` and a journal size
cap (`JournalConfig.max_bytes`). These are compiled defaults in the CLI and are
not accepted TOML keys. An embedded `RuntimeConfig::default()` has no journal;
the CLI enables one by default.
