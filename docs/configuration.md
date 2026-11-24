# Configuration

`anyserve serve` runs either from a TOML file (`--config server.toml`) or
zero-TOML using CLI flags and automatic environment fallbacks
(`ANYSERVE_CLIENT_TOKENS`, `ANYSERVE_WORKER_TOKENS`,
`ANYSERVE_PUBLIC_ENDPOINT`). Flags override environment and file settings;
unknown TOML keys are rejected. With no file or flags, gRPC listens on
`127.0.0.1:50052`, the SQLite journal is enabled and management HTTP is
disabled.

## Server file

This example shows the SQLite server file with default runtime and journal
values. PostgreSQL keys are described below. The TLS block requires your
certificate files. Paths are relative to the server's working directory.

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
# Optional scoped tokens binding role + tenant namespace (+ worker forced_pool):
# [[auth.scoped_tokens]]
# token_env = "ANYSERVE_ROBOT_A_WORKER_TOKEN"
# role = "worker"
# namespace = "robot-fleet-a"
# forced_pool = "jetson-local"
# Optional HMAC signing keys for decoupled auth plugins (default env: ANYSERVE_TOKEN_SIGNING_KEY):
# signing_keys_env = "ANYSERVE_TOKEN_SIGNING_KEY"
# revocation_url = "http://auth-github:3002/internal/revocations"
# revocation_poll_interval_ms = 2000

# Omit this section to disable management HTTP.
[console]
enabled = true
listen = "127.0.0.1:3001"
allowed_origins = []

[journal]
enabled = true
backend = "sqlite"
path = ".anyserve/journal.sqlite"
full_sync = true
queue_capacity = 4096
max_batch = 256
retention_secs = 604800

[runtime]
heartbeat_interval_ms = 5000
heartbeat_timeout_ms = 15000
# reconnect_grace_ms = 15000
max_attempts = 3
max_queue = 10000
# default_max_in_flight_per_key = 1
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
# max_in_flight_per_key = 1
#
# [[runtime.interfaces."demo.echo.v1".pool_tiers]]
# pool = "jetson-local"
# spillover_after_ms = 0
# reconnect_grace_ms = 1000
#
# [[runtime.interfaces."demo.echo.v1".pool_tiers]]
# pool = "cloud-burst"
# spillover_after_ms = 200
```

Token lists from TOML, CLI flags (`--client-token`, `--worker-token`), and
environment sources are combined. When `[auth].client_tokens_env` or
`[auth].worker_tokens_env` is omitted (including zero-TOML runs), `anyserve serve`
automatically reads comma-separated tokens from `ANYSERVE_CLIENT_TOKENS` and
`ANYSERVE_WORKER_TOKENS` when set. An explicit TOML `*_env` key names the
variable to read (and requires it to be set); it does not contain the token
itself. With client/worker/scoped/signing-key lists all empty (and `--auto-token` not
set), client/worker authentication is disabled and the `x-anyserve-namespace`
header is caller-asserted with no tenant isolation. Once enabled, clients need
client-role tokens, workers need worker-role tokens, and tenant isolation comes
from `[[auth.scoped_tokens]]` or stateless signed tokens (`ANYSERVE_TOKEN_SIGNING_KEY`).
On the management HTTP server, `/metrics` strictly requires an operator token
from `[auth].client_tokens` (or CLI/env `ANYSERVE_CLIENT_TOKENS`), while Console
`/api/*` accepts both operator tokens (global view) and tenant `client` tokens
(strictly scoped to their own namespace; cross-namespace requests receive `403 Forbidden`).

When a job sets a non-empty `concurrency_key` and neither `runtime.default_max_in_flight_per_key`
nor `runtime.interfaces.<name>.max_in_flight_per_key` is set, the scheduler defaults to `1`
(per-key serialization within each namespace).

TLS protects gRPC; terminate management HTTP TLS at your ingress or proxy. CORS
origins control browser access independently of authentication.

TLS clients use an `https://` endpoint. The supplied Rust/Python clients trust
their built-in public CA roots; they do not expose a custom CA option. Use a
certificate trusted by those roots, or supply a custom Rust transport for a
private CA.

## Limits and retention

`max_queue` limits queued/accepting jobs per interface; `max_pending_jobs` limits
unfinished jobs in the active Runtime. Admission beyond these limits returns
`RESOURCE_EXHAUSTED`. Heartbeat timeout closes a silent session, then gives its
leases `reconnect_grace_ms` (per-tier `pool_tiers.reconnect_grace_ms`, else
`[runtime].reconnect_grace_ms`, defaulting to `heartbeat_timeout_ms`) to reconnect.
Cancellation keeps a worker reservation until acknowledgement or `cancel_grace_ms` expires.

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
Durable results remain readable by ID until journal retention expires. ListJobs
lists jobs currently held in memory. `full_sync=false` selects SQLite NORMAL sync,
which weakens durability under power loss. Journal queue capacity bounds pending
operations; `max_batch` controls group commits for both SQLite and PostgreSQL.

For leader-standby high availability, select `backend="postgres"`, omit `path`, and set
`url_env="ANYSERVE_DATABASE_URL"`. Optional PG keys are `schema="anyserve"`,
`lease_duration_ms=5000` (500–300,000 ms), `poll_interval_ms=100` (10 ms up to half
the lease), and `max_connections=8` (at least 2, plus dedicated write and lease
connections). `full_sync=false` and the embedded SQLite `max_bytes` cap are rejected
for PG. PostgreSQL uses schema v3 (`scheduler_lease`, `jobs`, `objects`) and rejects
legacy v1/v2 schemas at startup. `--journal PATH` explicitly selects SQLite and
`--ephemeral` disables persistence. See
[PG deployment](deployment.md#postgresql-high-availability) for failover and routing.

For evicted durable jobs, Console previews read at most 4 KiB per inline output
directly from the journal, preserving the original size without reserving
full-result capacity. GetJob/Watch continue to return complete results and
account for their bytes. Console polling pauses in hidden browser tabs;
inactive panes and completed job details do not poll.
Opening an older SQLite journal can build missing preview/history indexes;
allow for startup time proportional to retained data.

Compiled protocol limits are 16 MiB per inline object, 32 MiB per job's input or
attempt result (including associated metadata), 32 KiB metadata, 1,024 objects,
64-byte interface names (`1..=64` bytes with no ASCII whitespace or control
characters), 256-byte job/worker/session/pool identifiers, 64 KiB chunks and a
4 MiB unacknowledged attempt window. The gRPC message cap is 64 MiB including
protobuf overhead. URI objects are opaque references; applications fetch them.

## Console Explore

Explore manually loads metadata into a local DuckDB-WASM database in a dedicated
Worker. `jobs` contains summary columns; `job_metadata(job_id, key, value)`
supports tag filters and joins. Durations include queue time. Request/result
bodies and media are excluded.

| Boundary | Behavior |
|---|---|
| Source | Completed durable history, or current retained jobs in memory |
| Time range | Creation time, at most 31 days; custom inputs use local time with second precision |
| Load | 10,000 jobs by default; at most 100,000 jobs or 32 MiB of serialized metadata |
| API | Authenticated `/api/analytics/jobs`, up to 250 rows per page with creation-time/job-ID cursors |
| SQL | One prepared SELECT relation; 256 MB DuckDB memory limit, 15-second deadline, no external access or extension installation |
| Results | At most 1,000 rows, 100 columns or 4 MiB; large cells shortened, 50 rows rendered per page |
| CSV | Displayed query results after the text filter; includes clipped values and the result limits |

Pages are independent observations, not one database snapshot. Partial loads are
marked; aggregates cover only the loaded rows. At high task rates, choose a
seconds-long custom window to stay within the row/byte limits.

Cancellation or timeout terminates the Worker and requires reloading data.
Leaving Explore releases its Worker and dataset. Only Explore loads the locally
served WASM/Worker assets; the EH WASM asset is about 36 MB before compression.
Use a browser with WebAssembly exception handling. The single-threaded bundle
needs no cross-origin isolation headers. See
[Console hosting](deployment.md#browser-console) for asset and API setup.

## CLI

Global flags: `--endpoint URL` (`job` commands; default
`http://127.0.0.1:50052`, accepts comma-separated URLs for sequential leader
discovery), `--log-level LEVEL` (default `info`), `-v/--verbose` (`debug`),
`-h/--help` and `-V/--version`. `RUST_LOG` takes precedence over the logging
flags. `job` commands read `ANYSERVE_TOKEN` and `ANYSERVE_NAMESPACE`.

| Command | Flags / arguments |
|---|---|
| `serve` | `--config PATH`, `--grpc-host HOST`, `--grpc-port PORT`, `--no-grpc`, `--grpc-tls-enabled[=true\|false]`, `--grpc-cert-file PATH`, `--grpc-key-file PATH`, `--journal PATH`, `--ephemeral`, `--console-listen ADDR`, `--public-endpoint URL`, `--client-token TOKEN`, `--worker-token TOKEN`, `--auto-token` |
| `job ls` (alias `list`) | Repeatable `--state queued\|running\|succeeded\|failed\|cancelled`, `--limit N` (100), `--page-token TOKEN` |
| `job inspect` | `JOB_ID` |
| `job cancel` | One or more `JOB_ID` arguments, or `--stdin` for IDs from standard input |

All `job` commands accept `-o/--output text|json` (default `text`) and have no
`--token` flag (use `ANYSERVE_TOKEN`). `--journal` and `--ephemeral` are
mutually exclusive. Use each command's `--help` for accepted values.

### Zero-TOML `serve` flags and environment fallbacks

| Flag | Default env fallback | Behavior |
|---|---|---|
| `--client-token TOKEN` | `ANYSERVE_CLIENT_TOKENS` | Repeatable; comma-separated values are split and trimmed. Combined with TOML tokens. Env fallback applies automatically whenever `[auth].client_tokens_env` is not set. |
| `--worker-token TOKEN` | `ANYSERVE_WORKER_TOKENS` | Repeatable; comma-separated values are split and trimmed. Combined with TOML tokens. Env fallback applies automatically whenever `[auth].worker_tokens_env` is not set. |
| `--console-listen ADDR` | — | Enables the management HTTP, WebSocket (`/ws/v2/*`), and embedded Console listener (e.g. `127.0.0.1:3001` or `0.0.0.0:3001`) without a TOML file; overrides `[console].listen`. |
| `--public-endpoint URL` | `ANYSERVE_PUBLIC_ENDPOINT` | Advertised endpoint for `/api/overview` and the Console "Connect worker" dialog. Precedence: `--public-endpoint` > `ANYSERVE_PUBLIC_ENDPOINT` > `[console].public_endpoint`. |
| `--no-grpc` | — | Disables the gRPC listener without a TOML file. Requires management HTTP (`--console-listen` or `[console].enabled = true`). |
| `--auto-token` | — | If no client, worker, or scoped tokens are configured from TOML, CLI, or env, generates random `as_c_*` (client) and `as_w_*` (worker) tokens and prints a ready-to-copy startup card. Persisted to `<journal_dir>/tokens.env` (`0600` on Unix) and reloaded on restart when durable SQLite is enabled; memory-only with `--ephemeral`. No-op if any tokens are already configured. |

At startup, `anyserve serve` prints a concise summary card showing active
listeners, embedded Console status, advertised public endpoint, and auth state
(static user tokens are never printed; generated/reloaded `--auto-token`
credentials are shown with copy-paste `ANYSERVE_ENDPOINT` / `ANYSERVE_TOKEN`
commands).

## Embedded Rust Runtime

The [Rust API](../crates/anyserve-core/src/runtime/api.rs) also exposes priority
levels, retry backoff, the attempt ceiling, protocol `Limits` and a journal size
cap (`JournalConfig.max_bytes`). These are compiled defaults in the CLI and are
not accepted TOML keys. An embedded `RuntimeConfig::default()` has no journal;
the CLI enables one by default.
