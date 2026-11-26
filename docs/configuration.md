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
# (or JSON array via ANYSERVE_SCOPED_TOKENS env var)
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
# Optional auth portal URL shown on the Console login gate and Sidebar (or ANYSERVE_CONSOLE_AUTH_PORTAL_URL):
# auth_portal_url = "https://relay.example.com/tokens"

[journal]
enabled = true
backend = "sqlite"
path = ".anyserve/journal.sqlite"
full_sync = true
queue_capacity = 4096
max_batch = 256
retention_secs = 604800
# Optional SQLite database byte cap (hard PRAGMA max_page_count ceiling + size-based
# terminal purge; or --journal-max-bytes / ANYSERVE_JOURNAL_MAX_BYTES):
# max_bytes = 536870912

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

# Optional per-tenant fairness/noisy-neighbor caps (Rust control plane only;
# not implemented in the Cloudflare Workers/DO native engine):
# [runtime.tenant_quotas]
# max_sessions_per_namespace = 32
# max_pending_jobs_per_namespace = 200
# max_interfaces_per_namespace = 32
#
# Optional longest-prefix overrides (e.g. tighter per-namespace and aggregate
# caps for ephemeral `guest-*` sandboxes so they cannot crowd out `gh-*` tenants;
# empty, invalid, or duplicate `prefix` entries are rejected at config load).
# Prefer a trailing delimiter such as `"guest-"` rather than `"guest"` or `"gh"`
# so an override cannot accidentally match unrelated namespaces like `"ghost-"`.
# [[runtime.tenant_quotas.prefix_overrides]]
# prefix = "guest-"
# max_sessions_per_namespace = 4
# max_pending_jobs_per_namespace = 10
# max_interfaces_per_namespace = 4
# max_total_sessions = 400
# max_total_pending_jobs = 400

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
origins control browser access independently of authentication: when
`allowed_origins` (or `ANYSERVE_ALLOWED_ORIGINS`) is configured, matched origins
receive `Access-Control-Expose-Headers: x-anyserve-grpc-code` on `/api/*` and
`/rpc/v2/*` responses; when unset, no `Vary` or `Access-Control-*` headers are
emitted.

Browser WebSocket clients and workers on `/ws/v2/*` (which cannot attach custom
`Authorization` headers) authenticate using `Sec-WebSocket-Protocol`: pass
`["anyserve.v2", "anyserve.bearer." + base64url_nopad(token)]`. The server runs
the exact same role, namespace, expiry, and revocation checks as `Authorization:
Bearer`, echoes only `anyserve.v2` in the 101 response, and rejects conflicting
`Authorization` + `anyserve.bearer.*` headers with HTTP `400` (`INVALID_ARGUMENT`).

**Optional Browser WebRTC P2P data-plane bypass** (Console Playground): default
traffic always uses the WebSocket relay, and the core runtime/protocol never
depends on WebRTC. Callers may opt in per job with metadata
`anyserve.transport=webrtc` on **EPHEMERAL** streaming `/ws/v2/client/invoke`
requests only — durable and `Submit` jobs reject or fall back to relay because
the control plane must persist and replay durable outputs.
- *Trust boundary*: in P2P mode, payload bytes flow directly over
  `RTCDataChannel` (client-counted `0 B` payload relayed over `/ws/v2/client/invoke`;
  SDP/ICE signaling and summary metadata still traverse the control plane); the
  control plane only sees scheduling, lease liveness, and worker-reported summary
  metadata (`anyserve.transport_actual`, `anyserve.p2p_bytes`, `anyserve.ice_ms`),
  which are not server-audited or replayable.
- *NAT & fallback*: public STUN only (`stun.l.google.com:19302`,
  `stun.cloudflare.com:3478`), **no TURN** — symmetric NATs and restrictive
  corporate firewalls automatically fall back to WebSocket relay after 3.5 s.
- *Failover consistency*: if the P2P peer drops before any output chunk is
  exposed to the caller, the client automatically re-schedules on another worker;
  once output has been exposed, disconnects fail fast with `output_interrupted`
  (matching relay semantics). Short text streams pay cold ICE setup (~50–200 ms);
  P2P is intended for large payloads where avoiding server egress matters.

TLS clients use an `https://` endpoint. The supplied Rust/Python clients trust
their built-in public CA roots; they do not expose a custom CA option. Use a
certificate trusted by those roots, or supply a custom Rust transport for a
private CA.

## Limits and retention

`max_queue` limits queued/accepting jobs per interface; `max_pending_jobs` limits
unfinished jobs in the active Runtime. Admission beyond these limits returns
`RESOURCE_EXHAUSTED`. Application-level protobuf `Heartbeat` messages are the
sole liveness signal for worker sessions: RFC 6455 WebSocket `Ping`/`Pong`
frames are transport keepalives only and intentionally do **not** extend
`heartbeat_timeout` (a wedged worker event loop can still reply with Pong).
Browser workers should run their session and heartbeat timer inside a Dedicated
Web Worker so background tab timer throttling does not starve heartbeats.
Heartbeat timeout closes a silent session, then gives its
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

Default protocol limits are 16 MiB per inline object (`[runtime].max_inline_object_bytes`),
32 MiB per job's input or attempt result including metadata (`[runtime].max_inline_total_bytes`),
32 KiB metadata, 1,024 objects, 64-byte interface names (`1..=64` bytes with no ASCII
whitespace or control characters), 256-byte job/worker/session/pool identifiers, 64 KiB
chunks and a 4 MiB unacknowledged attempt window. The gRPC message cap is 64 MiB including
protobuf overhead. URI objects are opaque references; applications fetch them.

## Console Explore

Explore manually loads metadata into a local DuckDB-WASM database in a dedicated
Worker. `jobs` contains summary columns (including `namespace`); `job_metadata(job_id, namespace, key, value)`
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

Global flags: `--endpoint URL` (CLI flag, else `ANYSERVE_ENDPOINT`, else default
`http://127.0.0.1:50052`, accepts comma-separated URLs for sequential leader
discovery), `--log-level LEVEL` (default `info`), `--log-format text|json`
(default `text`), `-v/--verbose` (`debug`), `-h/--help` and `-V/--version`.
`RUST_LOG` takes precedence over the logging level flags.

| Command | Flags / arguments |
|---|---|
| `serve` | `--config PATH`, `--grpc-host HOST`, `--grpc-port PORT`, `--no-grpc`, `--grpc-tls-enabled[=true\|false]`, `--grpc-cert-file PATH`, `--grpc-key-file PATH`, `--journal PATH`, `--journal-max-bytes BYTES`, `--memory-limit MB`, `--ephemeral`, `--console-listen ADDR`, `--public-endpoint URL`, `--client-token TOKEN`, `--worker-token TOKEN`, `--auto-token` |
| `job ls` (alias `list`) | Repeatable `--state queued\|running\|succeeded\|failed\|cancelled`, `--limit N` (100), `--page-token TOKEN` |
| `job inspect` | `JOB_ID` |
| `job cancel` | One or more `JOB_ID` arguments, or `--stdin` for IDs from standard input |
| `token mint` | `--namespace NS`, `--role client\|worker`, `--subject SUB`, `--ttl-secs SECS`, `--forced-pool POOL`, `--jti ID`, `--signing-key KEY`, `--json` |
| `token inspect` | `TOKEN`, optional `--signing-key KEY` (`ANYSERVE_TOKEN_SIGNING_KEY`) |
| `invoke` | `INTERFACE`, `--input TEXT\|-\|@PATH`, `--json`, `--token TOKEN`, `--namespace NS`, `--stream`, `--timeout-secs SECS` |
| `doctor` | Optional `--token TOKEN`, `--json` (probes `/healthz`, `/readyz`, `/api/info`, and `/api/overview`) |

All `job` commands accept `-o/--output text|json` (default `text`) and read
`ANYSERVE_TOKEN` / `ANYSERVE_NAMESPACE`. `--journal` and `--ephemeral` are
mutually exclusive. Use each command's `--help` for accepted values.

### Zero-TOML `serve` flags and environment fallbacks

| Flag | Default env fallback | Behavior |
|---|---|---|
| `--client-token TOKEN` | `ANYSERVE_CLIENT_TOKENS` | Repeatable; comma-separated values are split and trimmed. Combined with TOML tokens. Env fallback applies automatically whenever `[auth].client_tokens_env` is not set. |
| `--worker-token TOKEN` | `ANYSERVE_WORKER_TOKENS` | Repeatable; comma-separated values are split and trimmed. Combined with TOML tokens. Env fallback applies automatically whenever `[auth].worker_tokens_env` is not set. |
| — | `ANYSERVE_SCOPED_TOKENS` | Optional JSON array of `{ token, role, namespace, subject?, forced_pool? }` combined with `[[auth.scoped_tokens]]`. |
| — | `ANYSERVE_TOKEN_SIGNING_KEY` | Comma-separated HMAC signing secret(s) (newest first for key rotation) verifying `asv1.` stateless tokens; overrides omitted `[auth].signing_keys_env`. |
| — | `ANYSERVE_AUTH_REVOCATION_URL` | Optional internal revocation snapshot URL polled when signing keys are configured; falls back when `[auth].revocation_url` is unset. |
| `--console-listen ADDR` | — | Enables the management HTTP, WebSocket (`/ws/v2/*`), and embedded Console listener (e.g. `127.0.0.1:3001` or `0.0.0.0:3001`) without a TOML file; overrides `[console].listen`. |
| `--public-endpoint URL` | `ANYSERVE_PUBLIC_ENDPOINT` | Advertised endpoint for `/api/overview` and the Console "Connect worker" dialog. Precedence: `--public-endpoint` > `ANYSERVE_PUBLIC_ENDPOINT` > `[console].public_endpoint`. |
| — | `ANYSERVE_CONSOLE_AUTH_PORTAL_URL` | Optional portal URL exposed via unauthenticated `/api/info` and Console links ("Sign in with GitHub" / "Manage Tokens"); overrides `[console].auth_portal_url`. |
| — | `ANYSERVE_AUTH_GUEST_ENABLED` | Opt-in toggle on `anyserve-auth-github` (default `false`). When `true`, enables `POST /auth/api/guest` to mint stateless 30-minute `asv1.` client+worker pairs in isolated `guest-<12hex>` namespaces with zero SQLite writes, bounded by per-client (`ANYSERVE_AUTH_GUEST_RATE_LIMIT_PER_IP`, default `10`/10m; IPv6 bucketed by `/64`, IPv4-mapped IPv6 normalized to IPv4), global burst (`ANYSERVE_AUTH_GUEST_RATE_LIMIT_GLOBAL`, default `30`/m), and total active-TTL ceiling (`ANYSERVE_AUTH_GUEST_MAX_ACTIVE`, default `200` concurrent unexpired sandboxes). TTL via `ANYSERVE_AUTH_GUEST_TTL_SECS` (default `1800`, clamped `60..=86400`). Set `ANYSERVE_AUTH_TRUST_PROXY=true` only behind a trusted reverse proxy (e.g. Caddy) so client IP extraction uses the rightmost non-empty entry of the last `X-Forwarded-For` header line; when `false` (default), forwarding headers are ignored and peer socket IP is used directly. Note: `guest-*` sandboxes share global `[runtime]` limits (`max_sessions`, `max_pending_jobs`, `max_terminal_jobs`) with authenticated tenants, so keep `ANYSERVE_AUTH_GUEST_MAX_ACTIVE` and `[runtime.tenant_quotas]` with a `[[runtime.tenant_quotas.prefix_overrides]]` rule for `prefix = "guest-"` (Rust control plane only) configured before enabling guests in production. |
| `--memory-limit MB` | `ANYSERVE_MEMORY_LIMIT_MB` | Scales unspecified `max_pending_bytes` (25%), `max_result_bytes` (25%), and `max_event_bytes` (10%) from a total RAM budget while preserving explicit TOML limits. |
| `--journal-max-bytes BYTES` | `ANYSERVE_JOURNAL_MAX_BYTES` | Sets the SQLite database byte cap (hard `PRAGMA max_page_count` ceiling + size-based terminal purge during maintenance/backpressure); overrides `[journal].max_bytes`. |
| `--no-grpc` | — | Disables the gRPC listener without a TOML file. Requires management HTTP (`--console-listen` or `[console].enabled = true`). |
| `--auto-token` | — | If no static, scoped, or signing-key (`ANYSERVE_TOKEN_SIGNING_KEY`) auth is configured from TOML, CLI, or env, generates random `as_c_*` (client) and `as_w_*` (worker) tokens and prints a ready-to-copy startup card. Persisted to `<journal_dir>/tokens.env` (`0600` on Unix) and reloaded on restart when durable SQLite is enabled; memory-only with `--ephemeral`. No-op if any auth mechanism (including `ANYSERVE_TOKEN_SIGNING_KEY`) is already configured. |

At startup, `anyserve serve` prints a concise summary card showing active
listeners, embedded Console status, advertised public endpoint, and auth state
(static user tokens are never printed; generated/reloaded `--auto-token`
credentials are shown with copy-paste `ANYSERVE_ENDPOINT` / `ANYSERVE_TOKEN`
commands).

## Embedded Rust Runtime

The [Rust API](../crates/anyserve-core/src/runtime/api.rs) also exposes priority
levels, retry backoff, the attempt ceiling, and compiled protocol `Limits`
(metadata, object count, identifier, chunk, and window caps; only
`max_inline_object_bytes` and `max_inline_total_bytes` are accepted under
`[runtime]`). Those remaining fields are compiled defaults in the CLI and are
not accepted TOML keys. An embedded `RuntimeConfig::default()` has no journal;
the CLI enables SQLite by default.
