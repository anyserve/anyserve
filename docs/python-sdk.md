# Python SDK

Python 3.10+ is required. Install `anyserve-runtime` from the same release as the
server, or `pip install ./clients/python` from this checkout.

## Client

```python
from anyserve import AnyserveClient, AsyncAnyserveClient, ExecutionOptions, inline_object, uri_object

client = AnyserveClient()  # endpoint=None, token=None, namespace=None
output = client.call("demo.echo.v1", b"hello",
                     options=ExecutionOptions(total_timeout_ms=5000))
```

`AnyserveClient` is an alias of `ExecutionClient`; `AsyncAnyserveClient` wraps
`call`, `execute`, `invoke`, `submit`, `get`, `watch`, `cancel` and `result` for
`asyncio` applications (`await client.call(...)`,
`async with await client.invoke(...) as events: ...`; duplex `invoke_stream` is on
`AnyserveClient`). Endpoint resolution is
the explicit argument, then `ANYSERVE_ENDPOINT`, then `http://127.0.0.1:50052`
(comma-separated URLs probe sequentially on `UNAVAILABLE` and stick to the
active leader).
Token resolution is `token=`, then `ANYSERVE_TOKEN`; use a client-role token.
Namespace defaults to `namespace=`, then `ANYSERVE_NAMESPACE`, then `"default"`
(when scoped token auth is configured, the server derives namespace from the token).

`inputs` accepts bytes or a list of object dictionaries. `inline_object(data,
metadata=None)` builds `{"inline": bytes, "metadata": dict}`;
`uri_object(uri, metadata=None, *, sha256=None, size_bytes=None, content_type=None)`
attaches content digests (`anyserve.sha256` / `anyserve.size_bytes`) so
transient presigned URLs still deduplicate under `job_id` idempotency. Set
`@worker(..., uri_resolver=...)` to resolve URI inputs into bytes.

| Method | Result |
|---|---|
| `call(interface, payload=b"", *, options=None)` | One inline terminal result as bytes; requires a bytes-returning handler |
| `execute(interface, inputs=b"", *, options=None)` | Terminal output objects of an ephemeral invocation |
| `invoke(interface, inputs=b"", *, options=None)` | Iterable live event stream; closing cancels unfinished work |
| `invoke_stream(interface, *, options=None)` | Duplex event stream with `send(data, metadata=None)` and `finish_input()` |
| `submit(interface, inputs=b"", *, job_id=None, durability="durable", options=None)` | Accepted job dictionary with `created`; `durability="ephemeral"` is also supported |
| `get(job_id, *, include_outputs=True)` | Current job dictionary |
| `watch(job_id, *, after_sequence=0)` | Iterable event stream; closing leaves the job running |
| `cancel(job_id, reason="caller cancelled")` | Job dictionary after requesting cancellation |
| `result(job_id)` | Terminal output objects; waits if unfinished |

Reusing `job_id` with the same specification is idempotent while the server
retains that job; a different specification conflicts. `call`, `execute` and
`result` raise `RuntimeError` for a failed/cancelled job. `RpcError` is a
`RuntimeError` subclass with the numeric gRPC `status_code` for RPC failures.
Durable Submit requires an enabled journal. Closing a Watch leaves the job
running; closing an unfinished Invoke cancels it. Invoke and Watch consume
gRPC trailers before returning Finished, so closing at that event is safe.

Use event streams as context managers when you may leave before completion:

```python
# Run this worker in its own process first.
from anyserve import serve, worker

@worker(interface="demo.duplex.v1", streaming_input=True)
def duplex(parts):
    for part in parts:
        if not part["parameter_chunk"]:
            yield part["data"]

serve(duplex)
```

From a client process:

```python
from anyserve import AnyserveClient, ExecutionOptions

client = AnyserveClient()
with client.invoke_stream("demo.duplex.v1",
                          options=ExecutionOptions(total_timeout_ms=5000)) as events:
    events.send(b"first input")
    events.finish_input()
    for event in events:
        if event["type"] == "output":
            print(event["data"])
```

## ExecutionOptions

All maps default to `{}`, `params` to `b""` and numbers to zero.

| Field | Meaning |
|---|---|
| `required_attributes: dict[str, str]` | Worker attributes that must match |
| `preferred_attributes: dict[str, str]` | Worker preferences used for placement |
| `required_capacity: dict[str, int]` | Resource quantities reserved per attempt |
| `metadata: dict[str, str]` | Application metadata |
| `params: bytes` | Opaque handler parameters available in its context |
| `priority: int` | Higher runs first; clamped to 0–3 with default server settings |
| `queue_timeout_ms: int` | Limit each queued period; zero uses the interface default if set |
| `execution_timeout_ms: int` | Limit one attempt; zero leaves it unset |
| `total_timeout_ms: int` | Limit the entire job; zero leaves it unset |
| `spillover_after_ms: int` | Hold for all preferences before allowing partial matches; zero ranks immediately |
| `max_attempts: int` | Includes the first attempt; zero uses the server default (3), one disables retries; default ceiling is 20 |
| `affinity_key: str` | Sticky bounded-load Rendezvous routing preference (`< 0.85` utilization), excluding failed workers |
| `fairness_key: str` | Two-level Priority + per-key round-robin queue key preventing bulk bursts from starving peer tenants |
| `concurrency_key: str` | Per-key in-flight serialization / cap across workers |

Once a non-progress output event is exposed, an interrupted attempt ends with
`output_interrupted` instead of switching workers behind the reader's back.

## Jobs and events

Jobs contain `job_id`, `namespace`, `state`, `durability`, `interface`, `attempt_count`,
`current_attempt_id`, `current_worker_id`, `created_at_ms`, `updated_at_ms`,
`outputs`, `metadata`, `spec_hash` and `error`. State is `queued`, `running`,
`succeeded`, `failed` or `cancelled`. `error` is `None` or a dictionary with
`code`, `message` and `retryable`.

Every event has `job_id`, `sequence`, `attempt_id`, `at_ms` and `type`:

| `type` | Additional fields |
|---|---|
| `accepted` | — |
| `assigned` | `worker_id`, `attempt_number` |
| `output` | `data: bytes`, `metadata: dict`, `progress: bool` |
| `attempt_restarted` | `previous_attempt_id`, `error` |
| `finished` | `state`, `outputs`, `error` |

Sequence zero starts at the earliest retained event. Expired nonzero cursors
report cursor expiry. Live event history is held in memory; once evicted or
after a leader restart, `watch()` replays acceptance and the committed terminal
outcome from the journal. `get()` and `result()` can read a retained durable
result after older events expire. See [recovery behavior](deployment.md#restart-and-backup).

Runtime error codes include `queue_timeout`, `execution_timeout`, `total_timeout`,
`cancelled`, `worker_failed`, `worker_lost`, `worker_rejected`,
`attempts_exhausted`, `output_interrupted`, `output_too_large` and
`consumer_stalled`.

## Workers

```python
from anyserve import WorkerError, serve, worker

@worker(interface="demo.echo.v1", max_active_leases=4, pass_context=True)
def echo(payload, context):
    if context.cancelled:
        return b""
    return payload

serve(echo)
```

`@worker` requires `interface=`. Optional keywords are `attributes`, `capacity`
and `metadata` (empty maps), `codec="bytes"` (`"json"` decodes/encodes JSON),
`max_active_leases=1`, `pass_context=False`, `streaming_input=False`, `pool=""`,
and `uri_resolver=None`. Handlers may be synchronous
functions, synchronous generators, `async def` coroutines, or `async` generators.

`serve(...)` takes one handler and an optional endpoint
(`serve(handler, "http://host:50052")`), several handlers
(`serve(h1, h2, endpoint=...)`) or a list (`serve([h1, h2], endpoint)`), with
the keyword options `endpoint`, `worker_id`, `token`, `namespace`, `pool`,
`reconnect_interval_secs=1.0` and `connection`. It blocks until stopped or a
permanent connection error and multiplexes the handlers onto one session.
Endpoint/token defaults match the client, but
workers need a worker-role token. The default ID is hostname plus process ID;
`connection` is an optional transport supplied by embedded callers. Transport
failures reconnect; authentication and invalid-argument failures propagate.
On the main thread, `SIGTERM` triggers preemptive drain (`preempting=True`) so
unexposed in-flight jobs reschedule immediately; a server-sent `Drain` stops new
assignments while keeping in-flight handlers until completion or cancellation.

With `pass_context=True`, the second argument exposes `job_id`, `attempt_id`,
`lease_id`, `metadata`, `inputs`, `params`, `cancelled` and `terminal_replay`.
Poll `cancelled` in long-running handlers: cancellation is cooperative.
With `streaming_input=True`, the first argument is an iterator of fragments
containing `data`, `object_index`, `uri`, `metadata`, `parameter_chunk` and
`object_end`. They arrive before input is complete; JSON decoding is then the
handler's responsibility.

Raise `WorkerError(message, retryable=True, code="worker_failed")` for a failure
that can safely retry elsewhere (defaults: `retryable=False`, `code="worker_failed"`).
Unhandled exceptions also fail without retry using error code `"worker_failed"`.
Runtime execution retries may run the backend again, so make external
side effects idempotent. SDK result-delivery retries do not rerun the handler.
Delivery retries use exponential backoff and jitter, bounded by four negotiated
heartbeat timeouts per open/commit operation. Job deadlines remain enforced by
the server.
