# Python SDK

Python 3.12+ is required. Install `anyserve-runtime` from the same release as the
server, or `pip install ./clients/python` from this checkout.

## Client

```python
from anyserve import AnyserveClient, ExecutionOptions, inline_object, uri_object

client = AnyserveClient()  # endpoint=None, token=None
output = client.call("demo.echo.v1", b"hello",
                     options=ExecutionOptions(total_timeout_ms=5000))
```

`AnyserveClient` is an alias of `ExecutionClient`. Endpoint resolution is the
explicit argument, then `ANYSERVE_ENDPOINT`, then `http://127.0.0.1:50052`.
Token resolution is `token=`, then `ANYSERVE_TOKEN`; use a client-role token.

`inputs` accepts bytes or a list of object dictionaries. `inline_object(data,
metadata=None)` builds `{"inline": bytes, "metadata": dict}`;
`uri_object(uri, metadata=None)` builds `{"uri": str, "metadata": dict}`.
URI content is fetched by application code. The default `@worker` handler only
accepts inline objects; use `streaming_input=True` and inspect fragment `uri`
fields when a worker needs to fetch URI content.

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
| `max_attempts: int` | Includes the first attempt; zero uses the server default |

## Jobs and events

Jobs contain `job_id`, `state`, `durability`, `interface`, `attempt_count`,
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
report cursor expiry; history is bounded and does not survive a restart.
`get()` and `result()` can read a retained durable result without its old events.

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
`max_active_leases=1`, `pass_context=False` and `streaming_input=False`.
Handlers are synchronous; a return value becomes terminal output and a generator
yields live output frames.

`serve(handler, endpoint=None, *, worker_id=None, token=None,
reconnect_interval_secs=1.0, connection=None)` blocks until stopped or a permanent
connection error. Endpoint/token defaults match the client, but workers need a
worker-role token. The default ID is hostname plus process ID; `connection` is
an optional transport supplied by embedded callers. Transport failures reconnect;
authentication and invalid-argument failures propagate.

With `pass_context=True`, the second argument exposes `job_id`, `attempt_id`,
`lease_id`, `metadata`, `inputs`, `params`, `cancelled` and `terminal_replay`.
Poll `cancelled` in long-running handlers: cancellation is cooperative.
With `streaming_input=True`, the first argument is an iterator of fragments
containing `data`, `object_index`, `uri`, `metadata`, `parameter_chunk` and
`object_end`. They arrive before input is complete; JSON decoding is then the
handler's responsibility.

Raise `WorkerError(message, retryable=True)` for a failure that can safely retry
elsewhere. Its default is `retryable=False`; other exceptions also fail without
retry. Runtime execution retries may run the backend again, so make external
side effects idempotent. SDK result-delivery retries do not rerun the handler.
