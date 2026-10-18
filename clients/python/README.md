# Anyserve Python SDK

The package uses the native Rust transport for the current execution protocol.
Use the SDK and control-plane binary from the same release. Python 3.12+ is
required; the 0.4 control-plane protocol is not supported by this source tree.

Install from this checkout with `pip install ./clients/python`. Build and test
instructions are in [the workspace testing guide](../../docs/src/testing.md).

## Client execution

`ExecutionClient` (also exported as `AnyserveClient` in Python) provides:

- `submit(interface, inputs, job_id=..., options=...)`: complete-input submission,
  durable by default. An explicit ID is idempotent for the same specification.
- `invoke(interface, inputs, options=...)`: ephemeral execution events.
- `invoke_stream(interface, options=...)`: duplex invocation; call `send` and
  `finish_input`, and iterate the returned execution events. Closing the context
  cancels unfinished execution and closes both transport directions.
- `execute(...)`: collect an ephemeral invocation's terminal outputs.
- `get`, `watch`, `cancel` and `result`: inspect or follow an accepted job.

Inputs are bytes or object dictionaries from `inline_object` / `uri_object`.
`ExecutionOptions` carries required/preferred attributes, resource quantities,
metadata, params, priority, timeouts and `max_attempts` (zero means server default).
URI objects remain opaque references; the SDK does not automatically fetch them.

Pass `token=` or set `ANYSERVE_TOKEN` to a client bearer token. `RpcError` preserves
the numeric gRPC status in `status_code`. `result()` reads the job once and, if it
is still running, follows `watch(job_id, after_sequence=0)` until Finished.
A zero cursor starts at the earliest retained event, including after recovery;
expired nonzero cursors report cursor expiry. Use event streams as context managers
when you may stop reading before Finished. Durable jobs require an enabled
journal; live output history is bounded and does not survive restart.

## Workers

The existing [worker](../../examples/python/worker.py) and
[submitter](../../examples/python/submit.py) use the current API. `@worker` declares
an interface, attributes, capacity, codec and concurrency; `serve` runs the
outbound control session and bounded handler pool. Normal return values become
terminal outputs; generators produce output events. `pass_context=True` exposes
cancellation and assignment metadata. `streaming_input=True` supplies input
fragments without waiting for InputEnd.

Use a worker-role token. Permanent connection errors (InvalidArgument,
PermissionDenied, Unauthenticated) propagate to the caller. Transport failures
are logged and reconnect; a server Drain stops this session's new assignments,
and the worker reconnects when the session ends. It keeps in-flight handlers
until completion or cancellation/fencing from the server. Python callbacks cancel
cooperatively. A local interrupt stops serving.

The native attempt transport retries delivery with exponential backoff and
jitter, bounded by four negotiated heartbeat timeouts per open/commit operation.
Job deadlines are enforced by the server. Re-sending a completed result does not
run the handler again or wait for input replay to finish.
