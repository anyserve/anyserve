# Testing

Anyserve currently has four practical testing layers.

## 1. Unit Tests

Run the workspace tests:

```bash
mise run test
```

Current core tests cover:

- scheduler matching on generic attributes and capacity
- lease failure and requeue behavior
- in-memory stream append and pull ordering
- kernel-level stream write and read flow
- a full gRPC e2e covering submit, lease, attempts, streams, and completion

## 2. Local End-to-End

Start the control plane:

```bash
mise exec -- cargo run -p anyserve -- serve --grpc-port 50062
```

Start a demo worker:

```bash
mise exec -- cargo run -p anyserve-client -- --mode worker --endpoint http://127.0.0.1:50062
```

The `http://` prefix here is a gRPC channel URI used by the generated clients, not a REST endpoint.

Submit a demo job:

```bash
mise exec -- cargo run -p anyserve-client -- --mode submit --endpoint http://127.0.0.1:50062
```

Expected event flow:

- `accepted`
- `lease_granted`
- `started`
- `progress`
- `output_ready`
- `succeeded`

The submit client should also print one `output frame` line after the job completes.

## 3. Python SDK Smoke Test

Build the wheel:

```bash
mise run python-sdk
```

Install it into the active `mise` Python and verify the import surface:

```bash
mise run python-sdk-smoke
```

This smoke test intentionally does not require a running control plane. It verifies:

- wheel installation
- Python import surface
- SDK constants
- convenience object constructors

If you also want to exercise the Python SDK against a live server, start the control plane and demo worker first, then run:

```bash
mise exec -- python - <<'PY'
from anyserve import AnyserveClient, FRAME_DATA, inline_object, uri_object

client = AnyserveClient("http://127.0.0.1:50062")
print(client)
print(inline_object(b"hello"))
print(uri_object("file:///tmp/example"))
job = client.submit_job(
    interface_name="demo.echo.v1",
    required_attributes={"runtime": "demo"},
    required_capacity={"slot": 1},
)
stream = client.open_stream(job["job_id"], "input.default")
print(stream["stream_name"])
print(client.push_frames(stream["stream_id"], [(FRAME_DATA, b"hello", {})]))
PY
```

## 4. Standalone E2E Test

Run the dedicated gRPC end-to-end test:

```bash
mise run e2e
```

This test boots an in-process gRPC server, registers a worker, submits a job, pushes input frames, completes the lease, then verifies:

- job events
- attempt state
- stream listing
- output frame round-trip
- final job state
