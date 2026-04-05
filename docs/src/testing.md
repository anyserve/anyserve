# Testing

Anyserve currently has four practical testing layers.

## 1. Unit Tests

Run the workspace tests:

```bash
source "$HOME/.cargo/env"
cargo test -p anyserve-core -p anyserve
```

Current core tests cover:

- scheduler matching on generic attributes and capacity
- lease failure and requeue behavior
- in-memory stream append and pull ordering
- kernel-level stream write and read flow
- a full gRPC e2e covering submit, lease, attempts, streams, and completion

## 2. Local End-to-End

The checked-in examples are the easiest local smoke tests.

For the built-in OpenAI-compatible gateway and LLM worker, use:

- [Ollama Example README](https://github.com/anyserve/anyserve/blob/main/examples/ollama/README.md)
- [Ollama](ollama.md)

That path covers:

- `anyserve serve --config examples/ollama/anyserve.toml`
- `anyserve worker --config examples/ollama/worker.toml`
- `GET /v1/models`
- `POST /v1/chat/completions`
- `POST /v1/embeddings`

This is the user-facing path worth validating first because it exercises the built-in gateway and worker with the same shape you would use in practice.

## 3. Python Bindings Smoke Test

Install the package from local source into an active environment:

```bash
python -m pip install ./clients/python
```

Run the Python unit tests. These do not require a running control plane:

```bash
python -m unittest discover -s clients/python/tests -p 'test_*.py'
```

Build the local binaries once, then run the live Python SDK end-to-end suite:

```bash
source "$HOME/.cargo/env"
cargo build --workspace
python clients/python/tests/sdk_e2e.py
```

The install and unit-test path verifies:

- wheel installation
- Python import surface
- role-specific client helpers such as `list_jobs`
- bindings constants
- convenience object constructors

The Python unit tests additionally verify:

- low-level facade forwarding for `list_jobs`, `watch_job`, and `pull_frames`
- `@worker(...)` metadata capture
- `serve(...)` success paths for `bytes` and `json`
- failure handling for malformed JSON input

The Python SDK e2e suite additionally verifies:

- Python low-level submitter APIs against the demo worker
- Python high-level `@worker` / `serve()` against a live control plane
- streamed event and frame iteration against a real server

If you also want to exercise the Python bindings against a live server manually, start the control plane and a compatible worker first, then run:

```bash
python - <<'PY'
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
source "$HOME/.cargo/env"
cargo test -p anyserve-core --test grpc_e2e -- --nocapture
```

For Docker-backed persistence checks, run the ignored integration tests explicitly:

```bash
ANYSERVE_TEST_POSTGRES_DSN='postgres://postgres:postgres@127.0.0.1:55432/anyserve_test' \
cargo test -p anyserve-core --test postgres_memory_persistence -- --ignored --nocapture

ANYSERVE_TEST_POSTGRES_DSN='postgres://postgres:postgres@127.0.0.1:55432/anyserve_test' \
ANYSERVE_TEST_REDIS_URL='redis://127.0.0.1:56379' \
cargo test -p anyserve-core --test postgres_redis_integration -- --ignored --nocapture
```

This test boots an in-process gRPC server, registers a worker, submits a job, pushes input frames, completes the lease, then verifies:

- job events
- attempt state
- stream listing
- output frame round-trip
- final job state
