# Usage

## Prerequisites

- `mise`
- `protoc`
- Python 3.12+ if you want the Python bindings

## Bootstrap

```bash
mise trust
mise install
mise run build
```

## Fastest Local Workflow

The checked-in LLM stack is the fastest way to understand the production gateway shape.

For the built-in OpenAI-compatible gateway and LLM worker, start here:

- [LLM Example README](https://github.com/anyserve/anyserve/blob/main/examples/llm/README.md)
- [Ollama](ollama.md)

The smallest local stack is:

```bash
mise exec -- cargo run -p anyserve -- serve --config examples/llm/anyserve.toml
mise exec -- cargo run -p anyserve -- worker --config examples/llm/worker.toml
```

That example exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /v1/models`
- `POST /v1/chat/completions`
- `POST /v1/embeddings`

Keep the detailed commands, config explanation, and current behavior notes in the example README so they only need to be updated in one place.

## Production Direction

If you want the clearest production-facing entry point for Anyserve, read [Create Gateway](create-gateway.md).

That page defines the recommended first shape:

- create a hosted endpoint
- connect your own workers
- send requests through Anyserve
- expand to richer routing only after the first request succeeds

## Start the Control Plane

```bash
mise exec -- cargo run -p anyserve -- serve
```

Or use your own config file:

```bash
mise exec -- cargo run -p anyserve -- serve --config path/to/anyserve.toml
```

CLI flags override the config file if you provide both.

The gRPC health service is available on the same port as gRPC. Example endpoint strings like `http://127.0.0.1:50052` are gRPC channel URIs, not REST endpoints.

## Run the Sample Worker

```bash
mise exec -- cargo run -p anyserve-demo -- --mode worker
```

## Run the Built-In LLM Worker

```bash
mise exec -- cargo run -p anyserve -- worker --config examples/llm/worker.toml
```

The worker proxies `llm.chat.v1` and `llm.embed.v1` jobs to the configured OpenAI-compatible upstream. The full example walkthrough lives in the [LLM Example README](https://github.com/anyserve/anyserve/blob/main/examples/llm/README.md).

Call the built-in OpenAI gateway with any OpenAI SDK or plain HTTP. Example:

```bash
curl http://127.0.0.1:8080/v1/models
```

## Submit the Sample Job

```bash
mise exec -- cargo run -p anyserve-demo -- --mode submit
```

The sample client submits a job with:

- `interface_name = demo.echo.v1`
- `required_attributes = {"runtime": "demo"}`
- `required_capacity = {"slot": 1}`

It then:

- opens `input.default`
- pushes two input frames
- waits for job events
- pulls `output.default` after completion

You can also run the Python examples after installing the bindings:

```bash
# clean venv from local source
python -m venv .venv
. .venv/bin/activate
pip install ./clients/python

# or use the active mise Python
# mise run python-sdk-dev

python examples/python/worker.py
python examples/python/submit.py

# with the active mise Python instead
# mise exec -- python examples/python/worker.py
# mise exec -- python examples/python/submit.py
```

## Build the Python Bindings

```bash
mise run python-sdk
mise run python-sdk-dev
```

## Python Worker Decorator

```python
from anyserve import serve, worker


@worker(
    interface="demo.echo.v1",
    attributes={"runtime": "python"},
    capacity={"slot": 1},
    codec="bytes",
)
def echo(payload: bytes) -> bytes:
    return payload


serve(echo, endpoint="http://127.0.0.1:50052")
```

## Python Example

```python
from anyserve import AnyserveClient, FRAME_DATA

client = AnyserveClient("http://127.0.0.1:50052")
# gRPC channel URI used by the Rust-backed bindings client.
# Assumes the control plane and a compatible worker are already running.

job = client.submit_job(
    interface_name="demo.echo.v1",
    required_attributes={"runtime": "demo"},
    required_capacity={"slot": 1},
)

stream = client.open_stream(job["job_id"], "input.default")
client.push_frames(
    stream["stream_id"],
    [(FRAME_DATA, b"hello from python", {})],
)
client.close_stream(stream["stream_id"])

for event in client.watch_job(job["job_id"]):
    print(event["kind"], event["metadata"])
```

## Test the Workspace

```bash
mise run test
mise run e2e
mise run docs-build
mise run clippy
```
