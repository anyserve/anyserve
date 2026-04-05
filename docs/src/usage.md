# Usage

## Prerequisites

- an `anyserve` binary on `PATH`
- Python 3.12+ if you want the Python bindings

## Fastest Local Workflow

The checked-in Ollama stack is the fastest way to understand the production gateway shape.

For the built-in OpenAI-compatible gateway and LLM worker, start here:

- [Ollama Example README](https://github.com/anyserve/anyserve/blob/main/examples/ollama/README.md)
- [Google Colab Qwen Examples](https://github.com/anyserve/anyserve/blob/main/examples/google-colab/README.md)
- [Ollama](ollama.md)

From a repository checkout, the smallest local stack is:

```bash
anyserve serve --config examples/ollama/anyserve.toml
anyserve worker --config examples/ollama/worker.toml
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
anyserve serve
```

Or use your own config file:

```bash
anyserve serve --config path/to/anyserve.toml
```

CLI flags override the config file if you provide both.

The gRPC health service is available on the same port as gRPC. Example endpoint strings like `http://127.0.0.1:50052` are gRPC channel URIs, not REST endpoints.

## Run the Built-In LLM Worker

```bash
anyserve worker --config examples/ollama/worker.toml
```

The worker proxies `llm.chat.v1` and `llm.embed.v1` jobs to the configured OpenAI-compatible upstream. The full example walkthrough lives in the [Ollama Example README](https://github.com/anyserve/anyserve/blob/main/examples/ollama/README.md).

Call the built-in OpenAI gateway with any OpenAI SDK or plain HTTP. Example:

```bash
curl http://127.0.0.1:8080/v1/models
```

## Python Examples

You can run the Python examples after installing the bindings:

```bash
# clean venv from local source
python -m venv .venv
. .venv/bin/activate
pip install ./clients/python

python examples/python/worker.py
python examples/python/submit.py
```

## Install the Python Bindings from Local Source

```bash
pip install ./clients/python
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
# gRPC channel URI used by the bindings client.
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
source "$HOME/.cargo/env"
cargo test -p anyserve-core -p anyserve
```
