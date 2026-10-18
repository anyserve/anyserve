# Usage

## Prerequisites

- an `anyserve` binary on `PATH`
- Python 3.12+ if you want the Python bindings

## Fastest Local Workflow

For the built-in OpenAI-compatible gateway and worker, follow the canonical walkthrough in [examples/ollama/README.md](https://github.com/anyserve/anyserve/blob/main/examples/ollama/README.md). That is the single place with the full command sequence, config notes, and smoke tests.

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

When `[auth]` tokens are configured, the console API requires a client bearer
token, just like the HTTP gateways. The console prompts for that token and keeps
it in the current browser tab's session storage. CORS `allowed_origins` controls
which browser origins may access the console; it does not replace authentication.

The gRPC client and server both accept messages up to 64 MiB, including protobuf
overhead. HTTP gateways may enforce a smaller request-body limit.

## CLI authentication

`anyserve job ls`, `anyserve job inspect <job-id>` and `anyserve job cancel
<job-id>` read a single client bearer token from `ANYSERVE_TOKEN`. Set it to a
client token accepted by the server's `[auth]`; there is no token flag. Native
SDK clients use the same default environment variable. Worker credentials are
separate and can be selected with `token_env` in the worker configuration.

Gateway `/readyz` requires `Runtime::health().ready` and a connected, non-draining
worker matching the configured required demand. OpenAI uses its chat interface
as the primary readiness requirement; embeddings are reported separately. The
invoke gateway accepts any matching interface. Busy workers still count as live;
zero free credit does not itself mark a deployment unready.

## Run the Built-In LLM Worker

```bash
anyserve worker --config examples/ollama/worker.toml
```

The worker proxies `llm.chat.v1` and `llm.embed.v1` jobs to the configured OpenAI-compatible upstream. For the end-to-end flow, use the canonical Ollama walkthrough above.

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

## Client execution API

The installed SDK exposes `AnyserveClient.submit(interface, inputs, job_id=...)`,
`invoke(interface, inputs)`, `invoke_stream(interface)`, `watch(job_id)` and `get(job_id)`.
Inputs are bytes or object references; `ExecutionOptions` carries demand and policy;
`max_attempts=0` selects the server default. Submit defaults to durable and needs
an enabled journal. Invoke is ephemeral and closes when its caller disconnects.
See the existing [submitter](https://github.com/anyserve/anyserve/blob/main/examples/python/submit.py)
and [worker](https://github.com/anyserve/anyserve/blob/main/examples/python/worker.py).
There are no named-stream creation or frame-push client APIs.

`anyserve serve` enables `.anyserve/journal.sqlite`; use `serve --ephemeral` for
memory-only execution. Configuration and the 0.4 migration boundary are documented
in [architecture](architecture.md) and [configuration](configuration.md).

## Test the Workspace

```bash
source "$HOME/.cargo/env"
cargo test -p anyserve-core -p anyserve
```
