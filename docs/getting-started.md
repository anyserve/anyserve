# Getting Started

Anyserve routes opaque inputs to application workers. One server owns scheduling
and optional durable recovery; workers connect outbound and run your code.
Use a server and SDK from the same release.

## Start the server

Install Rust and `protoc`, then run from the repository root:

```bash
cargo build --locked -p anyserve
./target/debug/anyserve serve --config examples/server.toml
```

The example listens for gRPC on `127.0.0.1:50052` and management HTTP on
`127.0.0.1:3001`. It stores durable jobs in `.anyserve/journal.sqlite` relative
to the working directory. `serve --ephemeral` disables durable jobs;
`serve --journal /persistent/journal.sqlite` chooses a persistent path.

## Connect a worker

In a second terminal, install the Python SDK with Python 3.12+:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install ./clients/python
python examples/python/worker.py
```

The example is equivalent to:

```python
from anyserve import serve, worker

@worker(interface="demo.echo.v1", max_active_leases=4)
def echo(payload: bytes) -> bytes:
    return payload

serve(echo)
```

`serve()` and `AnyserveClient()` use `ANYSERVE_ENDPOINT`, falling back to
`http://127.0.0.1:50052`. An explicit endpoint overrides the environment.
Workers own model loading, backend calls, batching and data formats.

## Call or submit

In a third terminal using the same Python environment:

```python
from anyserve import AnyserveClient, ExecutionOptions

client = AnyserveClient()
print(client.call("demo.echo.v1", b"hello",
                  options=ExecutionOptions(total_timeout_ms=5000)))

job = client.submit("demo.echo.v1", b"saved input")
print(job["job_id"], client.result(job["job_id"]))
```

`call()` waits for a bytes result. Its invocation is cancelled if the caller
disconnects before completion. `submit()` returns after acceptance and defaults
to durable execution: input and final results survive a server restart when
the journal is on persistent storage. Closing a Watch does not cancel a Submit.
See the [Python SDK reference](python-sdk.md) for streaming, placement and errors.

## Inspect the server

```bash
./target/debug/anyserve job ls
./target/debug/anyserve job inspect JOB_ID
./target/debug/anyserve job cancel JOB_ID
curl -f http://127.0.0.1:3001/readyz
```

The management listener exposes `/healthz`, `/readyz`, `/metrics` and console
APIs. Run `npm ci && npm run dev` in `console/` to open the development console.
Readiness checks the Runtime and journal; check the Workers view to confirm that
an application has a connected worker advertising the required interface.

For remote access, configure [separate client and worker tokens and TLS](configuration.md).
Clients and the job CLI read `ANYSERVE_TOKEN`; each worker needs a worker-role
token. The console accepts a client token and keeps it in the browser tab's
session storage.

Use [deployment](deployment.md) for containers, Kubernetes and recovery.
[anyserve.universe](https://github.com/anyserve/anyserve.universe) contains
Triton, vLLM, ComfyUI, video and VLA examples, plus a
[configuration form](https://anyserve.github.io/anyserve.universe/setup/).
