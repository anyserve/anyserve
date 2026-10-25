# Usage

Anyserve runs a native execution control plane. Applications implement workers
using the Rust or Python SDK and keep backend-specific logic in those workers.
Read [architecture](architecture.md) for the execution contract and
[deployment and recovery](deployment.md) for operating boundaries.

## Start the control plane

Build from the repository root with Rust and `protoc` available:

```bash
cargo build -p anyserve
./target/debug/anyserve serve --config examples/server.toml
```

CLI flags override file settings. The example starts native gRPC on
`127.0.0.1:50052`, enables the management listener on `127.0.0.1:3001` and uses
`.anyserve/journal.sqlite`. `serve --ephemeral` disables durable jobs.
Endpoint strings such as `http://127.0.0.1:50052` are gRPC channel URIs.

## Run an application worker

Python 3.12+ is required. Install the SDK from the same checkout as the binary:

```bash
python -m venv .venv
. .venv/bin/activate
pip install ./clients/python
python examples/python/worker.py
```

The example owns its handler and connects outbound. The minimal worker shape is:

```python
from anyserve import serve, worker

@worker(interface="demo.echo.v1", max_active_leases=4)
def echo(payload: bytes) -> bytes:
    return payload

serve(echo, endpoint="http://127.0.0.1:50052")
```

The handler may instead execute a model or call another service. Tensor encoding,
model validation, engine/session lifetime and application-specific retry safety
belong to the worker and its caller. The Runtime routes opaque bytes and metadata.
There is no built-in backend proxy or provider protocol conversion.

## Submit or invoke

From another terminal using the same Python environment:

```bash
python examples/python/submit.py
```

Or call the execution API directly:

```python
from anyserve import AnyserveClient, ExecutionOptions

client = AnyserveClient("http://127.0.0.1:50052")
print(client.call("demo.echo.v1", b"hello", options=ExecutionOptions(total_timeout_ms=5000)))
```

`call` runs a bytes-in, bytes-out handler as an ephemeral Invoke and returns
its single inline result; `execute` returns the result objects of any handler.
`submit` defaults to durable execution
and returns an accepted job; `result`, `watch`, `get` and `cancel` operate by ID.
`invoke_stream` supports live input and output. Use event streams as context
managers when stopping before Finished so unfinished work is cancelled.
See the [Python SDK guide](https://github.com/anyserve/anyserve/blob/main/clients/python/README.md)
and the existing Python/Rust examples for native client and worker APIs.

## Authentication and job CLI

Configure separate client and worker bearer-token lists in `[auth]`. The console
API and metrics require a client token when authentication is enabled. The
console keeps its token in the current browser tab's session storage; CORS
`allowed_origins` controls browser origins independently of authentication.

Set `ANYSERVE_TOKEN` to a client token for native clients and the job CLI:

```bash
anyserve --endpoint http://127.0.0.1:50052 job ls
anyserve --endpoint http://127.0.0.1:50052 job inspect JOB_ID
anyserve --endpoint http://127.0.0.1:50052 job cancel JOB_ID
```

The CLI has no token flag. Application workers pass their worker-role token to
the SDK. Control-plane gRPC TLS is configured under `[server.tls]`.

## Health and metrics

The optional `[console]` listener hosts the management API:

| Endpoint | Purpose | Authentication |
|---|---|---|
| `/healthz` | Process liveness; HTTP 204 while the Runtime is alive | Public probe |
| `/readyz` | Runtime admission readiness; HTTP 503 during shutdown or journal failure | Public probe |
| `/metrics` | Prometheus snapshots of jobs, worker capacity, byte budgets and journal | Client token when enabled |
| `/api/overview`, `/api/jobs`, `/api/workers` | Console views of shared Runtime state | Client token when enabled |

Runtime readiness does not require an available worker for every interface.
Check worker connectivity, advertised capabilities and credits separately when
assessing whether a particular application can execute work. gRPC health alone
also does not establish backend readiness.

Metrics apply to native traffic regardless of which SDK submits it. State gauges
cover retained jobs; expired jobs leave those counts. The former HTTP-gateway
latency histogram is removed with that gateway. The current management exporter
does not provide per-request latency histograms or distributed trace export.

The native client and server accept gRPC messages up to 64 MiB including protobuf
overhead; separate per-object and job budgets are listed in
[configuration](configuration.md). URI objects remain opaque references.
