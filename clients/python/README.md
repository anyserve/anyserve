# anyserve Python Bindings

This package is a Rust-exported Python binding layer built with `PyO3` and `maturin`.
Its transport implementation comes from `crates/anyserve-client`; the public Python package adds
role-specific facades and higher-level worker helpers on top.

## Install

From PyPI:

```bash
pip install anyserve-runtime
```

The published distribution name is `anyserve-runtime`, while the import package remains:

```python
import anyserve
```

From a local wheel:

```bash
pip install target/wheels/anyserve_runtime-*.whl
```

From local source:

```bash
pip install ./clients/python
```

Source installs build the Rust extension locally, so they require a working Rust toolchain on `PATH`.
Inside this repository, `mise exec -- ...` provides the expected Python and Rust toolchains.

The top-level `AnyserveClient` exposes the generic control-plane surface and also offers
role-specific views through `submitter()` and `worker()`.

It also includes a high-level worker API for function-style handlers:

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

Examples use endpoint strings like `http://127.0.0.1:50052` because the bindings connect through a gRPC channel URI. This is not a REST base URL.

## API Surface

- `submit_job`
- `watch_job`
- `list_jobs`
- `get_job`
- `cancel_job`
- `get_attempt`
- `list_attempts`
- `register_worker`
- `heartbeat_worker`
- `poll_lease`
- `renew_lease`
- `report_event`
- `complete_lease`
- `fail_lease`
- `open_stream`
- `get_stream`
- `list_streams`
- `close_stream`
- `push_frames`
- `pull_frames`

`watch_job(...)` and `pull_frames(...)` return iterators so the Python API preserves streaming
semantics instead of materializing the full stream into a list.

It also includes two convenience constructors:

- `inline_object(...)`
- `uri_object(...)`

And high-level worker helpers:

- `worker(...)`
- `serve(...)`
- `get_handler_spec(...)`

## Local Development

Build and install from local source into the active environment:

```bash
pip install ./clients/python
```

Or install the extension directly into the active environment through `maturin`:

```bash
python3 -m pip install --user maturin
python3 -m maturin develop --manifest-path clients/python/Cargo.toml
```

Live end-to-end validation:

```bash
mise run python-sdk-e2e
```

## PyPI Publishing

The repository is set up to publish the Python package through PyPI Trusted Publishing.

- PyPI project name: `anyserve-runtime`
- GitHub repository: `anyserve/anyserve`
- Workflow path: `.github/workflows/python-publish.yml`
- GitHub environment: `pypi`

For the first release, create a pending Trusted Publisher for `anyserve-runtime` on PyPI,
then run the publish workflow or push a `v*` tag.

## Example

High-level worker example:

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

Low-level submitter example:

```python
from anyserve import AnyserveClient, FRAME_DATA

client = AnyserveClient("http://127.0.0.1:50052")
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
