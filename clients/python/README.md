# anyserve Python SDK

This package is a Rust-exported Python SDK built with `PyO3` and `maturin`.

It mirrors the generic control plane surface.

Examples use endpoint strings like `http://127.0.0.1:50052` because the SDK connects through a gRPC channel URI. This is not a REST base URL.

## API Surface

- `submit_job`
- `watch_job`
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

It also includes two convenience constructors:

- `inline_object(...)`
- `uri_object(...)`

## Local Development

```bash
python3 -m pip install --user maturin
python3 -m maturin develop --manifest-path clients/python/Cargo.toml
```

## Example

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

events = client.watch_job(job["job_id"])
for event in events:
    print(event["kind"], event["metadata"])
```
