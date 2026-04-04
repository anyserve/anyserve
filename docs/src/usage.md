# Usage

## Prerequisites

- `mise`
- `protoc`
- Python 3.9+ if you want the Python SDK

## Bootstrap

```bash
mise trust
mise install
mise run build
```

## Start the Control Plane

```bash
mise exec -- cargo run -p anyserve -- serve
```

gRPC listens on `0.0.0.0:50052`.

The gRPC health service is available on the same port.

Examples use endpoint strings like `http://127.0.0.1:50052` because the generated clients use gRPC channel URIs. That is not a REST endpoint.

## Run the Demo Worker

```bash
mise exec -- cargo run -p anyserve-client -- --mode worker
```

## Submit the Demo Job

```bash
mise exec -- cargo run -p anyserve-client -- --mode submit
```

The demo client submits a job with:

- `interface_name = demo.echo.v1`
- `required_attributes = {"runtime": "demo"}`
- `required_capacity = {"slot": 1}`

It then:

- opens `input.default`
- pushes two input frames
- waits for job events
- pulls `output.default` after completion

## Build the Python SDK

```bash
mise run python-sdk
mise run python-sdk-dev
```

## Python Example

```python
from anyserve import AnyserveClient, FRAME_DATA

client = AnyserveClient("http://127.0.0.1:50052")
# gRPC channel URI used by the generated SDK client.
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

## Test the Workspace

```bash
mise run test
mise run e2e
mise run docs-build
mise run clippy
```
