# Usage

## Prerequisites

- `mise`
- `protoc`
- Redis
- Python 3.9+

## Bootstrap

```bash
mise trust
mise install
mise run build
```

## Initialize Redis metadata

```bash
cargo run -p anyserve -- init redis://127.0.0.1:6379 myserve
```

## Start the servers

```bash
cargo run -p anyserve -- serve redis://127.0.0.1:6379
```

HTTP listens on `0.0.0.0:8848`.

gRPC listens on `0.0.0.0:50052`.

## Run the demo worker

```bash
cargo run -p anyserve-client -- --mode consume
```

## Run the demo producer

```bash
cargo run -p anyserve-client -- --mode produce
```

## Build the Python SDK

```bash
mise run python-sdk
mise run python-sdk-dev
```

Then:

```python
from anyserve import AnyserveClient

client = AnyserveClient("http://127.0.0.1:50052")
responses = client.infer(b"1 2 3", {"model_name": "test"})
```
