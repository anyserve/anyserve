# anyserve Python SDK

This package is a Rust-exported Python SDK built with `PyO3` and `maturin`.

It exposes a small synchronous client surface over the gRPC API:

- `AnyserveClient.infer(...)`
- `AnyserveClient.fetch_one(...)`
- `AnyserveClient.send_responses(...)`

## Local development

```bash
python3 -m pip install --user maturin
python3 -m maturin develop --manifest-path clients/python/Cargo.toml
```

## Example

```python
from anyserve import AnyserveClient, response_created, response_finished, response_processing

client = AnyserveClient("http://127.0.0.1:50052")

responses = client.infer(
    content=b"1 2 3",
    metadata={"model_name": "test"},
    queue="default",
)

for item in responses:
    print(item["request_id"], item["metadata"], item["content"])

request = client.fetch_one(metadata={"model_name": "test"})
if request is not None:
    client.send_responses(
        request["request_id"],
        [
            response_created(),
            response_processing(b"partial"),
            response_finished(),
        ],
    )
```
