# Python Examples

These examples exercise the installed `anyserve` Python package against a running `anyserve` control plane.

Before running them locally, install the bindings into a Python environment.

From local source in a clean virtual environment:

```bash
python -m venv .venv
. .venv/bin/activate
pip install ./clients/python
```

Start the control plane:

```bash
anyserve serve
```

Start the example worker:

```bash
python examples/python/worker.py
```

Submit an example job:

```bash
python examples/python/submit.py
```

The worker uses the high-level `@worker(...)` / `serve(...)` API.
The submitter uses the low-level `AnyserveClient` API.

## Real-Time Video Streaming Demo

This existing demo uses ephemeral duplex Invoke for JPEG frames and annotated
output events. It rotates invocations before the input replay budget fills, and
processes each frame before sending the next one. Video processing requires the
optional OpenCV and model dependencies.

Install the extra demo dependencies:

```bash
cd examples/python
uv python install 3.12
uv sync --python 3.12
```

Start the control plane:

```bash
cd /path/to/anyserve
anyserve serve
```

Start the real-time video worker:

```bash
cd examples/python
uv run python video_stream_worker.py --device auto
```

Start the local camera demo:

```bash
cd examples/python
uv run python video_stream_client.py --source 0
```

Useful variants:

```bash
# Force Apple Silicon GPU
uv run python video_stream_worker.py --device mps

# Use a local mp4 instead of the webcam
uv run python video_stream_client.py --source ./demo.mp4

# Switch model or tracker
uv run python video_stream_worker.py --model yolo11s.pt --tracker bytetrack.yaml

# Run without opening an OpenCV window
uv run python video_stream_client.py --source ./demo.mp4 --headless --max-frames 120
```

Press `q` in the OpenCV window to stop capture and finish the current invocation.
Closing an active invocation cancels it. No named input/output streams are used.

## Triton Inference Server

`triton_worker.py` serves one Triton model through anyserve. The payload is the
KServe v2 inference request that Triton accepts on `/v2/models/<name>/infer`,
and the result is Triton's response body, so the worker needs no knowledge of
the model's tensors. Triton keeps model loading and dynamic batching; anyserve
adds placement, deadlines and failover across hosts.

```bash
python examples/python/triton_worker.py --triton http://127.0.0.1:8000 --model MODEL
```

Call it with a KServe v2 request on the interface `triton/MODEL`:

```python
import json
from anyserve import AnyserveClient

client = AnyserveClient("http://127.0.0.1:50052")
request = {"inputs": [{"name": "input", "shape": [1, 4], "datatype": "FP32", "data": [0.1, 0.2, 0.3, 0.4]}]}
response = json.loads(client.call("triton/MODEL", json.dumps(request).encode()))
print(response["outputs"])
```

Set `--concurrency` to the model's instance count so Triton's batcher sees
enough concurrent requests. When Triton is unreachable the worker reports a
retryable failure and the control plane can run the job on another worker.
