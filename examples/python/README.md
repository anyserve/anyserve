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
