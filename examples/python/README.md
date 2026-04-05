# Python Examples

These examples exercise the installed `anyserve` Python package.

Before running them locally, install the bindings into a Python environment.

From local source in a clean virtual environment:

```bash
python -m venv .venv
. .venv/bin/activate
pip install ./clients/python
```

Or install them into the active `mise` Python:

```bash
mise run python-sdk-dev
```

Start the control plane:

```bash
mise exec -- cargo run -p anyserve -- serve
```

Start the example worker:

```bash
python examples/python/worker.py
```

Submit an example job:

```bash
python examples/python/submit.py
```

If you are using the active `mise` Python instead of a virtual environment, run:

```bash
mise exec -- python examples/python/worker.py
mise exec -- python examples/python/submit.py
```

The worker uses the high-level `@worker(...)` / `serve(...)` API.
The submitter uses the low-level `AnyserveClient` API.

## Real-Time Video Streaming Demo

This demo keeps one Anyserve job open and streams every captured frame through the data plane.
Frames stay in memory and are encoded to JPEG for transport. The worker processes each frame,
returns an annotated frame on a second stream, and the client displays the returned video.

Install the extra demo dependencies:

```bash
cd examples/python
uv python install 3.12
uv sync --python 3.12
```

Start the control plane:

```bash
cd /path/to/anyserve
mise exec -- cargo run -p anyserve -- serve
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

Press `q` in the OpenCV window to stop the client and close the input stream. The worker will
finish the lease after it drains the final frames and closes the output stream.
