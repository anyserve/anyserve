# Python Examples

`worker.py` is an echo worker on the high-level `@worker` / `serve` API and
`submit.py` submits a durable job to it with `AnyserveClient`. Install the
package, start a control plane, then run the worker and submitter in separate
terminals:

```bash
cargo build --locked -p anyserve
pip install ./clients/python
./target/debug/anyserve serve
python examples/python/worker.py
python examples/python/submit.py
```

Run from the repository root with Python 3.12+. See
[getting started](../../docs/getting-started.md) for the virtual environment and
the three-terminal walkthrough.

Workers for real backends live in
[anyserve.universe](https://github.com/anyserve/anyserve.universe): Triton
Inference Server, vLLM, ComfyUI and live video.
