# Python Examples

`worker.py` is an echo worker on the high-level `@worker` / `serve` API and
`submit.py` submits a durable job to it with `AnyserveClient`. Install the
package, start a control plane, then run them in two terminals:

```bash
pip install ./clients/python
anyserve serve
python examples/python/worker.py
python examples/python/submit.py
```

Workers for real backends live in
[anyserve.universe](https://github.com/anyserve/anyserve.universe): Triton
Inference Server, vLLM, ComfyUI and live video.
