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
