# Python Examples

These examples exercise the installed `anyserve` Python package.

Before running them locally, install the bindings into your active `mise` Python:

```bash
mise run python-sdk-dev
```

Start the control plane:

```bash
mise exec -- cargo run -p anyserve -- serve
```

Start the example worker:

```bash
mise exec -- python examples/python/worker.py
```

Submit an example job:

```bash
mise exec -- python examples/python/submit.py
```

The worker uses the high-level `@worker(...)` / `serve(...)` API.
The submitter uses the low-level `AnyserveClient` API.
