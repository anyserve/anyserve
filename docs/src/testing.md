# Testing

The acceptance gates use the workspace sources, the rebuilt Python wheel and the
console lockfile. No GPU, external database or broker is required for these
checks.

## Rust

With Rust and protoc available:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Core unit and `runtime_v2` integration tests cover capability matching, admission,
attempt budgets, stream exposure, cancellation, three memory budgets, SQLite
acceptance/terminal receipts, restart recovery and cold reads. CLI tests exercise
real native TCP services, token roles, management reads/health checks, large
payloads and worker cancellation. SDK tests cover delivery backoff and negotiated retry windows.

## Python

Build and install the current extension before testing; an older installed wheel
can hide protocol or binding regressions. Python 3.12+ is required.

```bash
cargo build -p anyserve
python -m pip install --force-reinstall ./clients/python
python -m unittest discover -s clients/python/tests -p 'test_*.py' -v
python clients/python/tests/sdk_e2e.py -v
```

`ANYSERVE_BIN` may point the existing process suite at a specific current binary;
its default is `target/debug/anyserve` from the repository root. The suite starts
its own authenticated control-plane processes and Python workers in temporary
directories. It covers durable idempotency, large payloads, duplex output before
InputEnd, disconnect cancellation, process-kill recovery and typed RPC errors.
The smaller unit suite covers handler dispatch, codecs and failure handling.

## Console

```bash
cd console
npm ci
npm run build
```

This checks both TypeScript projects and builds the production assets. Backend
console tests run as part of `cargo test --workspace`.

The [acceptance record](redesign.md) records historical outcomes and limitations,
including adapters that have since been removed.
GPU demos and historical performance reports are separate from these gates; they
must not be interpreted as current execution-protocol benchmark results.
