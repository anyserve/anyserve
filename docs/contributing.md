# Contributing

Use the Rust version in [rust-toolchain.toml](../rust-toolchain.toml), `protoc`,
Python 3.12+ and Node.js 24+. Run commands from the repository root.

## Checks

```bash
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo build --locked -p anyserve
python3 -m venv .venv
. .venv/bin/activate
pip install --force-reinstall ./clients/python
python -m unittest discover -s clients/python/tests -p 'test_*.py' -v
python clients/python/tests/sdk_e2e.py -v
npm --prefix console ci
npm --prefix console test
npm --prefix console run build
python scripts/check-doc-links.py
python scripts/check-release-version.py
```

Rebuild the Python extension after changing Rust bindings or the protocol.
The SDK process suite uses temporary authenticated servers/workers;
`ANYSERVE_BIN` overrides its default `target/debug/anyserve` binary.

Run the real PostgreSQL integration suite with Docker:

```bash
python3 scripts/test-postgres.py
```

It creates a disposable PostgreSQL 18 container and isolated schemas, covering
claims, cancellation, takeover fencing, attempt limits, shared cursors, receipts
and bounded reads. The script removes its own container. Alternatively, set
`ANYSERVE_TEST_POSTGRES_URL` to a disposable database with schema creation
privileges and run serially:

```bash
cargo test --locked -p anyserve-core postgres_cluster_ -- --ignored --test-threads=1
```

Deployment checks:

```bash
helm lint deploy/helm/anyserve
helm template anyserve deploy/helm/anyserve \
  --set worker.enabled=true --set worker.image=example/worker
docker build -t anyserve:local .
```

See [deployment](deployment.md) for Worker images, kind and PostgreSQL setup.

## Benchmarks

Build release binaries once and preserve them for comparisons:

```bash
cargo build --release --locked -p anyserve -p anyserve-bench
```

| Runner | Scope / entry point |
|---|---|
| `bench/load/run.sh` | Linux, fresh FULL-sync SQLite journal per case; `--repeat 2`, `--help` for options |
| `bench/load/console.py` | SQLite business traffic with/without ten HTTP observers, plus cold 16 MiB result reads checked for 4 KiB previews |
| `npm --prefix console run bench:analytics -- /tmp/analytics.json` | DuckDB-WASM imports/queries over 10k/100k synthetic metadata rows, three repetitions; excludes browser rendering/network |

The native scenarios are S1 ephemeral Invoke, S2 streamed Invoke (64 × 1 KiB
output frames), S3 durable Submit/Watch, S4 ephemeral Submit/Watch, and S5 mixed
S1/S3. The Linux runner's repeatability gate compares p50 within 10%; S5 gates
its durable path. Throughput and errors must also be checked.

Core scale checks measure queries over 10,000 jobs, dispatch with 129 workers
and recovery of 2,048 durable jobs:

```bash
cargo test --locked --release -p anyserve-core --test runtime_v2 scale_ -- --ignored --nocapture --test-threads=1
```

## CI and releases

[CI](../.github/workflows/anyserve-ci.yml) runs these checks and uploads benchmark
artifacts even when a gate fails. Its Rust job runs the real PG integration
suite serially against a disposable PostgreSQL 18 service.

Version tags build platform archives, container images and Python distributions.
Tags must match Rust, Python and chart application versions. Manual runs without
a tag validate the selected ref; supplying an existing tag republishes its
artifacts, with existing PyPI files skipped.
