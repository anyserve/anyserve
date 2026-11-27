# Contributing

Use the Rust version in [rust-toolchain.toml](../rust-toolchain.toml), `protoc`,
Python 3.10+ and Node.js 24+. Run commands from the repository root.

## Checks

```bash
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo clippy --locked -p anyserve-client -p anyserve --features direct --all-targets -- -D warnings
cargo test --locked -p anyserve-client --features direct
cargo test --locked -p anyserve --features direct --test direct_runtime_e2e
cargo test --locked -p anyserve --test rust_ts_differential
cargo build --locked -p anyserve
python3 -m venv .venv
. .venv/bin/activate
pip install --force-reinstall ./clients/python
python -m unittest discover -s clients/python/tests -p 'test_*.py' -v
python clients/python/tests/sdk_e2e.py -v
python clients/python/tests/native_do_e2e.py -v
npm --prefix console ci
npm --prefix console test
npm --prefix console run build
cargo clippy --locked -p anyserve --features embed-console --all-targets -- -D warnings
cargo test --locked -p anyserve --features embed-console serve_console
npm --prefix deploy/cloudflare ci
npm --prefix deploy/cloudflare run typecheck
npm --prefix deploy/cloudflare test
npm --prefix website ci
npm --prefix website run build
python3 scripts/test-devex-scripts.py
python3 scripts/check-doc-links.py
python3 scripts/check-release-version.py
```

Rebuild the Python extension after changing Rust bindings or the protocol.
The SDK process suite uses temporary authenticated servers/workers;
`ANYSERVE_BIN` overrides its default `target/debug/anyserve` binary.

## PostgreSQL integration tests

Run the real PostgreSQL integration suite with Docker:

```bash
python3 scripts/test-postgres.py
```

It creates a disposable PostgreSQL 18 container and isolated schemas, covering
single-leader lease acquisition, standby gating, crash/pause/graceful failover,
fencing, dual-backend scheduling parity and bounded reads. Alternatively, set
`ANYSERVE_TEST_POSTGRES_URL` to a disposable database and run serially:

```bash
cargo test --locked -p anyserve-core postgres_ -- --ignored --test-threads=1
```

## Deployment checks

```bash
helm lint deploy/helm/anyserve
helm template anyserve deploy/helm/anyserve \
  --set worker.enabled=true --set worker.image=example/worker
docker build -t anyserve:local .
```

See [deployment](deployment.md) for worker images, kind, and PostgreSQL setup.

## Releases

Bump versions across Rust, Python, Helm, Compose, and Console in one command:

```bash
python3 scripts/bump-version.py 0.7.0
git commit -am "release: v0.7.0"
git tag -a v0.7.0 -m "v0.7.0"
git push origin main v0.7.0
```

Pushing a `v*` tag triggers [Release](../.github/workflows/python-publish.yml),
which builds platform archives (Linux x86_64/arm64, macOS arm64/x86_64, Windows x86_64),
publishes the GHCR container image, publishes `anyserve-runtime` to PyPI, and creates
the GitHub Release with `SHA256SUMS.txt` and install instructions.
