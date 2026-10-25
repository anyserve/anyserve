# Contributing

The workspace checks use the Rust version in [rust-toolchain.toml](../rust-toolchain.toml),
`protoc`, Python 3.12+ and Node.js. Container and
chart checks also need Docker and Helm. No GPU, broker or external database is
required. Run from the repository root.

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
python scripts/check-doc-links.py
```

Rebuild the Python extension after changing Rust bindings or the protocol.
The SDK process suite starts authenticated servers and workers in temporary
directories. `ANYSERVE_BIN` overrides its default `target/debug/anyserve` binary.

```bash
npm --prefix console ci
npm --prefix console run build
helm lint deploy/helm/anyserve
helm template anyserve deploy/helm/anyserve \
  --set worker.enabled=true --set worker.image=example/worker
docker build -t anyserve .
```

Rust tests cover admission, placement, attempts, deadlines, cancellation,
bounded streams, journal commits/recovery and native TCP services. Python tests
cover handlers, large payloads, duplex output, disconnects and process-kill
recovery. The console build checks TypeScript and production assets.

## Core benchmarks

The Linux load runner builds release `anyserve` and `anyserve-bench`, then
starts a fresh production server and FULL-sync journal per case. Native mock
workers validate echoed payloads, frame counts and terminal success. Errors
during warm-up or measurement fail the run.

```bash
bench/load/run.sh --repeat 2
bench/load/run.sh --cases S1-c16,S3-c16 --duration 5s --warmup 2s --repeat 2
```

| Scenario | Workload |
|---|---|
| S1 | Ephemeral Invoke, 256 B echo as a terminal result |
| S2 | Streamed Invoke, 64 frames of 1 KiB |
| S3 | Durable Submit and Watch, 256 B echo with journal commits |
| S4 | Ephemeral Submit and Watch, 256 B echo |
| S5 | Clients alternate S1 and S3 |

The full matrix uses concurrency 1, 16 and 64, a 5 s warm-up and a 20 s
measurement per case. Each client has one outstanding request; requests started
in the measurement window are drained before computing throughput. Independent
gRPC channels default to `min(concurrency, 8)`. Workers number
`max(4, concurrency / 8)`, each with 16 slots and 2 Tokio threads. Client tasks
use 4 threads; the server uses its production default.

Results in `bench/load/results/<utc>_<sha>/` include JSON/Markdown summaries,
raw results, logs, configuration, environment and 500 ms CPU/RSS samples.
CPU 100% is one logical CPU; worker values are summed. RSS is a sampled peak.
Metadata records the commit, dirty state, benchmark source hash, compiler, CPU,
host load, commands and thread settings. Preserve these artifacts on failure.

Compare matching cases on the same host/configuration. `--repeat 2` requires
each gated p50 to differ by less than 10%; this measures repeatability within
the current checkout, not regression against another commit. For a change,
compare against a separately measured baseline on the same host. S5 uses its
durable-path p50; its aggregate p50 and Invoke latency depend on the mix of
fsync-bound calls. Also compare throughput. Short CI samples do not establish
WAN overhead or real-model throughput.

`bench/load/run.sh --profile` separately samples S1/c64, S2/c16 and S3/c16,
writing folded stacks, a flamegraph and stats per case. Profiling includes
startup/warm-up and is not a latency baseline. `--keep` leaves the final server
and echo workers running and records their PIDs/endpoint in `processes.json`;
stop those processes with SIGTERM when finished.

## CI and releases

Pull requests and main-branch pushes check Rust, the rebuilt SDK, console,
documentation links, a short repeated benchmark, container startup and Helm
rendering. Benchmark artifacts are uploaded even when a gate fails.

Version tags build platform archives, container images and Python distributions.
Release workflows validate that the tag matches the Rust, Python and chart
application versions. Manual runs build and check artifacts; publication requires
a version tag. Manual release runs check the selected ref, or an existing tag
when supplied. PyPI publication uses the repository's `pypi` environment and
trusted publisher configuration.
