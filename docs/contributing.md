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
python3 bench/load/postgres.py --self-check
```

| Runner | Scope / entry point |
|---|---|
| `bench/load/run.sh` | Linux, fresh FULL-sync SQLite journal per case; `--repeat 2`, `--help` for options |
| `bench/load/postgres.py` | macOS/Linux, two active replicas and disposable PG 18 schemas; S3 defaults to complete durable Submit/Watch round trips |
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

### PostgreSQL traffic and Console reads

```bash
python3 bench/load/postgres.py --scenario S6 --replicas 2 --cases 256:256 \
  --worker-concurrency 256 --submit-rate 1000 --warmup 30 --duration 300 \
  --max-jobs 400000 --drain-timeout 300 --queue-sample-interval 10 \
  --console-operators 10 --repeat 1 --out bench/load/results/pg-paced
```

This requests 2,000 sends/s across two clients. S6 measures durable Submit ACKs
and then separately verifies complete echo results and reconciles actual job IDs
against PG. Omitting `--submit-rate` uses a bounded closed loop. The default S3
scenario includes result delivery in each measured round trip.

| Option / artifact | Meaning |
|---|---|
| `--cases` | Per-replica client concurrency:payload bytes; defaults `64:256,256:256,4:1048576` |
| `--worker-concurrency` | Slots per replica; defaults to each case's client concurrency |
| `--max-jobs` | Attempt cap per S6 client, including warmup/failures; default 100,000; keep above requested warmup + measurement volume |
| `--drain-timeout` | Result-verification deadline, default 120 seconds; clients drain independently and may overlap another client's sends |
| `--payload-pattern`, `--payload-seed` | PG runner defaults to reproducible high-entropy `random`, seed 0; `repeat` reproduces the historical repeated-x workload |
| `rate_control` | Actual sends, skipped allocations, in-flight peak and sending window; no waiting send queue or catch-up bursts |
| `submit_ack_rps`, `drain` | ACKs over the union of Submit windows including response tails; separately, verified results over the whole run including warmup |
| `queue`, `id_reconciliation` | Shared PG state samples, backlog growth and full attempted/ACKed/committed/verified ID reconciliation |
| `resources`, `resources_whole_run` | Process CPU/RSS over Submit windows or whole run, with sample coverage; excludes PG and Python observer CPU |

PG runs use fresh schemas and remove their processes and schemas on exit.
`ANYSERVE_TEST_POSTGRES_URL` reuses a disposable database instead of Docker;
this needs `psql` and schema creation/drop privileges. S6 must verify every
committed job and finish with no queued/running jobs. Failed runs retain their
stage, available client results and a bounded database snapshot in `failure.json`.
Never treat missing client output as a measured attempt count.

`--console-operators` distributes serial 500 ms polling cycles across replicas:
overview, latest jobs, workers and a listed job's detail. Compare `0` and `10`
with otherwise identical settings. `console-http.json` records actual timings,
status, body bytes and errors through drain; every operator must exercise all
four operations during measurement. Threads stop before server cleanup.
The stock runner requests `/api/jobs?limit=100`; actual Queue/History pages use
state filters. Test these separately against a large completed history,
including an empty Queue, rather than extrapolating from the latest-jobs route.
Overview and worker counters are replica-local; use PG queue samples for backlog.

### Comparing and interpreting runs

- Use the same idle host, database durability settings, client binary hash,
  payload pattern/seed, concurrency and sampling interval. Freeze server binaries
  and record their source revisions; `--server-bin`, `--bench-bin` and
  `--source-revision` select them. Use separate output directories.
- Repeat matching cases in A/B/B/A order. Check actual sends, skipped allocations,
  completed jobs/s and pending-job growth, alongside p50/p95/p99. A configured
  rate or eventual successful drain does not establish sustained capacity.
- The default queue interval is one second. Increasing it to ten reduces count
  overhead but misses peaks. Completion estimates use samples wholly within all
  clients' overlapping send windows and may include warmup jobs.
- Random payloads reuse one deterministic buffer per client. They approximate
  compressed bytes, not media codecs or distinct assets. URI objects, 5 MiB
  deployment smoke tests and bounded Console previews do not establish media
  throughput or production storage capacity.
- Browser/Explore performance is separate from HTTP polling. Frontend tests run
  actual WASM queries and Arrow imports under Node; verify production assets in
  a browser when changing Worker loading. The 100k-row engine benchmark bypasses
  the UI's 32 MiB JSON load cap.

Results under `bench/load/results/` retain environment, hashes, raw measurements,
logs and resource samples; local results are ignored by Git. The Linux runner's
`--keep` leaves the final processes listed in `processes.json`; stop them with
SIGTERM. Short local runs do not predict production capacity.

### Recorded local baseline (2026-11-01)

These are macOS arm64 / Docker PostgreSQL 18.6 observations for server commit
`f0e0b05`, using 256 B repeated payloads. They are reference measurements, not
release capacity guarantees.

| Check | Observed result / scope |
|---|---|
| Empty Queue, 119,992 completed jobs | Same-data A/B/B/A against `3fd8683`, fresh process per case, 100 warmup and 100 measured cycles per endpoint: P50 41.55/42.12 ms before, 1.99/0.68 ms after |
| Concurrent tasks and real Queue/History filters | Two replicas, 256 client/worker slots each, 5 Queue + 5 History observers: 120,000 IDs fully verified, zero business/HTTP errors, 1,998.49 Submit ACKs/s over a 60-second sending window; result drain is separate |

The longer 2,000 sends/s runs showed backlog growth in one control case, so
stable 2,000 completed jobs/s remains unproven. Application-instance pause and
stale-result fencing were tested separately at 100 sends/s; PG primary failover, full-load
failover and large-media throughput were not established by these runs.

## CI and releases

[CI](../.github/workflows/anyserve-ci.yml) runs these checks and uploads benchmark
artifacts even when a gate fails. Its Rust job runs the real PG integration
suite serially against a disposable PostgreSQL 18 service.

Version tags build platform archives, container images and Python distributions.
Tags must match Rust, Python and chart application versions. Manual runs without
a tag validate the selected ref; supplying an existing tag republishes its
artifacts, with existing PyPI files skipped.
