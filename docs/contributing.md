# Contributing

Use the Rust version in [rust-toolchain.toml](../rust-toolchain.toml), `protoc`,
Python 3.12+ and Node.js. Run from the repository root.

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
The SDK process suite starts authenticated servers and workers in temporary
directories. `ANYSERVE_BIN` overrides its default `target/debug/anyserve` binary.

Deployment checks require Docker and Helm:

```bash
helm lint deploy/helm/anyserve
helm template anyserve deploy/helm/anyserve \
  --set worker.enabled=true --set worker.image=example/worker
docker build -t anyserve .
```

## Benchmarks

The multi-replica integration suite uses real PostgreSQL, isolated schemas and
a TCP load balancer. It covers competing submissions/claims, cancellation,
takeover fencing, attempt limits, shared progress cursors, receipts and bounded
result reads. Run it with Docker (the script removes only its own test container):

```bash
python3 scripts/test-postgres.py
```

Alternatively, set `ANYSERVE_TEST_POSTGRES_URL` to a disposable PostgreSQL database
whose user can create schemas. The same tests can be run directly with
`cargo test --locked -p anyserve-core postgres_cluster_ -- --ignored`.

Core measurements cover queries over 10,000 jobs, dispatch with 129 workers and
256 unmatched queued jobs, and recovery of 2,048 durable jobs. They also run on
macOS without external services:

```bash
cargo test --locked --release -p anyserve-core --test runtime_v2 scale_ -- --ignored --nocapture --test-threads=1
```

The Linux load runner uses release binaries and a fresh FULL-sync journal per
case. Warm-up or measurement errors fail the run:

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

Compare both checkouts on the same idle host and repeat matching cases.
`--repeat 2` checks p50 repeatability within 10%; compare against a separate
baseline to assess regressions. S5 gates its durable-path p50. Check throughput
as well as latency; short local runs do not predict production performance.

Results under `bench/load/results/` include summaries, raw measurements, logs,
environment and CPU/RSS samples. Use `bench/load/run.sh --help` for workload and
profiling options. `--keep` leaves the final processes running and records them
in `processes.json`; stop them with SIGTERM when finished.

For PG active-active benchmarks on macOS or Linux:

```bash
cargo build --release --locked -p anyserve -p anyserve-bench
python3 bench/load/postgres.py --out bench/load/results/pg-after --repeat 3
python3 bench/load/postgres.py --self-check
```

This runs two replicas against disposable Docker PostgreSQL 18, with an isolated
schema per case. Set `ANYSERVE_TEST_POSTGRES_URL` to reuse a test database instead
(requires `psql` and permission to create/drop schemas). The defaults cover 64
and 256 concurrent requests per replica with 256-byte payloads, plus four per
replica with 1 MiB payloads. Override `--cases 64:256,256:256,4:1048576` as needed.
The default `--scenario S3` measures durable Submit **and Watch through verified
output**, not submission-only QPS. Raw latency samples, errors, worker participation, process
CPU/RSS, PG settings, source revision and binary hashes are retained; processes
and schemas are removed automatically. PG CPU/RSS are excluded from process
totals. Use `--server-bin /path/to/baseline/anyserve --source-revision COMMIT`
and the same `--bench-bin` for matching before/after runs on an idle host.
PG runs default to `--payload-pattern random --payload-seed 0`: reproducible
high-entropy bytes generated with SplitMix64 before timing, avoiding the overly
optimistic TOAST compression of repeated bytes. Use `--payload-pattern repeat`
for the historical repeated-x workload. Pattern, seed and generator are recorded
alongside a post-run sample of logical/stored object bytes (excluding row/index
and TOAST page overhead). Rebuild and freeze one new bench binary for both old
and new servers; never compare different client hashes or payload patterns.
The same buffer is reused across jobs: this approximates compressed JPEG/video
bytes, not codecs, real media delivery or production storage capacity. The direct
bench `client` and `seed` commands accept these options but retain `repeat` as
their default, preserving the generic and Console runners' historical workloads.
They select input/echo bytes; S2's fixed streamed output frames are unchanged.

Use S6 to measure durable Submit acknowledgements separately from result verification:

```bash
python3 bench/load/postgres.py --scenario S6 --cases 64:256,256:256 \
  --worker-concurrency 256 --max-jobs 100000 --drain-timeout 120 \
  --out bench/load/results/pg-submit --repeat 2
```

`--worker-concurrency` fixes execution slots per replica independently of client
concurrency; omitting it retains the S3 default of matching each case's client
concurrency. S6 caps total attempts per client, including warmup and failures, at
`--max-jobs` (default 100,000). It records every attempted ID, then verifies complete
echo results within `--drain-timeout` seconds (default 120), including submissions
whose response failed after the job may have committed. Raw client JSON retains
all IDs and ACK latencies. Each client verifies at its Submit concurrency, recorded
as `drain_concurrency`. Jobs get a bounded total timeout of warmup + duration +
30 seconds for an in-flight Submit + drain timeout + 5 seconds, recorded as
`job_total_timeout_ms`. `submit_ack_rps` divides measured ACKs by the union of
actual Submit windows, including in-flight Submit responses and excluding result
drain. The separate `drain` metrics count verified results over the entire run,
including warmup; they are not Submit throughput. Clients enter drain independently,
so one may read results while another still finishes Submit calls. The summary
records `cross_client_submit_drain_overlap_seconds` and its fraction of the Submit
window; compare these alongside throughput, especially when `--max-jobs` stops a
client early.

S6 samples all five job states from the shared PG schema approximately once per
second through submission and drain. Artifacts retain the queue timeline, sampled
peaks, samples immediately before/after submission ends, and final ID reconciliation.
ACKed jobs must exist, committed jobs must have verified results, and the final
queued/running count must be zero. `resources` covers the Submit windows;
`resources_whole_run` covers submission and verification. Both record observed
sample coverage; client resource samples end when the first client exits.
Failed cases retain `failure.json` with the stage, client exit codes and available result counts, plus a database ID/state snapshot when its five-second collection succeeds; incomplete clients have no inferred attempt count.

For Console overhead and large-result previews on macOS or Linux, build both
release binaries and run the standard-library Python runner:

```bash
cargo build --release --locked -p anyserve -p anyserve-bench
python3 bench/load/console.py --out bench/load/results/console-after --repeat 3
```

Use `--server-bin /path/to/baseline/anyserve` with a separate output directory
to compare a saved baseline binary. Keep `--bench-bin` identical between runs.
The runner uses fresh local ports and journals, four echo workers, 64 concurrent
clients, and 20-second measurements after 5 seconds of warm-up. It compares S1
and S3 with and without ten simulated operators issuing four HTTP queries every
500 ms (about 80 requests/s total). A separate case saturates cold detail reads
with four readers. Four verified 16 MiB durable results are seeded before each
run and left on disk after restart. Every detail response is checked for the
original size and a truncated 4 KiB preview. HTTP errors, business errors,
latency samples, CPU/RSS, binary hashes and environment are recorded; measured
processes are stopped automatically. The HTTP load is held constant, so these
comparisons exclude frontend polling savings. These are short, closed-loop
local measurements, not a production capacity or large-media transfer test.

Explore's tests run actual DuckDB-WASM queries, Arrow IPC imports, query bounds,
error recovery and cancellation through a real Worker transport under Node.
The browser UI is tested separately for paging, partial loads and stale responses.
Measure the same engine over 10,000 and 100,000 synthetic metadata rows with
Node 24+, three repetitions per size:

```bash
npm --prefix console run bench:analytics -- /tmp/anyserve-analytics.json
```

This records initialization, 250-row batch imports, SQL timings and Node RSS.
It excludes network transfer, browser rendering and browser Worker messaging;
the 100,000-row engine case is independent of the UI's 32 MiB JSON input cap.
Validate production asset loading in a browser when changing the Worker setup.

## CI and releases

The [CI workflow](../.github/workflows/anyserve-ci.yml) runs these checks and
uploads benchmark artifacts even when a gate fails.

Version tags build platform archives, container images and Python distributions.
Tags must match the Rust, Python and chart application versions. Manual runs
without a tag validate the selected ref without publishing; supplying an
existing tag republishes its artifacts, with existing PyPI files skipped.
