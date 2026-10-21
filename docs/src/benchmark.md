# Benchmark Method

`bench/load` measures the native execution control plane with mock workers.
It runs on Linux with Rust, `protoc` and Python 3, without a GPU, external database
or broker. Run from the repository root:

```bash
bench/load/run.sh
bench/load/run.sh
```

Each run builds release `anyserve` and `anyserve-bench` binaries, then executes
S1–S5 at concurrency 1, 16 and 64. Each case has a fresh production control-plane
process, fresh FULL-sync SQLite journal, authentication disabled and management
HTTP disabled. Native worker processes number `max(4, C/8)`, with 16 slots each.

## Workloads

| Scenario | Path | Workload |
|---|---|---|
| S1 `invoke-buffered` | Native Invoke, terminal result | 256 B echo; native Invoke uses Streamed delivery internally, but this worker emits only terminal result objects |
| S2 `invoke-stream` | Native Invoke, output events | 64 frames × 1 KiB followed by an empty terminal result |
| S3 `submit-durable` | Durable Submit, Watch until Finished | 256 B echo with acceptance, attempt-start and terminal journal commits |
| S4 `submit-ephemeral` | Ephemeral Submit, Watch until Finished | 256 B echo without per-job journal writes |
| S5 `mixed` | Clients alternate S1 and S3 | Interference between ephemeral and durable execution |

Connections are established before a 5 s warm-up; the following 20 s are
measured. Each of C clients sends one request at a time. Calls started during
measurement are drained before calculating elapsed time and requests/second.
The driver validates every echoed byte, streamed frame, frame count and terminal
success, then reads gRPC trailers before reusing the channel. Errors in warm-up
or measurement make the run fail.

`client --connections N` controls independent native gRPC channels. Tasks are
assigned round-robin, with `min(C, 8)` channels by default. The control plane uses
its production default Tokio threads; mock workers use 2 threads per process,
and the client uses 4. Client resource sampling helps reveal driver bottlenecks.

## Artifacts and comparison

Results go to `bench/load/results/<utc>_<sha>/`: summary JSON/Markdown, raw client
results, logs, configuration, environment metadata and 500 ms process samples.
Metadata records the checkout and dirty state, benchmark source hash, compiler,
CPU, host load, commands and per-process thread overrides. Only resource samples
inside the client's measurement window enter the report. CPU 100% means one
logical CPU; worker resource figures sum all worker processes. RSS peaks are
sampling maxima rather than continuous peak measurements.

Compare matching rows using `abs(second.p50_ms / first.p50_ms - 1) * 100`;
every gated row must differ by less than 10%. S5 uses durable-subrow p50 and
aggregate throughput as gates. Its Invoke latency and bimodal aggregate p50 are
informational because closed-loop clients switch between Invoke and fsync-bound
Submit. Preserve both runs when a gate fails.

`--keep` leaves the final production server and native echo workers running and
writes their PIDs/endpoint to `processes.json`; stop those PIDs with SIGTERM
when finished. Selected workloads can also run against an existing server:

```bash
target/release/anyserve-bench worker --endpoint http://127.0.0.1:50052 \
  --interface bench.echo.v1 --concurrency 16 --mode echo
target/release/anyserve-bench client --endpoint http://127.0.0.1:50052 \
  --scenario S1 --concurrency 16 --duration 20s --warmup 5s --payload 256 --out result.json
```

For S2, advertise `bench.stream.v1` with `--mode stream:64:1024`. These workers
use the native SDK, including assignment acceptance, credits, heartbeats,
cancellation and reconnect/resume. They are benchmark fixtures, not a built-in
backend proxy in the production CLI.

## Separate CPU profiling

```bash
bench/load/run.sh --profile
```

This builds a separate release artifact with line debug information and samples
S1/c64, S2/c16 and S3/c16 with the production gRPC transport, auth layer and
runtime configuration. SIGTERM/SIGINT writes `flamegraph.svg`, `profile.folded`
and `stats.json`; startup and warm-up are included in profile sampling. Profile
latency is kept separate from baseline results. `pprof` is a benchmark-only
dependency.

The [2026-10-19 report](benchmark-results.md) preserves historical measurements,
including the now-removed S6 compatibility gateway. The former HTTP/LLM edge
suite has been removed. Current protocol overhead on real backends or WAN links
requires a separate paired direct/native test using the same inputs, batch,
concurrency, topology and backend configuration; mock results do not establish
that overhead or model throughput.
