# Benchmark Method

For the recorded 2026-10-19 control-plane baseline and two optimization rounds,
see [load benchmark results](benchmark-results.md). Those results identify the
measured revisions and host; they are separate from the edge/GPU suite below.

`bench/edge/bench.py` measures what anyserve adds on top of calling an edge
backend directly, and fails when that cost regresses. Run it after any change
to the runtime, workers, or gateways.

```bash
cd bench/edge
./bench.py run                        # quick suite, 3 paired rounds, compare with previous run
./bench.py run --suite full           # + LLM (vLLM) and text-to-image, needs the GPU
./bench.py run --repeat 3             # repeat whole runs, report spread across runs
./bench.py compare results/A results/B
```

Exit code is non-zero when a correctness gate fails or an overhead regresses.

## Method

1. **Fingerprint.** Each run builds `anyserve:bench-<sha>` from the working tree
   and records git sha, image id, GPU, host load. Results go to
   `bench/edge/results/<utc>_<sha>_<suite>/` (`summary.json`, `summary.md`, raw files).
2. **Calibrate.** WAN links are emulated with `tc netem`, half the RTT on each
   end (near 30 ms, far 70 ms). The run measures TCP connect time edge → center
   and aborts if it is more than 3 ms off.
3. **Fresh backends.** The stack is recreated per run; vLLM is restarted before
   the LLM suite because it slows down over long runs on T4.
4. **Paired, interleaved rounds.** Every scenario first runs one unrecorded
   warm-up round of both arms, then the direct call and the anyserve call back
   to back at the same RTT, for N rounds. The reported
   overhead is the median of per-round differences (min..max shown), so slow
   drift of the backend or host hits both arms equally.
5. **Correctness gates.** Zero errors; anyserve traffic served by the expected
   region's worker (a miss must reproduce on re-run, since a transient stall
   legitimately spills a preferred job); the session-failover scenario must migrate with no failed
   step and no step slower than far RTT + 60 ms.
6. **Regression check.** Against the previous run (or `--baseline DIR`): a p50
   overhead may grow by at most max(2 ms, 20 %), a p99 overhead by at most
   max(5 ms, 50 %) (tails are noisier: across repeated runs p99 overheads moved
   by up to 2.3 ms, p50 by at most 0.3 ms), and a throughput ratio
   (anyserve / direct) may drop by at most 0.05. A flagged scenario is re-run
   once and counts only if the re-run also fails;
   one-off slow scenarios on a shared host are reported as "not reproduced".
   Each scenario records the host load average for triage.

## Scenarios

| Suite | Scenario | Workload | Metrics |
|---|---|---|---|
| quick | `policy-rollout-{near,far}` | 16 closed-loop actors on the MLP policy | p50, p99, QPS |
| quick | `policy-control-{near,far}` | one 30 Hz control loop | p50, p99 |
| quick | `session-failover` | 30 Hz loop pinned to near; near cluster stopped at step 90 | errors, max step, migration |
| full | `llm-c{1,16}-near` | `vllm bench serve`, streaming chat, 256 in / 128 out | TTFT p50, TPOT p50, tok/s |
| full | `image-c1-near` | SD-Turbo 512 px, 1 step (~0.5 MB PNG) | p50, images/s |

Near traffic uses `x-anyserve-prefer: region=near` (the default invoke
spillover applies); far traffic uses `x-anyserve-require: region=far`.

## Limits

One host emulates every cluster: fixed delay, no loss or bandwidth caps, and
the backends share one GPU and CPU. Treat absolute numbers as indicative and
the paired overheads as the signal.

## Control-plane load benchmark

The Linux-only `bench/load` suite uses release binaries, loopback TCP and
`max(4, C/8)` mock worker processes with 16 slots each (4 at C=1/16, 8 at C=64).
It needs Rust, protoc and Python 3;
Docker, a GPU and external services are not required. Run from the repository
root:

```bash
bench/load/run.sh
bench/load/run.sh
```

Each run builds `anyserve` and `anyserve-bench`, then runs every case below at
concurrency 1, 16 and 64. Each case starts a fresh production control-plane
process with a fresh FULL-sync SQLite journal, authentication disabled and the
console disabled. Connections are established before a 5 s warm-up; the next
20 s are measured. Each of the C clients sends one request at a time. Requests
started in that interval are drained before calculating actual elapsed time
and successful requests per second. Failed requests and warm-up errors are
reported separately and make the run fail.

| Scenario | Path | Workload |
|---|---|---|
| S1 `invoke-buffered` | Native Invoke, complete input, terminal result | 256 B echo input/output; the gRPC adapter internally uses Streamed delivery, while this worker emits only terminal result objects |
| S2 `invoke-stream` | Native Invoke, streamed output | 64 frames × 1 KiB, followed by an empty terminal result |
| S3 `submit-durable` | Durable Submit, Watch until Finished | 256 B echo; journal acceptance, attempt start and completion commits |
| S4 `submit-ephemeral` | Ephemeral Submit, Watch until Finished | 256 B echo without journal writes for the job |
| S5 `mixed` | Each client alternates S1 and S3 | Interference between synchronous and durable traffic |
| S6 `openai-gateway` | HTTP chat completions, built-in HTTP worker, mock OpenAI upstream | Non-streaming JSON over persistent HTTP/1.1 connections |

The client validates echo bytes, every streamed frame, frame count and terminal
success, and reads the response trailer before reusing a gRPC channel.
`client --connections N` controls independent native gRPC channels, with tasks
assigned round-robin; the default is min(C, 8). S6 uses C HTTP/1.1 sockets to
preserve C requests in flight. The JSON records actual connection counts.
Every measured successful latency is saved in the case's `client.json`,
alongside p50/p95/p99/max, errors, first error, actual duration and throughput.
`process-samples.jsonl` contains 500 ms `/proc/<pid>/stat` CPU/RSS samples. Only
samples in the client's measurement window enter the summary. CPU 100% means one
fully used logical CPU; worker CPU and RSS sum the case's worker processes. RSS
peaks are the maximum observed at this sampling interval.
The summary also lists client CPU mean/peak, so a driver bottleneck is visible.
S5 includes `S5/invoke` and `S5/durable` subrows with separate latency
percentiles and requests per second. They share the parent measurement window
and resource samples. S5 repeatability and optimization gates use the durable
subrow's p50 and aggregate requests per second, each with deviation below 10%.
The invoke subrow is marked informational: its latency depends on how many
closed-loop clients are currently in the fsync-limited durable phase. This
phase coupling can move its p50 even while durable latency and total throughput
stay stable. The bimodal aggregate p50 also remains a reference.

Results go to `bench/load/results/<utc>_<sha>/summary.json` and `summary.md`.
`environment.json` records the commit, dirty state, benchmark source hash,
compiler, CPU, host load, commands and each process's thread override. The
control plane uses the production default with `TOKIO_WORKER_THREADS` unset;
mock/built-in workers and mock upstream use 2 Tokio threads per process, and
the client uses 4. The summary headers record these thread settings. Compare
matching rows from the two normal runs using
`abs(second.p50_ms / first.p50_ms - 1) * 100`; every scenario must be below 10%,
with S5 using durable p50 and aggregate requests per second as specified above.
Preserve both runs when a row fails this check.

`bench/load/run.sh --keep` leaves the final production server and its native
echo workers running, prints their PIDs and endpoint, and writes `processes.json`.
Stop those PIDs with SIGTERM after use. The standalone commands can also run a
selected load against an existing server:

```bash
target/release/anyserve-bench worker --endpoint http://127.0.0.1:50052 \
  --interface bench.echo.v1 --concurrency 16 --mode echo
target/release/anyserve-bench client --endpoint http://127.0.0.1:50052 \
  --scenario S1 --concurrency 64 --duration 20s --warmup 5s --payload 256 --out result.json
target/release/anyserve-bench upstream --listen 127.0.0.1:18090
```

For S2, run the same worker count advertising `bench.stream.v1` with
`--mode stream:64:1024`. Workers use SDK WorkerSession/Attempt, including
AssignReply, absolute credits, heartbeats, cancellation and reconnect/resume.

CPU sampling runs separately from the baseline:

```bash
bench/load/run.sh --profile
```

This builds a separate release artifact with line debug information, then runs
S1/c64, S2/c16 and S3/c16. The bench-only `serve --profile <out-dir>` assembles
Runtime and the two gRPC services with the production transport settings:
64 MiB message limits, 4 MiB stream/32 MiB connection windows, TCP_NODELAY,
20 s keepalive and the same reset-stream allowance, auth layer and tracing
subscriber. It uses
[pprof 0.15](https://docs.rs/crate/pprof/0.15.0) at 999 Hz with libc/libgcc/pthread
blocked. SIGTERM or SIGINT writes `flamegraph.svg`, `profile.folded` and
`stats.json` into each case directory before exit. Folded stacks preserve
thread names. Stats include journal batch
counts, mean/last batch size and last commit time. Sampling includes startup and
warm-up; profile-run latency numbers are kept separate from baseline numbers.
pprof is a dependency of the benchmark crate only.
