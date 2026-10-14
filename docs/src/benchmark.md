# Benchmark Method

`bench/edge/bench.py` measures what anyserve adds on top of calling an edge
backend directly, and fails when that cost regresses. Run it after any change
to the kernel, workers, or gateways.

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
   region's worker; the session-failover scenario must migrate with no failed
   step and no step slower than far RTT + 60 ms.
6. **Regression check.** Against the previous run (or `--baseline DIR`): a p50
   overhead may grow by at most max(2 ms, 20 %), a p99 overhead by at most
   max(5 ms, 50 %) (tails are noisier: across repeated runs p99 overheads moved
   by up to 2.3 ms, p50 by at most 0.3 ms), and a throughput ratio
   (anyserve / direct) may drop by at most 0.05.

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
