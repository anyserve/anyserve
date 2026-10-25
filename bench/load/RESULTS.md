# Control-plane load benchmark results, 2026-10-19

Historical measurements for the revisions listed below. The S6 compatibility
gateway and HTTP-based edge suite have since been removed; the current runner
measures native S1–S5. These numbers have not been rerun for the core-only tree.

Host: one `ip-10-0-3-60` instance, Intel Xeon Platinum 8259CL, 16 logical CPUs,
Linux 6.14 (aws), journal on local NVMe ext4. Release builds, production default
Tokio threads for the control plane, mock workers with 2 threads each
(`max(4, C/8)` processes × 16 slots), client with 4 threads and `min(C, 8)`
channels. Every cell starts a fresh control plane with a FULL-sync SQLite
journal, warms up 5 s and measures 20 s. Numbers below are the second of two
matrices per stage (both matrices agreed within 10% on every gated p50;
S5 is gated on its durable sub-row and aggregate throughput only).

| Stage | Commit | Change |
|---|---|---|
| Baseline | `22f0853` | the rebuilt runtime as first pushed |
| Round 1 | `755aaed` | incremental reserved capacity instead of recomputing it per dispatch; fewer lock acquisitions per job; terminal eviction amortised per completion; finished ephemeral logs keep only their outcome |
| Round 2 | `2621295` | evicted jobs and compacted events freed outside the lock; watch notifications off the frame path; `journal_attempts` table removed (5 statements per durable job instead of 7); answered attempt streams drained to EOF |

Throughput, requests per second (higher is better):

| Scenario | C | Baseline | Round 1 | Round 2 | Round 2 vs baseline |
|---|---:|---:|---:|---:|---:|
| S1 native invoke | 64 | 10792 | 11797 | 12359 | +14.5% |
| S2 streamed invoke (64 × 1 KiB) | 16 | 2836 | 2967 | 3487 | +22.9% |
| S2 streamed invoke | 64 | 3216 | 3565 | 4056 | +26.1% |
| S3 durable submit | 16 | 513 | 516 | 589 | +14.8% |
| S3 durable submit | 64 | 706 | 685 | 1018 | +44.3% |
| S4 ephemeral submit | 64 | 10340 | 10772 | 11324 | +9.5% |
| S5 mixed (aggregate) | 64 | 1408 | 1396 | 2046 | +45.4% |
| S6 OpenAI gateway | 64 | 10724 | 11467 | 12068 | +12.5% |

Latency, milliseconds (lower is better):

| Scenario | C | Metric | Baseline | Round 1 | Round 2 |
|---|---:|---|---:|---:|---:|
| S1 | 64 | p50 / p99 / max | 5.51 / 9.93 / 88.0 | 5.31 / 9.12 / 31.7 | 5.07 / 8.70 / 32.1 |
| S2 | 64 | p50 / p99 / max | 18.2 / 88.4 / 133 | 17.5 / 31.9 / 65.1 | 15.3 / 28.2 / 52.8 |
| S3 | 64 | p50 / p99 | 75.0 / 174 | 77.4 / 169 | 43.9 / 130 |
| S5 durable | 64 | p50 / p99 | 73.9 / 164 | 77.3 / 161 | 45.3 / 118 |
| S1 | 16 | p99 | 2.53 | 2.71 | 2.52 |
| S6 | 16 | p99 | 2.46 | 2.63 | 2.45 |

Control-plane peak RSS, MiB:

| Scenario | C | Baseline | Round 1 | Round 2 |
|---|---:|---:|---:|---:|
| S2 | 16 | 1899 | 385 | 391 |
| S2 | 64 | 1994 | 386 | 390 |
| S1 | 64 | 170 | 104 | 107 |

Notes:

- Round 1 raised c16 p99 by 7–9% on S1/S4/S6 because eviction and compaction
  freed memory under the lock on every completion; round 2 moved those frees
  outside the lock and the c16 tails returned to baseline.
- The baseline S2 RSS was finished streaming jobs keeping 64 frames each for the
  retention period; a finished ephemeral job now keeps only its outcome.
- S3 is bound by the serial journal writer. Replaying the journal's statements
  on the bench volume showed that the per-attempt table cost 9 extra WAL frames
  per 8-op batch; removing it cut batch time by 29–41% in the replay.
- Two correctness defects were found by the benchmark and fixed on the way:
  dropping an Invoke/Watch response right after `Finished` exhausted h2's
  local-reset allowance on the client (SDK now consumes the trailer first), and
  the server dropping an answered attempt stream before the worker closed it
  did the same on the worker connection (server now drains it).
- Controls that were ruled out: client connection count (1 vs 8 channels,
  −1.5%), worker fleet size (4 vs 8 processes, +5%), control-plane thread cap
  (4 vs default, +4.4% throughput for +37% CPU).

## 2026-10-23: timer compaction off the runtime lock

Working tree on top of `c1de7ed` (measured before commit), same host and
method as above, S1 at c=64 only, two runs per tree:

| Tree | req/s | p50 ms | p99 ms | p99.9 ms | max ms | samples > 20 ms |
|---|---:|---:|---:|---:|---:|---:|
| `c1de7ed` | 11720 / 11772 | 5.34 / 5.32 | 9.20 / 9.15 | 26.4 / 25.7 | 33.6 / 35.5 | 256 / 256 |
| with the change | 11796 / 11767 | 5.33 / 5.34 | 9.12 / 9.13 | 10.5 / 10.6 | 13.9 / 13.6 | 0 / 0 |

The 256 slow samples per baseline run are four stalls of all 64 in-flight
requests: about every 5 s the maintenance pass rebuilt the timer heap under the
runtime lock, checking each of roughly 60k keys against the job table. Jobs now
cancel their deadline timers when they end, and the timer loop rebuilds the heap
under its own lock only. Throughput and p50 are unchanged, as expected for a
change that touches only the tail.
