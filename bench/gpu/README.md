# Native ONNX GPU experiment

Approved baseline: `99f5db83a628dea5e5171342a81ebc50eacc74e0`.
CTO approval: `GO 99f5db83a628dea5e5171342a81ebc50eacc74e0`, 2026-10-21.
The approved plan and rulings are in `/tmp/anyserve-architecture-discussion/`.

The four scripts are one experiment, with no CI or backend changes. `worker.py`
loads the real holdem-spin ONNX model with CUDA. `client.py` runs the fixed
windows and records every request. `launch.py` runs Q1 before creating the only
Spot B and always calls `teardown.py`. The latter also supplies the detached
165-minute reaper. Raw files and credentials stay outside the checkout.

Use an isolated Python 3.12 environment with the release SDK wheel,
`onnxruntime-gpu[cuda,cudnn]==1.23.2` and `numpy==1.26.4`. B installs the same
frozen versions through its NAT; only the locally built SDK wheel is transferred.

After committing these six files, run from the repository root:

```bash
python bench/gpu/launch.py --run-dir /data/homecache/anyserve-aws-gpu/RUN_ID \
  --binary /path/to/verified/release/anyserve --wheel /path/to/release-sdk.whl \
  --model /data/workspace/micro/triton-model-repository/holdem-spin/1/model.onnx
```

The directory must be new. No automatic second instance or fallback is allowed.
Q1 uses separate client and Worker A processes and identical ORT settings.
Every window warms up for 10 seconds and measures for 30 seconds. Q1 runs
c1/c16 in AB/BA pairs; c1 native p50 increment must be ≤2 ms, with zero errors.
Q2 uses A/B at one slot, prefer A/spillover 5 ms, c16/c64, two runs each:
zero errors, formal B share >0, paired p50 difference <10%. Q3 kills B under
c64 load with two attempts allowed. Its report requires unfinished B work
matched to a native AttemptRestarted event, zero visible errors, all subsequent
assignments on A, 500 successful A-only requests and unchanged control-plane
PID/generation. A worker `finish` log records model completion before the SDK
terminal acknowledgement; missing in-flight evidence is inconclusive.

Inputs are 256 seeded standard-normal fp32[288] observations. Outputs must be
finite, match their model dtype/shape, and match each host's direct session
(integer exact; float rtol=1e-5/atol=1e-6). These are latency inputs, not a policy
quality evaluation. GPU evidence is CUDA as first provider plus the worker PID
in nvidia-smi. No operator profile or artificial execution gates are used.

Only one g4dn.xlarge Spot in us-east-1a is allowed, ≤3 GPU hours, target ≤60
minutes. Instance shutdown +180 terminates it; a detached local reaper starts
before provisioning and tears down at +165. B's SG admits SSH only from
10.0.3.60/32. The sole existing-SG change is tcp/50052 from B/32 on
sg-08424b17c71685f62, removed during cleanup with before/after snapshots.
Other existing groups and both Triton services remain untouched.

Stop immediately on Q1 failure, any visible client error, Triton anomaly or
deadline/budget trigger. Cleanup verifies terminated instance, closed Spot
request, no EBS/temporary SG, restored original SG rules and original Triton
processes. Do not publish a performance claim from an interrupted window.
Commit only the final report after collecting cleanup receipts; do not push.
