# Edge testbed

Two emulated clusters behind one control plane, on one GPU host.

| Service | Role |
|---|---|
| `center` | control plane: OpenAI gateway `:8080`, invoke `:8090`, gRPC `:50052` |
| `edge-near` / `edge-far` | workers (`edge-<region>-*.toml`), WAN RTT 30 / 70 ms via `tc netem` |
| `vllm` | Qwen2.5-1.5B-Instruct (LLM) |
| `policy` | MLP actor (RL rollouts, embodied control loop) |
| `image` | SD-Turbo (profile `image`, needs the GPU without vLLM) |
| `bench` | client; `bench-net` netem emulates direct WAN calls for baselines |

```bash
docker volume create anyserve-hf-cache
./bench.py run                 # quick: policy rollout / 30 Hz control / session failover
./bench.py run --suite full    # + LLM and text-to-image (GPU)
./bench.py run --repeat 3      # repeated runs with cross-run spread
```

Method, scenarios and regression thresholds: [docs/src/benchmark.md](../../docs/src/benchmark.md).
Results land in `results/<utc>_<sha>_<suite>/`.

`bench-net` and `bench` share a network namespace; recreate them together (bench.py does).
