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
docker build -t anyserve:edge-dev ../..
docker volume create anyserve-hf-cache
docker compose up -d
./run-policy.sh   # RL rollout + 30 Hz control loop + failover   -> results/policy.jsonl
./run-llm.sh      # vllm bench serve, streaming chat              -> results/llm-*.json
./run-image.sh    # text-to-image, stops vLLM                     -> results/image.jsonl
```

RTTs: `HALF_RTT_NEAR=15 HALF_RTT_FAR=35 docker compose up -d` (half applied on each end).
Recreating `bench-net` requires recreating `bench` (shared network namespace).
