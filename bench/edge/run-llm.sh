#!/usr/bin/env bash
# LLM serving benchmarks with `vllm bench serve` (streaming chat, random 256-in/128-out).
# Output: results/llm-<label>-c<concurrency>.json
set -euo pipefail
cd "$(dirname "$0")"
MODEL=${LLM_MODEL:-Qwen/Qwen2.5-1.5B-Instruct}
CONCURRENCY=${CONCURRENCY:-"1 16 64"}
mkdir -p results

bench_rtt() {
  docker compose exec -T bench-net bash -c "NETEM_RULES='172.31.20.:$1' /bench/netem-exec.sh true" >/dev/null
}
run() {  # label base-url
  local label=$1 url=$2
  for c in $CONCURRENCY; do
    local prompts=$(( c == 1 ? 16 : c * 8 ))
    docker compose exec -T -e OPENAI_API_KEY="${CLIENT_TOKEN:-bench-client-token}" bench \
      vllm bench serve --endpoint-type openai-chat --backend openai-chat --endpoint /v1/chat/completions \
        --base-url "$url" --model "$MODEL" --tokenizer "$MODEL" \
        --dataset-name random --random-input-len 256 --random-output-len 128 --ignore-eos \
        --num-prompts "$prompts" --max-concurrency "$c" \
        --percentile-metrics ttft,tpot,itl,e2el --metric-percentiles 50,99 \
        --save-result --result-dir /bench/results --result-filename "llm-$label-c$c.json" \
      | grep -E "Successful requests|Failed requests|Output token throughput|Median TTFT|P99 TTFT|Median TPOT|Median E2EL" | sed "s/^/[$label c$c] /"
  done
}

bench_rtt 0;  run direct-local    http://172.31.20.20:8000
bench_rtt 30; run direct-wan-30ms http://172.31.20.20:8000
bench_rtt 0;  run anyserve-near-30ms http://172.31.30.10:8080
docker compose stop edge-near >/dev/null; sleep 2
run anyserve-far-70ms http://172.31.30.10:8080
docker compose start edge-near >/dev/null
