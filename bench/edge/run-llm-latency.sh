#!/usr/bin/env bash
# LLM latency/throughput (c1, c16), interleaved across rounds so engine drift hits every
# arm equally. Output: results/llm-lat-<label>-r<round>.json
set -euo pipefail
cd "$(dirname "$0")"
MODEL=${LLM_MODEL:-Qwen/Qwen2.5-1.5B-Instruct}
ROUNDS=${ROUNDS:-3}
bench_rtt() { docker compose exec -T bench-net bash -c "NETEM_RULES='172.31.20.:$1' /bench/netem-exec.sh true" >/dev/null; }
one() {  # label url round concurrency
  docker compose exec -T -e OPENAI_API_KEY="${CLIENT_TOKEN:-bench-client-token}" bench \
    vllm bench serve --endpoint-type openai-chat --backend openai-chat --endpoint /v1/chat/completions \
      --base-url "$2" --model "$MODEL" --tokenizer "$MODEL" \
      --dataset-name random --random-input-len 256 --random-output-len 128 --ignore-eos \
      --num-prompts $(( $4 == 1 ? 12 : $4 * 4 )) --max-concurrency "$4" --percentile-metrics ttft,tpot,itl,e2el \
      --save-result --result-dir /bench/results --result-filename "llm-lat-$1-c$4-r$3.json" >/dev/null 2>&1
  echo "done $1 c$4 r$3"
}
# Fresh engine: vLLM on T4 slows down over long runs.
docker compose restart vllm >/dev/null
until [ "$(docker compose ps vllm --format '{{.Health}}')" = healthy ]; do sleep 5; done
for r in $(seq 1 "$ROUNDS"); do
  for c in 1 16; do
    bench_rtt 0;  one direct-local       http://172.31.20.20:8000 "$r" "$c"
    bench_rtt 30; one direct-wan-30ms    http://172.31.20.20:8000 "$r" "$c"
    bench_rtt 0;  one anyserve-near-30ms http://172.31.30.10:8080 "$r" "$c"
  done
done
