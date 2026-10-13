#!/usr/bin/env bash
# Text-to-image (SD-Turbo, 512px, 1 step): long jobs with ~0.5 MB binary output.
# Needs the GPU free: stops vLLM, starts the image backend. Output: results/image.jsonl
set -euo pipefail
cd "$(dirname "$0")"
OUT=/bench/results/image.jsonl
mkdir -p results && : > results/image.jsonl
docker compose stop vllm >/dev/null
docker compose --profile image up -d image >/dev/null
docker compose up -d edge-near edge-far >/dev/null
until docker compose exec -T bench curl -fs http://172.31.20.40:9100/healthz >/dev/null 2>&1; do sleep 5; done
BODY='{"prompt":"a robot arm stacking poker chips, studio photo","steps":1}'
bench_rtt() { docker compose exec -T bench-net bash -c "NETEM_RULES='172.31.20.:$1' /bench/netem-exec.sh true" >/dev/null; }
run() {
  local label=$1 url=$2; shift 2
  for c in 1 4; do
    docker compose exec -T bench python3 /bench/bench_policy.py rollout --url "$url" --label "$label-c$c" \
      --concurrency "$c" --duration 30 --warmup 5 --body "$BODY" --out "$OUT" "$@"
  done
}
bench_rtt 0;  run direct-local    http://172.31.20.40:9100/generate
bench_rtt 30; run direct-wan-30ms http://172.31.20.40:9100/generate
bench_rtt 0;  run anyserve-near-30ms http://172.31.30.10:8090/v1/invoke/image.generate.v1 \
  --token "${CLIENT_TOKEN:-bench-client-token}" --header "x-anyserve-prefer:region=near" --header "x-anyserve-timeout-ms:60000"
