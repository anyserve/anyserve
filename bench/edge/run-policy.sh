#!/usr/bin/env bash
# RL rollout + embodied control-loop benchmarks. Output: results/policy.jsonl
set -euo pipefail
cd "$(dirname "$0")"
DUR=${DUR:-20}
OUT=/bench/results/policy.jsonl
TOKEN=${CLIENT_TOKEN:-bench-client-token}
mkdir -p results && : > results/policy.jsonl

bench_rtt() {  # emulate direct WAN calls from the bench client to the backends
  docker compose exec -T bench-net bash -c "NETEM_RULES='172.31.20.:$1' /bench/netem-exec.sh true" >/dev/null
}
run() {  # label url extra-args...
  local label=$1 url=$2; shift 2
  for mode in rollout control; do
    docker compose exec -T bench python3 /bench/bench_policy.py "$mode" --url "$url" \
      --label "$label" --duration "$DUR" --concurrency 16 --hz 30 --out "$OUT" "$@"
  done
}
INVOKE=http://172.31.30.10:8090/v1/invoke/rl.policy.act.v1
AUTH=(--token "$TOKEN")

bench_rtt 0;  run "direct-local"        http://172.31.20.30:9000/act
bench_rtt 30; run "direct-wan-30ms"     http://172.31.20.30:9000/act
bench_rtt 70; run "direct-wan-70ms"     http://172.31.20.30:9000/act
bench_rtt 0
run "anyserve-near-30ms" "$INVOKE" "${AUTH[@]}" --header "x-anyserve-prefer:region=near"
run "anyserve-far-70ms"  "$INVOKE" "${AUTH[@]}" --header "x-anyserve-require:region=far"

# Failover: near cluster disappears; prefer-near traffic spills to far after 20 ms.
docker compose stop edge-near >/dev/null
sleep 2
run "anyserve-failover-to-far" "$INVOKE" "${AUTH[@]}" --header "x-anyserve-prefer:region=near"
docker compose start edge-near >/dev/null

# Embodied session failover: a 30 Hz loop pinned to the near worker; the near
# cluster dies mid-episode and the session migrates to far.
sleep 3
docker compose exec -T bench python3 /bench/bench_policy.py control --url "$INVOKE" "${AUTH[@]}" \
  --header "x-anyserve-prefer:region=near" --sticky --hz 30 --duration 16 \
  --label "anyserve-session-failover" --out "$OUT" &
sleep 9; docker compose stop edge-near >/dev/null
wait
docker compose start edge-near >/dev/null
