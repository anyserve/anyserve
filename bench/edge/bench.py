#!/usr/bin/env python3
"""Repeatable edge benchmark for anyserve.

Method (docs/src/benchmark.md):
  1. fingerprint the run (git sha, image, GPU, host)
  2. calibrate: measure the emulated WAN RTTs; abort if they drift
  3. fresh backends, then warm up
  4. paired, interleaved rounds: direct call and anyserve back to back at the
     same RTT; report the median of per-round differences
  5. correctness gates: zero errors, traffic lands where placement says
  6. compare against a baseline run; exit non-zero on regression

Usage:
  bench.py run [--suite quick|full] [--rounds 3] [--repeat 1] [--baseline auto|none|DIR]
  bench.py compare BASELINE_DIR CANDIDATE_DIR
"""
import argparse
import datetime
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
RESULTS = HERE / "results"
TOKEN = os.environ.get("CLIENT_TOKEN", "bench-client-token")
RTT = {"near": 30, "far": 70}
RTT_TOLERANCE_MS = 3
CENTER = "http://172.31.30.10"
POLICY = "http://172.31.20.30:9000/act"
VLLM = "http://172.31.20.20:8000"
IMAGE = "http://172.31.20.40:9100/generate"
INVOKE = f"{CENTER}:8090/v1/invoke"
MODEL = os.environ.get("LLM_MODEL", "Qwen/Qwen2.5-1.5B-Instruct")
IMAGE_BODY = '{"prompt":"a robot arm stacking poker chips, studio photo","steps":1}'

# Regression thresholds on the paired overhead (anyserve minus direct).
LATENCY_SLACK_MS = 2.0      # allowed growth of an overhead, absolute ...
LATENCY_SLACK_REL = 0.20    # ... or relative to the baseline overhead
THROUGHPUT_SLACK = 0.05     # allowed drop of the anyserve/direct throughput ratio


def sh(*args, capture=False, check=True, env=None):
    result = subprocess.run(args, cwd=HERE, text=True, capture_output=capture,
                            env={**os.environ, **(env or {})})
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed:\n{result.stderr or ''}")
    return result.stdout if capture else None


def compose(*args, capture=False, check=True):
    return sh("docker", "compose", "--profile", "image", *args, capture=capture, check=check,
              env={"ANYSERVE_IMAGE": os.environ["ANYSERVE_IMAGE"]})


def bench_exec(*args):
    return compose("exec", "-T", "-e", f"OPENAI_API_KEY={TOKEN}", "bench", *args, capture=True)


def log(message):
    print(f"[{datetime.datetime.now():%H:%M:%S}] {message}", flush=True)


# ---------------------------------------------------------------- environment

def build_image():
    sha = sh("git", "rev-parse", "--short", "HEAD", capture=True).strip()
    dirty = sh("git", "status", "--porcelain", capture=True).strip()
    tag = f"anyserve:bench-{sha}{'-dirty' if dirty else ''}"
    log(f"building {tag}")
    sh("docker", "build", "-q", "-t", tag, str(REPO), capture=True)
    os.environ["ANYSERVE_IMAGE"] = tag
    return sha, bool(dirty), tag


def fingerprint(sha, dirty, tag):
    gpu = sh("nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader",
             capture=True, check=False).strip()
    return {"git_sha": sha, "dirty": dirty, "image": tag,
            "image_id": sh("docker", "image", "inspect", "-f", "{{.Id}}", tag, capture=True).strip(),
            "gpu": gpu, "host_cpus": os.cpu_count(), "load_avg": os.getloadavg()[0],
            "rtt_config_ms": RTT, "model": MODEL,
            "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}


def start_stack():
    log("starting stack")
    compose("stop", "vllm", "image", capture=True, check=False)
    services = ["center", "edge-near", "edge-far", "policy", "bench-net", "bench"]
    # bench shares bench-net's network namespace, so recreate them together.
    compose("up", "-d", "--force-recreate", *services, capture=True)
    wait_for(lambda: invoke_ok(), "policy path through anyserve")


def wait_for(predicate, what, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError(f"timed out waiting for {what}")


def wait_healthy_vllm():
    log("waiting for vLLM")
    wait_for(lambda: compose("ps", "vllm", "--format", "{{.Health}}", capture=True).strip() == "healthy",
             "vLLM health", timeout=900)


def invoke_ok():
    out = bench_exec("curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "-X", "POST",
                     f"{INVOKE}/rl.policy.act.v1", "-H", f"authorization: Bearer {TOKEN}",
                     "-H", "content-type: application/json",
                     "-H", "x-anyserve-require: region=far", "-d", '{"obs":[0.1]}')
    return out.strip() == "200"


def set_bench_rtt(ms):
    compose("exec", "-T", "bench-net", "bash", "-c",
            f"NETEM_RULES='172.31.20.:{ms}' /bench/netem-exec.sh true", capture=True)


def calibrate():
    """Measured TCP connect time edge -> center equals one RTT."""
    measured = {}
    for region, subnet in (("near", "11"), ("far", "12")):
        out = compose("exec", "-T", f"edge-{region}", "sh", "-c",
                      f"for i in 1 2 3 4 5; do curl -s -o /dev/null -w '%{{time_connect}} ' "
                      f"http://172.31.{subnet}.10:8090/healthz; done", capture=True)
        measured[region] = round(statistics.median(float(x) * 1000 for x in out.split()), 1)
        if abs(measured[region] - RTT[region]) > RTT_TOLERANCE_MS:
            raise RuntimeError(f"{region} RTT {measured[region]} ms, expected {RTT[region]} ms")
    log(f"calibrated RTT {measured}")
    return measured


# ---------------------------------------------------------------- workloads

def policy(mode, url, label, raw, extra=(), duration=10):
    out = bench_exec("python3", "/bench/bench_policy.py", mode, "--url", url, "--label", label,
                     "--duration", str(duration), "--concurrency", "16", "--hz", "30",
                     "--warmup", "2", "--out", f"/bench/results/{raw}/policy.jsonl", *extra)
    return json.loads(out.strip().splitlines()[-1])


def llm(url, label, concurrency, raw, round_):
    name = f"llm-{label}-c{concurrency}-r{round_}.json"
    prompts = 12 if concurrency == 1 else concurrency * 4
    bench_exec("vllm", "bench", "serve", "--endpoint-type", "openai-chat", "--backend", "openai-chat",
               "--endpoint", "/v1/chat/completions", "--base-url", url, "--model", MODEL,
               "--tokenizer", MODEL, "--dataset-name", "random", "--random-input-len", "256",
               "--random-output-len", "128", "--ignore-eos", "--num-prompts", str(prompts),
               "--max-concurrency", str(concurrency), "--percentile-metrics", "ttft,tpot,itl,e2el",
               "--save-result", "--result-dir", f"/bench/results/{raw}", "--result-filename", name)
    data = json.loads((RESULTS / raw / name).read_text())
    return {"label": label, "ttft_p50_ms": data["median_ttft_ms"], "tpot_p50_ms": data["median_tpot_ms"],
            "tok_s": data["output_throughput"], "errors": data["num_prompts"] - data["completed"]}


AUTH = ("--token", TOKEN)


def pair_policy(region):
    """(direct, anyserve) runners for the policy path at one region's RTT."""
    direct = lambda mode, raw: (set_bench_rtt(RTT[region]),
                                policy(mode, POLICY, f"direct-{region}", raw))[1]
    placement = ("--header", "x-anyserve-prefer:region=near") if region == "near" \
        else ("--header", f"x-anyserve-require:region={region}")
    anyserve = lambda mode, raw: (set_bench_rtt(0),
                                  policy(mode, f"{INVOKE}/rl.policy.act.v1", f"anyserve-{region}",
                                         raw, AUTH + placement))[1]
    return direct, anyserve


def run_paired(results, raw, rounds, scenario, direct, anyserve, metrics, expect_worker=None):
    """Interleave direct/anyserve per round; collect per-round paired deltas."""
    rows = []
    for round_ in range(1, rounds + 1):
        d, a = direct(raw, round_), anyserve(raw, round_)
        rows.append({"round": round_, "direct": d, "anyserve": a})
        log(f"{scenario} r{round_}: " + ", ".join(
            f"{m} {d[m]:.1f} -> {a[m]:.1f}" for m in metrics))
    gates = []
    for row in rows:
        for arm in ("direct", "anyserve"):
            if row[arm].get("errors"):
                gates.append(f"{scenario} r{row['round']} {arm}: {row[arm]['errors']} errors")
        served = row["anyserve"].get("served_by")
        if expect_worker and served and set(served) != {expect_worker}:
            gates.append(f"{scenario} r{row['round']}: served by {served}, expected {expect_worker}")
    summary = {}
    for metric in metrics:
        d = [row["direct"][metric] for row in rows]
        a = [row["anyserve"][metric] for row in rows]
        deltas = [y - x for x, y in zip(d, a)]
        summary[metric] = {"direct": statistics.median(d), "anyserve": statistics.median(a),
                           "delta": statistics.median(deltas), "delta_min": min(deltas),
                           "delta_max": max(deltas),
                           "ratio": statistics.median(y / x for x, y in zip(d, a) if x)}
    results["scenarios"][scenario] = {"metrics": summary, "rounds": rows}
    results["gates"].extend(gates)


def session_failover(results, raw):
    """30 Hz loop pinned to the near worker; near cluster dies mid-episode."""
    set_bench_rtt(0)
    proc = subprocess.Popen(
        ["docker", "compose", "exec", "-T", "bench", "python3", "/bench/bench_policy.py", "control",
         "--url", f"{INVOKE}/rl.policy.act.v1", *AUTH, "--header", "x-anyserve-prefer:region=near",
         "--sticky", "--hz", "30", "--duration", "12", "--label", "session-failover",
         "--out", f"/bench/results/{raw}/policy.jsonl"],
        cwd=HERE, text=True, stdout=subprocess.PIPE, env={**os.environ})
    time.sleep(8)
    compose("stop", "edge-near", capture=True)
    out, _ = proc.communicate(timeout=120)
    compose("start", "edge-near", capture=True)
    wait_for(lambda: invoke_ok(), "near worker back")
    time.sleep(3)
    r = json.loads(out.strip().splitlines()[-1])
    r.pop("timeline_ms", None)
    results["scenarios"]["session-failover"] = {"result": r}
    if r["errors"]:
        results["gates"].append(f"session-failover: {r['errors']} failed steps")
    if set(r["served_by"]) != {"near-policy", "far-policy"}:
        results["gates"].append(f"session-failover: did not migrate ({r['served_by']})")
    if r["max_ms"] > RTT["far"] + 60:
        results["gates"].append(f"session-failover: {r['max_ms']} ms step during migration")
    log(f"session-failover: errors {r['errors']}, max step {r['max_ms']} ms, {r['served_by']}")


def suite_quick(results, raw, rounds):
    for region in ("near", "far"):
        direct, anyserve = pair_policy(region)
        for mode, metrics in (("rollout", ["p50_ms", "p99_ms", "qps"]), ("control", ["p50_ms", "p99_ms"])):
            run_paired(results, raw, rounds, f"policy-{mode}-{region}",
                       lambda raw, r, m=mode: direct(m, raw), lambda raw, r, m=mode: anyserve(m, raw),
                       metrics, expect_worker=f"{region}-policy")
    session_failover(results, raw)


def suite_llm(results, raw, rounds):
    # Fresh engine per run: vLLM on T4 slows down over long runs.
    compose("up", "-d", "--force-recreate", "vllm", capture=True)
    wait_healthy_vllm()
    for concurrency in (1, 16):
        run_paired(results, raw, rounds, f"llm-c{concurrency}-near",
                   lambda raw, r, c=concurrency: (set_bench_rtt(RTT["near"]), llm(VLLM, "direct-near", c, raw, r))[1],
                   lambda raw, r, c=concurrency: (set_bench_rtt(0), llm(f"{CENTER}:8080", "anyserve-near", c, raw, r))[1],
                   ["ttft_p50_ms", "tpot_p50_ms", "tok_s"])


def suite_image(results, raw, rounds):
    compose("stop", "vllm", capture=True)
    compose("up", "-d", "image", capture=True)
    wait_for(lambda: bench_exec("curl", "-fs", "http://172.31.20.40:9100/healthz") is not None,
             "image backend", timeout=900)
    extra = ("--body", IMAGE_BODY, "--concurrency", "1")
    run_paired(results, raw, rounds, "image-c1-near",
               lambda raw, r: (set_bench_rtt(RTT["near"]),
                               policy("rollout", IMAGE, "direct-near", raw, extra, duration=20))[1],
               lambda raw, r: (set_bench_rtt(0),
                               policy("rollout", f"{INVOKE}/image.generate.v1", "anyserve-near", raw,
                                      extra + AUTH + ("--header", "x-anyserve-require:region=near",
                                                      "--header", "x-anyserve-timeout-ms:60000"),
                                      duration=20))[1],
               ["p50_ms", "qps"], expect_worker="near-image")
    compose("stop", "image", capture=True)


# ---------------------------------------------------------------- reporting

def lower_is_better(metric):
    return not (metric in ("qps", "tok_s"))


def compare(baseline, candidate):
    """Regressions of candidate vs baseline on paired overheads."""
    problems, lines = [], []
    for name, scenario in candidate["scenarios"].items():
        base = baseline["scenarios"].get(name, {}).get("metrics")
        for metric, now in scenario.get("metrics", {}).items():
            if not base or metric not in base:
                continue
            was = base[metric]
            if lower_is_better(metric):
                limit = was["delta"] + max(LATENCY_SLACK_MS, abs(was["delta"]) * LATENCY_SLACK_REL)
                bad = now["delta"] > limit
                lines.append(f"| {name} | {metric} overhead | {was['delta']:+.1f} | {now['delta']:+.1f} | {'REGRESSION' if bad else 'ok'} |")
            else:
                bad = now["ratio"] < was["ratio"] - THROUGHPUT_SLACK
                lines.append(f"| {name} | {metric} ratio | {was['ratio']:.3f} | {now['ratio']:.3f} | {'REGRESSION' if bad else 'ok'} |")
            if bad:
                problems.append(f"{name} {metric}")
    table = ["| scenario | metric | baseline | candidate | verdict |", "|---|---|---|---|---|", *lines]
    return problems, "\n".join(table)


def render(results):
    out = [f"# anyserve edge benchmark `{results['fingerprint']['git_sha']}`", "",
           f"suite {results['suite']}, {results['rounds']} paired rounds, RTT measured "
           f"{results['calibration']}, image `{results['fingerprint']['image']}`", "",
           "| scenario | metric | direct | anyserve | overhead (median, min..max) |",
           "|---|---|---|---|---|"]
    for name, scenario in results["scenarios"].items():
        for metric, m in scenario.get("metrics", {}).items():
            span = f"{m['delta']:+.1f} ({m['delta_min']:+.1f}..{m['delta_max']:+.1f})"
            if not lower_is_better(metric):
                span += f", ratio {m['ratio']:.3f}"
            out.append(f"| {name} | {metric} | {m['direct']:.1f} | {m['anyserve']:.1f} | {span} |")
        if "result" in scenario:
            r = scenario["result"]
            out.append(f"| {name} | errors / max step | | {r['errors']} / {r['max_ms']} ms | {r['served_by']} |")
    out += ["", "Gates: " + ("PASS" if not results["gates"] else "FAIL"), *[f"- {g}" for g in results["gates"]]]
    if results.get("comparison"):
        out += ["", f"## vs baseline `{results['baseline']}`", "", results["comparison"]]
    return "\n".join(out) + "\n"


def previous_run(current):
    runs = sorted(p for p in RESULTS.glob("*/summary.json") if p.parent != current)
    return runs[-1].parent if runs else None


def run_once(args, sha, dirty, tag):
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = RESULTS / f"{stamp}_{sha}_{args.suite}"
    run_dir.mkdir(parents=True)
    raw = run_dir.name
    results = {"suite": args.suite, "rounds": args.rounds, "fingerprint": fingerprint(sha, dirty, tag),
               "scenarios": {}, "gates": []}
    try:
        start_stack()
        results["calibration"] = calibrate()
        suite_quick(results, raw, args.rounds)
        if args.suite == "full":
            suite_llm(results, raw, args.rounds)
            suite_image(results, raw, args.rounds)
        set_bench_rtt(0)
    finally:
        # The bench container writes as root; hand the results back to the caller.
        compose("exec", "-T", "bench", "chown", "-R", f"{os.getuid()}:{os.getgid()}",
                f"/bench/results/{raw}", capture=True, check=False)

    baseline = None if args.baseline == "none" else (
        previous_run(run_dir) if args.baseline == "auto" else Path(args.baseline))
    problems = []
    if baseline and (baseline / "summary.json").exists():
        problems, results["comparison"] = compare(json.loads((baseline / "summary.json").read_text()), results)
        results["baseline"] = baseline.name
        results["regressions"] = problems
    (run_dir / "summary.json").write_text(json.dumps(results, indent=2))
    report = render(results)
    (run_dir / "summary.md").write_text(report)
    print(report)
    return run_dir, not results["gates"] and not problems


def stability(run_dirs):
    """Spread of each paired overhead across repeated runs."""
    runs = [json.loads((d / "summary.json").read_text()) for d in run_dirs]
    lines = ["| scenario | metric | overhead per run | spread |", "|---|---|---|---|"]
    for name, scenario in runs[0]["scenarios"].items():
        for metric in scenario.get("metrics", {}):
            key = "delta" if lower_is_better(metric) else "ratio"
            values = [r["scenarios"][name]["metrics"][metric][key] for r in runs]
            fmt = "{:+.1f}" if key == "delta" else "{:.3f}"
            lines.append(f"| {name} | {metric} {key} | {' / '.join(fmt.format(v) for v in values)} "
                         f"| {fmt.format(max(values) - min(values)).lstrip('+')} |")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--suite", choices=["quick", "full"], default="quick")
    run.add_argument("--rounds", type=int, default=3)
    run.add_argument("--repeat", type=int, default=1)
    run.add_argument("--baseline", default="auto", help="auto (previous run), none, or a run directory")
    cmp = sub.add_parser("compare")
    cmp.add_argument("baseline")
    cmp.add_argument("candidate")
    args = parser.parse_args()

    if args.command == "compare":
        problems, table = compare(*(json.loads((Path(p) / "summary.json").read_text())
                                    for p in (args.baseline, args.candidate)))
        print(table)
        sys.exit(1 if problems else 0)

    sha, dirty, tag = build_image()
    run_dirs, ok = [], True
    for i in range(args.repeat):
        log(f"run {i + 1}/{args.repeat} ({args.suite})")
        run_dir, passed = run_once(args, sha, dirty, tag)
        run_dirs.append(run_dir)
        ok &= passed
    if len(run_dirs) > 1:
        table = stability(run_dirs)
        (run_dirs[-1] / "stability.md").write_text(table + "\n")
        print("## stability across runs\n\n" + table)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
