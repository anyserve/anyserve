"""Policy-inference load generator.

rollout: N closed-loop actors (RL self-play / rollout workers) hammer the
         policy; reports throughput and latency percentiles.
control: one fixed-rate control loop (embodied robot at --hz); each action must
         arrive within the control period; reports deadline-miss rate.
"""
import argparse
import asyncio
import json
import random
import statistics
import time

import aiohttp


def pct(values, q):
    if not values:
        return None
    values = sorted(values)
    return round(values[min(len(values) - 1, int(q * len(values)))], 2)


BODY = None  # --body overrides the random observation payload
ERRORS = {}  # "status: body" -> count, for diagnosing failed requests


async def step(session, url, headers, obs_dim, served, sticky=None):
    body = BODY if BODY is not None else {"obs": [random.random() for _ in range(obs_dim)]}
    started = time.perf_counter()
    async with session.post(url, json=body, headers=headers) as response:
        data = await response.read()
        ok = response.status == 200
        worker = response.headers.get("x-anyserve-worker-id", "direct")
    if ok:
        served[worker] = served.get(worker, 0) + 1
    else:
        key = f"{response.status}: {data[:160].decode(errors='replace')}"
        ERRORS[key] = ERRORS.get(key, 0) + 1
    if sticky is not None and ok and worker != "direct":
        # Session affinity: keep this episode on the worker holding its state,
        # spilling to another worker only if that one stops answering.
        sticky["x-anyserve-prefer"] = f"worker_id={worker}"
        sticky["x-anyserve-spillover-ms"] = "10"
    return (time.perf_counter() - started) * 1000, ok


async def rollout(args, session, headers):
    latencies, errors, served = [], 0, {}
    deadline = time.perf_counter() + args.duration

    async def actor():
        nonlocal errors
        while time.perf_counter() < deadline:
            ms, ok = await step(session, args.url, headers, args.obs_dim, served)
            latencies.append(ms) if ok else None
            errors += 0 if ok else 1

    started = time.perf_counter()
    await asyncio.gather(*(actor() for _ in range(args.concurrency)))
    elapsed = time.perf_counter() - started
    return {"requests": len(latencies), "errors": errors, "qps": round(len(latencies) / elapsed, 1),
            "p50_ms": pct(latencies, .5), "p90_ms": pct(latencies, .9), "p99_ms": pct(latencies, .99),
            "served_by": served}


async def control(args, session, headers):
    period = 1.0 / args.hz
    latencies, misses, errors, served = [], 0, 0, {}
    steps = int(args.duration * args.hz)
    sticky = dict(headers) if args.sticky else None
    timeline = []
    next_tick = time.perf_counter()
    for index in range(steps):
        if args.progress and index % 30 == 0:
            print(f"progress {index}", flush=True)
        ms, ok = await step(session, args.url, sticky or headers, args.obs_dim, served, sticky)
        timeline.append(round(ms, 1))
        if not ok:
            errors += 1
            continue
        latencies.append(ms)
        misses += ms > period * 1000
        next_tick += period
        await asyncio.sleep(max(0.0, next_tick - time.perf_counter()))
    return {"hz": args.hz, "budget_ms": round(period * 1000, 1), "steps": len(latencies), "errors": errors,
            "deadline_miss_pct": round(100 * misses / max(1, len(latencies)), 2),
            "p50_ms": pct(latencies, .5), "p99_ms": pct(latencies, .99),
            "jitter_ms": round(statistics.pstdev(latencies), 2) if latencies else None,
            "max_ms": round(max(latencies), 1) if latencies else None,
            "served_by": served, **({"timeline_ms": timeline} if args.sticky else {})}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["rollout", "control"])
    parser.add_argument("--url", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--token")
    parser.add_argument("--header", action="append", default=[], help="name:value")
    parser.add_argument("--duration", type=float, default=20)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--hz", type=float, default=30)
    parser.add_argument("--obs-dim", type=int, default=256)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--sticky", action="store_true", help="control mode: pin the session to one worker")
    parser.add_argument("--progress", action="store_true", help="control mode: print 'progress N' every 30 steps")
    parser.add_argument("--out")
    parser.add_argument("--body", help="fixed JSON request body (e.g. an image prompt)")
    args = parser.parse_args()
    global BODY
    BODY = json.loads(args.body) if args.body else None

    headers = dict(h.split(":", 1) for h in args.header)
    if args.token:
        headers["authorization"] = f"Bearer {args.token}"
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        warm = time.perf_counter() + args.warmup
        while time.perf_counter() < warm:
            await step(session, args.url, headers, args.obs_dim, {})
        result = await (rollout if args.mode == "rollout" else control)(args, session, headers)
    result = {"label": args.label, "mode": args.mode, **result, **({"error_detail": ERRORS} if ERRORS else {})}
    print(json.dumps(result))
    if args.out:
        with open(args.out, "a") as handle:
            handle.write(json.dumps(result) + "\n")


asyncio.run(main())
