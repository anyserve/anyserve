"""Fixed Q1/Q2/Q3 load windows. Stop on the first visible request error."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import itertools
import json
from pathlib import Path
import struct
import threading
import time

import numpy as np
from anyserve import AnyserveClient, ExecutionOptions
from worker import INTERFACE, MODEL_SHA, decode, infer, samples, session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["q1", "q2", "q3"], required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    args = parser.parse_args()
    root = args.run_dir
    ledger = json.loads((root / "resource-ledger.json").read_text())
    token = (root / "client.token").read_text().strip()
    pool = samples()
    goldens = {host: dict(np.load(root / f"{host}-goldens.npz"))
               for host in ("a", "b") if (root / f"{host}-goldens.npz").exists()}
    local = threading.local()
    stop = threading.Event()
    counter = itertools.count()
    engine = session(args.model) if args.phase == "q1" else None
    options = ExecutionOptions(required_attributes=dict(run_id=ledger["run_id"], model_sha256=MODEL_SHA),
        preferred_attributes={"host": "a"}, spillover_after_ms=5, queue_timeout_ms=5000,
        total_timeout_ms=30000, max_attempts=2 if args.phase == "q3" else 1)

    def check(index, values, host):
        expected = goldens[host]
        assert len(values) == len(expected)
        for i, value in enumerate(values):
            target = expected[f"o{i}"][index]
            assert value.dtype == target.dtype and value.shape == target.shape
            assert np.isfinite(value).all()
            if np.issubdtype(value.dtype, np.integer):
                np.testing.assert_array_equal(value, target)
            else:
                np.testing.assert_allclose(value, target, rtol=1e-5, atol=1e-6)

    def window(name, concurrency, seconds, direct=False, count=None):
        rows, lock = [], threading.Lock()
        started = time.monotonic()
        until = started + seconds
        tickets = itertools.count()
        if name == "q3-formal":
            (root / "q3-start.json").write_text(json.dumps(dict(time_ns=time.time_ns())) + "\n")
        with (root / f"{name}.jsonl").open("w", buffering=1) as output:
            def task():
                if not direct and not hasattr(local, "client"):
                    local.client = AnyserveClient("http://10.0.3.60:50052", token=token)
                while not stop.is_set():
                    ticket = next(tickets)
                    if (count is not None and ticket >= count) or (count is None and time.monotonic() >= until):
                        return
                    index = next(counter) % 256
                    row = dict(index=index, begin_ns=time.time_ns(), events=[])
                    begin = time.perf_counter_ns()
                    try:
                        if direct:
                            values, host = infer(engine, pool[index]), "a"
                        else:
                            payload = struct.pack("<I", index) + pool[index].tobytes()
                            terminal = None
                            with local.client.invoke(INTERFACE, payload, options=options) as stream:
                                for event in stream:
                                    if event["type"] == "finished":
                                        terminal = event
                                    elif event["type"] != "output":
                                        row["events"].append(dict(observed_ns=time.time_ns(), **event))
                            assert terminal and terminal["state"] == "succeeded", terminal
                            assert max((event.get("attempt_number", 0) for event in row["events"]), default=0) <= options.max_attempts
                            info, values = decode(terminal["outputs"][0]["inline"])
                            assert info["index"] == index
                            host = info["host"]
                        row.update(ms=(time.perf_counter_ns() - begin) / 1e6, end_ns=time.time_ns(), host=host)
                        check(index, values, host)
                    except Exception as exc:
                        row.update(error=f"{type(exc).__name__}: {exc}", end_ns=time.time_ns())
                        stop.set()
                    with lock:
                        rows.append(row)
                        output.write(json.dumps(row, separators=(",", ":")) + "\n")
                        if "error" in row and not (root / "client-error.json").exists():
                            (root / "client-error.json").write_text(json.dumps(row) + "\n")
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(task) for _ in range(concurrency)]
                for future in futures:
                    future.result()
        errors = [row for row in rows if "error" in row]
        if errors:
            raise RuntimeError(f"visible client error in {name}: {errors[0]}")
        latencies = np.array([row["ms"] for row in rows])
        assert len(latencies), "empty measurement window"
        result = dict(name=name, count=len(rows), errors=0, concurrency=concurrency,
                      p50_ms=float(np.percentile(latencies, 50)), p99_ms=float(np.percentile(latencies, 99)),
                      qps=len(rows) / (time.monotonic() - started), hosts=dict(Counter(row["host"] for row in rows)),
                      raw=str(root / f"{name}.jsonl"), p99_samples_sufficient=len(rows) >= 1000)
        print(json.dumps(result), flush=True)
        return result

    result = dict(phase=args.phase, status="FAIL", windows=[])
    try:
        if args.phase == "q1":
            for concurrency in [1, 16]:
                for repeat in [1, 2]:
                    pair = {}
                    for direct in ([True, False] if repeat == 1 else [False, True]):
                        mode = "direct" if direct else "native"
                        name = f"q1-c{concurrency}-r{repeat}-{mode}"
                        window(name + "-warmup", concurrency, 10, direct)
                        pair[mode] = window(name, concurrency, 30, direct)
                        result["windows"].append(pair[mode])
                    delta = pair["native"]["p50_ms"] - pair["direct"]["p50_ms"]
                    if concurrency == 1:
                        assert delta <= 2, f"Q1 p50 increment {delta:.6f} ms > 2 ms"
        elif args.phase == "q2":
            result["failures"] = []
            for concurrency in [16, 64]:
                pair = []
                for repeat in [1, 2]:
                    name = f"q2-c{concurrency}-r{repeat}"
                    window(name + "-warmup", concurrency, 10)
                    current = window(name, concurrency, 30)
                    result["windows"].append(current)
                    if current["hosts"].get("b", 0) == 0:
                        result["failures"].append(f"{name}: B share is zero")
                    pair.append(current)
                if abs(pair[1]["p50_ms"] / pair[0]["p50_ms"] - 1) >= .10:
                    result["failures"].append(f"c{concurrency}: p50 differs >=10%")
        else:
            window("q3-warmup", 64, 10)
            result["windows"].append(window("q3-formal", 64, 30))
            post = window("q3-post", 64, 30, count=500)
            result["windows"].append(post)
            assert post["hosts"] == {"a": 500}, "post-failure requests did not all finish on A"
        result["status"] = "FAIL" if result.get("failures") else "PASS"
    except Exception as exc:
        result["reason"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        (root / f"{args.phase}.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
