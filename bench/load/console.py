#!/usr/bin/env python3
"""Compare production binaries under business traffic and Console polling.

Uses only Python's standard library and release anyserve/anyserve-bench binaries.
Every case gets an isolated server, ports, FULL-sync journal and four echo workers.
Seeded 16 MiB results are cold (not recovered into memory after server restart).
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import math
import os
import platform
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise RuntimeError(f"process {process.pid} did not stop")


def get(connection, path):
    connection.request("GET", path)
    response = connection.getresponse()
    data = response.read()
    if response.status not in (200, 204):
        raise RuntimeError(f"{path}: HTTP {response.status}: {data[:200]!r}")
    return json.loads(data) if data else None


class Server:
    def __init__(self, args, directory, seed=None):
        self.args, self.directory = args, directory
        directory.mkdir()
        self.grpc, self.http = free_port(), free_port()
        while self.grpc == self.http:
            self.http = free_port()
        self.endpoint = f"http://127.0.0.1:{self.grpc}"
        self.journal = directory / "journal.sqlite"
        if seed:
            shutil.copyfile(seed, self.journal)
        config = directory / "server.toml"
        config.write_text(
            f'[server]\ngrpc_port = {self.grpc}\n'
            f'[console]\nenabled = true\nlisten = "127.0.0.1:{self.http}"\n'
            f'[journal]\npath = {json.dumps(str(self.journal))}\nfull_sync = true\n'
        )
        self.processes = []
        self.spawn([args.server_bin, "serve", "--config", config], "control")
        try:
            self.ready(False)
        except BaseException:
            self.close()
            raise

    def spawn(self, command, role):
        env = dict(os.environ, RUST_LOG="warn")
        env.pop("TOKIO_WORKER_THREADS", None)
        env.pop("ANYSERVE_CLIENT_TOKEN", None)
        if role != "control":
            env["TOKIO_WORKER_THREADS"] = "4" if role == "client" else "2"
        with (self.directory / f"{role}-{len(self.processes)}.log").open("w") as log:
            process = subprocess.Popen(list(map(str, command)), cwd=ROOT, env=env,
                                       stdout=log, stderr=subprocess.STDOUT)
        self.processes.append((role, process))
        return process

    def ready(self, workers):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if any(p.poll() is not None for _, p in self.processes):
                raise RuntimeError(f"benchmark process exited; see {self.directory}")
            connection = http.client.HTTPConnection("127.0.0.1", self.http, timeout=1)
            try:
                body = get(connection, "/api/workers" if workers else "/readyz")
                if not workers or body["healthy"] == 4:
                    return
            except (OSError, http.client.HTTPException):
                pass
            finally:
                connection.close()
            time.sleep(0.05)
        raise RuntimeError(f"server readiness timed out: {self.directory}")

    def workers(self):
        for _ in range(4):
            self.spawn([self.args.bench_bin, "worker", "--endpoint", self.endpoint,
                        "--concurrency", "16"], "worker")
        self.ready(True)

    def close(self):
        for _, process in reversed(self.processes):
            stop(process)


def percentile(values, q):
    return sorted(values)[max(0, math.ceil(q * len(values)) - 1)] if values else None


def http_load(port, seed, end, samples, errors, paths, interval, offset):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    end.wait(offset)
    iteration = 0
    try:
        while not end.is_set():
            cycle = time.monotonic()
            for name, path in paths:
                if end.is_set():
                    break
                if name == "detail":
                    path = "/api/jobs/" + seed["job_ids"][iteration % len(seed["job_ids"])]
                sent, started = time.time(), time.perf_counter()
                try:
                    body = get(connection, path)
                    if name == "detail":
                        output = body["job"]["outputs"][0]
                        assert output["size_bytes"] == seed["payload_bytes"]
                        assert output["truncated"] and len(output["preview"]) == 4096
                        assert body["job"]["demand"] is None, "expected a cold result"
                    samples.append((sent, name, (time.perf_counter() - started) * 1000))
                except Exception as error:
                    errors.append((sent, name, str(error)))
                    connection.close()
            iteration += 1
            end.wait(max(0, interval - (time.monotonic() - cycle)))
    finally:
        connection.close()


def cpu_seconds(value):
    days, clock = value.split("-", 1) if "-" in value else ("0", value)
    result = 0.0
    for component in clock.split(":"):
        result = result * 60 + float(component)
    return result + int(days) * 86400


def sample_resources(processes):
    roles = {p.pid: role for role, p in processes if p.poll() is None}
    result = subprocess.run(["ps", "-o", "pid=,rss=,time=", "-p",
                             ",".join(map(str, roles))], capture_output=True, text=True)
    totals = {}
    for line in result.stdout.splitlines():
        pid, rss, cpu = line.split()
        entry = totals.setdefault(roles[int(pid)], {"rss_mib": 0, "cpu_seconds": 0})
        entry["rss_mib"] += int(rss) / 1024
        entry["cpu_seconds"] += cpu_seconds(cpu)
    return {"unix_s": time.time(), "roles": totals}


def run_case(args, seed_dir, seed, name, repetition):
    directory = args.out / f"{name}-{repetition}"
    server = Server(args, directory, seed_dir / "journal.sqlite")
    ended, samples, errors, resources, threads = threading.Event(), [], [], [], []
    try:
        server.workers()
        connection = http.client.HTTPConnection("127.0.0.1", server.http)
        assert get(connection, "/api/overview")["history"]["total"] == 0
        connection.close()
        cold = name == "cold"
        polling = cold or name.endswith("-console")
        paths = [("detail", "")]
        if not cold:
            paths = [("overview", "/api/overview"),
                     ("jobs", "/api/jobs?limit=100"),
                     ("workers", "/api/workers")] + paths
        begin = time.time()
        client = None if cold else server.spawn([
            args.bench_bin, "client", "--endpoint", server.endpoint,
            "--scenario", name.split("-")[0], "--concurrency", str(args.concurrency),
            "--warmup", f"{args.warmup}s", "--duration", f"{args.duration}s",
            "--out", directory / "business.json"], "client")
        if polling:
            count = 4 if cold else args.operators
            for index in range(count):
                thread = threading.Thread(target=http_load, args=(server.http, seed, ended,
                    samples, errors, paths, 0 if cold else args.poll_interval,
                    0 if cold else index * args.poll_interval / count))
                thread.start()
                threads.append(thread)
        while (client.poll() is None if client else time.time() < begin + args.warmup + args.duration):
            resources.append(sample_resources(server.processes))
            ended.wait(0.5)
        ended.set()
        for thread in threads:
            thread.join()
        business = json.loads((directory / "business.json").read_text()) if client else None
        start = business["measurement_started_unix_s"] if business else begin + args.warmup
        finish = business["measurement_finished_unix_s"] if business else begin + args.warmup + args.duration
        measured = [s for s in samples if start <= s[0] <= finish]
        selected = [s for s in resources if start <= s["unix_s"] <= finish]
        summary = {"case": name, "repetition": repetition, "http": {}, "resources": {},
                   "http_errors": errors,
                   "business": {k: v for k, v in business.items() if k != "latency_samples_ms"} if business else None}
        for path in sorted({s[1] for s in measured}):
            values = [s[2] for s in measured if s[1] == path]
            summary["http"][path] = {"requests": len(values), "rps": len(values) / (finish - start),
                "p50_ms": percentile(values, 0.5), "p95_ms": percentile(values, 0.95),
                "p99_ms": percentile(values, 0.99)}
        for role in ("control", "worker", "client"):
            points = [(s["unix_s"], s["roles"][role]) for s in selected if role in s["roles"]]
            if len(points) > 1:
                summary["resources"][role] = {
                    "rss_peak_mib": max(p[1]["rss_mib"] for p in points),
                    "cpu_mean_percent": 100 * (points[-1][1]["cpu_seconds"] - points[0][1]["cpu_seconds"]) / (points[-1][0] - points[0][0])}
        write_json(directory / "http.json", {"samples": samples, "errors": errors})
        write_json(directory / "resources.json", resources)
        write_json(directory / "summary.json", summary)
        if errors or (client and client.returncode):
            raise RuntimeError(f"benchmark failed; see {directory}")
        print(json.dumps(summary), flush=True)
        return summary
    finally:
        ended.set()
        for thread in threads:
            thread.join()
        server.close()
        for suffix in ("", "-wal", "-shm", ".lock"):
            Path(str(server.journal) + suffix).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin", type=Path, default=ROOT / "target/release/anyserve")
    parser.add_argument("--bench-bin", type=Path, default=ROOT / "target/release/anyserve-bench")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=20)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--operators", type=int, default=10)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--cases", default="cold,S1,S1-console,S3,S3-console")
    args = parser.parse_args()
    if min(args.duration, args.repeat, args.concurrency, args.operators, args.poll_interval) <= 0 or args.warmup < 0:
        parser.error("duration/counts/poll interval must be positive; warmup must be nonnegative")
    cases = args.cases.split(",")
    if any(case not in {"cold", "S1", "S1-console", "S3", "S3-console"} for case in cases):
        parser.error("unknown case")
    args.server_bin, args.bench_bin, args.out = args.server_bin.resolve(), args.bench_bin.resolve(), args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    write_json(args.out / "environment.json", {
        "args": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        "platform": platform.platform(), "cpus": os.cpu_count(), "load_start": os.getloadavg(),
        "git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True),
        "server_sha256": digest(args.server_bin), "bench_sha256": digest(args.bench_bin),
        "runner_sha256": digest(Path(__file__)),
        "scope": "loopback; release; FULL SQLite sync; four workers; 256-byte business echo; four cold 16-MiB results; closed-loop client; default production control threads",
        "http_load": "cold: four continuous readers; business+console: ten operators by default, four sequential queries per poll, evenly staggered; identical HTTP traffic before/after (frontend savings excluded)",
        "cpu_unit": "100% = one logical CPU; ps cumulative CPU sampled every 0.5s",
    })
    seed_server = Server(args, args.out / "seed")
    try:
        seed_server.workers()
        process = seed_server.spawn([args.bench_bin, "seed", "--endpoint", seed_server.endpoint,
                                     "--out", args.out / "seed.json"], "client")
        if process.wait(timeout=120):
            raise RuntimeError(f"seed failed; see {seed_server.directory}")
    finally:
        seed_server.close()
    seed = json.loads((args.out / "seed.json").read_text())
    rows = []
    for repetition in range(1, args.repeat + 1):
        for name in cases if repetition % 2 else reversed(cases):
            rows.append(run_case(args, seed_server.directory, seed, name, repetition))
            write_json(args.out / "summary.json", rows)


if __name__ == "__main__":
    main()
