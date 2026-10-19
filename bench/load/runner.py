#!/usr/bin/env python3
"""Build, run and sample local benchmark processes without external services."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TICKS = os.sysconf("SC_CLK_TCK")
PAGE_MIB = os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


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
            raise RuntimeError(f"process {process.pid} did not stop after SIGTERM")


def wait_ready(process, log, port=None):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process {process.pid} exited: {log.read_text()[-4000:]}")
        if port is None:
            if "ready " in log.read_text():
                return
        else:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    return
            except OSError:
                pass
        time.sleep(0.025)
    raise RuntimeError(f"process {process.pid} readiness timed out; see {log}")


class Sampler:
    def __init__(self, processes):
        self.processes = processes
        self.previous = {}

    def sample(self):
        now = time.monotonic()
        result = {"unix_s": time.time(), "interval_seconds": 0.0, "processes": []}
        for role, process in self.processes:
            try:
                fields = Path(f"/proc/{process.pid}/stat").read_text().rsplit(")", 1)[1].split()
            except FileNotFoundError:
                if role == "client":
                    continue
                raise
            ticks = int(fields[11]) + int(fields[12])
            previous = self.previous.get(process.pid, (ticks, now))
            elapsed = now - previous[1]
            self.previous[process.pid] = (ticks, now)
            result["interval_seconds"] = max(result["interval_seconds"], elapsed)
            result["processes"].append({
                "role": role, "pid": process.pid,
                "cpu_percent": (ticks - previous[0]) / TICKS / elapsed * 100 if elapsed else 0,
                "rss_mib": int(fields[21]) * PAGE_MIB,
            })
        return result


def resources(samples, start, end):
    selected = [sample for sample in samples if start <= sample["unix_s"] <= end]
    if not selected:
        raise RuntimeError("no process samples in the measurement window")
    result = {"sample_count": len(selected), "processes": {}}
    for role in ("control", "worker", "client", "upstream"):
        totals = []
        for sample in selected:
            processes = [p for p in sample["processes"] if p["role"] == role]
            if processes:
                totals.append((sum(p["cpu_percent"] for p in processes),
                               sum(p["rss_mib"] for p in processes), sample["interval_seconds"]))
        if totals:
            result[role] = {
                "cpu_mean_percent": sum(cpu * elapsed for cpu, _, elapsed in totals) / sum(elapsed for _, _, elapsed in totals),
                "cpu_peak_percent": max(cpu for cpu, _, _ in totals),
                "rss_peak_mib": max(rss for _, rss, _ in totals),
            }
    for process in selected[-1]["processes"]:
        pid = process["pid"]
        values = [p for s in selected for p in s["processes"] if p["pid"] == pid]
        result["processes"][str(pid)] = {
            "role": process["role"],
            "cpu_mean_percent": sum(p["cpu_percent"] for p in values) / len(values),
            "cpu_peak_percent": max(p["cpu_percent"] for p in values),
            "rss_peak_mib": max(p["rss_mib"] for p in values),
        }
    return result


def summary_table(rows, threads=None):
    threads = threads or {"control": "default", "worker": 2, "client": 4}
    lines = [f"| Scenario | C | Connections | Workers | p50 ms | p95 ms | p99 ms | max ms | req/s | errors | CP CPU avg/peak % (threads: {threads['control']}) | CP RSS MiB | Workers CPU avg/peak % (threads/process: {threads['worker']}) | Workers RSS MiB | Client CPU avg/peak % (threads: {threads['client']}) |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    def latency(value):
        return f"{value:.3f}" if value is not None else "—"
    for row in rows:
        control = row["resources"]["control"]
        worker = row["resources"]["worker"]
        client = row["resources"]["client"]
        values = [f"{row['scenario']} {row['name']}", str(row["concurrency"]),
                  str(row.get("connections", row["concurrency"])), str(row.get("worker_processes", 4))]
        values += [latency(row[key]) for key in ("p50_ms", "p95_ms", "p99_ms", "max_ms")]
        values += [f"{row['throughput_rps']:.1f}", str(row["errors"]),
                   f"{control['cpu_mean_percent']:.1f}/{control['cpu_peak_percent']:.1f}", f"{control['rss_peak_mib']:.1f}",
                   f"{worker['cpu_mean_percent']:.1f}/{worker['cpu_peak_percent']:.1f}", f"{worker['rss_peak_mib']:.1f}",
                   f"{client['cpu_mean_percent']:.1f}/{client['cpu_peak_percent']:.1f}"]
        lines.append("| " + " | ".join(values) + " |")
        if row["scenario"] == "S5" and row.get("by_path"):
            for key, name in (("S1", "invoke"), ("S3", "durable")):
                part = row["by_path"][key]
                label = f"S5/{name}" + (" (informational)" if key == "S1" else "")
                values = [label, "—", "—", "—"]
                values += [latency(part[key]) for key in ("p50_ms", "p95_ms", "p99_ms", "max_ms")]
                values += [f"{part['requests'] / row['actual_duration_seconds']:.1f}"] + ["—"] * 6
                lines.append("| " + " | ".join(values) + " |")
    return lines


class Run:
    def __init__(self, profile, keep):
        self.profile, self.keep = profile, keep
        self.env = dict(os.environ, RUST_LOG="warn")
        # The control plane must use the production default even if the shell
        # running this script has a Tokio override. Load roles are sized below.
        self.env.pop("TOKIO_WORKER_THREADS", None)
        if not shutil.which("protoc") and not self.env.get("PROTOC"):
            candidates = sorted((Path.home() / ".cargo/registry/src").glob(
                "*/protoc-bin-vendored-linux-x86_64-*/bin/protoc"))
            if candidates:
                self.env["PROTOC"] = str(candidates[-1])
        self.sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        utc = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        self.out = ROOT / "bench/load/results" / f"{utc}_{self.sha[:8]}"
        self.out.mkdir(parents=True)
        self.processes = []
        self.rows = []
        self.workers_per_case = 4
        self.bin = ROOT / "target/release/anyserve-bench"
        self.server_bin = ROOT / "target/release/anyserve"
        self.meta = {
            "git_sha": self.sha, "utc": utc, "profile": profile,
            "git_status": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True),
            "rustc": subprocess.check_output(["rustc", "-vV"], text=True).strip(),
            "host": platform.node(), "platform": platform.platform(),
            "logical_cpus": os.cpu_count(), "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
            "load_start": os.getloadavg(),
            "tokio_worker_threads": {"control": "default", "worker": 2, "client": 4, "upstream": 2},
            "control_thread_source": "production default; TOKIO_WORKER_THREADS unset",
            "worker_processes": "max(4, concurrency // 8)", "worker_concurrency": 16,
            "native_channel_default": "min(concurrency, 8); tasks round-robin over independently connected channels",
            "http_connections": "one persistent HTTP/1.1 connection per S6 task",
            "duration_seconds": 20, "warmup_seconds": 5, "sample_interval_seconds": 0.5,
            "cpu_percent_unit": "100% is one fully used logical CPU; workers are summed",
            "latency_scope": "successful requests started after warmup; requests crossing the stop clock are drained",
            "runtime_scope": "fresh process and FULL SQLite journal per case; default runtime limits",
            "s1_delivery": "native gRPC Invoke is Streamed internally; S1 returns only final echo objects",
            "s5_comparison": "gate on durable path p50 and aggregate req/s; invoke path is informational due to closed-loop phase coupling",
            "commands": [],
            "process_environments": [],
        }
        digest = hashlib.sha256()
        for path in sorted((ROOT / "bench/load").rglob("*")):
            if path.is_file() and path.suffix in {".rs", ".py", ".toml", ".sh"} and "results" not in path.parts:
                digest.update(path.relative_to(ROOT).as_posix().encode())
                digest.update(path.read_bytes())
        self.meta["tool_sources_sha256"] = digest.hexdigest()
        write_json(self.out / "environment.json", self.meta)

    def build(self):
        env = self.env.copy()
        command = ["cargo", "build", "--release", "--locked", "-p", "anyserve", "-p", "anyserve-bench"]
        if self.profile:
            command = ["cargo", "build", "--release", "--locked", "-p", "anyserve-bench"]
            env["CARGO_PROFILE_RELEASE_DEBUG"] = "1"
            env["CARGO_TARGET_DIR"] = str(ROOT / "target/bench-profile")
            self.bin = ROOT / "target/bench-profile/release/anyserve-bench"
        self.meta["commands"].append(command)
        self.meta["build_debug_info"] = 1 if self.profile else 0
        print(f"building release; log: {self.out / 'build.log'}", flush=True)
        with (self.out / "build.log").open("w") as log:
            subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)

    def spawn(self, command, log, role):
        self.meta["commands"].append([str(arg) for arg in command])
        env = self.env.copy()
        if role != "control":
            env["TOKIO_WORKER_THREADS"] = str(self.meta["tokio_worker_threads"][role])
        with log.open("w") as handle:
            process = subprocess.Popen([str(arg) for arg in command], cwd=ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT)
        self.meta["process_environments"].append({
            "pid": process.pid, "role": role,
            "TOKIO_WORKER_THREADS": env.get("TOKIO_WORKER_THREADS"),
        })
        self.processes.append(process)
        return role, process

    def workers(self, directory, endpoint, scenario, upstream_port):
        workers = []
        for index in range(self.workers_per_case):
            log = directory / f"worker-{index}.log"
            if scenario == 6:
                config = directory / f"worker-{index}.toml"
                config.write_text(f'''base_url = "http://127.0.0.1:{upstream_port}/v1"
provider = "bench"
interfaces = ["llm.chat.v1"]
worker_id = "bench-http-{index}"
max_active_leases = 16
[capacity]
slot = 16
''')
                command = [self.server_bin, "--endpoint", endpoint, "worker", "--config", config]
            else:
                interface = "bench.stream.v1" if scenario == 2 else "bench.echo.v1"
                mode = "stream:64:1024" if scenario == 2 else "echo"
                command = [self.bin, "worker", "--endpoint", endpoint, "--interface", interface, "--concurrency", "16", "--mode", mode]
            item = self.spawn(command, log, "worker")
            if scenario != 6:
                wait_ready(item[1], log)
            workers.append(item)
        return workers

    def case(self, scenario, concurrency, keep):
        self.workers_per_case = max(4, concurrency // 8)
        name = f"S{scenario}-c{concurrency}"
        directory = self.out / name
        directory.mkdir()
        ports = set()
        while len(ports) < 3:
            ports.add(free_port())
        grpc_port, gateway_port, upstream_port = sorted(ports)
        endpoint = f"http://127.0.0.1:{grpc_port}"
        config = directory / "server.toml"
        config.write_text(f'''[server]
grpc_host = "127.0.0.1"
grpc_port = {grpc_port}
[journal]
path = "{directory / 'journal.sqlite'}"
full_sync = true
[auth]
client_tokens = []
worker_tokens = []
[console]
enabled = false
[openai]
enabled = {'true' if scenario == 6 else 'false'}
listen = "127.0.0.1:{gateway_port}"
models = ["bench"]
''')
        log = directory / "server.log"
        if self.profile:
            command = [self.bin, "serve", "--listen", f"127.0.0.1:{grpc_port}", "--journal", directory / "journal.sqlite", "--profile", directory]
        else:
            command = [self.server_bin, "serve", "--config", config]
        control = self.spawn(command, log, "control")
        wait_ready(control[1], log, grpc_port)
        processes = [control]
        if scenario == 6:
            upstream = self.spawn([self.bin, "upstream", "--listen", f"127.0.0.1:{upstream_port}"], directory / "upstream.log", "upstream")
            wait_ready(upstream[1], directory / "upstream.log", upstream_port)
            processes.append(upstream)
        workers = self.workers(directory, endpoint, scenario, upstream_port)
        processes.extend(workers)
        # S6 uses the existing gateway's public readiness endpoint. The native
        # workers signal readiness only after Welcome and Credit.
        if scenario == 6:
            import urllib.request
            deadline = time.monotonic() + 30
            while True:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{gateway_port}/readyz", timeout=1) as ready:
                        if ready.status == 200:
                            break
                except OSError:
                    pass
                if time.monotonic() >= deadline:
                    raise RuntimeError("OpenAI gateway did not become ready")
                time.sleep(0.05)
        client_endpoint = f"http://127.0.0.1:{gateway_port}" if scenario == 6 else endpoint
        raw = directory / "client.json"
        client = self.spawn([self.bin, "client", "--endpoint", client_endpoint, "--scenario", f"S{scenario}", "--concurrency", str(concurrency), "--duration", "20s", "--warmup", "5s", "--payload", "256", "--out", raw], directory / "client.log", "client")
        sampler = Sampler(processes + [client])
        samples = [sampler.sample()]
        print(f"running {name}; control pid={control[1].pid}", flush=True)
        while client[1].poll() is None:
            if any(process.poll() is not None for _, process in processes):
                raise RuntimeError(f"a service exited during {name}")
            time.sleep(0.5)
            if client[1].poll() is None:
                samples.append(sampler.sample())
        with (directory / "process-samples.jsonl").open("w") as output:
            for sample in samples:
                output.write(json.dumps(sample) + "\n")
        if not raw.exists():
            raise RuntimeError(f"client produced no result: {(directory / 'client.log').read_text()}")
        result = json.loads(raw.read_text())
        result.pop("latency_samples_ms")
        result["worker_processes"] = len(workers)
        result["resources"] = resources(samples, result["measurement_started_unix_s"], result["measurement_finished_unix_s"])
        result["raw_result"] = str(raw.relative_to(self.out))
        self.rows.append(result)
        self.summary()
        if client[1].returncode != 0:
            raise RuntimeError(f"{name} failed: {result['first_error']}")
        if keep:
            # Leave this case's native echo workers for the next client task.
            if scenario in (2, 6):
                for _, process in workers:
                    stop(process)
                for role, process in processes:
                    if role == "upstream":
                        stop(process)
                kept_directory = directory / "kept-workers"
                kept_directory.mkdir()
                workers = self.workers(kept_directory, endpoint, 1, upstream_port)
            kept = {"control_plane_pid": control[1].pid, "worker_pids": [p.pid for _, p in workers], "endpoint": endpoint, "interface": "bench.echo.v1"}
            write_json(self.out / "processes.json", kept)
            self.meta["kept_processes"] = kept
            for process in [control[1]] + [p for _, p in workers]:
                self.processes.remove(process)
            print(json.dumps(kept), flush=True)
        else:
            for _, process in workers:
                stop(process)
            for role, process in processes:
                if role == "upstream":
                    stop(process)
            stop(control[1])
            if self.profile:
                if control[1].returncode != 0:
                    raise RuntimeError(f"profile server failed: {log.read_text()[-4000:]}")
                for filename in ("flamegraph.svg", "profile.folded", "stats.json"):
                    if not (directory / filename).stat().st_size:
                        raise RuntimeError(f"empty profile artifact: {name}/{filename}")

    def summary(self):
        self.meta["load_end"] = os.getloadavg()
        write_json(self.out / "environment.json", self.meta)
        write_json(self.out / "summary.json", {"environment": self.meta, "results": self.rows})
        threads = self.meta["tokio_worker_threads"]
        lines = ["# Control-plane load benchmark", "", f"Commit: `{self.sha}`. Mode: {'PROFILE (sampled; not a baseline)' if self.profile else 'BASELINE (no sampling)'}. CPU 100% = one logical CPU; worker values sum the case's worker processes.", "", f"Tokio threads: control={threads['control']} ({self.meta['control_thread_source']}; {len(self.meta['cpu_affinity'])} available logical CPUs), workers={threads['worker']} per process, client={threads['client']}, upstream={threads['upstream']}.", ""]
        lines += summary_table(self.rows, threads)
        lines += ["", "S5 subrows share the parent workload and resource samples; their req/s uses the same elapsed measurement window. S5 gates use durable p50 and aggregate req/s; invoke is informational because its latency depends on the closed-loop clients' durable/fsync phases. Aggregate p50 is reference only."]
        (self.out / "summary.md").write_text("\n".join(lines) + "\n")

    def run(self):
        try:
            self.build()
            cases = [(1, 64), (2, 16), (3, 16)] if self.profile else [(s, c) for s in range(1, 7) for c in (1, 16, 64)]
            for index, (scenario, concurrency) in enumerate(cases):
                self.case(scenario, concurrency, self.keep and index == len(cases) - 1)
            self.summary()
            print(self.out / "summary.md", flush=True)
        except BaseException as error:
            self.meta["failure"] = str(error)
            self.summary()
            raise
        finally:
            for process in reversed(self.processes):
                stop(process)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--keep", action="store_true", help="leave the final production server and echo workers; print their PIDs")
    mode.add_argument("--profile", action="store_true", help="separately sample S1/c64, S2/c16 and S3/c16; emit per-case flamegraphs")
    args = parser.parse_args()
    # Python must run cleanup on SIGTERM as well as Ctrl-C.
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    Run(args.profile, args.keep).run()


if __name__ == "__main__":
    main()
