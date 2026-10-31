#!/usr/bin/env python3
"""Compare PG active-active durable Submit+Watch round trips on saved binaries.

Uses disposable Docker PostgreSQL 18 unless ANYSERVE_TEST_POSTGRES_URL is set
(external databases require psql). Each case owns and removes a unique schema.
"""
from __future__ import annotations

import argparse
import contextlib
import http.client
import json
import os
import platform
import secrets
import shutil
import signal
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit

from console import ROOT, digest, free_port, get, percentile, sample_resources, stop, write_json
from runner import wait_ready


def postgres_env(url):
    parsed = urlsplit(url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise ValueError("expected a PostgreSQL URL with a host")
    values = {"PGHOST": unquote(parsed.hostname), "PGPORT": str(parsed.port or 5432),
              "PGDATABASE": unquote(parsed.path.lstrip("/")),
              "PGUSER": unquote(parsed.username or ""), "PGPASSWORD": unquote(parsed.password or "")}
    # libpq does not expand a URI supplied through PGDATABASE.
    options = {key: "PG" + key.upper().replace("_", "") for key in (
        "host", "hostaddr", "port", "user", "password", "options", "sslmode", "sslcert",
        "sslkey", "sslrootcert", "sslcrl", "sslcrldir", "sslsni", "channel_binding",
        "target_session_attrs", "gssencmode", "krbsrvname", "service", "passfile")}
    options.update(dbname="PGDATABASE", application_name="PGAPPNAME", connect_timeout="PGCONNECT_TIMEOUT")
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key not in options:
            raise ValueError(f"unsupported psql URL option: {key}")
        values[options[key]] = value
    return {key: value for key, value in values.items() if value}


@contextlib.contextmanager
def database():
    url = os.environ.get("ANYSERVE_TEST_POSTGRES_URL")
    container = None
    try:
        if url:
            if not shutil.which("psql"):
                raise RuntimeError("external PostgreSQL requires psql in PATH")
            command = ["psql"]
            sql_env = dict(os.environ, PGCONNECT_TIMEOUT="5")
            sql_env.update(postgres_env(url))
        else:
            container = "anyserve-pg-bench-" + uuid.uuid4().hex[:12]
            password = secrets.token_hex(24)
            with tempfile.TemporaryDirectory(prefix="anyserve-pg-bench-") as temporary:
                config = Path(temporary) / "postgres.env"
                config.write_text(f"POSTGRES_USER=anyserve\nPOSTGRES_DB=anyserve\nPOSTGRES_PASSWORD={password}\n")
                config.chmod(0o600)
                subprocess.run(["docker", "run", "-d", "--rm", "--name", container,
                    "-p", "127.0.0.1::5432", "--env-file", str(config),
                    "postgres:18-bookworm"], check=True, stdout=subprocess.DEVNULL)
            ports = json.loads(subprocess.check_output([
                "docker", "inspect", "--format", "{{json .NetworkSettings.Ports}}", container]))
            url = f"postgresql://anyserve:{password}@127.0.0.1:{ports['5432/tcp'][0]['HostPort']}/anyserve"
            command = ["docker", "exec", "-i", container, "psql", "-h", "127.0.0.1", "-U", "anyserve", "-d", "anyserve"]
            sql_env = os.environ.copy()

        def sql(statement):
            # Credentials stay out of argv, artifacts and errors.
            result = subprocess.run(command + ["-XAt", "-v", "ON_ERROR_STOP=1"],
                input=statement, env=sql_env,
                capture_output=True, text=True, timeout=30)
            if result.returncode:
                raise RuntimeError("PostgreSQL command failed: " + result.stderr.replace(url, "<database>"))
            return result.stdout.strip()

        deadline = time.monotonic() + 60
        while True:
            try:
                sql("SELECT 1;")
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        yield url, sql
    finally:
        if container:
            subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def aggregate(results):
    latencies = [value for result in results for value in result["latency_samples_ms"]]
    start = min(result["measurement_started_unix_s"] for result in results)
    finish = max(result["measurement_finished_unix_s"] for result in results)
    return {
        "requests": sum(result["requests"] for result in results),
        "errors": sum(result["errors"] for result in results),
        "warmup_errors": sum(result["warmup_errors"] for result in results),
        "throughput_rps": len(latencies) / (finish - start),
        "p50_ms": percentile(latencies, 0.5), "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99), "max_ms": max(latencies, default=None),
        "measurement_started_unix_s": start, "measurement_finished_unix_s": finish,
        "start_skew_ms": 1000 * (max(result["measurement_started_unix_s"] for result in results) - start),
        "clients": [{k: v for k, v in result.items() if k != "latency_samples_ms"} for result in results],
    }


def run_case(args, url, sql, concurrency, payload, repetition):
    name = f"c{concurrency}-p{payload}-r{repetition}"
    directory = args.out / name
    directory.mkdir()
    schema = "bench_" + uuid.uuid4().hex
    processes, controls, workers, clients, resources = [], [], [], [], []
    env = dict(os.environ, RUST_LOG="warn", ANYSERVE_DATABASE_URL=url)
    env.pop("TOKIO_WORKER_THREADS", None)
    env.pop("ANYSERVE_CLIENT_TOKEN", None)

    def spawn(command, role, index):
        log = directory / f"{role}-{index}.log"
        child_env = env.copy()
        if role != "control":
            child_env["TOKIO_WORKER_THREADS"] = "4" if role == "client" else "2"
        with log.open("w") as handle:
            process = subprocess.Popen(list(map(str, command)), cwd=ROOT, env=child_env,
                                       stdout=handle, stderr=subprocess.STDOUT)
        processes.append((role, process))
        return process, log

    try:
        for index in range(args.replicas):
            grpc, http_port = free_port(), free_port()
            while grpc == http_port:
                http_port = free_port()
            endpoint = f"http://127.0.0.1:{grpc}"
            config = directory / f"server-{index}.toml"
            config.write_text(f'[server]\ngrpc_host = "127.0.0.1"\ngrpc_port = {grpc}\n'
                f'[console]\nlisten = "127.0.0.1:{http_port}"\n'
                f'[journal]\nbackend = "postgres"\nschema = "{schema}"\n'
                '[auth]\nclient_tokens = []\nworker_tokens = []\n')
            server, log = spawn([args.server_bin, "serve", "--config", config], "control", index)
            wait_ready(server, log, grpc)
            worker, log = spawn([args.bench_bin, "worker", "--endpoint", endpoint,
                "--concurrency", str(concurrency)], "worker", index)
            wait_ready(worker, log)
            connection = http.client.HTTPConnection("127.0.0.1", http_port, timeout=5)
            try:
                assert get(connection, "/readyz") is None
                assert get(connection, "/api/workers")["healthy"] == 1
            finally:
                connection.close()
            controls.append((server, endpoint))
            workers.append(worker)
        for index, (_, endpoint) in enumerate(controls):
            output = directory / f"client-{index}.json"
            client, _ = spawn([args.bench_bin, "client", "--endpoint", endpoint,
                "--scenario", "S3", "--concurrency", str(concurrency),
                "--connections", str(min(8, concurrency)), "--payload", str(payload),
                "--warmup", f"{args.warmup}s", "--duration", f"{args.duration}s",
                "--out", output], "client", index)
            clients.append((client, output))
        deadline = time.monotonic() + args.warmup + args.duration + 60
        while any(client.poll() is None for client, _ in clients):
            if any(process.poll() is not None for role, process in processes if role != "client"):
                raise RuntimeError(f"service exited during {name}; see {directory}")
            if time.monotonic() >= deadline:
                raise RuntimeError(f"clients timed out during {name}; see {directory}")
            sample = sample_resources(processes)
            if any(client.poll() is not None for client, _ in clients):
                sample["roles"].pop("client", None)
            resources.append(sample)
            time.sleep(0.5)
        if any(process.poll() is not None for role, process in processes if role != "client"):
            raise RuntimeError(f"service exited during {name}; see {directory}")
        results = [json.loads(path.read_text()) for _, path in clients]
        summary = aggregate(results)
        summary.update(case=name, repetition=repetition, replicas=args.replicas,
                       concurrency_per_replica=concurrency, payload_bytes=payload, schema=schema)
        # Includes warm-up: this checks actual execution, not just client routing.
        participation = json.loads(sql(f'SELECT COALESCE(json_object_agg(last_worker_id, completed), \'{{}}\') '
            f'FROM (SELECT last_worker_id, count(*) AS completed FROM "{schema}".jobs '
            "WHERE state='succeeded' GROUP BY last_worker_id) counts;"))
        summary["completed_by_replica_including_warmup"] = [
            {"replica": index, "worker_id": f"bench-{worker.pid}",
             "completed": participation.get(f"bench-{worker.pid}", 0)}
            for index, worker in enumerate(workers)]
        summary["database_states"] = json.loads(sql(f'SELECT json_object_agg(state, jobs) '
            f'FROM (SELECT state, count(*) AS jobs FROM "{schema}".jobs GROUP BY state) counts;'))
        summary["resources"] = {}
        selected = [s for s in resources if summary["measurement_started_unix_s"] <= s["unix_s"] <= summary["measurement_finished_unix_s"]]
        for role in ("control", "worker", "client"):
            points = [(s["unix_s"], s["roles"][role]) for s in selected if role in s["roles"]]
            if len(points) > 1:
                summary["resources"][role] = {
                    "rss_peak_mib": max(p[1]["rss_mib"] for p in points),
                    "cpu_mean_percent": 100 * (points[-1][1]["cpu_seconds"] - points[0][1]["cpu_seconds"]) / (points[-1][0] - points[0][0])}
        write_json(directory / "resources.json", resources)
        write_json(directory / "summary.json", summary)
        print(json.dumps({k: v for k, v in summary.items() if k not in {"clients", "resources"}}), flush=True)
        if any(client.returncode for client, _ in clients) or summary["errors"] or summary["warmup_errors"]:
            raise RuntimeError(f"benchmark returned errors; see {directory}")
        if not all(row["completed"] for row in summary["completed_by_replica_including_warmup"]):
            raise RuntimeError(f"not all replicas executed work; see {directory}")
        return summary
    finally:
        with contextlib.ExitStack() as cleanup:
            cleanup.callback(sql, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
            for _, process in processes:
                cleanup.callback(stop, process)


def self_check():
    parsed = postgres_env("postgresql://some%20user:p%40ss@127.0.0.1:5433/test%20db?sslmode=require")
    assert parsed == {"PGUSER": "some user", "PGPASSWORD": "p@ss", "PGHOST": "127.0.0.1",
                      "PGPORT": "5433", "PGDATABASE": "test db", "PGSSLMODE": "require"}
    rows = [{"latency_samples_ms": [1, 2], "requests": 2, "errors": 0, "warmup_errors": 1,
             "measurement_started_unix_s": 10, "measurement_finished_unix_s": 12},
            {"latency_samples_ms": [3, 10], "requests": 2, "errors": 1, "warmup_errors": 0,
             "measurement_started_unix_s": 11, "measurement_finished_unix_s": 13}]
    result = aggregate(rows)
    assert result["throughput_rps"] == 4 / 3
    assert (result["p50_ms"], result["p95_ms"], result["p99_ms"]) == (2, 10, 10)
    assert (result["errors"], result["warmup_errors"], result["start_skew_ms"]) == (1, 1, 1000)
    assert "latency_samples_ms" not in result["clients"][0]
    rows[0].update(latency_samples_ms=[], requests=0)
    empty = aggregate(rows[:1])
    assert empty["throughput_rps"] == 0 and empty["p99_ms"] is None
    print("postgres benchmark self-check passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin", type=Path, default=ROOT / "target/release/anyserve")
    parser.add_argument("--bench-bin", type=Path, default=ROOT / "target/release/anyserve-bench")
    parser.add_argument("--source-revision", help="revision used to build the selected server binary")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--duration", type=float, default=20)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--replicas", type=int, default=2)
    parser.add_argument("--cases", default="64:256,256:256,4:1048576", help="per-replica concurrency:payload bytes")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    # Python must run cleanup on SIGTERM as well as Ctrl-C.
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    if not args.out or min(args.duration, args.repeat, args.replicas) <= 0 or args.warmup < 0:
        parser.error("--out and positive duration/repeat/replicas are required; warmup must be nonnegative")
    try:
        cases = [tuple(map(int, case.split(":"))) for case in args.cases.split(",")]
        assert all(len(case) == 2 and min(case) > 0 for case in cases)
        assert len(set(cases)) == len(cases)
    except (ValueError, AssertionError):
        parser.error("cases must be distinct positive concurrency:payload pairs")
    args.server_bin, args.bench_bin, args.out = args.server_bin.resolve(), args.bench_bin.resolve(), args.out.resolve()
    for binary in (args.server_bin, args.bench_bin):
        if not os.access(binary, os.X_OK):
            parser.error(f"binary is not executable: {binary}; build release binaries first")
    args.out.mkdir(parents=True, exist_ok=False)
    metadata = {
        "args": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        "platform": platform.platform(), "cpus": os.cpu_count(), "load_start": os.getloadavg(),
        "git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True),
        "server_sha256": digest(args.server_bin), "bench_sha256": digest(args.bench_bin),
        "tool_sha256": {name: digest(Path(__file__).with_name(name)) for name in ("postgres.py", "console.py", "runner.py")},
        "scope": "durable Submit+Watch through verified echo output; successful measured round trips, not submission-only QPS; loopback closed-loop clients; fresh PG schema per case",
        "throughput_window": "pooled successes divided by earliest client measurement start to latest finish, including drain; raw client windows retained",
        "tokio_worker_threads": {"control": "production default", "worker": 2, "client": 4},
        "cpu_unit": "100% = one logical CPU; role sums exclude PostgreSQL; ps samples every 0.5s",
        "database": "external" if os.environ.get("ANYSERVE_TEST_POSTGRES_URL") else "disposable postgres:18-bookworm Docker",
    }
    with database() as (url, sql):
        metadata["postgres"] = json.loads(sql("SELECT json_build_object('version', version(), "
            "'fsync', current_setting('fsync'), 'synchronous_commit', current_setting('synchronous_commit'), "
            "'full_page_writes', current_setting('full_page_writes'), 'shared_buffers', current_setting('shared_buffers'));"))
        write_json(args.out / "environment.json", metadata)
        rows = []
        for repetition in range(1, args.repeat + 1):
            for concurrency, payload in cases if repetition % 2 else reversed(cases):
                rows.append(run_case(args, url, sql, concurrency, payload, repetition))
                write_json(args.out / "summary.json", rows)


if __name__ == "__main__":
    main()
