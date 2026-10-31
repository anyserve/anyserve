#!/usr/bin/env python3
"""Compare PG active-active durable round trips or Submit ACKs on saved binaries.

Uses disposable Docker PostgreSQL 18 unless ANYSERVE_TEST_POSTGRES_URL is set
(external databases require psql). Each case owns and removes a unique schema.
"""
from __future__ import annotations

import argparse
from collections import Counter
import contextlib
import http.client
import json
import math
import os
import platform
import secrets
import shutil
import signal
import subprocess
import sys
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

        def sql(statement, timeout=30):
            # Credentials stay out of argv, artifacts and errors.
            result = subprocess.run(command + ["-XAt", "-v", "ON_ERROR_STOP=1"],
                input=statement, env=sql_env,
                capture_output=True, text=True, timeout=timeout)
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


def union_windows(windows):
    merged = []
    for start, finish in sorted(windows):
        if finish < start:
            raise ValueError("measurement window ends before it starts")
        if finish == start:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(finish, merged[-1][1])
        else:
            merged.append([start, finish])
    return merged


def aggregate(results, scenario="S3"):
    latencies = [value for result in results for value in result["latency_samples_ms"]]
    start = min(result["measurement_started_unix_s"] for result in results)
    finish = max(result["measurement_finished_unix_s"] for result in results)
    summary = {
        "requests": sum(result["requests"] for result in results),
        "errors": sum(result["errors"] for result in results),
        "warmup_errors": sum(result["warmup_errors"] for result in results),
        "p50_ms": percentile(latencies, 0.5), "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99), "max_ms": max(latencies, default=None),
        "measurement_started_unix_s": start, "measurement_finished_unix_s": finish,
        "start_skew_ms": 1000 * (max(result["measurement_started_unix_s"] for result in results) - start),
        "clients": [{k: v for k, v in result.items() if k not in
                     {"latency_samples_ms", "attempts", "unresolved_ids"}} for result in results],
    }
    if scenario == "S3":
        summary["throughput_rps"] = len(latencies) / (finish - start)
        return summary
    windows = union_windows((result["measurement_started_unix_s"],
                             result["measurement_finished_unix_s"]) for result in results)
    seconds = sum(end - begin for begin, end in windows)
    run_start = min(result["submission_started_unix_s"] for result in results)
    run_finish = max(result["run_finished_unix_s"] for result in results)
    drain_windows = union_windows((result["drain_started_unix_s"],
                                   result["drain_finished_unix_s"]) for result in results)
    # Both unions are disjoint internally; intersect actual measured Submit and drain time.
    overlap = sum(max(0, min(end, drain_end) - max(begin, drain_begin))
                  for begin, end in windows for drain_begin, drain_end in drain_windows)
    verified = sum(result["verified_jobs"] for result in results)
    summary.update(submit_ack_rps=len(latencies) / seconds if seconds else 0,
                   submit_windows=windows, submit_window_seconds=seconds,
                   submit_attempts=sum(result["submit_attempts"] for result in results),
                   submit_acks=sum(result["submit_acks"] for result in results),
                   warmup_requests=sum(result["warmup_requests"] for result in results),
                   cross_client_submit_drain_overlap_seconds=overlap,
                   cross_client_submit_drain_overlap_fraction=overlap / seconds if seconds else None,
                   run_started_unix_s=run_start, run_finished_unix_s=run_finish,
                   drain={"verified_jobs": verified, "errors": sum(result["drain_errors"] for result in results),
                          "started_unix_s": min(result["drain_started_unix_s"] for result in results),
                          "finished_unix_s": max(result["drain_finished_unix_s"] for result in results),
                          "post_submission_seconds": max(result["drain_finished_unix_s"] for result in results) - finish,
                          "whole_run_seconds": run_finish - run_start,
                          "verified_over_whole_run_rps": verified / (run_finish - run_start) if run_finish > run_start else 0,
                          "scope": "all attempts including warmup; verified echo results / entire Submit+drain run, not Submit ACK throughput"})
    if all("observed_peak_inflight" in result for result in results):
        summary["observed_peak_inflight_by_client"] = [result["observed_peak_inflight"] for result in results]
    paced = [result.get("rate_control") for result in results]
    if any(paced):
        if not all(paced):
            raise ValueError("cannot aggregate paced and unpaced clients")
        sending_windows = union_windows((rate["measurement_sending_started_unix_s"],
                                         rate["measurement_sending_finished_unix_s"]) for rate in paced)
        sending_seconds = sum(end - begin for begin, end in sending_windows)
        counters = {phase: {key: sum(rate[phase][key] for rate in paced) for key in
                           ("scheduled", "offered", "sent", "skipped", "skipped_busy",
                            "skipped_expired", "skipped_max_jobs")} for phase in ("warmup", "measured")}
        summary["rate_control"] = {
            "target_submit_rps_per_client": [rate["target_submit_rps"] for rate in paced],
            "target_submit_rps_all_clients": sum(rate["target_submit_rps"] for rate in paced),
            "measurement_sending_windows": sending_windows, "measurement_sending_seconds": sending_seconds,
            "measurement_window_complete": all(rate["measurement_window_complete"] for rate in paced),
            "actual_sent_rps": counters["measured"]["sent"] / sending_seconds if sending_seconds else 0,
            "schedule_fully_sent": all(rate["measurement_window_complete"] and rate["measured"]["scheduled"] > 0
                                       and rate["measured"]["skipped"] == 0 for rate in paced),
            **counters,
            "scope": "per-client paced sending; actual sends / union of sending windows, excluding in-flight ACK tail and result drain; configured target is not achieved throughput"}
    return summary


def resource_summary(samples, windows):
    summary = {}
    windows = union_windows(windows)
    requested = sum(end - begin for begin, end in windows)
    for role in ("control", "worker", "client"):
        cpu, seconds, rss = 0, 0, []
        for begin, end in windows:
            points = [(s["unix_s"], s["roles"][role]) for s in samples
                      if begin <= s["unix_s"] <= end and role in s["roles"]]
            rss.extend(point[1]["rss_mib"] for point in points)
            if len(points) > 1:
                cpu += points[-1][1]["cpu_seconds"] - points[0][1]["cpu_seconds"]
                seconds += points[-1][0] - points[0][0]
        if seconds:
            summary[role] = {"rss_peak_mib": max(rss), "cpu_mean_percent": 100 * cpu / seconds,
                             "observed_seconds": seconds, "window_seconds": requested}
    return summary


def queue_sample(sql, schema):
    started = time.time()
    states = json.loads(sql(f'SELECT COALESCE(json_object_agg(state, jobs), \'{{}}\') '
        f'FROM (SELECT state, count(*) AS jobs FROM "{schema}".jobs GROUP BY state) counts;'))
    return {"started_unix_s": started, "unix_s": time.time(),
            "states": {state: states.get(state, 0) for state in
                       ("queued", "running", "succeeded", "failed", "cancelled")}}


def queue_summary(samples, results):
    start = min(result["submission_started_unix_s"] for result in results)
    finish = max(result["measurement_finished_unix_s"] for result in results)
    during = [sample for sample in samples if start <= sample["unix_s"] <= finish]
    before = [sample for sample in samples if sample["unix_s"] <= finish]
    after = [sample for sample in samples if sample["unix_s"] >= finish]
    summary = {"sample_interval_target_seconds": 1, "submission_started_unix_s": start,
            "submission_finished_unix_s": finish,
            "sampled_peak_queued_running_during_submission": max(
                (sample["states"]["queued"] + sample["states"]["running"] for sample in during), default=None),
            "sampled_peak_by_state_during_submission": {
                state: max((sample["states"][state] for sample in during), default=None)
                for state in ("queued", "running", "succeeded", "failed", "cancelled")},
            "last_sample_before_submission_end": before[-1] if before else None,
            "first_sample_after_submission_end": after[0] if after else None,
            "final": samples[-1], "scope": "shared PG schema; sampled peaks, not exact instantaneous maxima; local timestamps bracket each query"}
    paced = [result.get("rate_control") for result in results]
    if all(paced):
        begin = max(rate["measurement_sending_started_unix_s"] for rate in paced)
        end = max(begin, min(rate["measurement_sending_finished_unix_s"] for rate in paced))
        points = [sample for sample in samples if begin <= sample["started_unix_s"] <= sample["unix_s"] <= end]
        measured = {"common_sending_started_unix_s": begin, "common_sending_finished_unix_s": end,
                    "sample_count": len(points), "observed_seconds": 0,
                    "succeeded_delta": None, "sampled_completed_rps": None,
                    "pending_change": None, "pending_growth_per_second": None,
                    "scope": "shared counts within the overlap of all clients' measurement sending windows; includes jobs accepted during warmup that finish in this interval, not per-job latency or a steady-state pass verdict"}
        if len(points) >= 2 and points[-1]["unix_s"] > points[0]["unix_s"]:
            first, last = points[0], points[-1]
            seconds = last["unix_s"] - first["unix_s"]
            completed = last["states"]["succeeded"] - first["states"]["succeeded"]
            pending = lambda point: point["states"]["queued"] + point["states"]["running"]
            change = pending(last) - pending(first)
            measured.update(observed_seconds=seconds, succeeded_delta=completed,
                            sampled_completed_rps=completed / seconds, pending_change=change,
                            pending_growth_per_second=change / seconds,
                            first_sample=first, last_sample=last)
        summary["paced_measurement"] = measured
    return summary


def reconcile_ids(results, database_rows):
    attempts = [attempt for result in results for attempt in result["attempts"]]
    counts = Counter(attempt["job_id"] for attempt in attempts)
    attempted = set(counts)
    acknowledged = {attempt["job_id"] for attempt in attempts if attempt["submit_status"] == "accepted"}
    verified = {attempt["job_id"] for attempt in attempts if attempt["drain_status"] == "verified"}
    database = dict(database_rows)
    committed = set(database)
    succeeded = {job_id for job_id, state in database.items() if state == "succeeded"}
    without_ack = committed & (attempted - acknowledged)
    counter_mismatches = []
    for index, result in enumerate(results):
        rows = result["attempts"]
        expected = {"submit_attempts": len(rows),
                    "submit_acks": sum(row["submit_status"] == "accepted" for row in rows),
                    "verified_jobs": sum(row["drain_status"] == "verified" for row in rows),
                    "requests": sum(row["phase"] == "measurement" and row["submit_status"] == "accepted" for row in rows)}
        for key, count in expected.items():
            if result[key] != count:
                counter_mismatches.append({"client": index, "counter": key, "reported": result[key], "from_attempts": count})
        if len(result["latency_samples_ms"]) != expected["requests"]:
            counter_mismatches.append({"client": index, "counter": "latency_samples_ms", "from_attempts": expected["requests"]})
        if "observed_peak_inflight" in result and not 0 <= result["observed_peak_inflight"] <= result["concurrency"]:
            counter_mismatches.append({"client": index, "counter": "observed_peak_inflight",
                                       "reported": result["observed_peak_inflight"], "limit": result["concurrency"]})
        if rate := result.get("rate_control"):
            for phase, label in (("warmup", "warmup"), ("measured", "measurement")):
                phase_counts = rate[phase]
                actual = sum(row["phase"] == label for row in rows)
                for key, reported, expected_count in (
                    ("sent", phase_counts["sent"], actual),
                    ("scheduled", phase_counts["scheduled"], phase_counts["sent"] + phase_counts["skipped"]),
                    ("skipped", phase_counts["skipped"], sum(phase_counts[name] for name in
                      ("skipped_busy", "skipped_expired", "skipped_max_jobs"))),
                ):
                    if reported != expected_count:
                        counter_mismatches.append({"client": index, "counter": f"rate_control.{phase}.{key}",
                                                   "reported": reported, "expected": expected_count})
                offered_min = phase_counts["sent"] + phase_counts["skipped_max_jobs"]
                if not offered_min <= phase_counts["offered"] <= offered_min + phase_counts["skipped_expired"]:
                    counter_mismatches.append({"client": index, "counter": f"rate_control.{phase}.offered",
                                               "reported": phase_counts["offered"]})
    problems = {
        "client_counter_mismatches": counter_mismatches,
        "duplicate_attempt_ids": sorted(job_id for job_id, count in counts.items() if count != 1),
        "unattempted_database_ids": sorted(committed - attempted),
        "acknowledged_missing_ids": sorted(acknowledged - committed),
        "committed_unverified_ids": sorted(committed - verified),
        "verified_missing_or_unsuccessful_ids": sorted(verified - succeeded),
        "nonterminal_ids": sorted(job_id for job_id, state in database.items() if state in {"queued", "running"}),
        "failed_or_cancelled_ids": sorted(job_id for job_id, state in database.items() if state in {"failed", "cancelled"}),
    }
    reported_without_ack = sum(result["committed_without_ack"] for result in results)
    return {"attempted": len(attempted), "acknowledged": len(acknowledged),
            "database_committed": len(committed), "verified": len(verified),
            "uncommitted_attempt_ids": sorted(attempted - committed),
            "committed_without_ack_ids": sorted(without_ack),
            "client_reported_committed_without_ack": reported_without_ack,
            "committed_without_ack_count_matches": reported_without_ack == len(without_ack),
            **problems, "passed": not any(problems.values()) and reported_without_ack == len(without_ack)}


def save_failure(directory, stage, error, clients, sql, schema, scenario):
    report = {"stage": stage, "exception_type": type(error).__name__,
              "message": "Benchmark failed; original exception text is omitted to protect credentials.",
              "captured_unix_s": time.time(), "clients": [],
              "resources_artifact": "resources.json",
              "queue_artifact": "queue-samples.json" if scenario == "S6" else None}
    for client, path in clients:
        code = client.poll()
        row = {"artifact": path.name, "exit_code_at_failure": code,
               "running_at_failure": code is None, "exists": path.exists()}
        if row["exists"]:
            try:
                result = json.loads(path.read_text())
                row["json_readable"] = True
                row["recorded_counts"] = {key: result[key] for key in (
                    "requests", "errors", "warmup_errors", "submit_attempts", "submit_acks",
                    "verified_jobs", "drain_errors") if isinstance(result.get(key), int)}
                if isinstance(result.get("attempts"), list):
                    row["recorded_attempt_ids"] = len(result["attempts"])
                if isinstance(result.get("success"), bool):
                    row["reported_success"] = result["success"]
            except Exception as read_error:
                row.update(json_readable=False, read_error_type=type(read_error).__name__)
        report["clients"].append(row)
    # Persist basic evidence even if the database is unavailable or collection is interrupted.
    write_json(directory / "failure.json", report)
    snapshot = {"started_unix_s": time.time(), "timeout_seconds": 5}
    try:
        rows = json.loads(sql(f'SELECT COALESCE(json_agg(json_build_array(job_id,state)), \'[]\') '
                              f'FROM "{schema}".jobs;', timeout=5))
        write_json(directory / "failure-database-jobs.json", rows)
        snapshot.update(artifact="failure-database-jobs.json", jobs=len(rows),
                        states=dict(Counter(state for _, state in rows)))
    except Exception as database_error:
        snapshot["error_type"] = type(database_error).__name__
    snapshot["finished_unix_s"] = time.time()
    report["database_snapshot"] = snapshot
    write_json(directory / "failure.json", report)


def run_case(args, url, sql, concurrency, payload, repetition):
    name = f"c{concurrency}-p{payload}-r{repetition}"
    directory = args.out / name
    directory.mkdir()
    schema = "bench_" + uuid.uuid4().hex
    processes, controls, workers, clients, resources, queues = [], [], [], [], [], []
    stage, failed = "startup", False
    worker_concurrency = args.worker_concurrency or concurrency
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
                "--concurrency", str(worker_concurrency)], "worker", index)
            wait_ready(worker, log)
            connection = http.client.HTTPConnection("127.0.0.1", http_port, timeout=5)
            try:
                assert get(connection, "/readyz") is None
                assert get(connection, "/api/workers")["healthy"] == 1
            finally:
                connection.close()
            controls.append((server, endpoint))
            workers.append(worker)
        if args.scenario == "S6":
            queues.append(queue_sample(sql, schema))
        next_queue_sample = time.monotonic() + 1
        for index, (_, endpoint) in enumerate(controls):
            output = directory / f"client-{index}.json"
            command = [args.bench_bin, "client", "--endpoint", endpoint,
                "--scenario", args.scenario, "--concurrency", str(concurrency),
                "--connections", str(min(8, concurrency)), "--payload", str(payload),
                "--payload-pattern", args.payload_pattern, "--payload-seed", str(args.payload_seed),
                "--warmup", f"{args.warmup}s", "--duration", f"{args.duration}s",
                "--out", output]
            if args.scenario == "S6":
                command += ["--max-jobs", str(args.max_jobs), "--drain-timeout", f"{args.drain_timeout}s"]
                if args.submit_rate is not None:
                    command += ["--submit-rate", str(args.submit_rate)]
            client, _ = spawn(command, "client", index)
            clients.append((client, output))
        stage = "submit_and_drain"
        deadline = time.monotonic() + args.warmup + args.duration + 60 + (args.drain_timeout if args.scenario == "S6" else 0)
        while any(client.poll() is None for client, _ in clients):
            if any(process.poll() is not None for role, process in processes if role != "client"):
                raise RuntimeError(f"service exited during {name}; see {directory}")
            if time.monotonic() >= deadline:
                raise RuntimeError(f"clients timed out during {name}; see {directory}")
            sample = sample_resources(processes)
            if any(client.poll() is not None for client, _ in clients):
                sample["roles"].pop("client", None)
            resources.append(sample)
            if args.scenario == "S6" and time.monotonic() >= next_queue_sample:
                queues.append(queue_sample(sql, schema))
                next_queue_sample = time.monotonic() + 1
            time.sleep(0.5)
        if any(process.poll() is not None for role, process in processes if role != "client"):
            raise RuntimeError(f"service exited during {name}; see {directory}")
        stage = "reading_client_results"
        results = [json.loads(path.read_text()) for _, path in clients]
        summary = aggregate(results, args.scenario)
        summary.update(case=name, repetition=repetition, replicas=args.replicas,
                       concurrency_per_replica=concurrency, payload_bytes=payload, schema=schema,
                       payload_pattern=args.payload_pattern, payload_seed=args.payload_seed,
                       scenario=args.scenario, worker_concurrency_per_replica=worker_concurrency,
                       client_artifacts=[path.name for _, path in clients])
        if args.scenario == "S6":
            stage = "database_reconciliation"
            started = time.time()
            database_rows = json.loads(sql(f'SELECT COALESCE(json_agg(json_build_array(job_id,state)), \'[]\') '
                                           f'FROM "{schema}".jobs;'))
            states = Counter(state for _, state in database_rows)
            queues.append({"started_unix_s": started, "unix_s": time.time(), "states": {
                state: states[state] for state in ("queued", "running", "succeeded", "failed", "cancelled")}})
            reconciliation = reconcile_ids(results, database_rows)
            write_json(directory / "id-reconciliation.json", reconciliation)
            summary["id_reconciliation"] = {key: value for key, value in reconciliation.items() if not isinstance(value, list)}
            summary["id_reconciliation"]["artifact"] = "id-reconciliation.json"
            summary["queue"] = queue_summary(queues, results)
            summary["resources"] = resource_summary(resources, summary["submit_windows"])
            summary["resources_whole_run"] = resource_summary(resources, [
                (summary["run_started_unix_s"], summary["run_finished_unix_s"])])
        else:
            summary["resources"] = resource_summary(resources, [
                (summary["measurement_started_unix_s"], summary["measurement_finished_unix_s"])])
        stage = "post_run_statistics"
        # Outside the measurement window; content size excludes tuple/index/TOAST page overhead.
        summary["payload_storage_sample"] = json.loads(sql(f'SELECT COALESCE(json_agg(sample), \'[]\') '
            f'FROM (SELECT role, octet_length(content) AS logical_bytes, pg_column_size(content) AS stored_bytes '
            f'FROM "{schema}".objects WHERE content IS NOT NULL LIMIT 32) sample;'))
        # Includes warm-up: this checks actual execution, not just client routing.
        participation = json.loads(sql(f'SELECT COALESCE(json_object_agg(last_worker_id, completed), \'{{}}\') '
            f'FROM (SELECT last_worker_id, count(*) AS completed FROM "{schema}".jobs '
            "WHERE state='succeeded' GROUP BY last_worker_id) counts;"))
        summary["completed_by_replica_including_warmup"] = [
            {"replica": index, "worker_id": f"bench-{worker.pid}",
             "completed": participation.get(f"bench-{worker.pid}", 0)}
            for index, worker in enumerate(workers)]
        summary["database_states"] = dict(states) if args.scenario == "S6" else json.loads(sql(
            f'SELECT json_object_agg(state, jobs) '
            f'FROM (SELECT state, count(*) AS jobs FROM "{schema}".jobs GROUP BY state) counts;'))
        write_json(directory / "summary.json", summary)
        print(json.dumps({k: v for k, v in summary.items() if k not in {"clients", "resources"}}), flush=True)
        stage = "validating_results"
        if any(client.returncode for client, _ in clients) or summary["errors"] or summary["warmup_errors"]:
            raise RuntimeError(f"benchmark returned errors; see {directory}")
        if args.scenario == "S6" and (summary["drain"]["errors"] or not reconciliation["passed"]):
            raise RuntimeError(f"drain or database ID reconciliation failed; see {directory}")
        if not all(row["completed"] for row in summary["completed_by_replica_including_warmup"]):
            raise RuntimeError(f"not all replicas executed work; see {directory}")
        return summary
    except (Exception, KeyboardInterrupt) as error:
        failed = True
        try:
            save_failure(directory, stage, error, clients, sql, schema, args.scenario)
        except (Exception, KeyboardInterrupt) as capture_error:
            print(f"Failure evidence could not be completed: {type(capture_error).__name__}", file=sys.stderr)
        raise
    finally:
        try:
            with contextlib.ExitStack() as cleanup:
                cleanup.callback(sql, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
                for _, process in processes:
                    cleanup.callback(stop, process)
                write_json(directory / "resources.json", resources)
                if args.scenario == "S6":
                    write_json(directory / "queue-samples.json", queues)
        except (Exception, KeyboardInterrupt) as cleanup_error:
            if not failed:
                raise
            print(f"Cleanup also failed: {type(cleanup_error).__name__}", file=sys.stderr)


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
    # Different Submit stopping times and a long verification tail must not dilute ACK/s.
    submit_rows = [dict(row, submission_started_unix_s=9, submission_finished_unix_s=12, run_finished_unix_s=30,
                        submit_attempts=3, submit_acks=3, warmup_requests=1,
                        verified_jobs=3, drain_errors=0, drain_started_unix_s=12,
                        drain_finished_unix_s=30, attempts=[], unresolved_ids=[])
                   for row in rows]
    submit_rows[0].update(latency_samples_ms=[1, 2], requests=2)
    submit_rows[1].update(measurement_finished_unix_s=14, run_finished_unix_s=100,
                          submission_finished_unix_s=14, drain_started_unix_s=14, drain_finished_unix_s=100)
    result = aggregate(submit_rows, "S6")
    assert result["submit_ack_rps"] == 1 and result["submit_window_seconds"] == 4
    assert result["drain"]["verified_over_whole_run_rps"] == 6 / 91
    assert result["cross_client_submit_drain_overlap_seconds"] == 2
    assert result["cross_client_submit_drain_overlap_fraction"] == 0.5
    assert "throughput_rps" not in result and "attempts" not in result["clients"][0]
    assert union_windows([(10, 12), (11, 14), (17, 18), (18, 18)]) == [[10, 14], [17, 18]]
    fast_drain = [dict(submit_rows[0], drain_finished_unix_s=13, run_finished_unix_s=13),
                  dict(submit_rows[1], measurement_started_unix_s=10,
                       measurement_finished_unix_s=100, submission_finished_unix_s=100,
                       drain_started_unix_s=100, drain_finished_unix_s=101, run_finished_unix_s=101)]
    assert aggregate(fast_drain, "S6")["cross_client_submit_drain_overlap_seconds"] == 1
    submit_rows[0].update(drain_finished_unix_s=13, run_finished_unix_s=13)
    submit_rows[1].update(measurement_started_unix_s=15, measurement_finished_unix_s=17,
                          submission_finished_unix_s=17, drain_started_unix_s=17)
    disjoint = aggregate(submit_rows, "S6")
    assert disjoint["submit_window_seconds"] == 4
    assert disjoint["cross_client_submit_drain_overlap_seconds"] == 0
    zero = dict(submit_rows[0], latency_samples_ms=[], requests=0,
                measurement_started_unix_s=12, measurement_finished_unix_s=12)
    assert aggregate([zero], "S6")["submit_ack_rps"] == 0
    def rate_counts(sent, busy=0, expired=0, capped=0):
        skipped = busy + expired + capped
        return {"scheduled": sent + skipped, "offered": sent + capped,
                "sent": sent, "skipped": skipped, "skipped_busy": busy,
                "skipped_expired": expired, "skipped_max_jobs": capped}
    def rate_window(begin, end, sent, **skips):
        return {"target_submit_rps": 10, "measurement_window_complete": True,
                "measurement_sending_started_unix_s": begin,
                "measurement_sending_finished_unix_s": end,
                "warmup": rate_counts(1), "measured": rate_counts(sent, **skips)}
    # Sending rate excludes both ACK tail and drain; targets never substitute for actual sends.
    paced = [dict(submit_rows[0], rate_control=rate_window(10, 13, 29, busy=1)),
             dict(submit_rows[1], rate_control=rate_window(11, 14, 30))]
    rate = aggregate(paced, "S6")["rate_control"]
    assert rate["target_submit_rps_all_clients"] == 20
    assert rate["measurement_sending_seconds"] == 4 and rate["actual_sent_rps"] == 59 / 4
    assert rate["measured"]["skipped_busy"] == 1 and not rate["schedule_fully_sent"]
    assert rate["measurement_window_complete"]
    cap = dict(zero, rate_control=dict(rate_window(12, 12, 0), measurement_window_complete=False))
    early = aggregate([cap], "S6")["rate_control"]
    assert early["actual_sent_rps"] == 0 and early["measurement_sending_seconds"] == 0
    assert not early["measurement_window_complete"] and not early["schedule_fully_sent"]
    try:
        aggregate([paced[0], submit_rows[1]], "S6")
    except ValueError:
        pass
    else:
        raise AssertionError("mixed paced and unpaced clients were accepted")
    samples = [{"unix_s": timestamp, "roles": {"control": {"rss_mib": 10, "cpu_seconds": cpu}}}
               for timestamp, cpu in ((10, 0), (12, 2), (15, 100), (17, 104))]
    assert resource_summary(samples, [(10, 12), (15, 17)])["control"]["cpu_mean_percent"] == 150
    queues = [{"unix_s": timestamp, "states": {"queued": queued, "running": 0,
               "succeeded": 0, "failed": 0, "cancelled": 0}}
              for timestamp, queued in ((9, 0), (11, 2), (13, 7), (18, 50), (101, 0))]
    queue = queue_summary(queues, submit_rows)
    assert queue["sampled_peak_queued_running_during_submission"] == 7
    assert queue["last_sample_before_submission_end"]["unix_s"] == 13
    assert queue["first_sample_after_submission_end"]["unix_s"] == 18
    assert queue["final"]["states"]["queued"] == 0
    # Only queries wholly within both clients' sending windows contribute to completion rate.
    paced_queues = [dict(point, started_unix_s=timestamp - 0.1, unix_s=timestamp,
                         states=dict(point["states"], queued=index, succeeded=index * 10))
                    for index, (point, timestamp) in enumerate(zip(queues, (10, 11, 11.5, 12.5, 14)))]
    measured_queue = queue_summary(paced_queues, paced)["paced_measurement"]
    assert measured_queue["sample_count"] == 2 and measured_queue["observed_seconds"] == 1
    assert measured_queue["sampled_completed_rps"] == 10
    assert measured_queue["pending_growth_per_second"] == 1
    sparse = queue_summary(paced_queues[:3], paced)["paced_measurement"]
    assert sparse["sample_count"] == 1 and sparse["sampled_completed_rps"] is None
    assert queue_summary(paced_queues, [cap])["paced_measurement"]["sample_count"] == 0
    no_overlap = queue_summary(paced_queues, [paced[0], dict(paced[1], rate_control=rate_window(20, 21, 3))])["paced_measurement"]
    assert no_overlap["common_sending_started_unix_s"] == no_overlap["common_sending_finished_unix_s"]
    assert no_overlap["sample_count"] == 0 and no_overlap["sampled_completed_rps"] is None
    attempts = [{"job_id": name, "phase": phase, "submit_status": ack, "drain_status": drain}
                for name, phase, ack, drain in (
                    ("warmup", "warmup", "accepted", "verified"),
                    ("ack", "measurement", "accepted", "verified"),
                    ("ambiguous", "measurement", "rpc_error", "verified"),
                    ("rejected", "measurement", "rpc_error", "not_found"))]
    client = {"attempts": attempts, "committed_without_ack": 1, "submit_attempts": 4,
              "submit_acks": 2, "verified_jobs": 3, "requests": 1, "latency_samples_ms": [1]}
    database_rows = [(name, "succeeded") for name in ("warmup", "ack", "ambiguous")]
    checked = reconcile_ids([client], database_rows)
    assert checked["passed"] and checked["committed_without_ack_ids"] == ["ambiguous"]
    assert checked["uncommitted_attempt_ids"] == ["rejected"]
    missing = reconcile_ids([client], database_rows[:1])
    assert not missing["passed"] and missing["acknowledged_missing_ids"] == ["ack"]
    pending = reconcile_ids([client], database_rows + [("rejected", "running")])
    assert not pending["passed"] and pending["nonterminal_ids"] == ["rejected"]
    duplicate = reconcile_ids([client, client], database_rows)
    assert not duplicate["passed"] and duplicate["duplicate_attempt_ids"]
    mismatched = reconcile_ids([dict(client, submit_attempts=5)], database_rows)
    assert not mismatched["passed"] and mismatched["client_counter_mismatches"]
    paced_client = dict(client, rate_control=rate_window(10, 11, 3, busy=2, expired=1, capped=1))
    assert reconcile_ids([paced_client], database_rows)["passed"]
    for key in ("sent", "scheduled", "skipped", "offered"):
        rate = paced_client["rate_control"]
        invalid = dict(paced_client, rate_control=dict(rate, measured=dict(rate["measured"], **{key: 99})))
        invalid_counts = reconcile_ids([invalid], database_rows)
        assert not invalid_counts["passed"] and invalid_counts["client_counter_mismatches"]
    assert not reconcile_ids([dict(client, observed_peak_inflight=3, concurrency=2)], database_rows)["passed"]
    # Failure artifacts must distinguish recorded IDs from still-running clients.
    import io
    from types import SimpleNamespace
    from unittest.mock import patch
    with tempfile.TemporaryDirectory(prefix="anyserve-failure-check-") as temporary:
        directory = Path(temporary)
        recorded, absent, partial = [directory / f"client-{index}.json" for index in range(3)]
        write_json(recorded, {"requests": 1, "attempts": [{"job_id": "known"}], "success": False})
        partial.write_text('{"attempts":')
        exited = SimpleNamespace(poll=lambda: 1)
        active = SimpleNamespace(poll=lambda: None)
        def timed_out(statement, timeout):
            assert timeout == 5
            raise subprocess.TimeoutExpired("credential-sentinel", timeout)
        save_failure(directory, "submit_and_drain", RuntimeError("credential-sentinel"),
                     [(exited, recorded), (active, absent), (active, partial)], timed_out, "test", "S6")
        evidence = json.loads((directory / "failure.json").read_text())
        assert "credential-sentinel" not in json.dumps(evidence)
        assert evidence["database_snapshot"]["error_type"] == "TimeoutExpired"
        assert evidence["clients"][0]["recorded_counts"] == {"requests": 1}
        assert evidence["clients"][0]["recorded_attempt_ids"] == 1
        assert evidence["clients"][0]["exit_code_at_failure"] == 1
        assert evidence["clients"][1]["running_at_failure"]
        assert evidence["clients"][1]["exit_code_at_failure"] is None
        assert "recorded_counts" not in evidence["clients"][1]
        assert "recorded_attempt_ids" not in evidence["clients"][1]
        assert not evidence["clients"][2]["json_readable"]
        save_failure(directory, "validating_results", RuntimeError(), [],
                     lambda statement, timeout: '[["known","running"]]', "test", "S6")
        evidence = json.loads((directory / "failure.json").read_text())
        assert evidence["database_snapshot"]["states"] == {"running": 1}
        assert json.loads((directory / "failure-database-jobs.json").read_text()) == [["known", "running"]]

        # A broken diagnostic must neither replace the original error nor skip cleanup.
        original = RuntimeError("credential-sentinel")
        cleanup_order, ports, diagnostics = [], iter((10000, 10001)), io.StringIO()
        def fail_ready(*args):
            raise original
        def fail_capture(*args):
            raise OSError("credential-sentinel")
        arguments = SimpleNamespace(out=directory, worker_concurrency=1, replicas=1,
                                    server_bin=Path("unused"), scenario="S6")
        with patch.dict(globals(), {"free_port": lambda: next(ports), "wait_ready": fail_ready,
                                   "save_failure": fail_capture,
                                   "stop": lambda process: cleanup_order.append("stop")}), \
             patch.object(subprocess, "Popen", return_value=active), \
             contextlib.redirect_stderr(diagnostics):
            try:
                run_case(arguments, "credential-sentinel", lambda statement: cleanup_order.append("drop"), 1, 1, 1)
            except RuntimeError as error:
                assert error is original
            else:
                raise AssertionError("original benchmark error was swallowed")
        assert cleanup_order == ["stop", "drop"]
        assert "credential-sentinel" not in diagnostics.getvalue()
        assert (directory / "c1-p1-r1/resources.json").exists()
        assert (directory / "c1-p1-r1/queue-samples.json").exists()
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
    parser.add_argument("--scenario", choices=("S3", "S6"), default="S3",
                        help="S3: complete durable round trips; S6: Submit ACKs followed by separate result drain")
    parser.add_argument("--worker-concurrency", type=int,
                        help="slots per worker replica; defaults to each case's client concurrency")
    parser.add_argument("--max-jobs", type=int, default=100000,
                        help="S6 total attempts per client, including warmup and failed Submit calls")
    parser.add_argument("--drain-timeout", type=float, default=120,
                        help="S6 result drain deadline in seconds, excluded from Submit throughput")
    parser.add_argument("--submit-rate", type=int,
                        help="S6 target sends per second per client; bounded concurrency, missed sends are counted and skipped")
    parser.add_argument("--cases", default="64:256,256:256,4:1048576", help="per-replica concurrency:payload bytes")
    parser.add_argument("--payload-pattern", choices=("repeat", "random"), default="random",
                        help="random avoids optimistic compression of large payloads; repeat reproduces historical x payloads")
    parser.add_argument("--payload-seed", type=int, default=0, help="reproducible unsigned 64-bit seed; ignored for repeat")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    # Python must run cleanup on SIGTERM as well as Ctrl-C.
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    if (not args.out or min(args.duration, args.repeat, args.replicas) <= 0 or args.warmup < 0
            or not all(math.isfinite(value) for value in (args.duration, args.warmup, args.drain_timeout))):
        parser.error("--out and positive duration/repeat/replicas are required; warmup must be nonnegative")
    if not 0 < args.max_jobs < 2**32 or args.drain_timeout <= 0:
        parser.error("max-jobs must be a positive u32 and drain-timeout must be positive")
    if args.worker_concurrency is not None and not 0 < args.worker_concurrency < 2**32:
        parser.error("worker-concurrency must be a positive u32")
    if args.submit_rate is not None and (args.scenario != "S6" or not 0 < args.submit_rate < 2**32):
        parser.error("submit-rate requires S6 and a positive u32")
    if not 0 <= args.payload_seed < 2**64:
        parser.error("payload seed must be an unsigned 64-bit integer")
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
        "scope": ("S3 durable Submit+Watch through verified echo output; successful measured round trips"
                  if args.scenario == "S3" else
                  "S6 durable Submit ACK latency/throughput, then separate complete echo verification and authoritative DB ID reconciliation")
                 + ("; bounded paced loopback clients" if args.submit_rate else "; loopback closed-loop clients")
                 + "; fresh PG schema per case",
        "payload_scope": "one deterministic buffer reused per client; random uses SplitMix64 little-endian words to model already-compressed bytes, not media codecs or distinct content per job",
        "throughput_window": ("pooled successes divided by earliest client measurement start to latest finish, including drain"
                              if args.scenario == "S3" else
                              "pooled measured ACKs / union of actual Submit windows including in-flight Submit responses, excluding result drain"),
        "queue_sampling": "S6: shared PG jobs grouped by state approximately every 1s through submission and drain; raw query start/end times retained",
        "resource_windows": "S3: measurement window; S6: Submit-window union and whole run separately; CPU is over observed sample spans, with coverage seconds recorded; client samples stop when any client exits",
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
