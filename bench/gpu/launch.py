"""One approved run: Q1, detached reaper, one Spot B, Q2/Q3, unconditional cleanup."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.request

from teardown import (CONTROL_SG, LOCAL_INSTANCE, LOCAL_SGS, aws, canonical, cleanup,
                      groups_snapshot, pid_ticks, read, stop_process, update)

BASE_SHA = "99f5db83a628dea5e5171342a81ebc50eacc74e0"
MODEL_SHA = "c17eb830b28be4381ae141cf6eb477efeac424bfc18c867b964f5129e13ed083"
HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    args = parser.parse_args()
    root = args.run_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    root.chmod(0o700)
    run_id = "anyserve-gpu-" + root.name
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    assert subprocess.check_output(["git", "merge-base", "--is-ancestor", BASE_SHA, "HEAD"]) == b""
    assert not subprocess.check_output(["git", "status", "--porcelain=v1"]), "commit six files first"
    assert hashlib.sha256(args.model.read_bytes()).hexdigest() == MODEL_SHA
    update(root, run_id=run_id, baseline=BASE_SHA, scripts_commit=commit, processes=[], cloud_started=False)
    for path in [args.model, args.binary, args.wheel, *HERE.glob("*.py")]:
        with (root / "artifact-hashes.txt").open("a") as output:
            output.write(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path}\n")
    for name in ["client", "worker"]:
        path = root / f"{name}.token"
        path.write_text(secrets.token_urlsafe(32))
        path.chmod(0o600)
    env = dict(os.environ)
    env["ANYSERVE_CLIENT_TOKENS"] = (root / "client.token").read_text()
    env["ANYSERVE_WORKER_TOKENS"] = (root / "worker.token").read_text()
    baseline_triton = subprocess.check_output([
        "docker", "inspect", "--format", "{{.Name}} {{.State.Running}} {{.State.StartedAt}}",
        "triton", "gameplayai-micro-triton"], text=True)
    (root / "triton-before.txt").write_text(baseline_triton)
    next_health = 0

    def health():
        nonlocal next_health
        if time.monotonic() < next_health:
            return
        next_health = time.monotonic() + 3
        current = subprocess.check_output([
            "docker", "inspect", "--format", "{{.Name}} {{.State.Running}} {{.State.StartedAt}}",
            "triton", "gameplayai-micro-triton"], text=True)
        assert current == baseline_triton, "existing Triton changed state or restarted"
        for port in [18000, 18200]:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/v2/health/ready", timeout=3) as response:
                assert response.status == 200, "existing Triton readiness failed"
        gpu = subprocess.check_output(["nvidia-smi", "--query-gpu=memory.used,utilization.gpu", "--format=csv,noheader"], text=True)
        sample = dict(time=time.time(), gpu=gpu.strip())
        ledger = read(root)
        cp = ledger.get("control_plane")
        if cp:
            assert pid_ticks(cp["pid"]) == cp["ticks"], "control plane stopped"
            sample["control_plane_stat"] = Path(f"/proc/{cp['pid']}/stat").read_text()
            sample["control_plane_status"] = Path(f"/proc/{cp['pid']}/status").read_text()
        with (root / "process-samples.jsonl").open("a") as output:
            output.write(json.dumps(sample) + "\n")
        if ledger.get("t0"):
            assert time.time() < ledger["t0"] + 3600, "60-minute target/deadline reached"
        assert not (root / "client-error.json").exists(), "client-visible error; see client-error.json"

    def spawn(name, command, process_env=None):
        output = (root / f"{name}.log").open("w")
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                   env=process_env or env, start_new_session=True)
        output.close()
        item = dict(name=name, pid=process.pid, ticks=pid_ticks(process.pid))
        update(root, processes=read(root)["processes"] + [item])
        return process, item

    def wait(process, callback=None, timeout=1200):
        until = time.monotonic() + timeout
        while process.poll() is None:
            health()
            assert time.monotonic() < until, "process timeout"
            if callback:
                callback()
            time.sleep(.5)
        assert process.returncode == 0, f"process exit {process.returncode}; see run logs"

    def until_file(path, process=None, timeout=180):
        end = time.monotonic() + timeout
        while not path.exists():
            health()
            assert not process or process.poll() is None, "worker exited before ready"
            assert time.monotonic() < end, f"timeout waiting for {path.name}"
            time.sleep(.5)

    def worker_a(slots):
        (root / "a-ready.json").unlink(missing_ok=True)
        process, item = spawn(f"worker-a-{slots}", [sys.executable, str(HERE / "worker.py"),
            "--model", str(args.model), "--run-dir", str(root), "--run-id", run_id,
            "--host", "a", "--slots", str(slots), "--token-file", str(root / "worker.token")])
        until_file(root / "a-ready.json", process)
        return process, item

    def metrics(name):
        request = urllib.request.Request("http://127.0.0.1:3001/metrics", headers={
            "Authorization": "Bearer " + (root / "client.token").read_text()})
        with urllib.request.urlopen(request, timeout=5) as response:
            (root / f"metrics-{name}.txt").write_bytes(response.read())

    def generation():
        with sqlite3.connect(f"file:{root / 'journal.sqlite'}?mode=ro", uri=True) as database:
            return database.execute("SELECT value FROM journal_meta WHERE key='generation'").fetchone()[0]

    remote_base = "/home/ubuntu/anyserve-gpu"
    ssh_args = []

    def ssh(command, timeout=120):
        process, _ = spawn(f"ssh-{time.time_ns()}", ssh_args + ["set -eu; " + command])
        wait(process, timeout=timeout)

    def scp(source, destination):
        process, _ = spawn(f"scp-{time.time_ns()}", ["scp", *ssh_args[1:-1], str(source), str(destination)])
        wait(process, timeout=240)

    result = dict(status="INCOMPLETE")
    try:
        health()
        config = root / "server.toml"
        config.write_text(f'''[server]
grpc_host = "10.0.3.60"
grpc_port = 50052
[console]
listen = "127.0.0.1:3001"
[runtime]
heartbeat_interval_ms = 500
heartbeat_timeout_ms = 2000
[journal]
path = "{root / 'journal.sqlite'}"
[auth]
client_tokens_env = "ANYSERVE_CLIENT_TOKENS"
worker_tokens_env = "ANYSERVE_WORKER_TOKENS"
''')
        cp, cp_item = spawn("control-plane", [str(args.binary), "serve", "--config", str(config)])
        update(root, control_plane=cp_item)
        for _ in range(40):
            assert cp.poll() is None, "control plane exited"
            try:
                with urllib.request.urlopen("http://127.0.0.1:3001/readyz", timeout=2) as response:
                    assert response.status == 204
                break
            except OSError:
                time.sleep(.25)
        else:
            raise RuntimeError("control plane not ready")
        a, a_item = worker_a(16)
        metrics("q1-before")
        client_command = [sys.executable, str(HERE / "client.py"), "--run-dir", str(root), "--model", str(args.model)]
        client, _ = spawn("q1", client_command + ["--phase", "q1"])
        try:
            wait(client)
        except BaseException:
            print("Q1 FAIL/STOP（详见 q1.log 与原始记录）", flush=True)
            raise
        q1 = json.loads((root / "q1.json").read_text())
        print("Q1 " + q1["status"], flush=True)
        assert q1["status"] == "PASS", "Q1 FAIL; stop before provisioning"
        metrics("q1-after")
        stop_process(a_item)
        a.wait(timeout=10)
        a, a_item = worker_a(1)
        # No AWS mutation before Q1 passed and the independent reaper is alive.
        before = groups_snapshot()
        (root / "sg-before.json").write_text(json.dumps(before, indent=2) + "\n")
        identity = aws("sts", "get-caller-identity")
        assert identity["Account"] == "692744833360"
        image = aws("ec2", "describe-images", image_ids=["ami-0765437123d36eaa8"])["Images"][0]
        assert image["State"] == "available" and image["Architecture"] == "x86_64" and not image.get("ProductCodes")
        prices = aws("ec2", "describe-spot-price-history", instance_types=["g4dn.xlarge"],
            product_descriptions=["Linux/UNIX"], availability_zone="us-east-1b", max_items=1,
            start_time=datetime.now(timezone.utc).isoformat())
        (root / "spot-price.json").write_text(json.dumps(prices, indent=2) + "\n")
        if prices["SpotPriceHistory"]:
            assert float(prices["SpotPriceHistory"][0]["SpotPrice"]) <= .4
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(root / "ssh-key")], check=True)
        t0 = time.time()
        update(root, t0=t0, deadline=t0 + 165 * 60, client_token=run_id, launch_attempted=False)
        reaper_log = (root / "reaper.log").open("w")
        reaper = subprocess.Popen(["setsid", "nohup", sys.executable, str(HERE / "teardown.py"),
            "--reaper", "--run-dir", str(root), "--instance-id", "pending", "--deadline", str(t0 + 165 * 60)],
            stdin=subprocess.DEVNULL, stdout=reaper_log, stderr=subprocess.STDOUT, close_fds=True)
        reaper_log.close()
        until_file(root / "reaper-ready.json", reaper, 15)
        assert reaper.poll() is None
        update(root, reaper_pid=json.loads((root / "reaper-ready.json").read_text())["pid"], cloud_started=True)
        tags = [dict(Key=key, Value=value) for key, value in dict(Project="anyserve-gpu", Experiment=run_id,
            Owner="codex", ExpiresAt=datetime.fromtimestamp(t0 + 180 * 60, timezone.utc).isoformat()).items()]
        sg = aws("ec2", "create-security-group", group_name=run_id, description=run_id,
            vpc_id="vpc-0ea81bdde5fce6aa2", tag_specifications=[dict(ResourceType="security-group", Tags=tags)])["GroupId"]
        update(root, b_security_group=sg)
        aws("ec2", "authorize-security-group-ingress", group_id=sg, ip_permissions=[dict(
            IpProtocol="tcp", FromPort=22, ToPort=22, IpRanges=[dict(CidrIp="10.0.3.60/32", Description=run_id)])])
        public_key = (root / "ssh-key.pub").read_text().strip()
        user_data = "#!/bin/bash\nset -eu\nshutdown -h +180\ninstall -d -m 700 -o ubuntu -g ubuntu /home/ubuntu/.ssh\necho " + shlex.quote(public_key) + " >> /home/ubuntu/.ssh/authorized_keys\nchown ubuntu:ubuntu /home/ubuntu/.ssh/authorized_keys\nchmod 600 /home/ubuntu/.ssh/authorized_keys\n"
        request = dict(ImageId=image["ImageId"], InstanceType="g4dn.xlarge", MinCount=1, MaxCount=1,
            SubnetId="subnet-029892b73a654b840", SecurityGroupIds=[sg], ClientToken=run_id,
            InstanceInitiatedShutdownBehavior="terminate", UserData=user_data,
            InstanceMarketOptions=dict(MarketType="spot", SpotOptions=dict(MaxPrice="0.40", SpotInstanceType="one-time", InstanceInterruptionBehavior="terminate")),
            BlockDeviceMappings=[dict(DeviceName=image["RootDeviceName"], Ebs=dict(VolumeSize=40, VolumeType="gp3", DeleteOnTermination=True))],
            TagSpecifications=[dict(ResourceType=kind, Tags=tags) for kind in ["instance", "volume"]])
        request_file = root / "launch-request.json"
        request_file.write_text(json.dumps(request, indent=2) + "\n")
        update(root, launch_attempted=True, launch_resolved=False)
        try:
            launched = aws("ec2", "run-instances", cli_input_json="file://" + str(request_file))
        except RuntimeError as exc:
            if "An error occurred" in str(exc) and "RunInstances" in str(exc):
                update(root, launch_resolved=True)
            raise
        instance = launched["Instances"][0]
        update(root, launch_resolved=True, instance_id=instance["InstanceId"],
               spot_request_ids=[instance["SpotInstanceRequestId"]] if instance.get("SpotInstanceRequestId") else [])
        (root / "launch-response.json").write_text(json.dumps(launched, indent=2) + "\n")
        while instance["State"]["Name"] != "running":
            health()
            assert instance["State"]["Name"] == "pending", instance["State"]
            time.sleep(2)
            instance = aws("ec2", "describe-instances", instance_ids=[instance["InstanceId"]])["Reservations"][0]["Instances"][0]
        ip = instance["PrivateIpAddress"]
        update(root, b_private_ip=ip, volume_ids=[item["Ebs"]["VolumeId"] for item in instance["BlockDeviceMappings"]])
        shutdown_behavior = aws("ec2", "describe-instance-attribute", instance_id=instance["InstanceId"],
                                attribute="instanceInitiatedShutdownBehavior")
        assert shutdown_behavior["InstanceInitiatedShutdownBehavior"]["Value"] == "terminate"
        (root / "shutdown-behavior.json").write_text(json.dumps(shutdown_behavior, indent=2) + "\n")
        actual_before = groups_snapshot()
        assert canonical(actual_before) == canonical(before)
        (root / "sg-before-rule.json").write_text(json.dumps(actual_before, indent=2) + "\n")
        rule = aws("ec2", "authorize-security-group-ingress", group_id=CONTROL_SG, ip_permissions=[dict(
            IpProtocol="tcp", FromPort=50052, ToPort=50052, IpRanges=[dict(CidrIp=ip + "/32", Description=run_id)])])
        update(root, ingress_rule_ids=[item["SecurityGroupRuleId"] for item in rule["SecurityGroupRules"]])
        after = groups_snapshot()
        (root / "sg-after-rule.json").write_text(json.dumps(after, indent=2) + "\n")
        assert canonical(dict(SecurityGroups=[g for g in after["SecurityGroups"] if g["GroupId"] != CONTROL_SG])) == canonical(dict(SecurityGroups=[g for g in before["SecurityGroups"] if g["GroupId"] != CONTROL_SG]))
        ssh_args[:] = ["ssh", "-i", str(root / "ssh-key"), "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                       "-o", "StrictHostKeyChecking=accept-new", "-o", f"UserKnownHostsFile={root / 'known_hosts'}", "ubuntu@" + ip]
        end = time.monotonic() + 300
        while True:
            probe = subprocess.run(ssh_args + ["true"], capture_output=True, timeout=10)
            if probe.returncode == 0:
                break
            health()
            assert time.monotonic() < end, "B SSH startup timeout"
            time.sleep(3)
        ssh(f"mkdir -p {remote_base}; sudo test -s /run/systemd/shutdown/scheduled; sudo cat /run/systemd/shutdown/scheduled; python3 -m venv {remote_base}/venv")
        requirements = subprocess.check_output(["uv", "pip", "freeze", "--python", sys.executable], text=True)
        (root / "requirements.txt").write_text("\n".join(line for line in requirements.splitlines() if not line.startswith("anyserve-runtime")) + "\n")
        for source in [args.model, args.wheel, HERE / "worker.py", root / "requirements.txt", root / "worker.token"]:
            scp(source, "ubuntu@" + ip + ":" + remote_base + "/")
        ssh(f"chmod 600 {remote_base}/worker.token; {remote_base}/venv/bin/pip install -r {remote_base}/requirements.txt {remote_base}/{args.wheel.name}", timeout=900)
        ssh(f"cd {remote_base}; nohup venv/bin/python worker.py --model {shlex.quote(args.model.name)} --run-dir {remote_base} --run-id {run_id} --host b --slots 1 --token-file {remote_base}/worker.token > worker.log 2>&1 < /dev/null &")
        end = time.monotonic() + 180
        while True:
            probe = subprocess.run(ssh_args + [f"test -s {remote_base}/b-ready.json"], capture_output=True, timeout=10)
            if probe.returncode == 0:
                break
            health()
            assert time.monotonic() < end, "B worker startup timeout"
            time.sleep(2)
        for name in ["b-ready.json", "b-goldens.npz"]:
            scp("ubuntu@" + ip + ":" + remote_base + "/" + name, root / name)
        ready = json.loads((root / "b-ready.json").read_text())
        assert ready["generation"] == generation() == json.loads((root / "a-ready.json").read_text())["generation"]
        print("B 就绪 " + instance["InstanceId"] + " " + ip, flush=True)
        metrics("q2-before")
        client, _ = spawn("q2", client_command + ["--phase", "q2"])
        wait(client)
        metrics("q2-after")
        print("Q2 完成 " + json.loads((root / "q2.json").read_text())["status"], flush=True)
        original_generation = generation()
        killed = False

        def kill_b():
            nonlocal killed
            marker = root / "q3-start.json"
            if not killed and marker.exists() and time.time_ns() >= json.loads(marker.read_text())["time_ns"] + 10_000_000_000:
                before_kill = time.time_ns()
                ssh(f"kill -KILL {int(ready['pid'])}")
                killed = True
                (root / "kill.json").write_text(json.dumps(dict(before_ns=before_kill, after_ns=time.time_ns(), pid=ready["pid"])) + "\n")

        client, _ = spawn("q3", client_command + ["--phase", "q3"])
        wait(client, callback=kill_b)
        assert killed and cp.poll() is None and generation() == original_generation
        scp("ubuntu@" + ip + ":" + remote_base + "/worker.log", root / "worker-b.log")
        starts = {}
        for line in (root / "worker-b.log").read_text().splitlines():
            if not line.startswith("{"):
                continue
            event = json.loads(line)
            if event.get("kind") == "start":
                starts[event["attempt_id"]] = event
            elif event.get("kind") == "finish":
                starts.pop(event["attempt_id"], None)
        restarted = {event["previous_attempt_id"] for line in (root / "q3-formal.jsonl").read_text().splitlines()
                     for event in json.loads(line)["events"] if event["type"] == "attempt_restarted"}
        assert starts.keys() & restarted, "Q3 INCONCLUSIVE: no unfinished B attempt matched a restart"
        all_events = [event for line in (root / "q3-formal.jsonl").read_text().splitlines()
                      for event in json.loads(line)["events"]]
        lost_at = min(event["at_ms"] for event in all_events if event["type"] == "attempt_restarted"
                      and event["previous_attempt_id"] in restarted)
        assert all(event["worker_id"] == run_id + "-a" for event in all_events
                   if event["type"] == "assigned" and event["at_ms"] > lost_at), "new assignment after loss was not A"
        metrics("q3-after")
        print("Q3 完成 PASS", flush=True)
        q2_status = json.loads((root / "q2.json").read_text())["status"]
        result = dict(status="PASS" if q2_status == "PASS" else "FAIL", q1="PASS", q2=q2_status,
                      q3="PASS", generation=original_generation)
    except BaseException as exc:
        result = dict(status="STOP", reason=f"{type(exc).__name__}: {exc}")
        print("停线 " + result["reason"], flush=True)
    finally:
        (root / "outcome.json").write_text(json.dumps(result, indent=2) + "\n")
        if ssh_args:
            try:
                subprocess.run(["scp", *ssh_args[1:-1], "ubuntu@" + read(root)["b_private_ip"] + ":" + remote_base + "/worker.log",
                                str(root / "worker-b.log")], capture_output=True, timeout=10, check=True)
            except Exception:
                pass
        receipt = cleanup(root)
        (root / "triton-after.txt").write_text(subprocess.check_output([
            "docker", "inspect", "--format", "{{.Name}} {{.State.Running}} {{.State.StartedAt}}",
            "triton", "gameplayai-micro-triton"], text=True))
        print("teardown 完成 " + str(receipt["complete"]), flush=True)
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError("SIGTERM")))
    raise SystemExit(main())
