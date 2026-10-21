"""Idempotent cleanup, also used as the detached deadline reaper."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time

PROFILE, REGION = "default", "us-east-1"
LOCAL_INSTANCE = "i-0c99268b5164d8ded"
CONTROL_SG = "sg-08424b17c71685f62"
LOCAL_SGS = ["sg-0d43d388e68a453da", "sg-02e4b8df0d48605c1", CONTROL_SG,
             "sg-05c933ec88247826f", "sg-021f70d9942b81dd5"]


def aws(service, operation, **values):
    command = ["aws", "--profile", PROFILE, "--region", REGION, "--output", "json",
               "--cli-connect-timeout", "10", "--cli-read-timeout", "30", service, operation]
    for key, value in values.items():
        command += ["--" + key.replace("_", "-"), json.dumps(value) if isinstance(value, (dict, list)) else str(value)]
    result = subprocess.run(command, capture_output=True, text=True,
                            env={**os.environ, "AWS_MAX_ATTEMPTS": "1"}, timeout=50)
    if result.returncode:
        raise RuntimeError(f"{operation}: {result.stderr[-2000:]}")
    return json.loads(result.stdout or "{}")


def read(root):
    return json.loads((Path(root) / "resource-ledger.json").read_text())


def update(root, **fields):
    root = Path(root)
    with (root / "ledger.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        value = read(root) if (root / "resource-ledger.json").exists() else {}
        value.update(fields)
        temporary = root / "ledger.tmp"
        temporary.write_text(json.dumps(value, indent=2) + "\n")
        temporary.replace(root / "resource-ledger.json")
        return value


def pid_ticks(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()[19]
    except FileNotFoundError:
        return None


def stop_process(item):
    pid = item["pid"]
    if pid_ticks(pid) != item["ticks"]:
        return
    os.killpg(pid, signal.SIGTERM)
    for _ in range(25):
        if pid_ticks(pid) != item["ticks"]:
            return
        time.sleep(.2)
    if pid_ticks(pid) == item["ticks"]:
        os.killpg(pid, signal.SIGKILL)


def groups_snapshot():
    return aws("ec2", "describe-security-groups", group_ids=LOCAL_SGS)


def canonical(groups):
    def ordered(value):
        if isinstance(value, dict):
            return {key: ordered(item) for key, item in value.items()}
        if isinstance(value, list):
            return sorted((ordered(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
        return value
    return json.dumps(ordered(groups), sort_keys=True)


def cleanup(root):
    root = Path(root)
    ledger = read(root)
    receipt = dict(time=datetime.now(timezone.utc).isoformat(), instances=[], volumes=[], errors=[])
    for item in ledger.get("processes", []):
        try:
            stop_process(item)
        except ProcessLookupError:
            pass
    if ledger.get("cloud_started"):
        filters = [dict(Name="tag:Experiment", Values=[ledger["run_id"]])]
        found = aws("ec2", "describe-instances", filters=filters)
        instances = [item for reservation in found["Reservations"] for item in reservation["Instances"]]
        assert instances or not ledger.get("launch_attempted") or ledger.get("launch_resolved"), "launch API outcome still unknown; retain reaper and retry discovery"
        for instance in instances:
            assert instance["InstanceId"] != LOCAL_INSTANCE
            tags = {tag["Key"]: tag["Value"] for tag in instance.get("Tags", [])}
            assert tags.get("Project") == "anyserve-gpu" and tags.get("Owner") == "codex"
            if instance["State"]["Name"] != "terminated":
                aws("ec2", "terminate-instances", instance_ids=[instance["InstanceId"]])
        for _ in range(90):
            found = aws("ec2", "describe-instances", filters=filters)
            instances = [item for reservation in found["Reservations"] for item in reservation["Instances"]]
            if all(item["State"]["Name"] == "terminated" for item in instances):
                break
            time.sleep(2)
        assert all(item["State"]["Name"] == "terminated" for item in instances)
        receipt["instances"] = [dict(id=item["InstanceId"], state=item["State"]["Name"],
                                     launched=item.get("LaunchTime")) for item in instances]
        request_ids = list(set([item["SpotInstanceRequestId"] for item in instances
                               if item.get("SpotInstanceRequestId")] + ledger.get("spot_request_ids", [])))
        if request_ids:
            requests = aws("ec2", "describe-spot-instance-requests", spot_instance_request_ids=request_ids)
            active = [item["SpotInstanceRequestId"] for item in requests["SpotInstanceRequests"]
                      if item["State"] in ["open", "active"]]
            if active:
                aws("ec2", "cancel-spot-instance-requests", spot_instance_request_ids=active)
            requests = aws("ec2", "describe-spot-instance-requests", spot_instance_request_ids=request_ids)
            assert all(item["State"] not in ["open", "active"] for item in requests["SpotInstanceRequests"])
            receipt["spot"] = requests
        # Discover owned rules even if the create API response was lost.
        rules = aws("ec2", "describe-security-group-rules", filters=[dict(Name="group-id", Values=[CONTROL_SG])])
        for rule in rules["SecurityGroupRules"]:
            if rule.get("Description") == ledger["run_id"]:
                assert rule["FromPort"] == rule["ToPort"] == 50052 and rule["IpProtocol"] == "tcp"
                assert rule.get("CidrIpv4") == ledger["b_private_ip"] + "/32"
                aws("ec2", "revoke-security-group-ingress", group_id=CONTROL_SG,
                    security_group_rule_ids=[rule["SecurityGroupRuleId"]])
        after = groups_snapshot()
        (root / "sg-after-teardown.json").write_text(json.dumps(after, indent=2) + "\n")
        before = json.loads((root / "sg-before.json").read_text())
        assert canonical(before) == canonical(after), "original SG rules not restored"
        receipt["original_security_groups_restored"] = True
        for _ in range(60):
            volumes = aws("ec2", "describe-volumes", filters=filters)["Volumes"]
            if not volumes:
                break
            time.sleep(2)
        receipt["volumes"] = volumes
        assert not volumes, "owned EBS volume remains"
        groups = aws("ec2", "describe-security-groups", filters=filters)["SecurityGroups"]
        for group in groups:
            assert group["GroupId"] not in LOCAL_SGS and group["GroupName"] == ledger["run_id"]
            aws("ec2", "delete-security-group", group_id=group["GroupId"])
        assert not aws("ec2", "describe-security-groups", filters=filters)["SecurityGroups"]
        local = aws("ec2", "describe-instances", instance_ids=[LOCAL_INSTANCE])["Reservations"][0]["Instances"][0]
        assert {item["GroupId"] for item in local["SecurityGroups"]} == set(LOCAL_SGS)
        receipt["temporary_security_group_deleted"] = True
    for name in ["client.token", "worker.token", "ssh-key", "ssh-key.pub"]:
        (root / name).unlink(missing_ok=True)
    receipt["complete"] = True
    (root / "cleanup-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    update(root, cleanup_complete=True)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--reaper", action="store_true")
    parser.add_argument("--instance-id", default="pending")
    parser.add_argument("--deadline", type=float)
    args = parser.parse_args()
    if args.instance_id != "pending":
        assert args.instance_id == read(args.run_dir).get("instance_id")
    if args.reaper:
        assert args.deadline
        (args.run_dir / "reaper-ready.json").write_text(json.dumps(dict(pid=os.getpid(), deadline=args.deadline)) + "\n")
        while time.time() < args.deadline:
            if read(args.run_dir).get("cleanup_complete"):
                return
            time.sleep(min(5, max(.05, args.deadline - time.time())))
    while True:
        try:
            cleanup(args.run_dir)
            return
        except Exception as exc:
            print(json.dumps(dict(cleanup_error=str(exc), time=time.time())), flush=True)
            if not args.reaper:
                raise
            time.sleep(10)


if __name__ == "__main__":
    main()
