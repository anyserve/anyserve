#!/usr/bin/env python3
"""Run the active-active suite against an isolated real PostgreSQL server.

Set ANYSERVE_TEST_POSTGRES_URL to use an existing disposable database instead.
Each test creates and removes its own schema; the DB user needs CREATE SCHEMA.
"""
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    container = None
    try:
        if not env.get("ANYSERVE_TEST_POSTGRES_URL"):
            container = "anyserve-pg-test-" + uuid.uuid4().hex[:12]
            password = secrets.token_hex(24)
            with tempfile.TemporaryDirectory(prefix="anyserve-pg-") as temporary:
                config = Path(temporary) / "postgres.env"
                config.write_text(f"POSTGRES_USER=anyserve\nPOSTGRES_DB=anyserve\nPOSTGRES_PASSWORD={password}\n")
                config.chmod(0o600)
                subprocess.run([
                    "docker", "run", "-d", "--rm", "--name", container,
                    "-p", "127.0.0.1::5432", "--env-file", str(config),
                    "postgres:18-bookworm",
                ], check=True, stdout=subprocess.DEVNULL)
            ports = json.loads(subprocess.check_output([
                "docker", "inspect", "--format", "{{json .NetworkSettings.Ports}}", container,
            ]))
            port = ports["5432/tcp"][0]["HostPort"]
            deadline = time.monotonic() + 60
            while subprocess.run(["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "anyserve", "-d", "anyserve"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
                if time.monotonic() >= deadline:
                    raise RuntimeError("PostgreSQL did not become ready within 60 seconds")
                time.sleep(0.25)
            env["ANYSERVE_TEST_POSTGRES_URL"] = f"postgresql://anyserve:{password}@127.0.0.1:{port}/anyserve"
        return subprocess.run([
            "cargo", "test", "--locked", "-p", "anyserve-core", "postgres_cluster_", "--", "--ignored",
        ], cwd=root, env=env).returncode
    finally:
        if container:
            subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    raise SystemExit(main())
