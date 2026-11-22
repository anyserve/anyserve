"""Cross-language E2E verification of the unmodified Python `_native` SDK
against `node --experimental-strip-types deploy/cloudflare/src/node-native-gateway.ts`
over `ws://` (HTTP unary + RFC 6455 WebSocket streaming + R2 spill).
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import time
import unittest
import urllib.request
import uuid

from anyserve import (
    AnyserveClient,
    ExecutionOptions,
    RpcError,
    serve,
    worker,
)

CLIENT_TOKEN = "py-native-client-tok"
WORKER_TOKEN = "py-native-worker-tok"
REPO_ROOT = Path(__file__).resolve().parents[3]
GATEWAY_SCRIPT = REPO_ROOT / "deploy" / "cloudflare" / "src" / "node-native-gateway.ts"


def _run_py_worker(ws_endpoint: str, interface: str, mode: str) -> None:
    if mode == "stream":
        @worker(interface=interface, max_active_leases=2, streaming_input=True)
        def stream_handler(parts):
            buf = bytearray()
            for part in parts:
                if part["parameter_chunk"]:
                    continue
                buf.extend(part["data"])
                if part["object_end"]:
                    yield b"py-stream:" + bytes(buf)
                    buf.clear()

        serve(
            stream_handler,
            ws_endpoint,
            worker_id=f"py-native-stream-{os.getpid()}",
            token=WORKER_TOKEN,
            reconnect_interval_secs=0.1,
        )
    else:
        @worker(interface=interface, max_active_leases=4)
        def echo_handler(payload: bytes) -> bytes:
            return payload

        serve(
            echo_handler,
            ws_endpoint,
            worker_id=f"py-native-echo-{os.getpid()}",
            token=WORKER_TOKEN,
            reconnect_interval_secs=0.1,
        )


class PythonNativeGatewayE2E(unittest.TestCase):
    def setUp(self) -> None:
        env = os.environ.copy()
        env["HOST"] = "127.0.0.1"
        env["PORT"] = "0"
        env["ANYSERVE_TEST_HOOKS"] = "1"
        env["ANYSERVE_CLIENT_TOKENS"] = CLIENT_TOKEN
        env["ANYSERVE_WORKER_TOKENS"] = WORKER_TOKEN

        self.gateway = subprocess.Popen(
            ["node", "--experimental-strip-types", str(GATEWAY_SCRIPT)],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        assert self.gateway.stdout is not None
        deadline = time.monotonic() + 10.0
        http_url = None
        ws_url = None
        while time.monotonic() < deadline:
            line = self.gateway.stdout.readline()
            if not line:
                if self.gateway.poll() is not None:
                    raise AssertionError("node-native-gateway exited prematurely")
                continue
            if line.startswith("ANYSERVE_NATIVE_GATEWAY_LISTENING "):
                parts = line.strip().split()
                http_url, ws_url = parts[1], parts[2]
                break
        if not http_url or not ws_url:
            raise AssertionError("did not receive listening banner from node-native-gateway")

        self.http_url = http_url
        self.ws_url = ws_url
        self.workers: list[multiprocessing.Process] = []
        self.client = AnyserveClient(self.ws_url, token=CLIENT_TOKEN)

    def tearDown(self) -> None:
        for proc in self.workers:
            proc.terminate()
            proc.join(timeout=3)
            if proc.is_alive():
                proc.kill()
                proc.join(timeout=2)
        if self.gateway.poll() is None:
            self.gateway.terminate()
            try:
                self.gateway.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.gateway.kill()
                self.gateway.wait(timeout=2)
        if self.gateway.stdout is not None:
            self.gateway.stdout.close()

    def _start_worker(self, interface: str, mode: str = "echo") -> multiprocessing.Process:
        ctx = multiprocessing.get_context("spawn")
        proc = ctx.Process(target=_run_py_worker, args=(self.ws_url, interface, mode))
        proc.start()
        self.workers.append(proc)
        return proc

    def _stats(self) -> dict:
        with urllib.request.urlopen(f"{self.http_url}/__test/stats", timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def test_python_sdk_ws_invoke_stream_and_durable_2_5mib_r2_spill(self) -> None:
        self._start_worker("py.native.echo", "echo")
        self._start_worker("py.native.stream", "stream")

        # 1. Auth role errors over ws://
        bad_client = AnyserveClient(self.ws_url, token=WORKER_TOKEN)
        with self.assertRaises(RpcError) as denied:
            bad_client.get("missing")
        self.assertEqual(denied.exception.status_code, 7)

        # 2. Unary/streaming `Invoke` (`client.call` & `client.invoke_stream`) over ws://
        invoke_payload = os.urandom(512 * 1024)
        echoed = self.client.call(
            "py.native.echo",
            invoke_payload,
            options=ExecutionOptions(total_timeout_ms=15_000),
        )
        self.assertEqual(echoed, invoke_payload)

        with self.client.invoke_stream(
            "py.native.stream",
            options=ExecutionOptions(total_timeout_ms=15_000),
        ) as inv:
            chunk = b"hello-from-python-ws-" * 1024
            inv.send(chunk)
            expected = b"py-stream:" + chunk
            got = bytearray()
            while len(got) < len(expected):
                ev = next(inv)
                if ev["type"] == "output" and not ev.get("progress", False):
                    got.extend(ev["data"])
            self.assertEqual(bytes(got), expected)
            inv.finish_input()
            fin = [ev for ev in inv if ev["type"] == "finished"]
            self.assertEqual(len(fin), 1)
            self.assertEqual(fin[0]["state"], "succeeded")

        # 3. Durable `Submit` + `Watch` + `GetJob(include_outputs=True)` with 2.5 MiB R2 spill!
        before_puts = self._stats()["r2_put_count"]
        payload_2_5m = os.urandom(2 * 1024 * 1024 + 512 * 1024)
        expected_sha = hashlib.sha256(payload_2_5m).hexdigest()
        job_id = f"py-r2-{uuid.uuid4()}"

        submitted = self.client.submit(
            "py.native.echo",
            payload_2_5m,
            job_id=job_id,
            durability="durable",
            options=ExecutionOptions(total_timeout_ms=20_000),
        )
        self.assertTrue(submitted["created"])
        self.assertEqual(submitted["durability"], "durable")

        # Watch live events until Finished
        watched_outputs = None
        for ev in self.client.watch(job_id):
            if ev["type"] == "finished":
                self.assertEqual(ev["state"], "succeeded")
                watched_outputs = ev["outputs"]
                break
        assert watched_outputs is not None
        self.assertEqual(
            hashlib.sha256(watched_outputs[0]["inline"]).hexdigest(),
            expected_sha,
        )

        # Verify GetJob(include_outputs=True) and idempotent duplicate Submit
        fetched = self.client.get(job_id, include_outputs=True)
        self.assertEqual(fetched["state"], "succeeded")
        self.assertEqual(
            hashlib.sha256(fetched["outputs"][0]["inline"]).hexdigest(),
            expected_sha,
        )
        dedup = self.client.submit(
            "py.native.echo",
            payload_2_5m,
            job_id=job_id,
            durability="durable",
            options=ExecutionOptions(total_timeout_ms=20_000),
        )
        self.assertFalse(dedup["created"])

        after_puts = self._stats()["r2_put_count"]
        self.assertGreaterEqual(
            after_puts,
            before_puts + 2,
            "2.5 MiB input and output must spill to R2",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
