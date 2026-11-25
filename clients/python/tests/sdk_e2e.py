"""Exercise the built wheel against a real control-plane process.

Run after cargo build -p anyserve and installing the current wheel. No GPU,
external database, Redis, or additional Python dependency is required.
"""
from __future__ import annotations
import multiprocessing
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import uuid

from anyserve import AnyserveClient, ExecutionOptions, RpcError, WorkerConnection, inline_object, uri_object, serve, worker


def run_worker(endpoint: str, interface: str, kind: str, marker: str | None = None) -> None:
    if kind == "duplex":
        @worker(interface=interface, max_active_leases=2, pass_context=True, streaming_input=True)
        def handler(parts, context):
            buffer = bytearray()
            try:
                for part in parts:
                    if part["parameter_chunk"]:
                        continue
                    buffer.extend(part["data"])
                    if part["object_end"]:
                        yield b"result:" + bytes(buffer)
                        buffer.clear()
            finally:
                if marker and context.cancelled:
                    Path(marker).write_text("cancelled")
    else:
        @worker(interface=interface, max_active_leases=4, pass_context=True)
        def handler(payload, context):
            if kind == "slow":
                if marker:
                    Path(marker).write_text(context.attempt_id)
                until = time.monotonic() + 2
                while time.monotonic() < until:
                    if context.cancelled:
                        return b""
                    time.sleep(0.01)
            return payload
    serve(handler, endpoint, worker_id=f"python-{os.getpid()}", token="worker", reconnect_interval_secs=0.1)


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def eventually(check, timeout: float = 10):
    until = time.monotonic() + timeout
    while True:
        try:
            result = check()
            if result:
                return result
        except (RuntimeError, OSError):
            pass
        if time.monotonic() >= until:
            raise AssertionError("condition did not become true before its deadline")
        time.sleep(0.02)


def _fork_child_probe(endpoint: str, token: str, result_queue) -> None:
    try:
        client = AnyserveClient(endpoint, token=token)
        resp = client.list(limit=1)
        result_queue.put(("ok", isinstance(resp.get("jobs"), list)))
    except BaseException as exc:
        result_queue.put(("err", repr(exc)))


class SDKEndToEnd(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="anyserve-sdk-v2-")
        self.path = Path(self.directory.name)
        self.port = free_port()
        self.endpoint = f"http://127.0.0.1:{self.port}"
        self.binary = Path(os.environ.get("ANYSERVE_BIN", "target/debug/anyserve")).resolve()
        self.config = self.path / "server.toml"
        self.config.write_text(f'''[server]
grpc_host="127.0.0.1"
grpc_port={self.port}
[runtime]
heartbeat_interval_ms=500
heartbeat_timeout_ms=3000
[journal]
path="{self.path / 'journal.sqlite'}"
[auth]
client_tokens=["client"]
worker_tokens=["worker"]
''')
        self.workers = []
        self.process = None
        self.log = (self.path / "server.log").open("ab")
        self.start_server()

    def tearDown(self):
        for process in self.workers:
            process.terminate()
            process.join(timeout=3)
            if process.is_alive():
                process.kill()
                process.join(timeout=3)
        self.stop_server()
        self.log.close()
        self.directory.cleanup()

    def start_server(self):
        self.process = subprocess.Popen([str(self.binary), "serve", "--config", str(self.config)],
                                        stdout=self.log, stderr=self.log)
        def ready():
            if self.process.poll() is not None:
                self.log.flush()
                raise AssertionError((self.path / "server.log").read_text())
            client = AnyserveClient(self.endpoint, token="client")
            try:
                client.get("startup-probe")
            except RpcError as error:
                if error.status_code == 5:
                    return client
                raise
            raise AssertionError("startup probe unexpectedly exists")
        self.client = eventually(ready)

    def stop_server(self, kill=False):
        if self.process is not None and self.process.poll() is None:
            self.process.kill() if kill else self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)

    def start_worker(self, interface: str, kind="echo", marker: Path | None = None):
        process = multiprocessing.get_context("spawn").Process(target=run_worker,
            args=(self.endpoint, interface, kind, str(marker) if marker else None))
        process.start()
        self.workers.append(process)
        return process

    def test_durable_idempotency_and_large_inline_input_result(self):
        interface = "test.python.echo"
        self.start_worker(interface)
        payload = b"x" * (5 * 1024 * 1024)
        options = ExecutionOptions(total_timeout_ms=15_000, metadata={"business": "one"})
        job_id = str(uuid.uuid4())
        first = self.client.submit(interface, payload, job_id=job_id, options=options)
        self.assertTrue(first["created"])
        self.assertEqual(first["durability"], "durable")
        self.assertEqual(self.client.result(job_id)[0]["inline"], payload)
        self.assertFalse(self.client.submit(interface, payload, job_id=job_id, options=options)["created"])
        with self.assertRaises(RpcError) as conflict:
            self.client.submit(interface, b"different", job_id=job_id, options=options)
        self.assertEqual(conflict.exception.status_code, 6)

    def test_uri_objects_reject_empty_and_round_trip_opaque_references(self):
        interface = "test.python.uri"
        with self.assertRaisesRegex(ValueError, "URI must not be empty"):
            uri_object("")
        with self.assertRaisesRegex(ValueError, "URI must not be empty"):
            self.client.submit(interface, [{"uri": ""}])
        for invalid in [{}, {"inline": None, "uri": None}, {"inline": b"x", "uri": "video.mp4"}]:
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "exactly one"):
                self.client.submit(interface, [invalid])
        connection = WorkerConnection(self.endpoint, {
            "worker_id": "uri-worker", "interfaces": [interface], "max_concurrency": 1,
        }, "worker")
        try:
            objects = [inline_object(b""), uri_object("relative/video.mp4", {
                "content_type": "video/mp4", "size_bytes": "1099511627776",
            })]
            nullable_objects = [{"inline": b"", "uri": None, "metadata": None},
                                {**objects[1], "inline": None}]
            # Force URI input through InputData rather than the small Assign path.
            job = self.client.submit(interface, nullable_objects,
                options=ExecutionOptions(params=b"p" * (65 * 1024), total_timeout_ms=10000))
            message = eventually(lambda: connection.receive(0.1))
            self.assertEqual(message["type"], "assignment")
            connection.accept(message["lease_id"])
            attempt = message["assignment"].open()
            self.assertEqual(attempt.inputs, objects)
            with self.assertRaisesRegex(ValueError, "URI must not be empty"):
                attempt.complete([{"uri": ""}])
            attempt.complete(nullable_objects)
            self.assertEqual(self.client.result(job["job_id"]), objects)
        finally:
            connection.close()

    def test_duplex_output_arrives_before_input_end_with_large_object_boundaries(self):
        interface = "test.python.duplex"
        self.start_worker(interface, "duplex")
        with self.client.invoke_stream(interface, options=ExecutionOptions(total_timeout_ms=10_000)) as invocation:
            for payload in [b"first", b"x" * (256 * 1024)]:
                invocation.send(payload)
                expected = b"result:" + payload
                received = bytearray()
                while len(received) < len(expected):
                    event = next(invocation)
                    self.assertNotEqual(event["type"], "finished")
                    if event["type"] == "output":
                        received.extend(event["data"])
                self.assertEqual(bytes(received), expected)
            invocation.finish_input()
            terminal = [event for event in invocation if event["type"] == "finished"]
            self.assertEqual(terminal[0]["state"], "succeeded")

    def test_finished_then_close_or_drop_keeps_invoke_and_watch_usable(self):
        interface = "test.python.finished-drop"
        self.start_worker(interface)
        options = ExecutionOptions(total_timeout_ms=10_000)
        # Both methods share one native channel. Cross h2's local reset limit
        # without explicitly asking either iterator for EOF after Finished.
        for index in range(2048):
            payload = f"call-{index}".encode()
            for route in ("invoke", "watch"):
                if route == "invoke":
                    events = self.client.invoke(interface, payload, options=options)
                else:
                    job = self.client.submit(interface, payload, durability="ephemeral", options=options)
                    events = self.client.watch(job["job_id"])
                for event in events:
                    if event["type"] == "finished":
                        self.assertEqual(event["state"], "succeeded")
                        self.assertEqual(event["outputs"][0]["inline"], payload)
                        break
                else:
                    self.fail(f"{route} {index} ended without Finished")
                if index % 2 == 0:
                    events.close()
                del events

    def test_closing_an_invocation_cancels_a_handler_waiting_for_more_input(self):
        interface = "test.python.cancel"
        marker = self.path / "cancelled"
        self.start_worker(interface, "duplex", marker)
        invocation = self.client.invoke_stream(interface, options=ExecutionOptions(total_timeout_ms=10_000))
        invocation.send(b"first")
        job_id = None
        while True:
            event = next(invocation)
            job_id = event["job_id"]
            if event["type"] == "output":
                break
        invocation.close()
        eventually(lambda: marker.exists())
        eventually(lambda: self.client.get(job_id)["state"] == "cancelled")

    def test_sigkill_recovers_an_inflight_durable_job_and_fences_the_old_attempt(self):
        interface = "test.python.recovery"
        marker = self.path / "attempt"
        process = self.start_worker(interface, "slow", marker)
        job = self.client.submit(interface, b"recover me", options=ExecutionOptions(total_timeout_ms=20_000))
        eventually(lambda: marker.exists())
        self.stop_server(kill=True)
        self.start_server()
        self.assertTrue(process.is_alive(), "the worker should reconnect after a control-plane restart")
        self.assertEqual(self.client.result(job["job_id"])[0]["inline"], b"recover me")
        self.assertEqual(self.client.get(job["job_id"])["attempt_count"], 2)

    def test_worker_survives_graceful_server_restart(self):
        interface = "test.python.graceful"
        process = self.start_worker(interface)
        self.assertEqual(self.client.call(interface, b"before"), b"before")
        self.stop_server()
        self.assertTrue(process.is_alive(), "server Drain must not exit the Python worker")
        self.start_server()
        self.assertEqual(self.client.call(interface, b"after", options=ExecutionOptions(total_timeout_ms=10_000)), b"after")
        self.assertTrue(process.is_alive())

    def test_closing_a_connection_releases_the_gil_while_receive_holds_the_lock(self):
        from anyserve import WorkerConnection
        connection = WorkerConnection(self.endpoint, {"worker_id": "gil-close", "interfaces": ["test.gil"], "max_concurrency": 1}, "worker")
        entered = threading.Event()
        stop = threading.Event()
        ticks = []
        errors = []
        def receive():
            entered.set()
            try:
                connection.receive(0.4)
            except RuntimeError as error:
                errors.append(error)
        def tick():
            while not stop.is_set():
                ticks.append(True)
                time.sleep(0.005)
        receiver = threading.Thread(target=receive)
        ticker = threading.Thread(target=tick)
        receiver.start()
        ticker.start()
        try:
            self.assertTrue(entered.wait(timeout=2))
            time.sleep(0.03)
            before = len(ticks)
            connection.close()
            self.assertGreater(len(ticks) - before, 2, "close held the GIL while waiting for receive")
        finally:
            stop.set()
            receiver.join(timeout=2)
            ticker.join(timeout=2)
            connection.close()
        self.assertFalse(receiver.is_alive())
        self.assertEqual(errors, [])

    def test_role_errors_are_typed(self):
        for token, code in [("", 16), ("invalid", 16), ("worker", 7)]:
            client = AnyserveClient(self.endpoint, token=token)
            with self.assertRaises(RpcError) as denied:
                client.get("missing")
            self.assertEqual(denied.exception.status_code, code)

    def test_async_workers_multi_interface_and_async_client(self):
        import asyncio
        import json
        import subprocess
        import sys
        from pathlib import Path
        from anyserve import AsyncAnyserveClient

        worker_code = """
import asyncio
import sys
from anyserve import serve, worker

@worker(interface="test.python.async_unary", pool="edge")
async def async_unary(payload: bytes) -> bytes:
    await asyncio.sleep(0)
    return b"async:" + payload

@worker(interface="test.python.async_stream", codec="json", pool="edge")
async def async_stream(obj: dict):
    await asyncio.sleep(0)
    yield {"step": 1, "x": obj["x"]}
    yield {"step": 2, "x": obj["x"]}

serve([async_unary, async_stream], sys.argv[1], token="worker", worker_id="py-async-multi")
"""
        proc = subprocess.Popen(
            [sys.executable, "-c", worker_code, self.endpoint],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            async def run_async():
                client = AsyncAnyserveClient(self.endpoint, token="client")
                # Wait briefly until worker connects and completes first call.
                for _ in range(50):
                    try:
                        out = await client.call(
                            "test.python.async_unary",
                            b"hello",
                            options=ExecutionOptions(
                                affinity_key="robot-7",
                                fairness_key="vip",
                                concurrency_key="arm-1",
                                total_timeout_ms=5_000,
                            ),
                        )
                        assert out == b"async:hello", out
                        break
                    except RpcError:
                        await asyncio.sleep(0.05)
                else:
                    raise AssertionError("async worker did not become ready")

                chunks = []
                async with await client.invoke(
                    "test.python.async_stream",
                    b'{"x":41}',
                    options=ExecutionOptions(total_timeout_ms=5_000),
                ) as evs:
                    async for ev in evs:
                        if ev["type"] == "output":
                            chunks.append(ev["data"])
                return chunks

            chunks = asyncio.run(run_async())
            self.assertEqual([json.loads(c) for c in b"".join(chunks).splitlines()], [{"step": 1, "x": 41}, {"step": 2, "x": 41}])
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_uri_sha256_idempotency(self):
        from anyserve import uri_object

        digest = "a" * 64
        j1 = self.client.submit(
            "test.python.uri_idem",
            [uri_object("https://cdn.example/part.bin?sig=1", sha256=digest, size_bytes=10)],
            job_id="uri-idem-1",
            durability="durable",
            options=ExecutionOptions(affinity_key="a1", fairness_key="f1", concurrency_key="c1"),
        )
        self.assertTrue(j1["created"])
        j2 = self.client.submit(
            "test.python.uri_idem",
            [uri_object("https://cdn.example/part.bin?sig=2", sha256=digest, size_bytes=10)],
            job_id="uri-idem-1",
            durability="durable",
            options=ExecutionOptions(affinity_key="a2", fairness_key="f2", concurrency_key="c2"),
        )
        self.assertFalse(j2["created"])

    def test_async_streaming_invocation_list_and_conflict_policy_reject(self):
        import asyncio
        from anyserve import AsyncAnyserveClient, AsyncStreamingInvocation

        interface = "test.python.async_duplex"
        self.start_worker(interface, "duplex")

        async def run_duplex():
            ac = AsyncAnyserveClient(self.endpoint, token="client")
            async with await ac.invoke_stream(
                interface, options=ExecutionOptions(total_timeout_ms=10_000)
            ) as inv:
                self.assertIsInstance(inv, AsyncStreamingInvocation)
                await inv.send(b"hello-async")
                received = bytearray()
                async for ev in inv:
                    if ev["type"] == "output":
                        received.extend(ev["data"])
                        if bytes(received) == b"result:hello-async":
                            break
                    else:
                        self.assertNotEqual(ev["type"], "finished")
                self.assertEqual(bytes(received), b"result:hello-async")
                await inv.finish_input()
                terminal = None
                async for ev in inv:
                    if ev["type"] == "finished":
                        terminal = ev
                        break
                self.assertIsNotNone(terminal)
                self.assertEqual(terminal["state"], "succeeded")

            listed = await ac.list(states=["succeeded"], limit=1)
            self.assertEqual(len(listed["jobs"]), 1)
            return listed

        listed = asyncio.run(run_duplex())
        self.assertEqual(listed["jobs"][0]["state"], "succeeded")

        # conflict_policy="reject" rejects duplicate job_id even with identical spec.
        job_id = str(uuid.uuid4())
        first = self.client.submit("test.python.conflict", b"same", job_id=job_id, conflict_policy="reject")
        self.assertTrue(first["created"])
        with self.assertRaises(RpcError) as dup:
            self.client.submit("test.python.conflict", b"same", job_id=job_id, conflict_policy="reject")
        self.assertEqual(dup.exception.status_code, 6)

        # Submit a second queued job and test sync list pagination.
        job_id2 = str(uuid.uuid4())
        self.client.submit("test.python.conflict", b"other", job_id=job_id2)
        page1 = self.client.list(states=["queued"], limit=1)
        self.assertEqual(len(page1["jobs"]), 1)
        self.assertTrue(page1["next_page_token"])
        page2 = self.client.list(states=["queued"], limit=1, page_token=page1["next_page_token"])
        self.assertEqual(len(page2["jobs"]), 1)
        self.assertNotEqual(page1["jobs"][0]["job_id"], page2["jobs"][0]["job_id"])

    def test_update_load_validation_and_worker_error_codes(self):
        interface = "test.python.load_and_errors"
        conn = WorkerConnection(
            self.endpoint,
            {"worker_id": "load-worker", "interfaces": [interface], "max_concurrency": 1},
            "worker",
        )
        try:
            for bad in (float("nan"), float("inf"), float("-inf")):
                with self.subTest(bad_util=bad), self.assertRaises(ValueError):
                    conn.update_load(utilization=bad)
                with self.subTest(bad_custom=bad), self.assertRaises(ValueError):
                    conn.update_load(custom={"vram": bad})
            # Out-of-range finite utilization clamps cleanly.
            conn.update_load(queued=1, running=1, utilization=-0.5, custom={"vram": 4.0})
            conn.update_load(queued=2, running=1, utilization=1.5, custom={"vram": 8.0})
            conn.credit(1)

            # Default fail code is "worker_failed"; custom code is preserved.
            for expected_code, fail_kwargs in [
                ("worker_failed", {}),
                ("MY_CODE", {"code": "MY_CODE"}),
            ]:
                job = self.client.submit(
                    interface,
                    b"x",
                    options=ExecutionOptions(total_timeout_ms=10_000),
                )
                msg = eventually(lambda: conn.receive(0.1))
                self.assertEqual(msg["type"], "assignment")
                conn.accept(msg["lease_id"])
                attempt = msg["assignment"].open()
                attempt.fail("boom", **fail_kwargs)
                conn.credit(1)
                done = eventually(
                    lambda jid=job["job_id"]: (
                        g if (g := self.client.get(jid))["state"] == "failed" else None
                    )
                )
                self.assertEqual(done["error"]["code"], expected_code)
        finally:
            conn.close()

    def test_fork_safety_after_parent_uses_shared_runtime(self):
        if "fork" not in multiprocessing.get_all_start_methods():
            self.skipTest("fork start method not available on this platform")
        # Ensure parent initializes SHARED_RUNTIME before forking.
        self.assertIsInstance(self.client.list(limit=1)["jobs"], list)
        ctx = multiprocessing.get_context("fork")
        queue = ctx.Queue()
        proc = ctx.Process(target=_fork_child_probe, args=(self.endpoint, "client", queue))
        proc.start()
        proc.join(timeout=5)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=2)
            self.fail("forked child deadlocked using shared_runtime()")
        self.assertEqual(proc.exitcode, 0)
        self.assertEqual(queue.get(timeout=1), ("ok", True))


if __name__ == "__main__":
    unittest.main(verbosity=2)
