from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
from collections import deque
from unittest.mock import patch

from anyserve import (
    AsyncAnyserveClient,
    ExecutionClient,
    RpcError,
    ExecutionOptions,
    WorkerError,
    get_handler_spec,
    serve,
    uri_object,
    worker,
)
from anyserve.execution import _spec, _terminal_outputs
from anyserve.highlevel import _process_assignment


class Assignment:
    def __init__(self, lease_id="lease", payload=b"hello"):
        self.lease_id = lease_id
        self.job_id = lease_id
        self.cancelled = False
        self.reserved_capacity = {"slot": 1}
        self.attempt = Attempt(self, payload)

    def cancel(self):
        self.cancelled = True

    def open(self):
        return self.attempt


class Attempt:
    def __init__(self, assignment, payload):
        self.assignment = assignment
        self.terminal_replay = False
        self.inputs = [{"inline": payload, "metadata": {}}]
        self.frames = []
        self.outputs = None
        self.failure = None
        self.failure_code = None
        self.on_frame = None

    @property
    def cancelled(self):
        return self.assignment.cancelled

    def frame(self, data):
        self.frames.append(data)
        if self.on_frame:
            self.on_frame()

    def complete(self, outputs):
        self.outputs = outputs

    def fail(self, message, retryable=False, code=None):
        self.failure = (message, retryable)
        self.failure_code = code


class RPCFailure(RuntimeError):
    def __init__(self, code):
        super().__init__(f"test RPC status {code}")
        self.status_code = code


class Connection:
    def __init__(self, messages=(), stop_when=lambda: False):
        self.messages = deque(messages)
        self.stop_when = stop_when
        self.credits = []
        self.acks = []
        self.accepted = []
        self.closed = False
        self.closed_before_stop = None
        self.session_id = "session"
        self.resumed_lease_ids = []
        self.deadline = time.monotonic() + 3

    def credit(self, free):
        self.credits.append(free)

    def accept(self, lease_id, accepted, reason):
        self.accepted.append((lease_id, accepted))

    def receive(self, timeout):
        if self.closed:
            raise RPCFailure(14)
        if self.messages:
            message = self.messages.popleft()
            if isinstance(message, BaseException):
                raise message
            return message
        if self.stop_when():
            self.closed_before_stop = self.closed
            raise KeyboardInterrupt
        if time.monotonic() >= self.deadline:
            raise AssertionError("worker control loop did not reach the expected state")
        time.sleep(0.001)
        return None

    def cancel_ack(self, lease_id):
        self.acks.append(lease_id)

    def close(self):
        self.closed = True

    def drain(self):
        pass


class HighLevelTests(unittest.TestCase):
    def test_normal_handler_returns_a_terminal_result_without_a_second_stream(self):
        @worker(interface="echo", max_active_leases=4)
        def echo(payload):
            return payload.upper()

        assignment = Assignment()
        _process_assignment(echo, get_handler_spec(echo), assignment)
        self.assertEqual(assignment.attempt.outputs, [{"inline": b"HELLO", "metadata": {}}])
        self.assertEqual(assignment.attempt.frames, [])
        self.assertEqual(echo.spec.capacity["slot"], 4)

    def test_invalid_json_and_handler_failure_end_with_one_non_retryable_failure(self):
        @worker(interface="json", codec="json")
        def identity(value):
            return value

        assignment = Assignment(payload=b"not-json")
        _process_assignment(identity, identity.spec, assignment)
        self.assertIn("JSONDecodeError", assignment.attempt.failure[0])
        self.assertFalse(assignment.attempt.failure[1])
        self.assertIsNone(assignment.attempt.outputs)

        @worker(interface="failing")
        def failing(_):
            raise ValueError("bad input")

        assignment = Assignment()
        _process_assignment(failing, failing.spec, assignment)
        self.assertEqual(assignment.attempt.failure, ("ValueError: bad input", False))

    def test_worker_error_sets_the_failure_message_and_retryability(self):
        @worker(interface="flaky")
        def flaky(_):
            raise WorkerError("backend unreachable", retryable=True)

        assignment = Assignment()
        _process_assignment(flaky, flaky.spec, assignment)
        self.assertEqual(assignment.attempt.failure, ("backend unreachable", True))

        @worker(interface="strict")
        def strict(_):
            raise WorkerError("malformed request")

        assignment = Assignment()
        _process_assignment(strict, strict.spec, assignment)
        self.assertEqual(assignment.attempt.failure, ("malformed request", False))

    def test_generator_cancellation_closes_the_generator_and_never_completes(self):
        cleaned = threading.Event()

        @worker(interface="stream", pass_context=True)
        def stream(_, context):
            try:
                yield b"first"
                yield b"second"
            finally:
                cleaned.set()

        assignment = Assignment()
        assignment.attempt.on_frame = assignment.cancel
        _process_assignment(stream, stream.spec, assignment)
        self.assertEqual(assignment.attempt.frames, [b"first"])
        self.assertTrue(cleaned.is_set())
        self.assertIsNone(assignment.attempt.outputs)
        self.assertIsNone(assignment.attempt.failure)

    def test_known_cancellation_never_calls_the_handler(self):
        called = []

        @worker(interface="cancel")
        def handler(_):
            called.append(True)
            return b"unused"

        assignment = Assignment()
        assignment.cancel()
        _process_assignment(handler, handler.spec, assignment)
        self.assertEqual(called, [])

    def test_json_generator_has_record_boundaries(self):
        @worker(interface="json-stream", codec="json")
        def stream(_):
            yield {"n": 1}
            yield {"n": 2}

        assignment = Assignment(payload=b"{}")
        _process_assignment(stream, stream.spec, assignment)
        values = [json.loads(line) for line in b"".join(assignment.attempt.frames).splitlines()]
        self.assertEqual(values, [{"n": 1}, {"n": 2}])

    def test_serve_runs_concurrently_and_reports_capacity_on_completion(self):
        entered = threading.Barrier(2)
        assignments = [Assignment("one"), Assignment("two")]

        @worker(interface="concurrent", max_active_leases=2)
        def handler(payload):
            entered.wait(timeout=2)
            return payload

        class Connection:
            def __init__(self):
                self.messages = deque(assignments)
                self.credits = []

            def credit(self, free):
                self.credits.append(free)

            def accept(self, lease_id, accepted, reason):
                self.assert_accepted = accepted

            def receive(self, timeout):
                if self.messages:
                    assignment = self.messages.popleft()
                    return {"type": "assignment", "lease_id": assignment.lease_id, "assignment": assignment}
                if all(assignment.attempt.outputs is not None for assignment in assignments):
                    raise KeyboardInterrupt
                time.sleep(0.001)
                return None

            def drain(self):
                pass

            def close(self):
                pass

            def cancel_ack(self, lease_id):
                pass

        connection = Connection()
        with self.assertRaises(KeyboardInterrupt):
            serve(handler, connection=connection)
        self.assertTrue(all(assignment.attempt.outputs for assignment in assignments))
        self.assertTrue(connection.assert_accepted)
        self.assertTrue(all(0 <= credit <= 2 for credit in connection.credits))
        self.assertIn(2, connection.credits)

    def test_assignment_arriving_while_the_last_lease_finishes_is_accepted(self):
        # The control plane frees a slot when it commits the terminal message
        # and may assign the next job before the handler thread has returned.
        first, second = Assignment("one"), Assignment("two")
        decided = threading.Event()
        decisions = {}

        @worker(interface="single", max_active_leases=1)
        def handler(payload):
            return payload

        class Connection:
            def __init__(self):
                self.messages = deque([first])

            def credit(self, free):
                pass

            def accept(self, lease_id, accepted, reason):
                decisions[lease_id] = (accepted, reason)
                if lease_id == "two":
                    decided.set()

            def receive(self, timeout):
                if self.messages:
                    assignment = self.messages.popleft()
                    return {"type": "assignment", "lease_id": assignment.lease_id, "assignment": assignment}
                if second.attempt.outputs is not None or (decided.is_set() and not decisions["two"][0]):
                    raise KeyboardInterrupt
                time.sleep(0.001)
                return None

            def drain(self):
                pass

            def close(self):
                pass

            def cancel_ack(self, lease_id):
                pass

        connection = Connection()

        def complete_then_next(outputs):
            # The terminal message is committed: the next assignment is on the
            # wire while this thread is still inside the handler call.
            first.attempt.outputs = outputs
            connection.messages.append(second)
            self.assertTrue(decided.wait(timeout=2), "the control loop never saw the second assignment")

        first.attempt.complete = complete_then_next
        with self.assertRaises(KeyboardInterrupt):
            serve(handler, connection=connection)
        self.assertEqual(decisions["one"], (True, ""))
        self.assertEqual(decisions["two"], (True, ""))
        self.assertEqual(second.attempt.outputs, [{"inline": b"hello", "metadata": {}}])

    def test_success_requires_a_terminal_event(self):
        with self.assertRaisesRegex(RuntimeError, "without a terminal"):
            _terminal_outputs(iter([{"type": "output", "data": b"partial"}]))
        with self.assertRaisesRegex(RuntimeError, "output_interrupted"):
            _terminal_outputs(iter([{"type": "finished", "state": "failed", "error": {"code": "output_interrupted", "message": "worker lost"}}]))

    def test_execution_options_do_not_share_mutable_defaults(self):
        first = ExecutionOptions()
        first.metadata["tenant"] = "a"
        self.assertEqual(ExecutionOptions().metadata, {})

    def test_decorator_rejects_invalid_configuration(self):
        for kwargs in [{"interface": ""}, {"interface": "x", "codec": "pickle"}, {"interface": "x", "max_active_leases": 0}]:
            with self.assertRaises(ValueError):
                worker(**kwargs)

    def test_permanent_connection_errors_escape_without_retrying(self):
        @worker(interface="permanent")
        def handler(payload):
            return payload

        for code in [16, 7, 3]:
            with self.subTest(code=code), patch("anyserve.highlevel.WorkerConnection", side_effect=[RPCFailure(code), AssertionError("permanent error was retried")]) as connect, patch("anyserve.highlevel.time.sleep") as sleep:
                with self.assertRaises(RPCFailure):
                    serve(handler, "http://unused")
                self.assertEqual(connect.call_count, 1)
                sleep.assert_not_called()
            connection = Connection([RPCFailure(code)])
            with self.subTest(control_code=code), patch("anyserve.highlevel.WorkerConnection", return_value=connection) as connect, patch("anyserve.highlevel.time.sleep") as sleep:
                with self.assertRaises(RPCFailure):
                    serve(handler, "http://unused")
                self.assertEqual(connect.call_count, 1)
                sleep.assert_not_called()

    def test_endpoint_falls_back_to_the_environment_then_the_local_default(self):
        @worker(interface="env")
        def handler(payload):
            return payload

        for env, explicit, expected in [
            ({}, None, "http://127.0.0.1:50052"),
            ({"ANYSERVE_ENDPOINT": "http://cp:50052"}, None, "http://cp:50052"),
            ({"ANYSERVE_ENDPOINT": "http://cp:50052"}, "http://explicit:1", "http://explicit:1"),
        ]:
            with self.subTest(env=env, explicit=explicit), patch.dict(os.environ, env, clear=False), \
                    patch("anyserve.highlevel.WorkerConnection", side_effect=RPCFailure(3)) as connect:
                if "ANYSERVE_ENDPOINT" not in env:
                    os.environ.pop("ANYSERVE_ENDPOINT", None)
                with self.assertRaises(RPCFailure):
                    serve(handler, explicit)
                self.assertEqual(connect.call_args.args[0], expected)

    def test_transient_connection_errors_are_logged_before_retry(self):
        @worker(interface="transient")
        def handler(payload):
            return payload

        connection = Connection([KeyboardInterrupt()])
        with patch("anyserve.highlevel.WorkerConnection", side_effect=[RPCFailure(14), connection]) as connect, patch("anyserve.highlevel.time.sleep"), self.assertLogs("anyserve.highlevel", level="WARNING") as logs:
            with self.assertRaises(KeyboardInterrupt):
                serve(handler, "http://unused")
        self.assertEqual(connect.call_count, 2)
        self.assertTrue(any("reconnecting" in record for record in logs.output))

    def test_fenced_cancellation_acknowledges_without_reconnecting_other_leases(self):
        @worker(interface="fenced")
        def handler(payload):
            return payload

        for code in [9, 10]:
            assignment = Assignment()
            def complete(_outputs, code=code):
                assignment.cancel()
                raise RPCFailure(code)
            assignment.attempt.complete = complete
            connection = Connection([{"type": "assignment", "lease_id": assignment.lease_id, "assignment": assignment}], stop_when=lambda: bool(connection.acks) and connection.credits[-1] == 1)
            with self.subTest(code=code), self.assertRaises(KeyboardInterrupt):
                serve(handler, connection=connection)
            self.assertEqual(connection.acks, [assignment.lease_id])
            self.assertFalse(connection.closed_before_stop)

    def test_transport_failure_closes_the_session_and_reject_refreshes_credit(self):
        @worker(interface="transport")
        def handler(payload):
            return payload

        for code in [14, 4, 13, 2, 1]:
            assignment = Assignment()
            def complete(_outputs, code=code):
                raise RPCFailure(code)
            assignment.attempt.complete = complete
            connection = Connection([{"type": "assignment", "lease_id": assignment.lease_id, "assignment": assignment}])
            with self.subTest(code=code), self.assertRaises(RPCFailure):
                serve(handler, connection=connection)
            self.assertTrue(connection.closed)

        assignment = Assignment()
        assignment.reserved_capacity = {"gpu": 1}
        connection = Connection([{"type": "assignment", "lease_id": assignment.lease_id, "assignment": assignment}, KeyboardInterrupt()])
        with self.assertRaises(KeyboardInterrupt):
            serve(handler, connection=connection)
        self.assertEqual(connection.accepted, [(assignment.lease_id, False)])
        self.assertEqual(connection.credits, [1, 1])

    def test_drain_stops_new_work_without_stopping_or_cancelling_the_handler(self):
        entered = threading.Event()
        release = threading.Event()
        assignment = Assignment()

        @worker(interface="drain")
        def handler(payload):
            entered.set()
            release.wait(timeout=0.5)
            return payload

        observed = []
        def stop_when():
            if entered.is_set() and not release.is_set():
                observed.append(not assignment.cancelled and connection.credits[-1] == 0)
                release.set()
            return assignment.attempt.outputs is not None and len(connection.credits) >= 3
        connection = Connection([{"type": "assignment", "lease_id": assignment.lease_id, "assignment": assignment}, {"type": "drain"}], stop_when=stop_when)
        try:
            with self.assertRaises(KeyboardInterrupt):
                serve(handler, connection=connection)
        finally:
            release.set()
        self.assertEqual(observed, [True])
        self.assertIsNotNone(assignment.attempt.outputs)
        self.assertFalse(assignment.cancelled)
        self.assertEqual(connection.credits[-1], 0)

    def test_call_returns_the_single_inline_result(self):
        class Transport:
            def __init__(self, outputs):
                self.outputs = outputs
                self.specs = []
            def execute(self, spec):
                self.specs.append(spec)
                return {"type": "finished", "state": "succeeded", "outputs": self.outputs}
        client = ExecutionClient.__new__(ExecutionClient)
        client._transport = Transport([{"inline": b"HELLO", "metadata": {}}])
        self.assertEqual(client.call("echo", b"hello"), b"HELLO")
        self.assertEqual(client._transport.specs[0]["inputs"], [{"inline": b"hello", "metadata": {}}])
        self.assertEqual(client._transport.specs[0]["durability"], "ephemeral")
        for outputs in ([], [{"uri": "s3://x", "metadata": {}}], [{"inline": b"a"}, {"inline": b"b"}]):
            client._transport = Transport(outputs)
            with self.subTest(outputs=outputs), self.assertRaisesRegex(RuntimeError, "expects one inline result"):
                client.call("echo", b"hello")

    def test_result_reads_once_then_watches_from_zero(self):
        class Events:
            def __init__(self, code=None):
                self.code = code
                self.closed = False
            def __next__(self):
                if self.code is not None:
                    error = RpcError("test status")
                    error.status_code = self.code
                    raise error
                return {"type": "finished", "state": "succeeded", "outputs": [{"inline": b"done"}]}
            def close(self):
                self.closed = True
        class Transport:
            def __init__(self, code=None):
                self.calls = []
                self.events = Events(code)
            def get(self, job_id, include_outputs):
                self.calls.append(("get", job_id, include_outputs))
                return {"state": "queued"}
            def watch(self, job_id, after_sequence):
                self.calls.append(("watch", job_id, after_sequence))
                return self.events
        client = ExecutionClient.__new__(ExecutionClient)
        client._transport = Transport()
        self.assertEqual(client.result("job"), [{"inline": b"done"}])
        self.assertEqual(client._transport.calls, [("get", "job", True), ("watch", "job", 0)])
        self.assertTrue(client._transport.events.closed)
        for code in (11, 14):
            with self.subTest(code=code):
                client._transport = Transport(code)
                with self.assertRaises(RpcError):
                    client.result("job")
                self.assertEqual(client._transport.calls, [("get", "job", True), ("watch", "job", 0)])
                self.assertTrue(client._transport.events.closed)

    def test_async_handler_and_async_generator_are_supported(self):
        @worker(interface="async.unary")
        async def handle_unary(payload):
            await asyncio.sleep(0)
            return payload + b"!"

        assignment = Assignment(payload=b"hi")
        _process_assignment(handle_unary, handle_unary.spec, assignment)
        self.assertEqual(assignment.attempt.outputs, [{"inline": b"hi!", "metadata": {}}])

        @worker(interface="async.stream", codec="json")
        async def handle_stream(obj):
            await asyncio.sleep(0)
            yield {"v": obj["n"] + 1}
            yield {"v": obj["n"] + 2}

        assignment2 = Assignment(payload=b'{"n":10}')
        _process_assignment(handle_stream, handle_stream.spec, assignment2)
        frames = [json.loads(line) for line in b"".join(assignment2.attempt.frames).splitlines()]
        self.assertEqual(frames, [{"v": 11}, {"v": 12}])
        self.assertEqual(assignment2.attempt.outputs, [])

    def test_uri_resolver_and_sha256_metadata(self):
        payload = b"tensor-blob"
        digest = hashlib.sha256(payload).hexdigest()

        @worker(interface="uri.custom", uri_resolver=lambda uri, meta: payload if uri == "s3://b/k" else b"")
        def resolve_custom(data):
            return data.upper()

        a = Assignment()
        a.attempt.inputs = [uri_object("s3://b/k", sha256=digest)]
        _process_assignment(resolve_custom, resolve_custom.spec, a)
        self.assertEqual(a.attempt.outputs, [{"inline": b"TENSOR-BLOB", "metadata": {}}])

    def test_direct_uri_without_resolver_fails_pre_handler_as_direct_unsupported(self):
        called = False

        @worker(interface="uri.nodirect")
        def handler(data):
            nonlocal called
            called = True
            return data

        a = Assignment()
        a.attempt.inputs = [
            uri_object(
                "anyserve+iroh://abc123endpoint/deadbeef0123456789abcdef01234567",
                sha256="0" * 64,
                size_bytes=128,
            )
        ]
        _process_assignment(handler, handler.spec, a)
        self.assertFalse(called, "handler must not execute when direct input is unsupported")
        self.assertIsNotNone(a.attempt.failure)
        msg, retryable = a.attempt.failure
        self.assertIn("anyserve+iroh://", msg)
        self.assertFalse(retryable)
        self.assertEqual(a.attempt.failure_code, "direct_unsupported")

    def test_multi_interface_serve_routes_by_interface_and_merges_capacity(self):
        @worker(interface="iface.a", capacity={"slot": 1}, max_active_leases=1)
        def ha(payload):
            return b"A:" + payload

        @worker(interface="iface.b", capacity={"slot": 2}, max_active_leases=2)
        def hb(payload):
            return b"B:" + payload

        a1 = Assignment("l1", b"one")
        a1.interface = "iface.a"
        a2 = Assignment("l2", b"two")
        a2.interface = "iface.b"

        class MultiConn:
            def __init__(self):
                self.msgs = deque([a1, a2])
                self.credits = []

            def credit(self, free):
                self.credits.append(free)

            def accept(self, lease_id, accepted, reason):
                pass

            def receive(self, timeout):
                if self.msgs:
                    a = self.msgs.popleft()
                    return {"type": "assignment", "lease_id": a.lease_id, "assignment": a}
                if a1.attempt.outputs is not None and a2.attempt.outputs is not None:
                    raise KeyboardInterrupt
                time.sleep(0.001)
                return None

            def cancel_ack(self, lease_id):
                pass

            def close(self):
                pass

            def drain(self):
                pass

        with self.assertRaises(KeyboardInterrupt):
            serve([ha, hb], connection=MultiConn())
        self.assertEqual(a1.attempt.outputs, [{"inline": b"A:one", "metadata": {}}])
        self.assertEqual(a2.attempt.outputs, [{"inline": b"B:two", "metadata": {}}])

    def test_uri_object_and_scheduling_options_and_async_client(self):
        obj = uri_object("https://cdn/1?sig=x", sha256="dead", size_bytes=99, content_type="application/octet-stream")
        self.assertEqual(obj["metadata"]["anyserve.sha256"], "dead")
        self.assertEqual(obj["metadata"]["anyserve.size_bytes"], "99")
        self.assertEqual(obj["metadata"]["content_type"], "application/octet-stream")

        opts = ExecutionOptions(affinity_key="rob-1", fairness_key="vip", concurrency_key="arm-l")
        raw_spec = _spec("vla", b"hello", opts, "ephemeral")
        self.assertEqual(raw_spec["affinity_key"], "rob-1")
        self.assertEqual(raw_spec["fairness_key"], "vip")
        self.assertEqual(raw_spec["concurrency_key"], "arm-l")

        class FakeEvents:
            def __init__(self):
                self.closed = False
                self._items = iter([{"type": "output", "data": b"tok"}, {"type": "finished", "state": "succeeded", "outputs": []}])

            def __iter__(self):
                return self

            def __next__(self):
                return next(self._items)

            def close(self):
                self.closed = True

        class FakeTransport:
            def invoke(self, spec):
                return FakeEvents()

        async def run_check():
            client = AsyncAnyserveClient.__new__(AsyncAnyserveClient)
            sync_c = ExecutionClient.__new__(ExecutionClient)
            sync_c._transport = FakeTransport()
            client._sync = sync_c
            seen = []
            async with await client.invoke("chat", b"hi") as evs:
                async for ev in evs:
                    seen.append(ev["type"])
            self.assertEqual(seen, ["output", "finished"])

        asyncio.run(run_check())

    def test_serve_sigterm_triggers_preemptive_drain_and_restores_handler(self):
        import os
        import signal

        @worker(interface="sigterm.v1")
        def echo(payload: bytes) -> bytes:
            return payload

        class FakeConn:
            def __init__(self):
                self.drain_calls = []
                self.closed = False

            def credit(self, free):
                pass

            def receive(self, timeout):
                os.kill(os.getpid(), signal.SIGTERM)
                time.sleep(0.05)
                return None

            def drain(self, preempting=False):
                self.drain_calls.append(preempting)

            def close(self):
                self.closed = True

        before = signal.getsignal(signal.SIGTERM)
        conn = FakeConn()
        serve(echo, connection=conn)
        self.assertEqual(conn.drain_calls, [True])
        self.assertTrue(conn.closed)
        self.assertEqual(signal.getsignal(signal.SIGTERM), before)

    def test_worker_error_code_async_runner_load_provider_graceful_shutdown_and_client_extensions(self):
        from anyserve.highlevel import _AsyncLoopRunner, _reconnect_backoff

        # 1. WorkerError preserves custom error code.
        err = WorkerError("bad input", retryable=True, code="INVALID_SHAPE")
        self.assertEqual(err.code, "INVALID_SHAPE")

        a = Assignment("lease-code")
        seen_fail = []
        a.attempt.fail = lambda message, retryable=False, code=None: seen_fail.append((message, retryable, code))

        @worker(interface="fail.code")
        def fail_handler(_payload: bytes) -> bytes:
            raise WorkerError("bad input", retryable=True, code="INVALID_SHAPE")

        _process_assignment(fail_handler, get_handler_spec(fail_handler), a)
        self.assertEqual(seen_fail, [("bad input", True, "INVALID_SHAPE")])

        # 2. _AsyncLoopRunner reuses one persistent event loop across async calls & async generators.
        runner = _AsyncLoopRunner()
        try:
            loops = []

            async def record_loop():
                loops.append(asyncio.get_running_loop())
                return len(loops)

            self.assertEqual(runner.run(record_loop()), 1)
            self.assertEqual(runner.run(record_loop()), 2)
            self.assertIs(loops[0], loops[1])

            async_gen_assignment = Assignment("lease-ag", b"x")

            @worker(interface="async.gen")
            async def agen_handler(payload: bytes):
                loops.append(asyncio.get_running_loop())
                yield payload + b"1"
                yield payload + b"2"

            _process_assignment(
                agen_handler, get_handler_spec(agen_handler), async_gen_assignment, async_runner=runner
            )
            self.assertIs(loops[2], loops[0])
            self.assertEqual(async_gen_assignment.attempt.frames, [b"x1", b"x2"])
        finally:
            runner.close()

        # 3. _reconnect_backoff is jittered exponential and bounded.
        for att, lo, hi in [(0, 0.5, 1.0), (2, 2.0, 4.0), (10, 5.0, 10.0)]:
            val = _reconnect_backoff(att, 1.0, 10.0)
            self.assertGreaterEqual(val, lo)
            self.assertLessEqual(val, hi)

        # 4. serve() pushes load_provider and waits up to graceful_shutdown_timeout before cancelling.
        finished = threading.Event()
        slow_assignment = Assignment("lease-slow")

        @worker(interface="slow.v1")
        def slow_handler(_payload: bytes) -> bytes:
            time.sleep(0.08)
            finished.set()
            return b"done"

        class GracefulConn:
            def __init__(self):
                self.msgs = deque([slow_assignment])
                self.loads = []
                self.drain_calls = 0

            def credit(self, free):
                pass

            def accept(self, lease_id, accepted, reason):
                pass

            def update_load(self, **kwargs):
                self.loads.append(kwargs)

            def receive(self, timeout):
                if self.msgs:
                    a = self.msgs.popleft()
                    return {"type": "assignment", "lease_id": a.lease_id, "assignment": a}
                if self.drain_calls == 0:
                    time.sleep(0.02)
                    raise KeyboardInterrupt
                return None

            def drain(self, preempting=False):
                self.drain_calls += 1

            def close(self):
                pass

        conn = GracefulConn()
        with self.assertRaises(KeyboardInterrupt):
            serve(
                slow_handler,
                connection=conn,
                graceful_shutdown_timeout=2.0,
                load_provider=lambda: {"queued": 3, "running": 1, "utilization": 0.5, "custom": {"vram": 8.0}},
            )
        self.assertTrue(finished.is_set(), "graceful shutdown must wait for in-flight handler")
        self.assertFalse(slow_assignment.cancelled)
        self.assertGreaterEqual(len(conn.loads), 1)
        self.assertEqual(conn.loads[0]["queued"], 3)

        # 5. ExecutionClient & AsyncAsync/Client list, execute, and conflict_policy forwarding.
        calls = []

        class MockTransport:
            def submit(self, spec, job_id, conflict_policy=None):
                calls.append(("submit", job_id, conflict_policy))
                return {"job_id": job_id or "j1", "state": "queued"}

            def execute(self, spec):
                calls.append(("execute", spec["interface"]))
                return {"type": "finished", "state": "succeeded", "outputs": [{"inline": b"fast", "metadata": {}}]}

            def list(self, *, states=None, limit=100, page_token=""):
                calls.append(("list", states, limit, page_token))
                return {"jobs": [{"job_id": "j1"}], "next_page_token": "p2"}

        sync_client = ExecutionClient.__new__(ExecutionClient)
        sync_client._transport = MockTransport()
        sync_client.submit("echo", b"hi", job_id="j1", conflict_policy="reject")
        self.assertEqual(sync_client.execute("echo", b"hi"), [{"inline": b"fast", "metadata": {}}])
        self.assertEqual(sync_client.list(states=["queued"], limit=10, page_token="p1")["next_page_token"], "p2")

        async def run_async():
            ac = AsyncAnyserveClient.__new__(AsyncAnyserveClient)
            ac._sync = sync_client
            res = await ac.list(states=["running"], limit=5)
            self.assertEqual(res["jobs"][0]["job_id"], "j1")

        asyncio.run(run_async())
        self.assertIn(("submit", "j1", "reject"), calls)
        self.assertIn(("execute", "echo"), calls)
        self.assertIn(("list", ["running"], 5, ""), calls)

    def test_default_and_custom_worker_error_codes(self):
        self.assertEqual(WorkerError("boom").code, "worker_failed")
        self.assertEqual(WorkerError("boom", code="").code, "worker_failed")
        self.assertEqual(WorkerError("boom", code="MY_CODE").code, "MY_CODE")

        @worker(interface="err.unhandled")
        def unhandled(_payload: bytes) -> bytes:
            raise RuntimeError("boom")

        a1 = Assignment("l-unhandled")
        _process_assignment(unhandled, unhandled.spec, a1)
        self.assertEqual(a1.attempt.failure_code, "worker_failed")

        @worker(interface="err.default")
        def default_err(_payload: bytes) -> bytes:
            raise WorkerError("bad")

        a2 = Assignment("l-default")
        _process_assignment(default_err, default_err.spec, a2)
        self.assertEqual(a2.attempt.failure_code, "worker_failed")

        @worker(interface="err.custom")
        def custom_err(_payload: bytes) -> bytes:
            raise WorkerError("bad", code="MY_CODE")

        a3 = Assignment("l-custom")
        _process_assignment(custom_err, custom_err.spec, a3)
        self.assertEqual(a3.attempt.failure_code, "MY_CODE")

    def test_async_handler_and_async_generator_cancel_promptly_on_cancel_and_fence(self):
        for kind in ("cancel", "fence"):
            with self.subTest(kind=kind, mode="coroutine"):
                entered = threading.Event()
                cleaned = threading.Event()
                assignment = Assignment(f"async-{kind}")

                @worker(interface=f"async.{kind}")
                async def slow_async(_payload: bytes) -> bytes:
                    entered.set()
                    try:
                        await asyncio.sleep(60)
                        return b"never"
                    finally:
                        cleaned.set()

                sent_cancel = False

                class CancelConn(Connection):
                    def receive(self, timeout):
                        nonlocal sent_cancel
                        if not sent_cancel and entered.is_set():
                            sent_cancel = True
                            return {"type": kind, "lease_id": assignment.lease_id, "reason": "stop"}
                        return super().receive(timeout)

                conn = CancelConn(
                    [{"type": "assignment", "lease_id": assignment.lease_id, "assignment": assignment}],
                    stop_when=lambda: assignment.lease_id in conn.acks and conn.credits[-1] == 1,
                )
                started = time.monotonic()
                with self.assertRaises(KeyboardInterrupt):
                    serve(slow_async, connection=conn, graceful_shutdown_timeout=1.0)
                elapsed = time.monotonic() - started
                self.assertLess(elapsed, 2.0)
                self.assertTrue(cleaned.is_set())
                self.assertTrue(assignment.cancelled)
                self.assertIsNone(assignment.attempt.outputs)
                self.assertIsNone(assignment.attempt.failure)
                self.assertEqual(conn.acks, [assignment.lease_id])

            with self.subTest(kind=kind, mode="async_generator"):
                entered = threading.Event()
                cleaned = threading.Event()
                assignment = Assignment(f"agen-{kind}")

                @worker(interface=f"agen.{kind}")
                async def slow_agen(_payload: bytes):
                    try:
                        yield b"first"
                        entered.set()
                        await asyncio.sleep(60)
                        yield b"never"
                    finally:
                        cleaned.set()

                sent_cancel = False

                class CancelGenConn(Connection):
                    def receive(self, timeout):
                        nonlocal sent_cancel
                        if not sent_cancel and entered.is_set():
                            sent_cancel = True
                            return {"type": kind, "lease_id": assignment.lease_id, "reason": "stop"}
                        return super().receive(timeout)

                conn = CancelGenConn(
                    [{"type": "assignment", "lease_id": assignment.lease_id, "assignment": assignment}],
                    stop_when=lambda: assignment.lease_id in conn.acks and conn.credits[-1] == 1,
                )
                started = time.monotonic()
                with self.assertRaises(KeyboardInterrupt):
                    serve(slow_agen, connection=conn, graceful_shutdown_timeout=1.0)
                elapsed = time.monotonic() - started
                self.assertLess(elapsed, 2.0)
                self.assertTrue(cleaned.is_set())
                self.assertEqual(assignment.attempt.frames, [b"first"])
                self.assertIsNone(assignment.attempt.outputs)
                self.assertIsNone(assignment.attempt.failure)

    def test_async_handler_exceeding_graceful_shutdown_timeout_does_not_hang(self):
        entered = threading.Event()
        cleaned = threading.Event()
        assignment = Assignment("async-timeout")

        @worker(interface="async.timeout")
        async def slow_async(_payload: bytes) -> bytes:
            entered.set()
            try:
                await asyncio.sleep(60)
                return b"never"
            finally:
                cleaned.set()

        conn = Connection(
            [{"type": "assignment", "lease_id": assignment.lease_id, "assignment": assignment}],
            stop_when=entered.is_set,
        )
        started = time.monotonic()
        with self.assertRaises(KeyboardInterrupt):
            serve(slow_async, connection=conn, graceful_shutdown_timeout=0.05)
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 2.0)
        self.assertTrue(cleaned.is_set())
        self.assertTrue(assignment.cancelled)
        self.assertTrue(conn.closed)
        self.assertIsNone(assignment.attempt.failure)

        # Stubborn handler ignoring CancelledError must still let serve() exit & close conn.
        entered2 = threading.Event()
        assignment2 = Assignment("async-stubborn")

        @worker(interface="async.stubborn")
        async def stubborn_async(_payload: bytes) -> bytes:
            entered2.set()
            while True:
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    continue

        conn2 = Connection(
            [{"type": "assignment", "lease_id": assignment2.lease_id, "assignment": assignment2}],
            stop_when=entered2.is_set,
        )
        started2 = time.monotonic()
        with self.assertRaises(KeyboardInterrupt):
            serve(stubborn_async, connection=conn2, graceful_shutdown_timeout=0.05)
        elapsed2 = time.monotonic() - started2
        self.assertLess(elapsed2, 3.5)
        self.assertTrue(conn2.closed)

    def test_shutdown_never_sends_positive_credit_after_drain_and_sigterm_preempts(self):
        # 1. Completing in-flight handler during graceful shutdown must not emit positive credit after drain.
        entered = threading.Event()
        assignment = Assignment("graceful-credit")

        @worker(interface="graceful.credit")
        def handler(_payload: bytes) -> bytes:
            entered.set()
            time.sleep(0.06)
            return b"ok"

        timeline: list[tuple[str, object]] = []

        class RecordingConn(Connection):
            def credit(self, free):
                timeline.append(("credit", free))
                super().credit(free)

            def drain(self, preempting=False):
                timeline.append(("drain", preempting))

        conn = RecordingConn(
            [{"type": "assignment", "lease_id": assignment.lease_id, "assignment": assignment}],
            stop_when=entered.is_set,
        )
        with self.assertRaises(KeyboardInterrupt):
            serve(handler, connection=conn, graceful_shutdown_timeout=2.0)
        self.assertIsNotNone(assignment.attempt.outputs)
        drain_idx = next(i for i, (ev, _) in enumerate(timeline) if ev == "drain")
        after_drain_credits = [val for ev, val in timeline[drain_idx + 1 :] if ev == "credit"]
        self.assertTrue(
            all(c == 0 for c in after_drain_credits),
            f"positive credit sent after drain: {timeline}",
        )

        # 2. SIGTERM sends drain(preempting=True) and cancels active async leases immediately without waiting 30s.
        import signal

        sig_entered = threading.Event()
        sig_cleaned = threading.Event()
        sig_assignment = Assignment("sigterm-async")

        @worker(interface="sigterm.async")
        async def slow_sigterm(_payload: bytes) -> bytes:
            sig_entered.set()
            try:
                await asyncio.sleep(60)
                return b"never"
            finally:
                sig_cleaned.set()

        sig_timeline: list[tuple[str, object]] = []

        class SigtermConn(Connection):
            def credit(self, free):
                sig_timeline.append(("credit", free))
                super().credit(free)

            def drain(self, preempting=False):
                sig_timeline.append(("drain", preempting))

            def receive(self, timeout):
                if self.messages:
                    return self.messages.popleft()
                if sig_entered.is_set():
                    os.kill(os.getpid(), signal.SIGTERM)
                    time.sleep(0.05)
                else:
                    time.sleep(0.005)
                return None

        sig_conn = SigtermConn(
            [{"type": "assignment", "lease_id": sig_assignment.lease_id, "assignment": sig_assignment}]
        )
        started = time.monotonic()
        serve(slow_sigterm, connection=sig_conn, graceful_shutdown_timeout=30.0)
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 2.0)
        self.assertTrue(sig_cleaned.is_set())
        self.assertTrue(sig_conn.closed)
        self.assertIn(("drain", True), sig_timeline)
        sig_drain_idx = next(i for i, (ev, _) in enumerate(sig_timeline) if ev == "drain")
        self.assertTrue(
            all(val == 0 for ev, val in sig_timeline[sig_drain_idx + 1 :] if ev == "credit"),
            f"positive credit sent after preempting drain: {sig_timeline}",
        )

    def test_streaming_invocation_exports_and_async_aexit_uses_aclose(self):
        import anyserve
        from anyserve import AsyncStreamingInvocation, StreamingInvocation

        self.assertIn("StreamingInvocation", anyserve.__all__)
        self.assertIn("AsyncStreamingInvocation", anyserve.__all__)
        self.assertIs(anyserve.StreamingInvocation, StreamingInvocation)
        self.assertIs(anyserve.AsyncStreamingInvocation, AsyncStreamingInvocation)

        close_threads: list[int] = []
        sent_chunks: list[bytes] = []
        finished_input = False

        class FakeNativeInvocation:
            def __init__(self):
                self._items = iter([
                    {"type": "output", "data": b"echo:hi"},
                    {"type": "finished", "state": "succeeded", "outputs": []},
                ])
                self.events = self

            def __iter__(self):
                return self

            def __next__(self):
                return next(self._items)

            def send(self, data, metadata=None):
                sent_chunks.append(data)

            def finish_input(self):
                nonlocal finished_input
                finished_input = True

            def close(self):
                close_threads.append(threading.get_ident())

        class FakeTransport:
            def invoke_stream(self, spec):
                return FakeNativeInvocation()

        async def run_check():
            loop_tid = threading.get_ident()
            client = AsyncAnyserveClient.__new__(AsyncAnyserveClient)
            sync_c = ExecutionClient.__new__(ExecutionClient)
            sync_c._transport = FakeTransport()
            client._sync = sync_c

            seen = []
            async with await client.invoke_stream("duplex") as inv:
                self.assertIsInstance(inv, AsyncStreamingInvocation)
                await inv.send(b"hi")
                await inv.finish_input()
                async for ev in inv:
                    seen.append(ev["type"])
            self.assertEqual(seen, ["output", "finished"])
            self.assertEqual(sent_chunks, [b"hi"])
            self.assertTrue(finished_input)
            self.assertEqual(len(close_threads), 1)
            self.assertNotEqual(close_threads[0], loop_tid, "__aexit__ must close off the event loop thread")

        asyncio.run(run_check())


if __name__ == "__main__":
    unittest.main()
