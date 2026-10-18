from __future__ import annotations

import json
import threading
import time
import unittest
from collections import deque
from unittest.mock import patch

from anyserve import ExecutionClient, RpcError, ExecutionOptions, get_handler_spec, serve, worker
from anyserve.execution import _terminal_outputs
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

    def fail(self, message, retryable=False):
        self.failure = (message, retryable)


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


if __name__ == "__main__":
    unittest.main()
