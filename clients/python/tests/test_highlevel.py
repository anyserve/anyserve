from __future__ import annotations

import unittest

from anyserve import get_handler_spec, serve, worker


class StopServing(KeyboardInterrupt):
    pass


class FakeWorkerClient:
    def __init__(self, grants, streams, frames) -> None:
        self._grants = list(grants)
        self._streams = dict(streams)
        self._frames = dict(frames)
        self._endpoint = None
        self.register_calls = []
        self.heartbeat_calls = []
        self.renew_calls = []
        self.report_calls = []
        self.open_calls = []
        self.push_calls = []
        self.close_calls = []
        self.complete_calls = []
        self.fail_calls = []

    def register_worker(self, **kwargs):
        self.register_calls.append(kwargs)
        return {"worker_id": kwargs.get("worker_id") or "worker-1"}

    def heartbeat_worker(self, worker_id, available_capacity=None, active_leases=0, metadata=None):
        self.heartbeat_calls.append(
            {
                "worker_id": worker_id,
                "available_capacity": dict(available_capacity or {}),
                "active_leases": active_leases,
                "metadata": dict(metadata or {}),
            }
        )
        return {}

    def renew_lease(self, worker_id, lease_id):
        self.renew_calls.append({"worker_id": worker_id, "lease_id": lease_id})
        return {"lease_id": lease_id}

    def poll_lease(self, worker_id):
        if self._grants:
            return self._grants.pop(0)
        raise StopServing()

    def list_streams(self, job_id):
        return list(self._streams.get(job_id, []))

    def pull_frames(self, stream_id, after_sequence=0, follow=False):
        return iter(self._frames.get(stream_id, []))

    def report_event(self, worker_id, lease_id, kind, payload=None, metadata=None):
        self.report_calls.append(
            {
                "worker_id": worker_id,
                "lease_id": lease_id,
                "kind": kind,
                "payload": payload,
                "metadata": dict(metadata or {}),
            }
        )

    def open_stream(
        self,
        job_id,
        stream_name,
        scope="job",
        direction="worker_to_client",
        metadata=None,
        attempt_id=None,
        worker_id=None,
        lease_id=None,
    ):
        record = {
            "job_id": job_id,
            "stream_name": stream_name,
            "scope": scope,
            "direction": direction,
            "metadata": dict(metadata or {}),
            "attempt_id": attempt_id,
            "worker_id": worker_id,
            "lease_id": lease_id,
        }
        self.open_calls.append(record)
        return {"stream_id": f"{stream_name}-stream", "stream_name": stream_name}

    def push_frames(self, stream_id, frames, worker_id=None, lease_id=None):
        self.push_calls.append(
            {
                "stream_id": stream_id,
                "frames": list(frames),
                "worker_id": worker_id,
                "lease_id": lease_id,
            }
        )
        return {"stream": {"stream_id": stream_id}, "last_sequence": len(frames), "written_frames": len(frames)}

    def close_stream(self, stream_id, worker_id=None, lease_id=None, metadata=None):
        self.close_calls.append(
            {
                "stream_id": stream_id,
                "worker_id": worker_id,
                "lease_id": lease_id,
                "metadata": dict(metadata or {}),
            }
        )
        return {"stream_id": stream_id}

    def complete_lease(self, worker_id, lease_id, outputs=None, metadata=None):
        self.complete_calls.append(
            {
                "worker_id": worker_id,
                "lease_id": lease_id,
                "outputs": outputs,
                "metadata": metadata,
            }
        )

    def fail_lease(self, worker_id, lease_id, reason, retryable=False, metadata=None):
        self.fail_calls.append(
            {
                "worker_id": worker_id,
                "lease_id": lease_id,
                "reason": reason,
                "retryable": retryable,
                "metadata": dict(metadata or {}),
            }
        )


def _grant(job_id="job-1", attempt_id="attempt-1", lease_id="lease-1"):
    return {
        "lease": {"lease_id": lease_id, "job_id": job_id, "worker_id": "worker-1"},
        "attempt": {"attempt_id": attempt_id, "job_id": job_id, "worker_id": "worker-1", "lease_id": lease_id},
        "job": {"job_id": job_id, "spec": {}, "state": "leased"},
    }


class HighLevelTests(unittest.TestCase):
    def test_worker_decorator_preserves_local_call_and_spec(self):
        @worker(
            interface="demo.echo.v1",
            attributes={"runtime": "python"},
            capacity={"slot": 1},
            codec="bytes",
        )
        def echo(payload: bytes) -> bytes:
            return payload + b"!"

        self.assertEqual(echo(b"hi"), b"hi!")
        spec = get_handler_spec(echo)
        self.assertEqual(spec.interface, "demo.echo.v1")
        self.assertEqual(spec.attributes, {"runtime": "python"})
        self.assertEqual(spec.capacity, {"slot": 1})
        self.assertEqual(spec.codec, "bytes")

    def test_async_handlers_are_rejected(self):
        async def async_echo(payload):
            return payload

        with self.assertRaises(TypeError):
            worker(interface="demo.echo.v1")(async_echo)

    def test_worker_rejects_max_active_leases_other_than_one(self):
        with self.assertRaises(ValueError):
            worker(interface="demo.echo.v1", max_active_leases=2)

    def test_serve_processes_bytes_handler(self):
        @worker(interface="demo.echo.v1", attributes={"runtime": "python"}, capacity={"slot": 1})
        def echo(payload: bytes) -> bytes:
            return payload.upper()

        client = FakeWorkerClient(
            grants=[_grant()],
            streams={"job-1": [{"stream_id": "input-1", "stream_name": "input.default"}]},
            frames={
                "input-1": [
                    {"kind": "data", "payload": b"hello "},
                    {"kind": "data", "payload": b"world"},
                ]
            },
        )

        with self.assertRaises(StopServing):
            serve(
                echo,
                client=client,
                heartbeat_interval_secs=0.01,
                poll_interval_secs=0,
            )

        self.assertEqual(client.register_calls[0]["interfaces"], ["demo.echo.v1"])
        self.assertEqual(client.report_calls[0]["kind"], "started")
        self.assertEqual(client.report_calls[1]["kind"], "output_ready")
        self.assertEqual(client.report_calls[1]["metadata"], {"stream_name": "output.default"})
        self.assertEqual(
            client.push_calls[0]["frames"],
            [("data", b"HELLO WORLD", {})],
        )
        self.assertEqual(client.complete_calls[0]["lease_id"], "lease-1")
        self.assertEqual(client.fail_calls, [])

    def test_serve_processes_json_handler(self):
        @worker(interface="demo.json.v1", codec="json")
        def transform(payload):
            return {"reply": payload["message"].upper()}

        client = FakeWorkerClient(
            grants=[_grant(job_id="job-json", attempt_id="attempt-json", lease_id="lease-json")],
            streams={"job-json": [{"stream_id": "input-json", "stream_name": "input.default"}]},
            frames={"input-json": [{"kind": "data", "payload": b'{"message":"hello"}'}]},
        )

        with self.assertRaises(StopServing):
            serve(transform, client=client, heartbeat_interval_secs=0.01, poll_interval_secs=0)

        self.assertEqual(
            client.push_calls[0]["frames"],
            [("data", b'{"reply":"HELLO"}', {})],
        )
        self.assertEqual(client.fail_calls, [])

    def test_serve_fails_lease_on_json_decode_error(self):
        @worker(interface="demo.json.v1", codec="json")
        def transform(payload):
            return payload

        client = FakeWorkerClient(
            grants=[_grant(job_id="job-json", attempt_id="attempt-json", lease_id="lease-json")],
            streams={"job-json": []},
            frames={},
        )

        with self.assertRaises(StopServing):
            serve(transform, client=client, heartbeat_interval_secs=0.01, poll_interval_secs=0)

        self.assertEqual(client.complete_calls, [])
        self.assertEqual(len(client.fail_calls), 1)
        self.assertIn("ValueError: json input stream was empty", client.fail_calls[0]["reason"])

    def test_serve_requires_decorated_handler(self):
        def raw_handler(payload):
            return payload

        client = FakeWorkerClient(grants=[], streams={}, frames={})

        with self.assertRaises(TypeError):
            serve(raw_handler, client=client)
        self.assertEqual(client.register_calls, [])


if __name__ == "__main__":
    unittest.main()
