from __future__ import annotations

import unittest

from anyserve.client import AnyserveClient


class FakeNative:
    def __init__(self) -> None:
        self.calls = []

    def list_jobs(self):
        self.calls.append(("list_jobs", (), {}))
        return [{"job_id": "job-1"}]

    def register_worker(self, *args, **kwargs):
        self.calls.append(("register_worker", args, kwargs))
        return {"worker_id": "worker-1"}

    def pull_frames(self, *args, **kwargs):
        self.calls.append(("pull_frames", args, kwargs))
        return iter(
            [
                {"stream_id": "stream-1", "sequence": 1, "kind": "data", "payload": b"hello"},
            ]
        )


def make_client(native: FakeNative) -> AnyserveClient:
    client = AnyserveClient.__new__(AnyserveClient)
    client._native = native
    return client


class ClientTests(unittest.TestCase):
    def test_forwards_low_level_methods(self):
        native = FakeNative()
        client = make_client(native)

        self.assertEqual(client.list_jobs(), [{"job_id": "job-1"}])
        self.assertEqual(
            client.register_worker(interfaces=["demo.echo.v1"], total_capacity={"slot": 1}),
            {"worker_id": "worker-1"},
        )
        self.assertEqual(
            list(client.pull_frames("stream-1", after_sequence=3, follow=True)),
            [
                {
                    "stream_id": "stream-1",
                    "sequence": 1,
                    "kind": "data",
                    "payload": b"hello",
                }
            ],
        )
        self.assertEqual(
            native.calls,
            [
                ("list_jobs", (), {}),
                (
                    "register_worker",
                    (),
                    {"interfaces": ["demo.echo.v1"], "total_capacity": {"slot": 1}},
                ),
                (
                    "pull_frames",
                    ("stream-1",),
                    {"after_sequence": 3, "follow": True},
                ),
            ],
        )

    def test_submitter_and_worker_are_compatibility_aliases(self):
        client = make_client(FakeNative())

        self.assertIs(client.submitter(), client)
        self.assertIs(client.worker(), client)


if __name__ == "__main__":
    unittest.main()
