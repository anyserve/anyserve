from __future__ import annotations

import unittest

from anyserve.client import SubmitterClient, WorkerClient


class FakeNative:
    def __init__(self) -> None:
        self.calls = []

    def list_jobs(self):
        self.calls.append(("list_jobs", (), {}))
        return [{"job_id": "job-1"}]

    def watch_job(self, *args, **kwargs):
        self.calls.append(("watch_job", args, kwargs))
        return iter(
            [
                {"job_id": "job-1", "sequence": 1, "kind": "accepted"},
                {"job_id": "job-1", "sequence": 2, "kind": "started"},
            ]
        )

    def pull_frames(self, *args, **kwargs):
        self.calls.append(("pull_frames", args, kwargs))
        return iter(
            [
                {"stream_id": "stream-1", "sequence": 1, "kind": "data", "payload": b"hello"},
            ]
        )


class ClientFacadeTests(unittest.TestCase):
    def test_submitter_forwards_list_jobs_and_watch_job(self):
        native = FakeNative()
        client = SubmitterClient(native, "http://127.0.0.1:50052")

        self.assertEqual(client.list_jobs(), [{"job_id": "job-1"}])
        self.assertEqual(
            list(client.watch_job("job-1", after_sequence=1)),
            [
                {"job_id": "job-1", "sequence": 1, "kind": "accepted"},
                {"job_id": "job-1", "sequence": 2, "kind": "started"},
            ],
        )
        self.assertEqual(
            native.calls,
            [
                ("list_jobs", (), {}),
                ("watch_job", ("job-1",), {"after_sequence": 1}),
            ],
        )

    def test_worker_forwards_pull_frames_iterator(self):
        native = FakeNative()
        client = WorkerClient(native, "http://127.0.0.1:50052")

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
                ("pull_frames", ("stream-1",), {"after_sequence": 3, "follow": True}),
            ],
        )


if __name__ == "__main__":
    unittest.main()
