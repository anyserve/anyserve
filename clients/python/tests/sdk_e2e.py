from __future__ import annotations

import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from socket import AF_INET, SOCK_STREAM, socket

from anyserve import AnyserveClient, FRAME_DATA

TERMINAL_JOB_STATES = {"succeeded", "failed", "cancelled"}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _reserve_port() -> int:
    with socket(AF_INET, SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class ManagedProcess:
    def __init__(self, args: list[str], *, cwd: Path, name: str) -> None:
        self._args = args
        self._cwd = cwd
        self._name = name
        self._tmpdir = tempfile.TemporaryDirectory(prefix=f"anyserve-{name}-")
        self._log_path = Path(self._tmpdir.name) / f"{name}.log"
        self._log_handle = None
        self._proc: subprocess.Popen[str] | None = None

    def start(self) -> "ManagedProcess":
        self._log_handle = self._log_path.open("w", encoding="utf-8")
        self._proc = subprocess.Popen(
            self._args,
            cwd=self._cwd,
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        return self

    def stop(self) -> None:
        proc = self._proc
        if proc is None:
            return
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        if self._log_handle is not None:
            self._log_handle.close()
            self._log_handle = None
        self._tmpdir.cleanup()

    def poll(self) -> int | None:
        if self._proc is None:
            return None
        return self._proc.poll()

    def log_tail(self, lines: int = 40) -> str:
        if not self._log_path.exists():
            return f"<no {self._name} log>"
        content = self._log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        if not content:
            return f"<empty {self._name} log>"
        return "\n".join(content[-lines:])

    def __enter__(self) -> "ManagedProcess":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()


class PythonSdkE2ETests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = _repo_root()
        cls.repo_root = root
        cls.cli_bin = root / "target" / "debug" / "anyserve"
        cls.demo_bin = root / "target" / "debug" / "anyserve-demo"
        if not cls.cli_bin.exists():
            raise unittest.SkipTest(f"missing control-plane binary: {cls.cli_bin}")
        if not cls.demo_bin.exists():
            raise unittest.SkipTest(f"missing demo worker binary: {cls.demo_bin}")

    def test_low_level_submitter_round_trip_with_rust_worker(self) -> None:
        port = _reserve_port()
        endpoint = f"http://127.0.0.1:{port}"
        with ManagedProcess(
            [str(self.cli_bin), "serve", "--grpc-port", str(port)],
            cwd=self.repo_root,
            name="control-plane",
        ) as server:
            self._wait_for_server(endpoint, server)
            with ManagedProcess(
                [str(self.demo_bin), "--mode", "worker", "--endpoint", endpoint],
                cwd=self.repo_root,
                name="rust-worker",
            ) as worker:
                try:
                    time.sleep(0.5)
                    client = AnyserveClient(endpoint)
                    job = client.submit_job(
                        interface_name="demo.echo.v1",
                        required_attributes={"runtime": "demo"},
                        required_capacity={"slot": 1},
                        metadata={"source": "python-sdk-e2e"},
                    )
                    self.assertIn(job["job_id"], {item["job_id"] for item in client.list_jobs()})

                    stream = client.open_stream(job["job_id"], "input.default")
                    client.push_frames(
                        stream["stream_id"],
                        [
                            (FRAME_DATA, b"hello ", {}),
                            (FRAME_DATA, b"from python sdk", {}),
                        ],
                    )
                    client.close_stream(stream["stream_id"])

                    final_job = self._wait_for_terminal_job(client, job["job_id"])
                    event_kinds = [event["kind"] for event in client.watch_job(job["job_id"])]
                    payload = self._read_output_payload(client, job["job_id"])

                    self.assertEqual(final_job["state"], "succeeded")
                    self.assertEqual(
                        event_kinds,
                        [
                            "accepted",
                            "lease_granted",
                            "started",
                            "progress",
                            "output_ready",
                            "succeeded",
                        ],
                    )
                    self.assertEqual(payload, b"hello from python sdk")
                except Exception as exc:
                    raise AssertionError(
                        self._failure_context(
                            "low-level SDK round trip failed",
                            server=server,
                            worker=worker,
                        )
                    ) from exc

    def test_high_level_worker_round_trip(self) -> None:
        port = _reserve_port()
        endpoint = f"http://127.0.0.1:{port}"
        python_worker = textwrap.dedent(
            f"""
            from anyserve import serve, worker

            @worker(
                interface="demo.echo.v1",
                attributes={{"runtime": "python"}},
                capacity={{"slot": 1}},
                codec="bytes",
            )
            def echo(payload: bytes) -> bytes:
                return payload.upper()

            serve(
                echo,
                endpoint="{endpoint}",
                heartbeat_interval_secs=1,
                poll_interval_secs=0.2,
            )
            """
        ).strip()

        with ManagedProcess(
            [str(self.cli_bin), "serve", "--grpc-port", str(port)],
            cwd=self.repo_root,
            name="control-plane",
        ) as server:
            self._wait_for_server(endpoint, server)
            with ManagedProcess(
                [sys.executable, "-u", "-c", python_worker],
                cwd=self.repo_root,
                name="python-worker",
            ) as worker:
                try:
                    time.sleep(0.5)
                    client = AnyserveClient(endpoint)
                    job = client.submit_job(
                        interface_name="demo.echo.v1",
                        required_attributes={"runtime": "python"},
                        required_capacity={"slot": 1},
                        metadata={"source": "python-sdk-highlevel-e2e"},
                    )

                    stream = client.open_stream(job["job_id"], "input.default")
                    client.push_frames(
                        stream["stream_id"],
                        [(FRAME_DATA, b"hello decorator", {})],
                    )
                    client.close_stream(stream["stream_id"])

                    final_job = self._wait_for_terminal_job(client, job["job_id"])
                    event_kinds = [event["kind"] for event in client.watch_job(job["job_id"])]
                    payload = self._read_output_payload(client, job["job_id"])

                    self.assertEqual(final_job["state"], "succeeded")
                    self.assertEqual(
                        event_kinds,
                        ["accepted", "lease_granted", "started", "output_ready", "succeeded"],
                    )
                    self.assertEqual(payload, b"HELLO DECORATOR")
                except Exception as exc:
                    raise AssertionError(
                        self._failure_context(
                            "high-level worker round trip failed",
                            server=server,
                            worker=worker,
                        )
                    ) from exc

    def _wait_for_server(self, endpoint: str, server: ManagedProcess, timeout_secs: float = 10) -> None:
        deadline = time.monotonic() + timeout_secs
        command = [str(self.cli_bin), "--endpoint", endpoint, "job", "ls"]
        while time.monotonic() < deadline:
            if server.poll() is not None:
                raise RuntimeError(f"control plane exited early\n{server.log_tail()}")
            result = subprocess.run(
                command,
                cwd=self.repo_root,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return
            time.sleep(0.2)
        raise TimeoutError(f"timed out waiting for control plane at {endpoint}")

    def _wait_for_terminal_job(
        self,
        client: AnyserveClient,
        job_id: str,
        timeout_secs: float = 15,
    ) -> dict[str, object]:
        deadline = time.monotonic() + timeout_secs
        last_job = None
        while time.monotonic() < deadline:
            last_job = client.get_job(job_id)
            if last_job["state"] in TERMINAL_JOB_STATES:
                return last_job
            time.sleep(0.2)
        raise TimeoutError(f"timed out waiting for terminal job state: {last_job}")

    def _read_output_payload(self, client: AnyserveClient, job_id: str) -> bytes:
        streams = client.list_streams(job_id)
        output_stream = next(
            stream for stream in streams if stream["stream_name"] == "output.default"
        )
        return b"".join(
            frame["payload"]
            for frame in client.pull_frames(output_stream["stream_id"], follow=False)
            if frame["kind"] == "data"
        )

    def _failure_context(
        self,
        summary: str,
        *,
        server: ManagedProcess,
        worker: ManagedProcess,
    ) -> str:
        return (
            f"{summary}\n\n"
            f"SERVER LOG\n{server.log_tail()}\n\n"
            f"WORKER LOG\n{worker.log_tail()}"
        )


if __name__ == "__main__":
    unittest.main()
