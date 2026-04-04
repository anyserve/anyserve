from __future__ import annotations

from ._native import AnyserveClient as _NativeAnyserveClient


class SubmitterClient:
    def __init__(self, native: _NativeAnyserveClient, endpoint: str) -> None:
        self._native = native
        self._endpoint = endpoint

    def submit_job(self, *args, **kwargs):
        return self._native.submit_job(*args, **kwargs)

    def watch_job(self, *args, **kwargs):
        return self._native.watch_job(*args, **kwargs)

    def list_jobs(self, *args, **kwargs):
        return self._native.list_jobs(*args, **kwargs)

    def get_job(self, *args, **kwargs):
        return self._native.get_job(*args, **kwargs)

    def cancel_job(self, *args, **kwargs):
        return self._native.cancel_job(*args, **kwargs)

    def get_attempt(self, *args, **kwargs):
        return self._native.get_attempt(*args, **kwargs)

    def list_attempts(self, *args, **kwargs):
        return self._native.list_attempts(*args, **kwargs)

    def open_stream(self, *args, **kwargs):
        return self._native.open_stream(*args, **kwargs)

    def get_stream(self, *args, **kwargs):
        return self._native.get_stream(*args, **kwargs)

    def list_streams(self, *args, **kwargs):
        return self._native.list_streams(*args, **kwargs)

    def close_stream(self, *args, **kwargs):
        return self._native.close_stream(*args, **kwargs)

    def push_frames(self, *args, **kwargs):
        return self._native.push_frames(*args, **kwargs)

    def pull_frames(self, *args, **kwargs):
        return self._native.pull_frames(*args, **kwargs)

    def __repr__(self) -> str:
        return f"SubmitterClient({self._native!r})"


class WorkerClient:
    def __init__(self, native: _NativeAnyserveClient, endpoint: str) -> None:
        self._native = native
        self._endpoint = endpoint

    def register_worker(self, *args, **kwargs):
        return self._native.register_worker(*args, **kwargs)

    def heartbeat_worker(self, *args, **kwargs):
        return self._native.heartbeat_worker(*args, **kwargs)

    def poll_lease(self, *args, **kwargs):
        return self._native.poll_lease(*args, **kwargs)

    def renew_lease(self, *args, **kwargs):
        return self._native.renew_lease(*args, **kwargs)

    def report_event(self, *args, **kwargs):
        return self._native.report_event(*args, **kwargs)

    def complete_lease(self, *args, **kwargs):
        return self._native.complete_lease(*args, **kwargs)

    def fail_lease(self, *args, **kwargs):
        return self._native.fail_lease(*args, **kwargs)

    def open_stream(self, *args, **kwargs):
        return self._native.open_stream(*args, **kwargs)

    def get_stream(self, *args, **kwargs):
        return self._native.get_stream(*args, **kwargs)

    def list_streams(self, *args, **kwargs):
        return self._native.list_streams(*args, **kwargs)

    def close_stream(self, *args, **kwargs):
        return self._native.close_stream(*args, **kwargs)

    def push_frames(self, *args, **kwargs):
        return self._native.push_frames(*args, **kwargs)

    def pull_frames(self, *args, **kwargs):
        return self._native.pull_frames(*args, **kwargs)

    def __repr__(self) -> str:
        return f"WorkerClient({self._native!r})"


class AnyserveClient:
    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        self._native = _NativeAnyserveClient(endpoint)
        self._submitter = SubmitterClient(self._native, endpoint)
        self._worker = WorkerClient(self._native, endpoint)

    def submitter(self) -> SubmitterClient:
        return self._submitter

    def worker(self) -> WorkerClient:
        return self._worker

    def __getattr__(self, name: str):
        return getattr(self._native, name)

    def __repr__(self) -> str:
        return repr(self._native)
