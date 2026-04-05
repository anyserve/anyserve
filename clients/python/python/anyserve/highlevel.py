from __future__ import annotations

import functools
import inspect
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .client import AnyserveClient

_CODEC_BYTES = "bytes"
_CODEC_JSON = "json"
_FRAME_DATA = "data"
_STREAM_SCOPE_JOB = "job"
_STREAM_DIRECTION_WORKER_TO_CLIENT = "worker_to_client"
_EVENT_STARTED = "started"
_EVENT_OUTPUT_READY = "output_ready"


@dataclass(frozen=True)
class HandlerSpec:
    interface: str
    attributes: dict[str, str] = field(default_factory=dict)
    capacity: dict[str, int] = field(default_factory=dict)
    metadata: dict[str, str] = field(default_factory=dict)
    codec: str = _CODEC_BYTES
    input_stream: str = "input.default"
    output_stream: str = "output.default"
    max_active_leases: int = 1


class WorkerHandler:
    def __init__(self, fn: Callable[[Any], Any], spec: HandlerSpec) -> None:
        functools.update_wrapper(self, fn)
        self._fn = fn
        self.spec = spec

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)


@dataclass
class _LeaseState:
    active_lease_id: Optional[str] = None
    active_leases: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def start(self, lease_id: str) -> None:
        with self.lock:
            self.active_lease_id = lease_id
            self.active_leases += 1

    def finish(self, lease_id: str) -> None:
        with self.lock:
            if self.active_lease_id == lease_id:
                self.active_lease_id = None
            if self.active_leases > 0:
                self.active_leases -= 1

    def snapshot(self) -> tuple[Optional[str], int]:
        with self.lock:
            return self.active_lease_id, self.active_leases


def worker(
    *,
    interface: str,
    attributes: Optional[dict[str, str]] = None,
    capacity: Optional[dict[str, int]] = None,
    metadata: Optional[dict[str, str]] = None,
    codec: str = _CODEC_BYTES,
    input_stream: str = "input.default",
    output_stream: str = "output.default",
    max_active_leases: int = 1,
) -> Callable[[Callable[[Any], Any]], WorkerHandler]:
    spec = _build_handler_spec(
        interface=interface,
        attributes=attributes,
        capacity=capacity,
        metadata=metadata,
        codec=codec,
        input_stream=input_stream,
        output_stream=output_stream,
        max_active_leases=max_active_leases,
    )

    def decorator(fn: Callable[[Any], Any]) -> WorkerHandler:
        if isinstance(fn, WorkerHandler):
            raise TypeError("handler is already decorated with @worker(...)")
        if inspect.iscoroutinefunction(fn):
            raise TypeError("async handlers are not supported by serve(...) yet")
        return WorkerHandler(fn, spec)

    return decorator


def get_handler_spec(handler: Callable[[Any], Any]) -> HandlerSpec:
    spec = getattr(handler, "spec", None)
    if not isinstance(spec, HandlerSpec):
        raise TypeError("handler must be decorated with @worker(...) before serve(...)")
    return spec


def serve(
    handler: Callable[[Any], Any],
    endpoint: Optional[str] = None,
    *,
    worker_id: Optional[str] = None,
    heartbeat_interval_secs: float = 5,
    poll_interval_secs: float = 1,
    client: Any = None,
) -> None:
    spec = get_handler_spec(handler)
    _validate_loop_intervals(heartbeat_interval_secs, poll_interval_secs)
    if client is None:
        if endpoint is None:
            raise ValueError("endpoint is required when client is not provided")
        worker_client = AnyserveClient(endpoint)
    else:
        worker_client = client

    registered = worker_client.register_worker(
        interfaces=[spec.interface],
        attributes=dict(spec.attributes),
        total_capacity=dict(spec.capacity),
        max_active_leases=spec.max_active_leases,
        metadata=dict(spec.metadata),
        worker_id=worker_id,
    )
    registered_worker_id = registered["worker_id"]

    state = _LeaseState()
    stop_event = threading.Event()
    heartbeat = threading.Thread(
        target=_heartbeat_loop,
        args=(
            worker_client,
            registered_worker_id,
            spec,
            state,
            stop_event,
            heartbeat_interval_secs,
        ),
        daemon=True,
    )
    heartbeat.start()

    try:
        while True:
            grant = worker_client.poll_lease(registered_worker_id)
            if grant is None:
                time.sleep(poll_interval_secs)
                continue
            _process_grant(worker_client, registered_worker_id, handler, spec, state, grant)
    finally:
        stop_event.set()
        heartbeat.join(timeout=0.5)


def _process_grant(
    worker_client: Any,
    worker_id: str,
    handler: Callable[[Any], Any],
    spec: HandlerSpec,
    state: _LeaseState,
    grant: dict[str, Any],
) -> None:
    lease = grant["lease"]
    attempt = grant["attempt"]
    job = grant["job"]
    lease_id = lease["lease_id"]
    state.start(lease_id)
    try:
        worker_client.report_event(worker_id, lease_id, _EVENT_STARTED)
        payload = _read_input_payload(worker_client, job["job_id"], spec.input_stream)
        input_value = _decode_payload(spec.codec, payload)
        output_value = handler(input_value)
        output_payload = _encode_payload(spec.codec, output_value)

        output_stream = worker_client.open_stream(
            job["job_id"],
            spec.output_stream,
            scope=_STREAM_SCOPE_JOB,
            direction=_STREAM_DIRECTION_WORKER_TO_CLIENT,
            metadata={},
            attempt_id=attempt["attempt_id"],
            worker_id=worker_id,
            lease_id=lease_id,
        )
        worker_client.push_frames(
            output_stream["stream_id"],
            [(_FRAME_DATA, output_payload, {})],
            worker_id=worker_id,
            lease_id=lease_id,
        )
        worker_client.close_stream(
            output_stream["stream_id"],
            worker_id=worker_id,
            lease_id=lease_id,
            metadata={},
        )
        worker_client.report_event(
            worker_id,
            lease_id,
            _EVENT_OUTPUT_READY,
            metadata={"stream_name": spec.output_stream},
        )
        worker_client.complete_lease(worker_id, lease_id)
    except Exception as exc:
        worker_client.fail_lease(
            worker_id,
            lease_id,
            _failure_reason(exc),
            retryable=False,
        )
    finally:
        state.finish(lease_id)


def _heartbeat_loop(
    worker_client: Any,
    worker_id: str,
    spec: HandlerSpec,
    state: _LeaseState,
    stop_event: threading.Event,
    interval_secs: float,
) -> None:
    while not stop_event.wait(interval_secs):
        active_lease_id, active_leases = state.snapshot()
        try:
            worker_client.heartbeat_worker(
                worker_id,
                available_capacity=_available_capacity(spec.capacity, active_leases),
                active_leases=active_leases,
                metadata=dict(spec.metadata),
            )
        except Exception:
            pass

        if active_lease_id is None:
            continue

        try:
            worker_client.renew_lease(worker_id, active_lease_id)
        except Exception:
            pass


def _read_input_payload(worker_client: Any, job_id: str, stream_name: str) -> bytes:
    streams = worker_client.list_streams(job_id)
    stream = next((item for item in streams if item["stream_name"] == stream_name), None)
    if stream is None:
        return b""

    frames = worker_client.pull_frames(stream["stream_id"])
    payload = bytearray()
    for frame in frames:
        if frame["kind"] == _FRAME_DATA:
            payload.extend(frame["payload"])
    return bytes(payload)


def _decode_payload(codec: str, payload: bytes) -> Any:
    if codec == _CODEC_BYTES:
        return payload
    if codec == _CODEC_JSON:
        if not payload:
            raise ValueError("json input stream was empty")
        return json.loads(payload.decode("utf-8"))
    raise ValueError(f"unsupported codec: {codec}")


def _encode_payload(codec: str, value: Any) -> bytes:
    if codec == _CODEC_BYTES:
        if not isinstance(value, (bytes, bytearray)):
            raise TypeError("bytes codec requires the handler to return bytes")
        return bytes(value)
    if codec == _CODEC_JSON:
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    raise ValueError(f"unsupported codec: {codec}")


def _failure_reason(exc: Exception) -> str:
    return f"{exc.__class__.__name__}: {exc}"


def _build_handler_spec(
    *,
    interface: str,
    attributes: Optional[dict[str, str]],
    capacity: Optional[dict[str, int]],
    metadata: Optional[dict[str, str]],
    codec: str,
    input_stream: str,
    output_stream: str,
    max_active_leases: int,
) -> HandlerSpec:
    if not interface:
        raise ValueError("worker interface must be non-empty")
    if codec not in {_CODEC_BYTES, _CODEC_JSON}:
        raise ValueError(f"unsupported codec: {codec}")
    if not input_stream:
        raise ValueError("input_stream must be non-empty")
    if not output_stream:
        raise ValueError("output_stream must be non-empty")
    if max_active_leases != 1:
        raise ValueError("serve(...) currently only supports max_active_leases=1")

    return HandlerSpec(
        interface=interface,
        attributes=dict(attributes or {}),
        capacity=dict(capacity or {}),
        metadata=dict(metadata or {}),
        codec=codec,
        input_stream=input_stream,
        output_stream=output_stream,
        max_active_leases=max_active_leases,
    )


def _validate_loop_intervals(heartbeat_interval_secs: float, poll_interval_secs: float) -> None:
    if heartbeat_interval_secs <= 0:
        raise ValueError("heartbeat_interval_secs must be greater than 0")
    if poll_interval_secs < 0:
        raise ValueError("poll_interval_secs must be non-negative")


def _available_capacity(capacity: dict[str, int], active_leases: int) -> dict[str, int]:
    if active_leases <= 0:
        return dict(capacity)
    return {name: 0 for name in capacity}
