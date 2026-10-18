from __future__ import annotations

import functools
import inspect
import json
import time
import threading
import logging
import os
import socket
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from ._native import WorkerConnection

_LOG = logging.getLogger(__name__)
_PERMANENT_RPC_CODES = {3, 7, 16}
_TRANSPORT_RPC_CODES = {1, 2, 4, 13, 14}
_RETIRED_RPC_CODES = {9, 10}

_CODEC_BYTES = "bytes"
_CODEC_JSON = "json"


@dataclass(frozen=True)
class HandlerSpec:
    interface: str
    attributes: dict[str, str] = field(default_factory=dict)
    capacity: dict[str, int] = field(default_factory=dict)
    metadata: dict[str, str] = field(default_factory=dict)
    codec: str = _CODEC_BYTES
    max_active_leases: int = 1
    pass_context: bool = False
    streaming_input: bool = False


class WorkerHandler:
    def __init__(self, fn: Callable[[Any], Any], spec: HandlerSpec) -> None:
        functools.update_wrapper(self, fn)
        self._fn = fn
        self.spec = spec

    def __call__(self, *args, **kwargs):
        return self._fn(*args, **kwargs)


def worker(
    *,
    interface: str,
    attributes: Optional[dict[str, str]] = None,
    capacity: Optional[dict[str, int]] = None,
    metadata: Optional[dict[str, str]] = None,
    codec: str = _CODEC_BYTES,
    max_active_leases: int = 1,
    pass_context: bool = False,
    streaming_input: bool = False,
) -> Callable[[Callable[[Any], Any]], WorkerHandler]:
    spec = _build_handler_spec(
        interface=interface,
        attributes=attributes,
        capacity=capacity,
        metadata=metadata,
        codec=codec,
        max_active_leases=max_active_leases,
        pass_context=pass_context,
        streaming_input=streaming_input,
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
    handler: Callable[[Any], Any], endpoint: Optional[str] = None, *,
    worker_id: Optional[str] = None, token: Optional[str] = None,
    reconnect_interval_secs: float = 1.0, connection: Any = None,
) -> None:
    """Run handlers over the outbound worker session with bounded concurrency.

    A generator handler emits output frames. A normal return value becomes the
    terminal result. Callbacks can observe cancellation through a context when
    decorated with ``pass_context=True``; Python callbacks cancel cooperatively.
    """
    spec = get_handler_spec(handler)
    if reconnect_interval_secs <= 0:
        raise ValueError("reconnect_interval_secs must be positive")
    if connection is None and endpoint is None:
        raise ValueError("endpoint is required")
    worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
    active: dict[str, tuple[Any, Future]] = {}
    active_lock = threading.Lock()
    credit_lock = threading.Lock()
    pool = ThreadPoolExecutor(max_workers=spec.max_active_leases)
    session_id = ""
    conn = connection
    credit_sent = False
    session_draining = False

    def report_capacity(current: Any) -> None:
        with credit_lock:
            entries = _active_snapshot(active, active_lock)
            free = 0 if session_draining or any(item.cancelled for _, (item, _) in entries) else spec.max_active_leases - len(entries)
            current.credit(free)

    def completed(lease_id: str, future: Future) -> None:
        with active_lock:
            entry = active.pop(lease_id, None)
        failure = None if future.cancelled() else future.exception()
        if failure is not None and (entry is None or not entry[0].cancelled):
            _LOG.warning("attempt %s ended: %s", lease_id, failure)
        current = conn
        if current is not None and entry is not None:
            try:
                if entry[0].cancelled:
                    current.cancel_ack(lease_id)
                if getattr(failure, "status_code", None) in _TRANSPORT_RPC_CODES:
                    # Omit the failed lease on reconnect; preserve other attempts.
                    current.close()
                    return
                report_capacity(current)
            except RuntimeError as exc:
                _LOG.warning("worker control update failed: %s", exc)

    try:
        while True:
            if conn is None:
                try:
                    conn = WorkerConnection(endpoint, {
                        "worker_id": worker_id, "interfaces": [spec.interface],
                        "attributes": {**spec.attributes, "worker_id": worker_id},
                        "capacity": spec.capacity, "metadata": spec.metadata, "max_concurrency": spec.max_active_leases,
                        "resume_session_id": session_id, "held_lease_ids": _active_ids(active, active_lock),
                    }, token)
                    session_draining = False
                    session_id = conn.session_id
                    for lease_id, (assignment, _) in _active_snapshot(active, active_lock):
                        if lease_id not in conn.resumed_lease_ids:
                            assignment.cancel()
                except RuntimeError as exc:
                    if getattr(exc, "status_code", None) in _PERMANENT_RPC_CODES:
                        raise
                    _LOG.warning("worker connection failed; reconnecting: %s", exc)
                    time.sleep(reconnect_interval_secs)
                    continue
            try:
                if not credit_sent:
                    report_capacity(conn)
                    credit_sent = True
                message = conn.receive(0.2)
                if message is None:
                    continue
                kind = message["type"]
                lease_id = message.get("lease_id")
                if kind == "assignment":
                    assignment = message["assignment"]
                    entries = _active_snapshot(active, active_lock)
                    used: dict[str, int] = {}
                    for _, (held, _) in entries:
                        for resource, amount in held.reserved_capacity.items():
                            used[resource] = used.get(resource, 0) + amount
                    accepted = not session_draining and len(entries) < spec.max_active_leases and all(
                        used.get(resource, 0) + amount <= spec.capacity.get(resource, 0)
                        for resource, amount in assignment.reserved_capacity.items()
                    )
                    conn.accept(lease_id, accepted, "local capacity limit" if not accepted else "")
                    if accepted:
                        future = pool.submit(_process_assignment, handler, spec, assignment)
                        with active_lock:
                            active[lease_id] = (assignment, future)
                        future.add_done_callback(lambda done, lease_id=lease_id: completed(lease_id, done))
                    else:
                        report_capacity(conn)
                elif kind in {"cancel", "fence"}:
                    with active_lock:
                        entry = active.get(lease_id)
                    if entry is not None:
                        entry[0].cancel()
                        report_capacity(conn)
                    elif kind == "cancel":
                        conn.cancel_ack(lease_id)
                elif kind == "drain":
                    session_draining = True
                    report_capacity(conn)
            except RuntimeError as exc:
                conn.close()
                if getattr(exc, "status_code", None) in _PERMANENT_RPC_CODES or connection is not None:
                    raise
                _LOG.warning("worker session interrupted; reconnecting: %s", exc)
                conn = None
                credit_sent = False
                time.sleep(reconnect_interval_secs)
    finally:
        if conn is not None:
            try:
                conn.drain()
            except RuntimeError:
                pass
        for _, (assignment, _) in _active_snapshot(active, active_lock):
            assignment.cancel()
        pool.shutdown(wait=True, cancel_futures=True)
        if conn is not None:
            conn.close()


def _active_ids(active: dict, lock: threading.Lock) -> list[str]:
    with lock:
        return list(active)


def _active_snapshot(active: dict, lock: threading.Lock) -> list:
    with lock:
        return list(active.items())


def _process_assignment(handler: Callable, spec: HandlerSpec, assignment: Any) -> None:
    attempt = assignment.open_stream() if spec.streaming_input else assignment.open()
    if attempt.cancelled or attempt.terminal_replay:
        return
    try:
        if spec.streaming_input:
            value = _input_fragments(attempt)
        else:
            inputs = attempt.inputs
            if any("inline" not in item for item in inputs):
                raise ValueError("serve(...) requires inline inputs")
            value = _decode_payload(spec.codec, b"".join(item["inline"] for item in inputs))
        result = handler(value, attempt) if spec.pass_context else handler(value)
        if inspect.isgenerator(result):
            try:
                for item in result:
                    if attempt.cancelled:
                        return
                    payload = _encode_payload(spec.codec, item)
                    if spec.codec == _CODEC_JSON:
                        payload += b"\n"
                    attempt.frame(payload)
            finally:
                result.close()
            outputs = []
        else:
            outputs = [{"inline": _encode_payload(spec.codec, result), "metadata": {}}]
        if not attempt.cancelled:
            attempt.complete(outputs)
    except Exception as exc:
        code = getattr(exc, "status_code", None)
        if code in _TRANSPORT_RPC_CODES or code in _RETIRED_RPC_CODES:
            raise
        if not attempt.cancelled:
            attempt.fail(_failure_reason(exc), retryable=code == 8)


def _input_fragments(attempt: Any):
    for index, item in enumerate(attempt.inputs):
        yield {"object_index": index, "data": item.get("inline", b""), "uri": item.get("uri", ""),
               "metadata": item.get("metadata", {}), "object_end": True, "parameter_chunk": False}
    while True:
        part = attempt.input_chunk()
        if part is None:
            return
        yield part


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
    max_active_leases: int,
    pass_context: bool,
    streaming_input: bool,
) -> HandlerSpec:
    if not interface:
        raise ValueError("worker interface must be non-empty")
    if codec not in {_CODEC_BYTES, _CODEC_JSON}:
        raise ValueError(f"unsupported codec: {codec}")
    if streaming_input and codec != _CODEC_BYTES:
        raise ValueError("streaming_input uses byte fragments; decode inside the handler")
    if max_active_leases < 1:
        raise ValueError("max_active_leases must be positive")

    resources = dict(capacity or {})
    resources.setdefault("slot", max_active_leases)
    return HandlerSpec(
        interface=interface,
        attributes=dict(attributes or {}),
        capacity=resources,
        metadata=dict(metadata or {}),
        codec=codec,
        max_active_leases=max_active_leases,
        pass_context=pass_context,
        streaming_input=streaming_input,
    )
