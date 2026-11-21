from __future__ import annotations

import asyncio
import functools
import inspect
import json
import logging
import os
import signal
import socket
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence

from ._native import WorkerConnection
from .execution import resolve_endpoint

_LOG = logging.getLogger(__name__)
_PERMANENT_RPC_CODES = {3, 7, 16}
_TRANSPORT_RPC_CODES = {1, 2, 4, 13, 14}
_RETIRED_RPC_CODES = {9, 10}

_CODEC_BYTES = "bytes"
_CODEC_JSON = "json"


class _PreemptShutdown(BaseException):
    """Internal control-flow signal raised on SIGTERM so serve() drains preemptively."""


class WorkerError(Exception):
    """A failure the handler reports for its attempt.

    ``retryable=True`` lets the control plane start another attempt, possibly
    on another worker, within the job's attempt budget. Any other exception
    raised by a handler fails the attempt without a retry.
    """

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


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
    pool: str = ""
    uri_resolver: Optional[Callable[[str, dict[str, str]], bytes]] = None


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
    pool: str = "",
    uri_resolver: Optional[Callable[[str, dict[str, str]], bytes]] = None,
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
        pool=pool,
        uri_resolver=uri_resolver,
    )

    def decorator(fn: Callable[[Any], Any]) -> WorkerHandler:
        if isinstance(fn, WorkerHandler):
            raise TypeError("handler is already decorated with @worker(...)")
        return WorkerHandler(fn, spec)

    return decorator


def get_handler_spec(handler: Callable[[Any], Any]) -> HandlerSpec:
    spec = getattr(handler, "spec", None)
    if not isinstance(spec, HandlerSpec):
        raise TypeError("handler must be decorated with @worker(...) before serve(...)")
    return spec


def _normalize_serve_args(
    first: Callable[[Any], Any] | Sequence[Callable[[Any], Any]],
    second: Optional[str | Callable[[Any], Any]],
    rest: tuple[Callable[[Any], Any], ...],
    explicit_endpoint: Optional[str],
) -> tuple[list[tuple[Callable[[Any], Any], HandlerSpec]], Optional[str]]:
    if isinstance(first, (list, tuple)):
        handlers = list(first)
        if not handlers:
            raise ValueError("serve(...) requires at least one handler")
        resolved_endpoint = explicit_endpoint if explicit_endpoint is not None else (second if isinstance(second, str) else None)
    elif second is not None and not isinstance(second, str):
        handlers = [first, second, *rest]
        resolved_endpoint = explicit_endpoint
    else:
        handlers = [first, *rest]
        resolved_endpoint = explicit_endpoint if explicit_endpoint is not None else second
    pairs = [(h, get_handler_spec(h)) for h in handlers]
    seen_interfaces: set[str] = set()
    for _, spec in pairs:
        if spec.interface in seen_interfaces:
            raise ValueError(f"duplicate handler interface '{spec.interface}' in serve(...)")
        seen_interfaces.add(spec.interface)
    return pairs, resolved_endpoint


def serve(
    handler: Callable[[Any], Any] | Sequence[Callable[[Any], Any]],
    endpoint_or_handler: Optional[str | Callable[[Any], Any]] = None,
    *more_handlers: Callable[[Any], Any],
    endpoint: Optional[str] = None,
    worker_id: Optional[str] = None,
    token: Optional[str] = None,
    namespace: Optional[str] = None,
    pool: Optional[str] = None,
    reconnect_interval_secs: float = 1.0,
    connection: Any = None,
) -> None:
    """Run one or more ``@worker`` handlers over the outbound worker session.

    Supports sync functions, sync generators, ``async def`` coroutines, and
    ``async`` generators. Multiple handlers are multiplexed on one session.
    """
    handler_pairs, resolved_endpoint = _normalize_serve_args(
        handler, endpoint_or_handler, more_handlers, endpoint
    )
    if reconnect_interval_secs <= 0:
        raise ValueError("reconnect_interval_secs must be positive")
    resolved_endpoint = resolve_endpoint(resolved_endpoint)
    worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
    by_interface = {spec.interface: (fn, spec) for fn, spec in handler_pairs}
    specs = [spec for _, spec in handler_pairs]
    total_concurrency = (
        specs[0].max_active_leases
        if len(specs) == 1
        else sum(s.max_active_leases for s in specs)
    )
    merged_attributes: dict[str, str] = {}
    merged_capacity: dict[str, int] = {}
    merged_metadata: dict[str, str] = {}
    for s in specs:
        merged_attributes.update(s.attributes)
        merged_metadata.update(s.metadata)
        for k, v in s.capacity.items():
            merged_capacity[k] = merged_capacity.get(k, 0) + v if len(specs) > 1 else v
    merged_capacity["slot"] = total_concurrency
    session_pool = pool if pool is not None else specs[0].pool
    active: dict[str, tuple[Any, Future, dict[str, int]]] = {}
    active_lock = threading.Lock()
    # Leases whose terminal message is being sent. The control plane frees the
    # slot when it commits that message and may assign the next job before the
    # handler thread has returned here, so these no longer count as occupied.
    finishing: set[str] = set()

    def mark_finishing(lease_id: str) -> None:
        with active_lock:
            finishing.add(lease_id)
    credit_lock = threading.Lock()
    executor = ThreadPoolExecutor(max_workers=total_concurrency)
    session_id = ""
    conn = connection
    credit_sent = False
    session_draining = False
    preempting_shutdown = False
    prev_sigterm = None
    installed_sigterm = False

    def _on_sigterm(_signum: int, _frame: Any) -> None:
        nonlocal preempting_shutdown
        preempting_shutdown = True
        raise _PreemptShutdown()

    if threading.current_thread() is threading.main_thread():
        prev_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, _on_sigterm)
        installed_sigterm = True

    def report_capacity(current: Any) -> None:
        with credit_lock:
            with active_lock:
                busy = len(active)
                blocked = session_draining or any(entry[0].cancelled for entry in active.values())
            current.credit(0 if blocked else total_concurrency - busy)

    def completed(lease_id: str, future: Future) -> None:
        with active_lock:
            entry = active.pop(lease_id, None)
            finishing.discard(lease_id)
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
                    hello = {
                        "worker_id": worker_id,
                        "interfaces": [s.interface for s in specs],
                        "attributes": {**merged_attributes, "worker_id": worker_id},
                        "capacity": merged_capacity,
                        "metadata": merged_metadata,
                        "max_concurrency": total_concurrency,
                        "resume_session_id": session_id,
                        "held_lease_ids": _active_ids(active, active_lock),
                    }
                    if session_pool:
                        hello["pool"] = session_pool
                    if namespace is None:
                        conn = WorkerConnection(resolved_endpoint, hello, token)
                    else:
                        conn = WorkerConnection(
                            resolved_endpoint, hello, token, namespace=namespace
                        )
                    session_draining = False
                    session_id = conn.session_id
                    for item in _active_values(active, active_lock):
                        assignment = item[0]
                        if assignment.lease_id not in conn.resumed_lease_ids:
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
                    iface = getattr(assignment, "interface", specs[0].interface)
                    target = by_interface.get(iface)
                    req_cap = assignment.reserved_capacity
                    with active_lock:
                        busy = 0
                        used: dict[str, int] = {}
                        for lid, (_, _, held_cap) in active.items():
                            if lid in finishing:
                                continue
                            busy += 1
                            for resource, amount in held_cap.items():
                                used[resource] = used.get(resource, 0) + amount
                    accepted = (
                        target is not None
                        and not session_draining
                        and busy < total_concurrency
                        and all(
                            used.get(resource, 0) + amount <= merged_capacity.get(resource, 0)
                            for resource, amount in req_cap.items()
                        )
                    )
                    reason = (
                        ""
                        if accepted
                        else ("unknown interface" if target is None else "local capacity limit")
                    )
                    conn.accept(lease_id, accepted, reason)
                    if accepted:
                        fn, target_spec = target
                        future = executor.submit(
                            _process_assignment, fn, target_spec, assignment,
                            lambda lease_id=lease_id: mark_finishing(lease_id),
                        )
                        with active_lock:
                            active[lease_id] = (assignment, future, req_cap)
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
    except _PreemptShutdown:
        preempting_shutdown = True
    finally:
        if installed_sigterm:
            signal.signal(signal.SIGTERM, prev_sigterm)
        if conn is not None:
            try:
                if preempting_shutdown:
                    try:
                        conn.drain(preempting=True)
                    except TypeError:
                        conn.drain()
                else:
                    conn.drain()
            except RuntimeError:
                pass
        for item in _active_values(active, active_lock):
            item[0].cancel()
        executor.shutdown(wait=True, cancel_futures=True)
        if conn is not None:
            if preempting_shutdown:
                time.sleep(0.05)
            conn.close()


def _active_ids(active: dict, lock: threading.Lock) -> list[str]:
    with lock:
        return list(active)


def _active_values(active: dict, lock: threading.Lock) -> list:
    with lock:
        return list(active.values())


async def _drive_async_generator(agen: Any, spec: HandlerSpec, attempt: Any) -> None:
    try:
        async for item in agen:
            if attempt.cancelled:
                return
            attempt.frame(_encode_stream_frame(spec.codec, item))
    finally:
        await agen.aclose()


def _process_assignment(
    handler: Callable, spec: HandlerSpec, assignment: Any,
    finishing: Callable[[], None] = lambda: None,
) -> None:
    attempt = assignment.open_stream() if spec.streaming_input else assignment.open()
    if attempt.cancelled or attempt.terminal_replay:
        return
    try:
        if spec.streaming_input:
            value = _input_fragments(attempt)
        else:
            inputs = attempt.inputs
            if spec.uri_resolver is None:
                if len(inputs) == 1:
                    item = inputs[0]
                    if "inline" not in item:
                        raise ValueError("serve(...) requires inline inputs")
                    raw = item["inline"]
                else:
                    if any("inline" not in item for item in inputs):
                        raise ValueError("serve(...) requires inline inputs")
                    raw = b"".join(item["inline"] for item in inputs)
            else:
                parts: list[bytes] = []
                for item in inputs:
                    if "inline" in item:
                        parts.append(item["inline"])
                    elif "uri" in item:
                        meta = item.get("metadata") or {}
                        parts.append(spec.uri_resolver(item["uri"], meta))
                    else:
                        raise ValueError("input object has neither inline nor uri")
                raw = b"".join(parts)
            value = _decode_payload(spec.codec, raw)
        result = handler(value, attempt) if spec.pass_context else handler(value)
        if inspect.iscoroutine(result):
            result = asyncio.run(result)
        if inspect.isasyncgen(result):
            asyncio.run(_drive_async_generator(result, spec, attempt))
            outputs: list[dict[str, Any]] = []
        elif inspect.isgenerator(result):
            try:
                for item in result:
                    if attempt.cancelled:
                        return
                    attempt.frame(_encode_stream_frame(spec.codec, item))
            finally:
                result.close()
            outputs = []
        else:
            outputs = [{"inline": _encode_payload(spec.codec, result), "metadata": {}}]
        if not attempt.cancelled:
            finishing()
            attempt.complete(outputs)
    except Exception as exc:
        code = getattr(exc, "status_code", None)
        if code in _TRANSPORT_RPC_CODES or code in _RETIRED_RPC_CODES:
            raise
        if not attempt.cancelled:
            retryable = getattr(exc, "retryable", code == 8)
            finishing()
            attempt.fail(_failure_reason(exc), retryable=bool(retryable))


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
        if type(value) is bytes:
            return value
        if not isinstance(value, (bytes, bytearray)):
            raise TypeError("bytes codec requires the handler to return bytes")
        return bytes(value)
    if codec == _CODEC_JSON:
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    raise ValueError(f"unsupported codec: {codec}")


def _encode_stream_frame(codec: str, value: Any) -> bytes:
    if codec == _CODEC_JSON:
        return (json.dumps(value, separators=(",", ":"), ensure_ascii=True) + "\n").encode("utf-8")
    return _encode_payload(codec, value)


def _failure_reason(exc: Exception) -> str:
    if isinstance(exc, WorkerError):
        return str(exc)
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
    pool: str = "",
    uri_resolver: Optional[Callable[[str, dict[str, str]], bytes]] = None,
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
        pool=pool,
        uri_resolver=uri_resolver,
    )
