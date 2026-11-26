from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import inspect
import json
import logging
import os
import random
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
_DEFAULT_ERROR_CODE = "worker_failed"


class _PreemptShutdown(BaseException):
    """Internal control-flow signal raised on SIGTERM so serve() drains preemptively."""


class WorkerError(Exception):
    """A failure the handler reports for its attempt.

    ``retryable=True`` lets the control plane start another attempt, possibly
    on another worker, within the job's attempt budget. Any other exception
    raised by a handler fails the attempt without a retry.
    """

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        code: str = _DEFAULT_ERROR_CODE,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.code = code if code and code.strip() else _DEFAULT_ERROR_CODE


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


def _reconnect_backoff(attempt: int, base: float, cap: float) -> float:
    exp = min(cap, base * (2 ** min(max(attempt, 0), 16)))
    return exp * random.uniform(0.5, 1.0)


def _push_load(conn: Any, load_provider: Callable[[], dict[str, Any]]) -> None:
    try:
        data = load_provider() or {}
        conn.update_load(
            queued=int(data.get("queued", 0)),
            running=int(data.get("running", 0)),
            utilization=float(data.get("utilization", 0.0)),
            custom={str(k): float(v) for k, v in (data.get("custom") or {}).items()}
            if data.get("custom") is not None
            else None,
        )
    except Exception as exc:
        _LOG.warning("load_provider failed: %s", exc)


def _resolve_cancelled(fut: concurrent.futures.Future[Any]) -> None:
    try:
        fut.set_exception(asyncio.CancelledError())
    except concurrent.futures.InvalidStateError:
        pass


class _AsyncLoopRunner:
    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._lock = threading.Lock()
        self._closed = False
        # key -> [done_fut, task, cancel_requested]
        self._entries: dict[Any, list[Any]] = {}
        self._thread = threading.Thread(
            target=self._run, name="anyserve-worker-async", daemon=True
        )
        self._thread.start()
        self._ready.wait()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()

    def run(
        self,
        coro: Any,
        *,
        key: Any = None,
        is_cancelled: Optional[Callable[[], bool]] = None,
    ) -> Any:
        done_fut: concurrent.futures.Future[Any] = concurrent.futures.Future()
        entry: list[Any] = [done_fut, None, False]
        entry_key = object() if key is None else key
        with self._lock:
            if self._closed or (is_cancelled is not None and is_cancelled()):
                coro.close()
                raise asyncio.CancelledError()
            self._entries[entry_key] = entry

        def _start() -> None:
            with self._lock:
                if self._closed or entry[2]:
                    if self._entries.get(entry_key) is entry:
                        self._entries.pop(entry_key, None)
                    coro.close()
                    _resolve_cancelled(done_fut)
                    return
                task = self._loop.create_task(coro)
                entry[1] = task

            def _on_done(finished: asyncio.Task[Any]) -> None:
                with self._lock:
                    if self._entries.get(entry_key) is entry:
                        self._entries.pop(entry_key, None)
                if finished.cancelled():
                    _resolve_cancelled(done_fut)
                    return
                exc = finished.exception()
                try:
                    if exc is not None:
                        done_fut.set_exception(exc)
                    else:
                        done_fut.set_result(finished.result())
                except concurrent.futures.InvalidStateError:
                    pass

            task.add_done_callback(_on_done)

        try:
            self._loop.call_soon_threadsafe(_start)
        except RuntimeError:
            with self._lock:
                if self._entries.get(entry_key) is entry:
                    self._entries.pop(entry_key, None)
            coro.close()
            raise asyncio.CancelledError()
        return done_fut.result()

    def cancel(self, key: Any, *, force: bool = False) -> None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            entry[2] = True
            done_fut, task, _ = entry
        if task is not None and not self._loop.is_closed():
            try:
                self._loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass
        if force:
            _resolve_cancelled(done_fut)

    def close(self, timeout: float = 0.5) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            entries = list(self._entries.values())
            self._entries.clear()
        for entry in entries:
            entry[2] = True
        if not self._loop.is_closed() and self._thread.is_alive():
            async def _shutdown() -> None:
                try:
                    current = asyncio.current_task()
                    pending = [
                        t
                        for t in asyncio.all_tasks(self._loop)
                        if t is not current and not t.done()
                    ]
                    for task in pending:
                        task.cancel()
                    if pending:
                        done, _ = await asyncio.wait(pending, timeout=timeout)
                        for task in done:
                            if not task.cancelled():
                                task.exception()
                    ag_task = self._loop.create_task(self._loop.shutdown_asyncgens())
                    done_ag, pending_ag = await asyncio.wait({ag_task}, timeout=timeout)
                    for task in done_ag:
                        if not task.cancelled():
                            task.exception()
                    for task in pending_ag:
                        task.cancel()
                except Exception:
                    pass
                finally:
                    self._loop.stop()

            shutdown_coro = _shutdown()
            try:
                self._loop.call_soon_threadsafe(self._loop.create_task, shutdown_coro)
            except RuntimeError:
                shutdown_coro.close()
                try:
                    self._loop.call_soon_threadsafe(self._loop.stop)
                except RuntimeError:
                    pass
            self._thread.join(timeout=2.0)
        for entry in entries:
            _resolve_cancelled(entry[0])
        if not self._thread.is_alive() and not self._loop.is_closed():
            self._loop.close()


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
    max_reconnect_interval_secs: float = 30.0,
    graceful_shutdown_timeout: float = 30.0,
    load_provider: Optional[Callable[[], dict[str, Any]]] = None,
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
    if max_reconnect_interval_secs < reconnect_interval_secs:
        raise ValueError("max_reconnect_interval_secs must be >= reconnect_interval_secs")
    if graceful_shutdown_timeout < 0:
        raise ValueError("graceful_shutdown_timeout must be non-negative")
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
    async_runner = _AsyncLoopRunner()
    session_id = ""
    conn = connection
    credit_sent = False
    session_draining = False
    preempting_shutdown = False
    reconnect_attempts = 0
    connected_at = 0.0
    last_load_push = 0.0
    prev_sigterm = None
    installed_sigterm = False

    def _on_sigterm(_signum: int, _frame: Any) -> None:
        nonlocal preempting_shutdown
        preempting_shutdown = True
        raise _PreemptShutdown()

    def backoff(what: str, exc: BaseException) -> None:
        nonlocal reconnect_attempts
        delay = _reconnect_backoff(
            reconnect_attempts, reconnect_interval_secs, max_reconnect_interval_secs
        )
        reconnect_attempts += 1
        _LOG.warning("worker %s; reconnecting in %.2fs: %s", what, delay, exc)
        time.sleep(delay)

    if threading.current_thread() is threading.main_thread():
        prev_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, _on_sigterm)
        installed_sigterm = True

    def cancel_active(lease_id: str, *, force: bool = False) -> Optional[Any]:
        with active_lock:
            entry = active.get(lease_id)
        if entry is not None:
            entry[0].cancel()
        async_runner.cancel(lease_id, force=force)
        return entry

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
                    with active_lock:
                        session_draining = False
                    session_id = conn.session_id
                    connected_at = time.monotonic()
                    for item in _active_values(active, active_lock):
                        assignment = item[0]
                        if assignment.lease_id not in conn.resumed_lease_ids:
                            cancel_active(assignment.lease_id)
                except RuntimeError as exc:
                    if getattr(exc, "status_code", None) in _PERMANENT_RPC_CODES:
                        raise
                    backoff("connection failed", exc)
                    continue
            try:
                if load_provider is not None and (
                    not credit_sent or time.monotonic() - last_load_push >= 1.0
                ):
                    _push_load(conn, load_provider)
                    last_load_push = time.monotonic()
                if not credit_sent:
                    report_capacity(conn)
                    credit_sent = True
                message = conn.receive(0.2)
                if message is None:
                    continue
                reconnect_attempts = 0
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
                            _process_assignment,
                            fn,
                            target_spec,
                            assignment,
                            lambda lease_id=lease_id: mark_finishing(lease_id),
                            async_runner,
                        )
                        with active_lock:
                            active[lease_id] = (assignment, future, req_cap)
                        future.add_done_callback(lambda done, lease_id=lease_id: completed(lease_id, done))
                    else:
                        report_capacity(conn)
                elif kind in {"cancel", "fence"}:
                    entry = cancel_active(lease_id)
                    if entry is not None:
                        report_capacity(conn)
                    elif kind == "cancel":
                        conn.cancel_ack(lease_id)
                elif kind == "drain":
                    with active_lock:
                        session_draining = True
                    report_capacity(conn)
            except RuntimeError as exc:
                conn.close()
                if getattr(exc, "status_code", None) in _PERMANENT_RPC_CODES or connection is not None:
                    raise
                # A session that stayed up long enough restarts the backoff schedule.
                if connected_at and time.monotonic() - connected_at >= max(reconnect_interval_secs, 5.0):
                    reconnect_attempts = 0
                conn = None
                credit_sent = False
                backoff("session interrupted", exc)
    except _PreemptShutdown:
        preempting_shutdown = True
    finally:
        if installed_sigterm:
            signal.signal(signal.SIGTERM, prev_sigterm)
        with credit_lock:
            with active_lock:
                session_draining = True
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
        try:
            if not preempting_shutdown and graceful_shutdown_timeout > 0:
                concurrent.futures.wait(
                    [entry[1] for entry in _active_values(active, active_lock)],
                    timeout=graceful_shutdown_timeout,
                )
            remaining = [
                (lid, entry)
                for lid, entry in _active_items(active, active_lock)
                if not entry[1].done()
            ]
            for lid, (assignment, _, _) in remaining:
                assignment.cancel()
                async_runner.cancel(lid)
            if remaining:
                _, still_running = concurrent.futures.wait(
                    [entry[1] for _, entry in remaining],
                    timeout=1.0,
                )
                if still_running:
                    for lid, (_, fut, _) in remaining:
                        if not fut.done():
                            async_runner.cancel(lid, force=True)
            executor.shutdown(wait=True, cancel_futures=True)
        finally:
            try:
                async_runner.close()
            finally:
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


def _active_items(active: dict, lock: threading.Lock) -> list:
    with lock:
        return list(active.items())


async def _drive_async_generator(agen: Any, spec: HandlerSpec, attempt: Any) -> None:
    try:
        async for item in agen:
            if attempt.cancelled:
                return
            await asyncio.to_thread(attempt.frame, _encode_stream_frame(spec.codec, item))
            if attempt.cancelled:
                return
    finally:
        await agen.aclose()


def _process_assignment(
    handler: Callable, spec: HandlerSpec, assignment: Any,
    finishing: Callable[[], None] = lambda: None,
    async_runner: Optional[_AsyncLoopRunner] = None,
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
        handler_started = time.monotonic()
        result = handler(value, attempt) if spec.pass_context else handler(value)
        def _run_coro(coro: Any) -> Any:
            if async_runner is not None:
                return async_runner.run(
                    coro,
                    key=assignment.lease_id,
                    is_cancelled=lambda: attempt.cancelled,
                )
            return asyncio.run(coro)

        if inspect.iscoroutine(result):
            result = _run_coro(result)
        if inspect.isasyncgen(result):
            _run_coro(_drive_async_generator(result, spec, attempt))
            outputs: list[dict[str, Any]] = []
        elif inspect.isgenerator(result):
            try:
                for item in result:
                    if attempt.cancelled:
                        return
                    attempt.frame(_encode_stream_frame(spec.codec, item))
                    if attempt.cancelled:
                        return
            finally:
                result.close()
            outputs = []
        else:
            outputs = [{"inline": _encode_payload(spec.codec, result), "metadata": {}}]
        handler_ms = max(0, int(round((time.monotonic() - handler_started) * 1000)))
        if not attempt.cancelled:
            finishing()
            _attempt_complete(attempt, outputs, handler_ms)
    except (asyncio.CancelledError, concurrent.futures.CancelledError) as exc:
        if attempt.cancelled:
            return
        _LOG.exception("worker handler cancelled for interface %s", spec.interface)
        handler_ms = max(0, int(round((time.monotonic() - handler_started) * 1000))) if "handler_started" in locals() else None
        finishing()
        _attempt_fail(attempt, _failure_reason(exc), retryable=False, code=_DEFAULT_ERROR_CODE, handler_duration_ms=handler_ms)
    except Exception as exc:
        code = getattr(exc, "status_code", None)
        if code in _TRANSPORT_RPC_CODES or code in _RETIRED_RPC_CODES:
            raise
        if attempt.cancelled:
            return
        if not isinstance(exc, WorkerError):
            _LOG.exception("worker handler failed for interface %s", spec.interface)
        retryable = getattr(exc, "retryable", code == 8)
        err_code = exc.code if isinstance(exc, WorkerError) else _DEFAULT_ERROR_CODE
        handler_ms = max(0, int(round((time.monotonic() - handler_started) * 1000))) if "handler_started" in locals() else None
        finishing()
        _attempt_fail(attempt, _failure_reason(exc), retryable=bool(retryable), code=err_code, handler_duration_ms=handler_ms)


def _attempt_complete(attempt: Any, outputs: list[dict[str, Any]], handler_duration_ms: Optional[int]) -> None:
    if handler_duration_ms is not None:
        try:
            attempt.complete(outputs, handler_duration_ms=handler_duration_ms)
            return
        except TypeError:
            pass
    attempt.complete(outputs)


def _attempt_fail(
    attempt: Any,
    message: str,
    *,
    retryable: bool,
    code: str,
    handler_duration_ms: Optional[int],
) -> None:
    if handler_duration_ms is not None:
        try:
            attempt.fail(message, retryable=retryable, code=code, handler_duration_ms=handler_duration_ms)
            return
        except TypeError:
            pass
    attempt.fail(message, retryable=retryable, code=code)


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
