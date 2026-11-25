from __future__ import annotations

from ._native import RpcError, WorkerConnection, WorkerAttempt as AttemptContext
from .execution import (
    AsyncExecutionClient,
    AsyncExecutionClient as AsyncAnyserveClient,
    AsyncExecutionEvents,
    AsyncStreamingInvocation,
    ExecutionClient,
    ExecutionClient as AnyserveClient,
    ExecutionEvents,
    ExecutionOptions,
    StreamingInvocation,
)
from .highlevel import HandlerSpec, WorkerError, WorkerHandler, get_handler_spec, serve, worker


def inline_object(content: bytes, metadata: dict[str, str] | None = None) -> dict[str, object]:
    return {"inline": content, "metadata": dict(metadata or {})}


def uri_object(
    uri: str,
    metadata: dict[str, str] | None = None,
    *,
    sha256: str | None = None,
    size_bytes: int | None = None,
    content_type: str | None = None,
) -> dict[str, object]:
    if uri == "":
        raise ValueError("object URI must not be empty")
    meta = dict(metadata or {})
    if sha256 is not None:
        meta["anyserve.sha256"] = sha256
    if size_bytes is not None:
        meta["anyserve.size_bytes"] = str(size_bytes)
    if content_type is not None:
        meta["content_type"] = content_type
    return {"uri": uri, "metadata": meta}


__all__ = [
    "AnyserveClient",
    "AsyncAnyserveClient",
    "AsyncExecutionClient",
    "AsyncExecutionEvents",
    "AsyncStreamingInvocation",
    "ExecutionClient",
    "ExecutionEvents",
    "ExecutionOptions",
    "StreamingInvocation",
    "RpcError",
    "WorkerConnection",
    "AttemptContext",
    "HandlerSpec",
    "WorkerError",
    "WorkerHandler",
    "worker",
    "serve",
    "get_handler_spec",
    "inline_object",
    "uri_object",
]
