from __future__ import annotations

from ._native import RpcError, WorkerConnection, WorkerAttempt as AttemptContext
from .execution import ExecutionClient, ExecutionClient as AnyserveClient, ExecutionEvents, ExecutionOptions
from .highlevel import HandlerSpec, WorkerError, WorkerHandler, get_handler_spec, serve, worker


def inline_object(content: bytes, metadata: dict[str, str] | None = None) -> dict[str, object]:
    return {"inline": content, "metadata": dict(metadata or {})}


def uri_object(uri: str, metadata: dict[str, str] | None = None) -> dict[str, object]:
    if uri == "":
        raise ValueError("object URI must not be empty")
    return {"uri": uri, "metadata": dict(metadata or {})}


__all__ = ["AnyserveClient", "ExecutionClient", "ExecutionEvents", "ExecutionOptions",
           "RpcError", "WorkerConnection", "AttemptContext", "HandlerSpec", "WorkerError", "WorkerHandler", "worker",
           "serve", "get_handler_spec", "inline_object", "uri_object"]
