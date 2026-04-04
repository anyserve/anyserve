from __future__ import annotations

from .client import AnyserveClient, SubmitterClient, WorkerClient
from .highlevel import HandlerSpec, WorkerHandler, get_handler_spec, serve, worker

EVENT_ACCEPTED = "accepted"
EVENT_LEASE_GRANTED = "lease_granted"
EVENT_STARTED = "started"
EVENT_PROGRESS = "progress"
EVENT_OUTPUT_READY = "output_ready"
EVENT_SUCCEEDED = "succeeded"
EVENT_FAILED = "failed"
EVENT_CANCELLED = "cancelled"
EVENT_LEASE_EXPIRED = "lease_expired"
EVENT_REQUEUED = "requeued"

STREAM_SCOPE_JOB = "job"
STREAM_SCOPE_ATTEMPT = "attempt"
STREAM_SCOPE_LEASE = "lease"

STREAM_DIRECTION_CLIENT_TO_WORKER = "client_to_worker"
STREAM_DIRECTION_WORKER_TO_CLIENT = "worker_to_client"
STREAM_DIRECTION_BIDIRECTIONAL = "bidirectional"
STREAM_DIRECTION_INTERNAL = "internal"

FRAME_OPEN = "open"
FRAME_DATA = "data"
FRAME_CLOSE = "close"
FRAME_ERROR = "error"
FRAME_CHECKPOINT = "checkpoint"
FRAME_CONTROL = "control"


def inline_object(content: bytes, metadata: dict[str, str] | None = None) -> dict[str, object]:
    return {"inline": content, "metadata": dict(metadata or {})}


def uri_object(uri: str, metadata: dict[str, str] | None = None) -> dict[str, object]:
    return {"uri": uri, "metadata": dict(metadata or {})}


__all__ = [
    "AnyserveClient",
    "SubmitterClient",
    "WorkerClient",
    "HandlerSpec",
    "WorkerHandler",
    "worker",
    "serve",
    "get_handler_spec",
    "EVENT_ACCEPTED",
    "EVENT_LEASE_GRANTED",
    "EVENT_STARTED",
    "EVENT_PROGRESS",
    "EVENT_OUTPUT_READY",
    "EVENT_SUCCEEDED",
    "EVENT_FAILED",
    "EVENT_CANCELLED",
    "EVENT_LEASE_EXPIRED",
    "EVENT_REQUEUED",
    "STREAM_SCOPE_JOB",
    "STREAM_SCOPE_ATTEMPT",
    "STREAM_SCOPE_LEASE",
    "STREAM_DIRECTION_CLIENT_TO_WORKER",
    "STREAM_DIRECTION_WORKER_TO_CLIENT",
    "STREAM_DIRECTION_BIDIRECTIONAL",
    "STREAM_DIRECTION_INTERNAL",
    "FRAME_OPEN",
    "FRAME_DATA",
    "FRAME_CLOSE",
    "FRAME_ERROR",
    "FRAME_CHECKPOINT",
    "FRAME_CONTROL",
    "inline_object",
    "uri_object",
]
