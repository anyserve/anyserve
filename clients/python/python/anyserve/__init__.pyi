from typing import Any, TypedDict
from .execution import (
    AsyncExecutionClient as AsyncAnyserveClient,
    AsyncExecutionClient as AsyncExecutionClient,
    AsyncExecutionEvents as AsyncExecutionEvents,
    AsyncStreamingInvocation as AsyncStreamingInvocation,
    ExecutionClient as ExecutionClient,
    ExecutionClient as AnyserveClient,
    ExecutionOptions as ExecutionOptions,
    ExecutionEvents as ExecutionEvents,
    StreamingInvocation as StreamingInvocation,
)
from .highlevel import HandlerSpec as HandlerSpec, WorkerError as WorkerError, WorkerHandler as WorkerHandler, get_handler_spec as get_handler_spec, serve as serve, worker as worker

class ObjectRef(TypedDict, total=False):
    inline: bytes
    uri: str
    metadata: dict[str, str]

def inline_object(content: bytes, metadata: dict[str, str] | None = ...) -> ObjectRef: ...
def uri_object(
    uri: str,
    metadata: dict[str, str] | None = ...,
    *,
    sha256: str | None = ...,
    size_bytes: int | None = ...,
    content_type: str | None = ...,
) -> ObjectRef: ...

class AttemptContext:
    job_id: str
    attempt_id: str
    lease_id: str
    metadata: dict[str, str]
    inputs: list[ObjectRef]
    params: bytes
    cancelled: bool
    terminal_replay: bool
    def input_chunk(self) -> dict[str, Any] | None: ...
    def frame(self, data: bytes, metadata: dict[str, str] | None = ..., progress: bool = ...) -> None: ...
    def complete(self, outputs: list[ObjectRef] | None = ...) -> None: ...
    def fail(self, message: str, retryable: bool = ..., code: str | None = ...) -> None: ...

class WorkerConnection:
    session_id: str
    generation: int
    resumed_lease_ids: list[str]
    def __init__(self, endpoint: str, hello: dict[str, Any], token: str | None = ..., namespace: str | None = ...) -> None: ...
    def receive(self, timeout_secs: float = ...) -> dict[str, Any] | None: ...
    def credit(self, free_slots: int) -> None: ...
    def accept(self, lease_id: str, accepted: bool = ..., reason: str = ...) -> None: ...
    def cancel_ack(self, lease_id: str) -> None: ...
    def drain(self, preempting: bool = ...) -> None: ...
    def update_load(
        self,
        queued: int = ...,
        running: int = ...,
        utilization: float = ...,
        custom: dict[str, float] | None = ...,
    ) -> None: ...
    def close(self) -> None: ...

class RpcError(RuntimeError):
    status_code: int
