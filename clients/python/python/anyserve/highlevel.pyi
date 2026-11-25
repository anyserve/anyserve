from dataclasses import dataclass
from typing import Any, Callable, Literal, Sequence

HandlerCodec = Literal["bytes", "json"]

@dataclass(frozen=True)
class HandlerSpec:
    interface: str
    attributes: dict[str, str]
    capacity: dict[str, int]
    metadata: dict[str, str]
    codec: str
    max_active_leases: int
    pass_context: bool
    streaming_input: bool
    pool: str = ...
    uri_resolver: Callable[[str, dict[str, str]], bytes] | None = ...

class WorkerError(Exception):
    retryable: bool
    code: str
    def __init__(self, message: str, *, retryable: bool = ..., code: str = ...) -> None: ...

class WorkerHandler:
    spec: HandlerSpec
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...

def worker(*, interface: str, attributes: dict[str, str] | None = ..., capacity: dict[str, int] | None = ...,
           metadata: dict[str, str] | None = ..., codec: str = ..., max_active_leases: int = ...,
           pass_context: bool = ..., streaming_input: bool = ..., pool: str = ...,
           uri_resolver: Callable[[str, dict[str, str]], bytes] | None = ...) -> Callable[[Callable[..., Any]], WorkerHandler]: ...
def get_handler_spec(handler: Callable[..., Any]) -> HandlerSpec: ...
def serve(
    handler: Callable[..., Any] | Sequence[Callable[..., Any]],
    endpoint_or_handler: str | Callable[..., Any] | None = ...,
    *more_handlers: Callable[..., Any],
    endpoint: str | None = ...,
    worker_id: str | None = ...,
    token: str | None = ...,
    namespace: str | None = ...,
    pool: str | None = ...,
    reconnect_interval_secs: float = ...,
    max_reconnect_interval_secs: float = ...,
    graceful_shutdown_timeout: float = ...,
    load_provider: Callable[[], dict[str, Any]] | None = ...,
    connection: Any = ...,
) -> None: ...
