from dataclasses import dataclass
from typing import Any, Callable, Literal

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

class WorkerHandler:
    spec: HandlerSpec
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...

def worker(*, interface: str, attributes: dict[str, str] | None = ..., capacity: dict[str, int] | None = ...,
           metadata: dict[str, str] | None = ..., codec: str = ..., max_active_leases: int = ...,
           pass_context: bool = ..., streaming_input: bool = ...) -> Callable[[Callable[..., Any]], WorkerHandler]: ...
def get_handler_spec(handler: Callable[..., Any]) -> HandlerSpec: ...
def serve(handler: Callable[..., Any], endpoint: str | None = ..., *, worker_id: str | None = ...,
          token: str | None = ..., reconnect_interval_secs: float = ..., connection: Any = ...) -> None: ...
