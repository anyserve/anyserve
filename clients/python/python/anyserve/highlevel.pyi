from typing import Any, Callable, Dict, Literal, Optional

HandlerCodec = Literal["bytes", "json"]

class HandlerSpec:
    interface: str
    attributes: Dict[str, str]
    capacity: Dict[str, int]
    metadata: Dict[str, str]
    codec: HandlerCodec | str
    input_stream: str
    output_stream: str
    max_active_leases: int
    def __init__(
        self,
        interface: str,
        attributes: Optional[Dict[str, str]] = ...,
        capacity: Optional[Dict[str, int]] = ...,
        metadata: Optional[Dict[str, str]] = ...,
        codec: HandlerCodec | str = ...,
        input_stream: str = ...,
        output_stream: str = ...,
        max_active_leases: int = ...,
    ) -> None: ...

class WorkerHandler:
    spec: HandlerSpec
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...

def worker(
    *,
    interface: str,
    attributes: Optional[Dict[str, str]] = ...,
    capacity: Optional[Dict[str, int]] = ...,
    metadata: Optional[Dict[str, str]] = ...,
    codec: HandlerCodec | str = ...,
    input_stream: str = ...,
    output_stream: str = ...,
    max_active_leases: int = ...,
) -> Callable[[Callable[[Any], Any]], WorkerHandler]: ...

def get_handler_spec(handler: Callable[[Any], Any]) -> HandlerSpec: ...

def serve(
    handler: Callable[[Any], Any],
    endpoint: Optional[str] = ...,
    *,
    worker_id: Optional[str] = ...,
    heartbeat_interval_secs: float = ...,
    poll_interval_secs: float = ...,
    client: Any = ...,
) -> None: ...
