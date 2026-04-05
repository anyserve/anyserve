from typing import Any, Callable, Dict, Iterator, List, Literal, Optional, Tuple, TypedDict

EventKind = Literal[
    "accepted",
    "lease_granted",
    "started",
    "progress",
    "output_ready",
    "succeeded",
    "failed",
    "cancelled",
    "lease_expired",
    "requeued",
]

StreamScope = Literal["job", "attempt", "lease"]
StreamDirection = Literal[
    "client_to_worker",
    "worker_to_client",
    "bidirectional",
    "internal",
]
FrameKind = Literal["open", "data", "close", "error", "checkpoint", "control"]
HandlerCodec = Literal["bytes", "json"]

EVENT_ACCEPTED: str
EVENT_LEASE_GRANTED: str
EVENT_STARTED: str
EVENT_PROGRESS: str
EVENT_OUTPUT_READY: str
EVENT_SUCCEEDED: str
EVENT_FAILED: str
EVENT_CANCELLED: str
EVENT_LEASE_EXPIRED: str
EVENT_REQUEUED: str

STREAM_SCOPE_JOB: str
STREAM_SCOPE_ATTEMPT: str
STREAM_SCOPE_LEASE: str

STREAM_DIRECTION_CLIENT_TO_WORKER: str
STREAM_DIRECTION_WORKER_TO_CLIENT: str
STREAM_DIRECTION_BIDIRECTIONAL: str
STREAM_DIRECTION_INTERNAL: str

FRAME_OPEN: str
FRAME_DATA: str
FRAME_CLOSE: str
FRAME_ERROR: str
FRAME_CHECKPOINT: str
FRAME_CONTROL: str

class ObjectRef(TypedDict, total=False):
    inline: bytes
    uri: str
    metadata: Dict[str, str]

class JobEvent(TypedDict):
    job_id: str
    sequence: int
    kind: EventKind | str
    payload: bytes
    metadata: Dict[str, str]
    created_at_ms: int

class JobSpecRecord(TypedDict):
    interface_name: str
    inputs: List[ObjectRef]
    params: bytes
    required_attributes: Dict[str, str]
    preferred_attributes: Dict[str, str]
    required_capacity: Dict[str, int]
    policy: Dict[str, object]
    metadata: Dict[str, str]

class JobRecord(TypedDict):
    job_id: str
    state: str
    spec: JobSpecRecord
    outputs: List[ObjectRef]
    lease_id: Optional[str]
    version: int
    created_at_ms: int
    updated_at_ms: int
    last_error: Optional[str]
    current_attempt_id: Optional[str]

class AttemptRecord(TypedDict):
    attempt_id: str
    job_id: str
    worker_id: str
    lease_id: str
    state: str
    created_at_ms: int
    started_at_ms: Optional[int]
    finished_at_ms: Optional[int]
    last_error: Optional[str]
    metadata: Dict[str, str]

class WorkerSpecRecord(TypedDict):
    interfaces: List[str]
    attributes: Dict[str, str]
    total_capacity: Dict[str, int]
    max_active_leases: int
    metadata: Dict[str, str]

class WorkerStatusRecord(TypedDict):
    available_capacity: Dict[str, int]
    active_leases: int
    metadata: Dict[str, str]
    last_seen_at_ms: int

class WorkerRecord(TypedDict):
    worker_id: str
    registered_at_ms: int
    expires_at_ms: int
    spec: WorkerSpecRecord
    status: WorkerStatusRecord

class LeaseRecord(TypedDict):
    lease_id: str
    job_id: str
    worker_id: str
    issued_at_ms: int
    expires_at_ms: int

class LeaseGrant(TypedDict):
    lease: LeaseRecord
    attempt: AttemptRecord
    job: JobRecord

class StreamRecord(TypedDict):
    stream_id: str
    job_id: str
    attempt_id: Optional[str]
    lease_id: Optional[str]
    stream_name: str
    scope: StreamScope | str
    direction: StreamDirection | str
    state: str
    metadata: Dict[str, str]
    created_at_ms: int
    closed_at_ms: Optional[int]
    last_sequence: int

class Frame(TypedDict):
    stream_id: str
    sequence: int
    kind: FrameKind | str
    payload: bytes
    metadata: Dict[str, str]
    created_at_ms: int

class PushSummary(TypedDict):
    stream: Optional[StreamRecord]
    last_sequence: int
    written_frames: int

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

class AnyserveClient:
    def __init__(self, endpoint: str) -> None: ...
    def submitter(self) -> AnyserveClient: ...
    def worker(self) -> AnyserveClient: ...
    def submit_job(
        self,
        interface_name: str,
        inputs: Optional[List[ObjectRef]] = ...,
        params: Optional[bytes] = ...,
        required_attributes: Optional[Dict[str, str]] = ...,
        preferred_attributes: Optional[Dict[str, str]] = ...,
        required_capacity: Optional[Dict[str, int]] = ...,
        metadata: Optional[Dict[str, str]] = ...,
        profile: Optional[str] = ...,
        priority: int = ...,
        lease_ttl_secs: Optional[int] = ...,
        job_id: Optional[str] = ...,
    ) -> JobRecord: ...
    def watch_job(self, job_id: str, after_sequence: int = ...) -> Iterator[JobEvent]: ...
    def list_jobs(self) -> List[JobRecord]: ...
    def get_job(self, job_id: str) -> JobRecord: ...
    def cancel_job(self, job_id: str) -> JobRecord: ...
    def get_attempt(self, attempt_id: str) -> AttemptRecord: ...
    def list_attempts(self, job_id: str) -> List[AttemptRecord]: ...
    def register_worker(
        self,
        interfaces: List[str],
        attributes: Optional[Dict[str, str]] = ...,
        total_capacity: Optional[Dict[str, int]] = ...,
        max_active_leases: int = ...,
        metadata: Optional[Dict[str, str]] = ...,
        worker_id: Optional[str] = ...,
    ) -> WorkerRecord: ...
    def heartbeat_worker(
        self,
        worker_id: str,
        available_capacity: Optional[Dict[str, int]] = ...,
        active_leases: int = ...,
        metadata: Optional[Dict[str, str]] = ...,
    ) -> WorkerRecord: ...
    def poll_lease(self, worker_id: str) -> Optional[LeaseGrant]: ...
    def renew_lease(self, worker_id: str, lease_id: str) -> LeaseRecord: ...
    def report_event(
        self,
        worker_id: str,
        lease_id: str,
        kind: EventKind | str,
        payload: Optional[bytes] = ...,
        metadata: Optional[Dict[str, str]] = ...,
    ) -> None: ...
    def complete_lease(
        self,
        worker_id: str,
        lease_id: str,
        outputs: Optional[List[ObjectRef]] = ...,
        metadata: Optional[Dict[str, str]] = ...,
    ) -> None: ...
    def fail_lease(
        self,
        worker_id: str,
        lease_id: str,
        reason: str,
        retryable: bool = ...,
        metadata: Optional[Dict[str, str]] = ...,
    ) -> None: ...
    def open_stream(
        self,
        job_id: str,
        stream_name: str,
        scope: StreamScope | str = ...,
        direction: StreamDirection | str = ...,
        metadata: Optional[Dict[str, str]] = ...,
        attempt_id: Optional[str] = ...,
        worker_id: Optional[str] = ...,
        lease_id: Optional[str] = ...,
    ) -> StreamRecord: ...
    def get_stream(self, stream_id: str) -> StreamRecord: ...
    def list_streams(self, job_id: str) -> List[StreamRecord]: ...
    def close_stream(
        self,
        stream_id: str,
        worker_id: Optional[str] = ...,
        lease_id: Optional[str] = ...,
        metadata: Optional[Dict[str, str]] = ...,
    ) -> StreamRecord: ...
    def push_frames(
        self,
        stream_id: str,
        frames: List[Tuple[FrameKind | str, bytes, Dict[str, str]]],
        worker_id: Optional[str] = ...,
        lease_id: Optional[str] = ...,
    ) -> PushSummary: ...
    def pull_frames(
        self,
        stream_id: str,
        after_sequence: int = ...,
        follow: bool = ...,
    ) -> Iterator[Frame]: ...

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
def inline_object(content: bytes, metadata: Optional[Dict[str, str]] = ...) -> ObjectRef: ...
def uri_object(uri: str, metadata: Optional[Dict[str, str]] = ...) -> ObjectRef: ...
