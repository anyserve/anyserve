from typing import Dict, List, Optional, Tuple, TypedDict

RESPONSE_CREATED: str
RESPONSE_PROCESSING: str
RESPONSE_FINISHED: str
RESPONSE_FAILED: str

class InferChunk(TypedDict):
    request_id: str
    content: bytes
    metadata: Dict[str, str]

class FetchedRequest(TypedDict):
    request_id: str
    content: bytes
    metadata: Dict[str, str]

class AnyserveClient:
    def __init__(self, endpoint: str) -> None: ...
    def infer(
        self,
        content: bytes,
        metadata: Optional[Dict[str, str]] = ...,
        queue: Optional[str] = ...,
        request_id: Optional[str] = ...,
    ) -> List[InferChunk]: ...
    def fetch_one(
        self,
        metadata: Optional[Dict[str, str]] = ...,
        queue: Optional[str] = ...,
    ) -> Optional[FetchedRequest]: ...
    def send_responses(
        self,
        request_id: str,
        responses: List[Tuple[bytes, Dict[str, str]]],
    ) -> None: ...

def response_created(
    content: bytes = ...,
    metadata: Optional[Dict[str, str]] = ...,
) -> Tuple[bytes, Dict[str, str]]: ...
def response_processing(
    content: bytes = ...,
    metadata: Optional[Dict[str, str]] = ...,
) -> Tuple[bytes, Dict[str, str]]: ...
def response_finished(
    content: bytes = ...,
    metadata: Optional[Dict[str, str]] = ...,
) -> Tuple[bytes, Dict[str, str]]: ...
def response_failed(
    content: bytes = ...,
    metadata: Optional[Dict[str, str]] = ...,
) -> Tuple[bytes, Dict[str, str]]: ...
