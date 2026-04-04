from ._native import AnyserveClient

RESPONSE_CREATED = "response.created"
RESPONSE_PROCESSING = "response.processing"
RESPONSE_FINISHED = "response.finished"
RESPONSE_FAILED = "response.failed"


def response_created(content: bytes = b"", metadata: dict[str, str] | None = None) -> tuple[bytes, dict[str, str]]:
    payload = dict(metadata or {})
    payload["@type"] = RESPONSE_CREATED
    return content, payload


def response_processing(
    content: bytes = b"",
    metadata: dict[str, str] | None = None,
) -> tuple[bytes, dict[str, str]]:
    payload = dict(metadata or {})
    payload["@type"] = RESPONSE_PROCESSING
    return content, payload


def response_finished(content: bytes = b"", metadata: dict[str, str] | None = None) -> tuple[bytes, dict[str, str]]:
    payload = dict(metadata or {})
    payload["@type"] = RESPONSE_FINISHED
    return content, payload


def response_failed(content: bytes = b"", metadata: dict[str, str] | None = None) -> tuple[bytes, dict[str, str]]:
    payload = dict(metadata or {})
    payload["@type"] = RESPONSE_FAILED
    return content, payload


__all__ = [
    "AnyserveClient",
    "RESPONSE_CREATED",
    "RESPONSE_PROCESSING",
    "RESPONSE_FINISHED",
    "RESPONSE_FAILED",
    "response_created",
    "response_processing",
    "response_finished",
    "response_failed",
]
