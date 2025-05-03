from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import any_pb2 as _any_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class InferCore(_message.Message):
    __slots__ = ("content", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    content: bytes
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, content: _Optional[bytes] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class InferRequest(_message.Message):
    __slots__ = ("infer",)
    INFER_FIELD_NUMBER: _ClassVar[int]
    infer: InferCore
    def __init__(self, infer: _Optional[_Union[InferCore, _Mapping]] = ...) -> None: ...

class InferResponse(_message.Message):
    __slots__ = ("request_id", "response")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    response: InferCore
    def __init__(self, request_id: _Optional[str] = ..., response: _Optional[_Union[InferCore, _Mapping]] = ...) -> None: ...

class FetchInferRequest(_message.Message):
    __slots__ = ("metadata",)
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    METADATA_FIELD_NUMBER: _ClassVar[int]
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class FetchInferResponse(_message.Message):
    __slots__ = ("request_id", "infer")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    INFER_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    infer: InferCore
    def __init__(self, request_id: _Optional[str] = ..., infer: _Optional[_Union[InferCore, _Mapping]] = ...) -> None: ...

class SendResponseRequest(_message.Message):
    __slots__ = ("request_id", "response", "metrics")
    class MetricsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    response: InferCore
    metrics: _containers.ScalarMap[str, str]
    def __init__(self, request_id: _Optional[str] = ..., response: _Optional[_Union[InferCore, _Mapping]] = ..., metrics: _Optional[_Mapping[str, str]] = ...) -> None: ...
