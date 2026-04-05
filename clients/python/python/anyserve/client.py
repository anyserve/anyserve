from __future__ import annotations

from ._native import AnyserveClient as _NativeAnyserveClient


class AnyserveClient:
    def __init__(self, endpoint: str) -> None:
        self._native = _NativeAnyserveClient(endpoint)

    def submitter(self) -> AnyserveClient:
        return self

    def worker(self) -> AnyserveClient:
        return self

    def __getattr__(self, name: str):
        return getattr(self._native, name)

    def __repr__(self) -> str:
        return repr(self._native)
