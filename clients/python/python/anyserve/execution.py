"""Execution API for live inference and recoverable asynchronous jobs."""
from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from typing import Any, Iterator

from ._native import ExecutionTransport

DEFAULT_ENDPOINT = "http://127.0.0.1:50052"


def resolve_endpoint(endpoint: str | None) -> str:
    """The explicit endpoint, else ``ANYSERVE_ENDPOINT``, else the local default."""
    return endpoint or os.environ.get("ANYSERVE_ENDPOINT") or DEFAULT_ENDPOINT


@dataclass(frozen=True)
class ExecutionOptions:
    required_attributes: dict[str, str] = field(default_factory=dict)
    preferred_attributes: dict[str, str] = field(default_factory=dict)
    required_capacity: dict[str, int] = field(default_factory=dict)
    metadata: dict[str, str] = field(default_factory=dict)
    params: bytes = b""
    priority: int = 0
    queue_timeout_ms: int = 0
    execution_timeout_ms: int = 0
    total_timeout_ms: int = 0
    spillover_after_ms: int = 0
    max_attempts: int = 0


class ExecutionEvents(Iterator[dict[str, Any]]):
    """Use as a context manager when leaving a live response before its end."""

    def __init__(self, events: Any) -> None:
        self._events = events

    def __iter__(self) -> ExecutionEvents:
        return self

    def __next__(self) -> dict[str, Any]:
        return next(self._events)

    def close(self) -> None:
        self._events.close()

    def __enter__(self) -> ExecutionEvents:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class StreamingInvocation(ExecutionEvents):
    def __init__(self, invocation: Any) -> None:
        super().__init__(invocation.events)
        self._invocation = invocation

    def send(self, data: bytes, metadata: dict[str, str] | None = None) -> None:
        self._invocation.send(data, metadata)

    def finish_input(self) -> None:
        self._invocation.finish_input()

    def close(self) -> None:
        self._invocation.close()


class ExecutionClient:
    def __init__(self, endpoint: str | None = None, *, token: str | None = None) -> None:
        self._transport = ExecutionTransport(resolve_endpoint(endpoint), token)

    def submit(
        self, interface: str, inputs: bytes | list[dict[str, Any]] = b"", *,
        job_id: str | None = None, durability: str = "durable",
        options: ExecutionOptions | None = None,
    ) -> dict[str, Any]:
        """Return after acceptance. A durable acknowledgement includes saved input.

        Reusing a job_id returns the same job only when its specification matches.
        Idempotency lasts for the server's job retention period.
        """
        return self._transport.submit(_spec(interface, inputs, options, durability), job_id)

    def invoke(
        self, interface: str, inputs: bytes | list[dict[str, Any]] = b"", *,
        options: ExecutionOptions | None = None,
    ) -> ExecutionEvents:
        """Return live events. Closing the response cancels unfinished execution."""
        return ExecutionEvents(self._transport.invoke(_spec(interface, inputs, options, "ephemeral")))

    def invoke_stream(
        self, interface: str, *, options: ExecutionOptions | None = None,
    ) -> StreamingInvocation:
        native = self._transport.invoke_stream(_spec(interface, [], options, "ephemeral"))
        return StreamingInvocation(native)

    def execute(
        self, interface: str, inputs: bytes | list[dict[str, Any]] = b"", *,
        options: ExecutionOptions | None = None,
    ) -> list[dict[str, Any]]:
        with self.invoke(interface, inputs, options=options) as events:
            return _terminal_outputs(events)

    def call(
        self, interface: str, payload: bytes = b"", *, options: ExecutionOptions | None = None,
    ) -> bytes:
        """Run a bytes-in, bytes-out handler and return its one inline result.

        This is the client side of a ``@worker`` handler that returns bytes.
        Handlers that stream frames or return several objects need ``invoke``
        or ``execute``.
        """
        outputs = self.execute(interface, payload, options=options)
        if len(outputs) != 1 or "inline" not in outputs[0]:
            raise RuntimeError(
                f"{interface} returned {len(outputs)} result objects; call() expects one inline result"
            )
        return outputs[0]["inline"]

    def get(self, job_id: str, *, include_outputs: bool = True) -> dict[str, Any]:
        return self._transport.get(job_id, include_outputs)

    def cancel(self, job_id: str, reason: str = "caller cancelled") -> dict[str, Any]:
        return self._transport.cancel(job_id, reason)

    def watch(self, job_id: str, *, after_sequence: int = 0) -> ExecutionEvents:
        return ExecutionEvents(self._transport.watch(job_id, after_sequence))

    def result(self, job_id: str) -> list[dict[str, Any]]:
        job = self.get(job_id)
        if job["state"] in {"succeeded", "failed", "cancelled"}:
            return _outputs(job)
        with self.watch(job_id, after_sequence=0) as events:
            return _terminal_outputs(events)


def _spec(
    interface: str, inputs: bytes | list[dict[str, Any]],
    options: ExecutionOptions | None, durability: str,
) -> dict[str, Any]:
    if isinstance(inputs, bytes):
        inputs = [{"inline": inputs, "metadata": {}}]
    return {**asdict(options or ExecutionOptions()), "interface": interface,
            "inputs": inputs, "durability": durability}


def _terminal_outputs(events: Iterator[dict[str, Any]]) -> list[dict[str, Any]]:
    for event in events:
        if event["type"] == "finished":
            return _outputs(event)
    raise RuntimeError("execution ended without a terminal event")


def _outputs(result: dict[str, Any]) -> list[dict[str, Any]]:
    if result["state"] != "succeeded":
        info = result.get("error") or {}
        raise RuntimeError(f"{info.get('code', result['state'])}: {info.get('message', 'execution failed')}")
    return result.get("outputs", [])
