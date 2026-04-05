from __future__ import annotations

import json
import time
from typing import Any

import cv2
import numpy as np
from anyserve import AnyserveClient

VIDEO_INTERFACE = "demo.video.detect.v1"
INPUT_STREAM_NAME = "input.video"
OUTPUT_STREAM_NAME = "output.video"
FRAME_CONTENT_TYPE = "image/jpeg"


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


def json_loads(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    return json.loads(raw)


def encode_jpeg(frame: np.ndarray, quality: int) -> bytes:
    ok, encoded = cv2.imencode(
        ".jpg",
        frame,
        [int(cv2.IMWRITE_JPEG_QUALITY), quality],
    )
    if not ok:
        raise RuntimeError("encode jpeg frame")
    return encoded.tobytes()


def decode_jpeg(payload: bytes) -> np.ndarray:
    frame = cv2.imdecode(np.frombuffer(payload, dtype=np.uint8), cv2.IMREAD_COLOR)
    if frame is None:
        raise RuntimeError("decode jpeg frame")
    return frame


def parse_source_arg(value: str) -> int | str:
    return int(value) if value.isdigit() else value


def connect_anyserve_client(
    endpoint: str,
    timeout_secs: float,
    role: str,
) -> AnyserveClient:
    deadline = time.monotonic() + timeout_secs
    last_error: Exception | None = None
    while True:
        try:
            return AnyserveClient(endpoint)
        except Exception as exc:
            last_error = exc
        if time.monotonic() >= deadline:
            message = (
                f"timed out connecting {role} to Anyserve at {endpoint}. "
                "Start the control plane first with "
                "`mise exec -- cargo run -p anyserve -- serve`."
            )
            raise RuntimeError(message) from last_error
        time.sleep(0.25)
