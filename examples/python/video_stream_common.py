"""Application framing for the video demo; Anyserve transports opaque bytes."""
from __future__ import annotations
import json
import time
from typing import Any

VIDEO_INTERFACE = "demo.video.detect.v1"
FRAME_CONTENT_TYPE = "image/jpeg"
MAX_FRAME_BYTES = 16 * 1024 * 1024
MAX_HEADER_BYTES = 64 * 1024


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def encode_jpeg(frame: Any, quality: int) -> bytes:
    import cv2
    ok, encoded = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
    if not ok:
        raise RuntimeError("could not encode JPEG")
    return encoded.tobytes()


def decode_jpeg(payload: bytes) -> Any:
    import cv2
    import numpy as np
    frame = cv2.imdecode(np.frombuffer(payload, dtype=np.uint8), cv2.IMREAD_COLOR)
    if frame is None:
        raise RuntimeError("could not decode JPEG")
    return frame


def assemble_inputs(chunks):
    payload = bytearray()
    metadata = {}
    index = None
    for chunk in chunks:
        if chunk["parameter_chunk"]:
            continue
        if chunk["uri"]:
            raise ValueError("video input must contain JPEG bytes")
        if index is None:
            index = chunk["object_index"]
            metadata = chunk["metadata"]
        elif index != chunk["object_index"]:
            raise RuntimeError("input object ended without its final fragment")
        payload.extend(chunk["data"])
        if len(payload) > MAX_FRAME_BYTES:
            raise ValueError("video frame exceeds the inline budget")
        if chunk["object_end"]:
            yield bytes(payload), metadata
            payload.clear()
            metadata = {}
            index = None
    if index is not None:
        raise RuntimeError("video input ended in a partial frame")


def pack_frame(payload: bytes, metadata: dict[str, Any]) -> bytes:
    if len(payload) > MAX_FRAME_BYTES:
        raise ValueError("video frame exceeds the inline budget")
    header = json.dumps({"size": len(payload), "metadata": metadata}, separators=(",", ":")).encode()
    if len(header) > MAX_HEADER_BYTES:
        raise ValueError("frame metadata exceeds the budget")
    return len(header).to_bytes(4, "big") + header + payload


class FrameDecoder:
    """Incrementally decode records across arbitrary transport fragments."""
    def __init__(self) -> None:
        self.buffer = bytearray()
        self.header = None

    def feed(self, data: bytes):
        self.buffer.extend(data)
        if len(self.buffer) > MAX_FRAME_BYTES + MAX_HEADER_BYTES + 4:
            raise RuntimeError("video output exceeds the decode budget")
        frames = []
        while True:
            if self.header is None:
                if len(self.buffer) < 4:
                    break
                size = int.from_bytes(self.buffer[:4], "big")
                if size > MAX_HEADER_BYTES:
                    raise RuntimeError("invalid video header length")
                if len(self.buffer) < 4 + size:
                    break
                self.header = json.loads(self.buffer[4:4 + size])
                del self.buffer[:4 + size]
                if not 0 <= self.header["size"] <= MAX_FRAME_BYTES:
                    raise RuntimeError("invalid video payload length")
            size = self.header["size"]
            if len(self.buffer) < size:
                break
            frames.append((bytes(self.buffer[:size]), self.header["metadata"]))
            del self.buffer[:size]
            self.header = None
        return frames

    def finish(self) -> None:
        if self.buffer or self.header is not None:
            raise RuntimeError("video output ended in a partial frame")
