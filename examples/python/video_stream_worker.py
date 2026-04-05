from __future__ import annotations

import argparse
import time
from dataclasses import dataclass, field
from threading import Event, Lock, Thread
from typing import Any

import cv2
from ultralytics import YOLO

from anyserve import AnyserveClient, EVENT_OUTPUT_READY, EVENT_PROGRESS, EVENT_STARTED, FRAME_DATA

from video_stream_common import (
    FRAME_CONTENT_TYPE,
    INPUT_STREAM_NAME,
    OUTPUT_STREAM_NAME,
    VIDEO_INTERFACE,
    connect_anyserve_client,
    decode_jpeg,
    encode_jpeg,
    json_dumps,
    now_ms,
)

try:
    import torch
except ImportError:  # pragma: no cover - dependency is optional until the demo is used.
    torch = None


@dataclass
class LeaseState:
    active_lease_id: str | None = None
    active_leases: int = 0
    lock: Lock = field(default_factory=Lock)

    def start(self, lease_id: str) -> None:
        with self.lock:
            self.active_lease_id = lease_id
            self.active_leases += 1

    def finish(self, lease_id: str) -> None:
        with self.lock:
            if self.active_lease_id == lease_id:
                self.active_lease_id = None
            if self.active_leases > 0:
                self.active_leases -= 1

    def snapshot(self) -> tuple[str | None, int]:
        with self.lock:
            return self.active_lease_id, self.active_leases


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a real-time frame-by-frame video worker over Anyserve streams.",
    )
    parser.add_argument(
        "--endpoint",
        default="http://127.0.0.1:50052",
        help="Anyserve gRPC endpoint",
    )
    parser.add_argument(
        "--worker-id",
        default=None,
        help="Optional worker ID to reuse",
    )
    parser.add_argument(
        "--model",
        default="yolo11n.pt",
        help="Ultralytics model name or local path",
    )
    parser.add_argument(
        "--device",
        default="auto",
        help="Inference device: auto, mps, or cpu",
    )
    parser.add_argument(
        "--tracker",
        default=None,
        help="Optional Ultralytics tracker config such as bytetrack.yaml",
    )
    parser.add_argument(
        "--conf",
        type=float,
        default=0.25,
        help="Confidence threshold",
    )
    parser.add_argument(
        "--iou",
        type=float,
        default=0.45,
        help="IoU threshold",
    )
    parser.add_argument(
        "--imgsz",
        type=int,
        default=640,
        help="Inference image size",
    )
    parser.add_argument(
        "--jpeg-quality",
        type=int,
        default=80,
        help="JPEG quality for returned annotated frames",
    )
    parser.add_argument(
        "--heartbeat-interval-secs",
        type=float,
        default=5.0,
        help="Seconds between heartbeats and lease renewals",
    )
    parser.add_argument(
        "--poll-interval-secs",
        type=float,
        default=0.25,
        help="Seconds between empty lease polls",
    )
    parser.add_argument(
        "--stream-wait-timeout-secs",
        type=float,
        default=30.0,
        help="How long to wait for the submitter to create the input stream",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=30,
        help="Emit a progress event every N processed frames; 0 disables it",
    )
    parser.add_argument(
        "--connect-timeout-secs",
        type=float,
        default=30.0,
        help="How long to wait for the Anyserve control plane before failing",
    )
    parser.add_argument(
        "--input-poll-interval-secs",
        type=float,
        default=0.02,
        help="Polling interval while waiting for new input frames",
    )
    return parser.parse_args()


def resolve_device(requested: str) -> str:
    if requested != "auto":
        return requested
    if (
        torch is not None
        and hasattr(torch.backends, "mps")
        and torch.backends.mps.is_available()
    ):
        return "mps"
    return "cpu"


def available_capacity(active_leases: int) -> dict[str, int]:
    return {"slot": 1 if active_leases <= 0 else 0}


def heartbeat_loop(
    endpoint: str,
    worker_id: str,
    state: LeaseState,
    stop_event: Event,
    interval_secs: float,
    metadata: dict[str, str],
    connect_timeout_secs: float,
) -> None:
    client = connect_anyserve_client(
        endpoint,
        timeout_secs=connect_timeout_secs,
        role="worker heartbeat",
    )
    while not stop_event.wait(interval_secs):
        active_lease_id, active_leases = state.snapshot()
        try:
            client.heartbeat_worker(
                worker_id,
                available_capacity=available_capacity(active_leases),
                active_leases=active_leases,
                metadata=metadata,
            )
        except Exception:
            pass

        if active_lease_id is None:
            continue

        try:
            client.renew_lease(worker_id, active_lease_id)
        except Exception:
            pass


def wait_for_stream(
    worker_client: Any,
    job_id: str,
    stream_name: str,
    timeout_secs: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_secs
    while True:
        streams = worker_client.list_streams(job_id)
        stream = next((item for item in streams if item["stream_name"] == stream_name), None)
        if stream is not None:
            return stream
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out waiting for stream {stream_name!r}")
        time.sleep(0.05)


def extract_detections(result: Any, limit: int = 32) -> list[dict[str, Any]]:
    boxes = result.boxes
    if boxes is None or len(boxes) == 0:
        return []

    xyxy = boxes.xyxy.cpu().tolist()
    cls_ids = boxes.cls.cpu().tolist() if boxes.cls is not None else []
    confidences = boxes.conf.cpu().tolist() if boxes.conf is not None else []
    track_ids = (
        boxes.id.int().cpu().tolist()
        if getattr(boxes, "id", None) is not None
        else [None] * len(xyxy)
    )
    names = result.names

    detections: list[dict[str, Any]] = []
    for index, box in enumerate(xyxy[:limit]):
        class_id = int(cls_ids[index]) if index < len(cls_ids) else -1
        if isinstance(names, dict):
            class_name = names.get(class_id, str(class_id))
        elif isinstance(names, list) and 0 <= class_id < len(names):
            class_name = str(names[class_id])
        else:
            class_name = str(class_id)
        detections.append(
            {
                "track_id": track_ids[index] if index < len(track_ids) else None,
                "class_id": class_id,
                "class_name": class_name,
                "confidence": round(float(confidences[index]), 4)
                if index < len(confidences)
                else None,
                "xyxy": [round(float(value), 2) for value in box],
            }
        )
    return detections


def annotate_frame(
    frame: Any,
    frame_index: int,
    inference_ms: float,
    e2e_ms: int | None,
    detection_count: int,
) -> Any:
    annotated = frame.copy()
    lines = [
        f"frame={frame_index}",
        f"infer={inference_ms:.1f}ms",
        f"objects={detection_count}",
    ]
    if e2e_ms is not None:
        lines.append(f"worker_e2e={e2e_ms}ms")

    for index, text in enumerate(lines):
        cv2.putText(
            annotated,
            text,
            (16, 28 + index * 24),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (0, 0, 0),
            3,
            cv2.LINE_AA,
        )
        cv2.putText(
            annotated,
            text,
            (16, 28 + index * 24),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.7,
            (40, 255, 40),
            2,
            cv2.LINE_AA,
        )
    return annotated


def process_grant(
    worker_client: Any,
    model: YOLO,
    worker_id: str,
    grant: dict[str, Any],
    args: argparse.Namespace,
    stop_event: Event,
) -> None:
    lease = grant["lease"]
    attempt = grant["attempt"]
    job = grant["job"]
    lease_id = lease["lease_id"]

    worker_client.report_event(worker_id, lease_id, EVENT_STARTED)
    output_stream = worker_client.open_stream(
        job["job_id"],
        OUTPUT_STREAM_NAME,
        direction="worker_to_client",
        metadata={"content_type": FRAME_CONTENT_TYPE},
        attempt_id=attempt["attempt_id"],
        worker_id=worker_id,
        lease_id=lease_id,
    )
    worker_client.report_event(
        worker_id,
        lease_id,
        EVENT_OUTPUT_READY,
        metadata={"stream_name": OUTPUT_STREAM_NAME},
    )
    print(
        "opened output stream "
        f"stream_id={output_stream['stream_id']} for job_id={job['job_id']}"
    )

    processed_frames = 0
    try:
        print(f"waiting for input stream {INPUT_STREAM_NAME!r}")
        input_stream = wait_for_stream(
            worker_client,
            job["job_id"],
            INPUT_STREAM_NAME,
            args.stream_wait_timeout_secs,
        )
        print(f"input stream ready stream_id={input_stream['stream_id']}")

        after_sequence = 0
        seen_first_frame = False
        while True:
            if stop_event.is_set():
                raise KeyboardInterrupt

            frames = worker_client.pull_frames(
                input_stream["stream_id"],
                after_sequence=after_sequence,
                follow=False,
            )
            frame_batch = list(frames)
            if not frame_batch:
                stream_state = worker_client.get_stream(input_stream["stream_id"])["state"]
                if stream_state in {"closed", "error"}:
                    break
                time.sleep(args.input_poll_interval_secs)
                continue

            for frame in frame_batch:
                after_sequence = max(after_sequence, int(frame["sequence"]))
                if frame["kind"] != FRAME_DATA:
                    continue

                frame_index = int(frame["metadata"].get("frame_index", "0"))
                captured_at_ms = frame["metadata"].get("captured_at_ms")
                worker_received_at_ms = now_ms()
                original = decode_jpeg(frame["payload"])
                if not seen_first_frame:
                    print(f"processing first frame frame_index={frame_index}")
                    seen_first_frame = True

                started = time.perf_counter()
                track_kwargs = {
                    "persist": True,
                    "verbose": False,
                    "conf": args.conf,
                    "iou": args.iou,
                    "imgsz": args.imgsz,
                    "device": args.device,
                }
                if args.tracker:
                    track_kwargs["tracker"] = args.tracker
                result = model.track(original, **track_kwargs)[0]
                inference_ms = (time.perf_counter() - started) * 1000.0

                detections = extract_detections(result)
                worker_sent_at_ms = now_ms()
                worker_e2e_ms = (
                    worker_sent_at_ms - int(captured_at_ms)
                    if captured_at_ms is not None
                    else None
                )
                annotated = annotate_frame(
                    result.plot(),
                    frame_index=frame_index,
                    inference_ms=inference_ms,
                    e2e_ms=worker_e2e_ms,
                    detection_count=len(detections),
                )
                encoded = encode_jpeg(annotated, args.jpeg_quality)

                worker_client.push_frames(
                    output_stream["stream_id"],
                    [
                        (
                            FRAME_DATA,
                            encoded,
                            {
                                "frame_index": str(frame_index),
                                "captured_at_ms": str(captured_at_ms or ""),
                                "worker_received_at_ms": str(worker_received_at_ms),
                                "worker_sent_at_ms": str(worker_sent_at_ms),
                                "worker_inference_ms": f"{inference_ms:.2f}",
                                "detection_count": str(len(detections)),
                                "content_type": FRAME_CONTENT_TYPE,
                                "detections_json": json_dumps(detections),
                            },
                        )
                    ],
                    worker_id=worker_id,
                    lease_id=lease_id,
                )
                processed_frames += 1
                if args.progress_every > 0 and processed_frames % args.progress_every == 0:
                    print(f"processed_frames={processed_frames}")
                    worker_client.report_event(
                        worker_id,
                        lease_id,
                        EVENT_PROGRESS,
                        payload=f"processed {processed_frames} frames".encode("utf-8"),
                        metadata={"processed_frames": str(processed_frames)},
                    )

        print(f"input stream drained processed_frames={processed_frames}")
        worker_client.close_stream(
            output_stream["stream_id"],
            worker_id=worker_id,
            lease_id=lease_id,
            metadata={"processed_frames": str(processed_frames)},
        )
        worker_client.complete_lease(
            worker_id,
            lease_id,
            metadata={"processed_frames": str(processed_frames)},
        )
    except KeyboardInterrupt:
        print("interrupt received, failing active lease so the job can be retried")
        worker_client.fail_lease(
            worker_id,
            lease_id,
            "KeyboardInterrupt: worker interrupted",
            retryable=True,
            metadata={"processed_frames": str(processed_frames)},
        )
        raise
    except Exception as exc:
        try:
            worker_client.fail_lease(
                worker_id,
                lease_id,
                f"{exc.__class__.__name__}: {exc}",
                retryable=False,
                metadata={"processed_frames": str(processed_frames)},
            )
        finally:
            raise


def main() -> None:
    args = parse_args()
    args.device = resolve_device(args.device)

    print(f"loading model={args.model} device={args.device}")
    model = YOLO(args.model)
    worker_client = connect_anyserve_client(
        args.endpoint,
        timeout_secs=args.connect_timeout_secs,
        role="worker",
    )
    registration = worker_client.register_worker(
        interfaces=[VIDEO_INTERFACE],
        attributes={"runtime": "python", "device": args.device},
        total_capacity={"slot": 1},
        max_active_leases=1,
        metadata={"example": "video-stream-worker", "device": args.device},
        worker_id=args.worker_id,
    )
    worker_id = registration["worker_id"]
    print(f"registered worker_id={worker_id} endpoint={args.endpoint}")

    state = LeaseState()
    stop_event = Event()
    heartbeat = Thread(
        target=heartbeat_loop,
        args=(
            args.endpoint,
            worker_id,
            state,
            stop_event,
            args.heartbeat_interval_secs,
            {"example": "video-stream-worker", "device": args.device},
            args.connect_timeout_secs,
        ),
        daemon=True,
    )
    heartbeat.start()

    try:
        while True:
            grant = worker_client.poll_lease(worker_id)
            if grant is None:
                time.sleep(args.poll_interval_secs)
                continue

            lease_id = grant["lease"]["lease_id"]
            state.start(lease_id)
            try:
                print(f"lease granted job_id={grant['job']['job_id']} lease_id={lease_id}")
                process_grant(worker_client, model, worker_id, grant, args, stop_event)
                print(f"lease completed lease_id={lease_id}")
            finally:
                state.finish(lease_id)
    except KeyboardInterrupt:
        print("stopping worker")
    finally:
        stop_event.set()
        heartbeat.join(timeout=1.0)


if __name__ == "__main__":
    main()
