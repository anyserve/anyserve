from __future__ import annotations

import argparse
import math
import sys
import time
from dataclasses import dataclass, field
from threading import Event, Lock, Thread
from typing import Any

import cv2

from anyserve import FRAME_DATA

from video_stream_common import (
    FRAME_CONTENT_TYPE,
    INPUT_STREAM_NAME,
    OUTPUT_STREAM_NAME,
    VIDEO_INTERFACE,
    connect_anyserve_client,
    decode_jpeg,
    encode_jpeg,
    json_loads,
    now_ms,
    parse_source_arg,
)


@dataclass
class OutputState:
    frame: Any | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    latest_metrics: dict[str, float] = field(default_factory=dict)
    received_frames: int = 0
    output_closed: bool = False
    error: str | None = None
    latency_samples: dict[str, list[float]] = field(
        default_factory=lambda: {
            "to_worker_ms": [],
            "worker_total_ms": [],
            "inference_ms": [],
            "worker_overhead_ms": [],
            "to_client_ms": [],
            "e2e_ms": [],
        }
    )
    lock: Lock = field(default_factory=Lock)

    def update(self, frame: Any, metadata: dict[str, str]) -> None:
        received_at_ms = now_ms()
        metadata = dict(metadata)
        metadata["client_received_at_ms"] = str(received_at_ms)
        metrics = compute_latency_metrics(metadata)
        with self.lock:
            self.frame = frame
            self.metadata = metadata
            self.latest_metrics = metrics
            self.received_frames += 1
            for key, value in metrics.items():
                self.latency_samples[key].append(value)

    def mark_closed(self) -> None:
        with self.lock:
            self.output_closed = True

    def mark_error(self, message: str) -> None:
        with self.lock:
            self.error = message

    def snapshot(
        self,
    ) -> tuple[Any | None, dict[str, str], dict[str, float], int, bool, str | None]:
        with self.lock:
            frame = self.frame.copy() if self.frame is not None else None
            return (
                frame,
                dict(self.metadata),
                dict(self.latest_metrics),
                self.received_frames,
                self.output_closed,
                self.error,
            )

    def latency_summary(self) -> dict[str, dict[str, float]]:
        with self.lock:
            return {
                key: summarize_samples(values)
                for key, values in self.latency_samples.items()
                if values
            }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream every captured frame through Anyserve and display returned annotated frames.",
    )
    parser.add_argument(
        "--endpoint",
        default="http://127.0.0.1:50052",
        help="Anyserve gRPC endpoint",
    )
    parser.add_argument(
        "--source",
        default="0",
        help="Video source: webcam index like 0 or a local video path",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=1280,
        help="Requested webcam width when the source is a camera",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=720,
        help="Requested webcam height when the source is a camera",
    )
    parser.add_argument(
        "--jpeg-quality",
        type=int,
        default=80,
        help="JPEG quality for frames sent to the worker",
    )
    parser.add_argument(
        "--lease-ttl-secs",
        type=int,
        default=120,
        help="Lease TTL for the streaming job",
    )
    parser.add_argument(
        "--output-wait-timeout-secs",
        type=float,
        default=30.0,
        help="How long to wait for the worker to open the output stream",
    )
    parser.add_argument(
        "--max-frames",
        type=int,
        default=0,
        help="Optional frame limit for file playback; 0 means unlimited",
    )
    parser.add_argument(
        "--window-name",
        default="Anyserve Video Demo",
        help="OpenCV window title",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Skip OpenCV window display so the demo can run in headless environments",
    )
    parser.add_argument(
        "--connect-timeout-secs",
        type=float,
        default=30.0,
        help="How long to wait for the Anyserve control plane before failing",
    )
    return parser.parse_args()


def wait_for_output_stream(
    client: Any,
    job_id: str,
    timeout_secs: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_secs
    last_state: str | None = None
    last_attempt_state: str | None = None
    next_log_at = time.monotonic()
    print("waiting for worker to create output stream...")
    while True:
        streams = client.list_streams(job_id)
        stream = next((item for item in streams if item["stream_name"] == OUTPUT_STREAM_NAME), None)
        if stream is not None:
            return stream
        now = time.monotonic()
        if now >= next_log_at:
            job = client.get_job(job_id)
            if job["state"] != last_state:
                print(f"job state={job['state']}")
                last_state = job["state"]
            attempts = client.list_attempts(job_id)
            if attempts:
                latest_attempt = attempts[-1]
                if latest_attempt["state"] != last_attempt_state:
                    print(
                        "latest attempt="
                        f"{latest_attempt['attempt_id']} state={latest_attempt['state']}"
                    )
                    last_attempt_state = latest_attempt["state"]
            next_log_at = now + 1.0
            if job["state"] in {"failed", "cancelled"}:
                raise RuntimeError(
                    f"job became terminal before output stream was created: state={job['state']}"
                )
        if time.monotonic() >= deadline:
            job = client.get_job(job_id)
            attempts = client.list_attempts(job_id)
            latest_attempt_state = attempts[-1]["state"] if attempts else "none"
            raise TimeoutError(
                "timed out waiting for output stream. "
                f"job_state={job['state']} latest_attempt_state={latest_attempt_state}. "
                "Check the worker terminal and make sure it printed "
                "'registered worker_id=...' and then 'lease granted ...'."
            )
        time.sleep(0.05)


def output_reader_loop(
    endpoint: str,
    stream_id: str,
    state: OutputState,
    stop_event: Event,
    connect_timeout_secs: float,
) -> None:
    client = connect_anyserve_client(
        endpoint,
        timeout_secs=connect_timeout_secs,
        role="client output reader",
    )
    try:
        for frame in client.pull_frames(stream_id, follow=True):
            if stop_event.is_set():
                break
            if frame["kind"] != FRAME_DATA:
                continue
            state.update(decode_jpeg(frame["payload"]), frame["metadata"])
    except Exception as exc:
        state.mark_error(f"{exc.__class__.__name__}: {exc}")
        stop_event.set()
    finally:
        state.mark_closed()


def compute_latency_metrics(metadata: dict[str, str]) -> dict[str, float]:
    captured_at_ms = int(metadata["captured_at_ms"])
    worker_received_at_ms = int(metadata["worker_received_at_ms"])
    worker_sent_at_ms = int(metadata["worker_sent_at_ms"])
    client_received_at_ms = int(metadata["client_received_at_ms"])
    inference_ms = float(metadata["worker_inference_ms"])
    worker_total_ms = float(worker_sent_at_ms - worker_received_at_ms)
    return {
        "to_worker_ms": float(worker_received_at_ms - captured_at_ms),
        "worker_total_ms": worker_total_ms,
        "inference_ms": inference_ms,
        "worker_overhead_ms": max(0.0, worker_total_ms - inference_ms),
        "to_client_ms": float(client_received_at_ms - worker_sent_at_ms),
        "e2e_ms": float(client_received_at_ms - captured_at_ms),
        "added_by_worker_ms": worker_total_ms,
        "added_by_system_ms": max(
            0.0,
            float(worker_received_at_ms - captured_at_ms)
            + float(client_received_at_ms - worker_sent_at_ms)
            + max(0.0, worker_total_ms - inference_ms),
        ),
    }


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    index = max(0, min(len(values) - 1, math.ceil(q * len(values)) - 1))
    return values[index]


def summarize_samples(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        "avg": round(sum(ordered) / len(ordered), 2),
        "p50": round(percentile(ordered, 0.50), 2),
        "p95": round(percentile(ordered, 0.95), 2),
        "max": round(ordered[-1], 2),
    }


def format_metric(value: float | None) -> str:
    if value is None:
        return "?"
    return f"{value:.0f}ms"


def annotate_display(
    frame: Any,
    metadata: dict[str, str],
    metrics: dict[str, float],
    pushed_frames: int,
    received_frames: int,
) -> Any:
    annotated = frame.copy()
    frame_index = metadata.get("frame_index", "?")
    to_worker_ms = format_metric(metrics.get("to_worker_ms"))
    worker_total_ms = format_metric(metrics.get("worker_total_ms"))
    to_client_ms = format_metric(metrics.get("to_client_ms"))
    e2e_ms = format_metric(metrics.get("e2e_ms"))
    inference_ms = format_metric(metrics.get("inference_ms"))
    worker_overhead_ms = format_metric(metrics.get("worker_overhead_ms"))
    added_by_worker_ms = format_metric(metrics.get("added_by_worker_ms"))
    added_by_system_ms = format_metric(metrics.get("added_by_system_ms"))
    lines = [
        f"sent={pushed_frames} returned={received_frames}",
        f"frame={frame_index} added_by_worker={added_by_worker_ms}",
        f"added_by_system={added_by_system_ms} infer={inference_ms}",
        f"to_worker={to_worker_ms} worker={worker_total_ms} back={to_client_ms}",
        f"worker_overhead={worker_overhead_ms}",
        f"e2e={e2e_ms}",
        f"objects={metadata.get('detection_count', '?')}",
    ]
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
            (255, 255, 255),
            2,
            cv2.LINE_AA,
        )
    return annotated


def open_video_source(args: argparse.Namespace) -> cv2.VideoCapture:
    source = parse_source_arg(args.source)
    if isinstance(source, int) and sys.platform == "darwin":
        capture = cv2.VideoCapture(source, cv2.CAP_AVFOUNDATION)
    else:
        capture = cv2.VideoCapture(source)
    if not capture.isOpened():
        raise SystemExit(f"failed to open video source {args.source!r}")
    if isinstance(source, int):
        capture.set(cv2.CAP_PROP_FRAME_WIDTH, args.width)
        capture.set(cv2.CAP_PROP_FRAME_HEIGHT, args.height)
        capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        print(
            "opened camera "
            f"source={source} backend={capture.getBackendName()} "
            f"size={int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))}x{int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))}"
        )
    else:
        print(f"opened video source={args.source!r} backend={capture.getBackendName()}")
    return capture


def wait_for_terminal_job_state(client: Any, job_id: str, timeout_secs: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_secs
    while True:
        job = client.get_job(job_id)
        if job["state"] in {"succeeded", "failed", "cancelled"}:
            return job
        if time.monotonic() >= deadline:
            return job
        time.sleep(0.1)


def print_latency_summary(state: OutputState) -> None:
    summary = state.latency_summary()
    if not summary:
        return
    print("latency summary:")
    for key in (
        "added_by_worker_ms",
        "added_by_system_ms",
        "to_worker_ms",
        "worker_total_ms",
        "inference_ms",
        "worker_overhead_ms",
        "to_client_ms",
        "e2e_ms",
    ):
        stats = summary.get(key)
        if not stats:
            continue
        print(
            f"  {key}: avg={stats['avg']}ms p50={stats['p50']}ms "
            f"p95={stats['p95']}ms max={stats['max']}ms"
        )


def maybe_sleep_for_source_fps(capture: cv2.VideoCapture, next_deadline: float | None) -> float | None:
    fps = capture.get(cv2.CAP_PROP_FPS)
    if fps <= 0:
        return None
    if next_deadline is None:
        return time.monotonic()
    next_deadline += 1.0 / fps
    remaining = next_deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)
    return next_deadline


def main() -> None:
    args = parse_args()
    submitter = connect_anyserve_client(
        args.endpoint,
        timeout_secs=args.connect_timeout_secs,
        role="client",
    )
    output_state = OutputState()
    stop_event = Event()
    parsed_source = parse_source_arg(args.source)

    job = submitter.submit_job(
        interface_name=VIDEO_INTERFACE,
        required_attributes={"runtime": "python"},
        required_capacity={"slot": 1},
        metadata={"example": "video-stream-client", "source": args.source},
        lease_ttl_secs=args.lease_ttl_secs,
    )
    print(f"submitted job_id={job['job_id']}")

    input_stream = submitter.open_stream(
        job["job_id"],
        INPUT_STREAM_NAME,
        metadata={"content_type": FRAME_CONTENT_TYPE},
    )
    print(f"opened input stream_id={input_stream['stream_id']}")

    output_stream = wait_for_output_stream(submitter, job["job_id"], args.output_wait_timeout_secs)
    print(f"opened output stream_id={output_stream['stream_id']}")

    output_thread = Thread(
        target=output_reader_loop,
        args=(
            args.endpoint,
            output_stream["stream_id"],
            output_state,
            stop_event,
            args.connect_timeout_secs,
        ),
        daemon=True,
    )
    output_thread.start()

    capture = open_video_source(args)
    pushed_frames = 0
    next_deadline: float | None = None
    exit_reason = "stream_ended"
    if not args.headless:
        cv2.namedWindow(args.window_name, cv2.WINDOW_NORMAL)

    try:
        while not stop_event.is_set():
            ok, frame = capture.read()
            if not ok:
                exit_reason = "source_ended"
                if isinstance(parsed_source, int):
                    print(
                        "camera read returned no frame; exiting. "
                        "Check macOS camera permission for this terminal and make sure "
                        "no other app is holding the camera."
                    )
                else:
                    print(f"video source {args.source!r} returned end-of-stream")
                break
            if not isinstance(parsed_source, int):
                next_deadline = maybe_sleep_for_source_fps(capture, next_deadline)

            pushed_frames += 1
            captured_at_ms = now_ms()
            submitter.push_frames(
                input_stream["stream_id"],
                [
                    (
                        FRAME_DATA,
                        encode_jpeg(frame, args.jpeg_quality),
                        {
                            "frame_index": str(pushed_frames),
                            "captured_at_ms": str(captured_at_ms),
                            "content_type": FRAME_CONTENT_TYPE,
                        },
                    )
                ],
            )

            latest_frame, latest_metadata, latest_metrics, received_frames, _, error = output_state.snapshot()
            display_frame = latest_frame if latest_frame is not None else frame
            display_frame = annotate_display(
                display_frame,
                latest_metadata,
                latest_metrics,
                pushed_frames=pushed_frames,
                received_frames=received_frames,
            )
            if error:
                cv2.putText(
                    display_frame,
                    error,
                    (16, display_frame.shape[0] - 24),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.6,
                    (0, 0, 255),
                    2,
                    cv2.LINE_AA,
                )
            if not args.headless:
                cv2.imshow(args.window_name, display_frame)
                key = cv2.waitKey(1) & 0xFF
                if key == ord("q"):
                    exit_reason = "user_quit"
                    stop_event.set()
                    break
            if args.max_frames > 0 and pushed_frames >= args.max_frames:
                exit_reason = "frame_limit"
                break
    except KeyboardInterrupt:
        exit_reason = "keyboard_interrupt"
        print("interrupt received, closing input stream...")
    finally:
        capture.release()
        try:
            submitter.close_stream(input_stream["stream_id"], metadata={"pushed_frames": str(pushed_frames)})
        except Exception:
            pass

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            latest_frame, latest_metadata, latest_metrics, received_frames, output_closed, _ = output_state.snapshot()
            if latest_frame is not None and not args.headless:
                cv2.imshow(
                    args.window_name,
                    annotate_display(
                        latest_frame,
                        latest_metadata,
                        latest_metrics,
                        pushed_frames=pushed_frames,
                        received_frames=received_frames,
                    ),
                )
                cv2.waitKey(1)
            if output_closed:
                break
            time.sleep(0.05)

        stop_event.set()
        output_thread.join(timeout=2.0)
        final_job = wait_for_terminal_job_state(submitter, job["job_id"], timeout_secs=10.0)
        if final_job["state"] not in {"succeeded", "failed", "cancelled"}:
            print(
                "job did not terminate after input stream closed; "
                f"canceling job_id={job['job_id']} exit_reason={exit_reason}"
            )
            final_job = submitter.cancel_job(job["job_id"])
        snapshot = output_state.snapshot()
        detections = json_loads(snapshot[1].get("detections_json"), [])
        print(
            "final state="
            f"{final_job['state']} pushed_frames={pushed_frames} returned_frames={snapshot[3]} "
            f"last_detections={len(detections)}"
        )
        print_latency_summary(output_state)
        if not args.headless:
            cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
