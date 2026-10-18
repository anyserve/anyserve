from __future__ import annotations
import argparse

from anyserve import AnyserveClient, ExecutionOptions
from video_stream_common import VIDEO_INTERFACE, FrameDecoder, decode_jpeg, encode_jpeg, now_ms


def main() -> None:
    parser = argparse.ArgumentParser(description="Send live JPEG frames and render results before InputEnd.")
    parser.add_argument("--endpoint", default="http://127.0.0.1:50052")
    parser.add_argument("--source", default="0")
    parser.add_argument("--width", type=int, default=1280)
    parser.add_argument("--height", type=int, default=720)
    parser.add_argument("--jpeg-quality", type=int, default=80)
    parser.add_argument("--max-frames", type=int, default=120, help="0 continues until the source ends or the window is closed")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()
    import cv2
    source = int(args.source) if args.source.isdigit() else args.source
    capture = cv2.VideoCapture(source)
    if not capture.isOpened():
        raise SystemExit(f"could not open {args.source!r}")
    capture.set(cv2.CAP_PROP_FRAME_WIDTH, args.width)
    capture.set(cv2.CAP_PROP_FRAME_HEIGHT, args.height)
    client = AnyserveClient(args.endpoint)
    index = 0
    ended = False
    preferred_worker = None
    try:
        while not ended and (args.max_frames == 0 or index < args.max_frames):
            options = ExecutionOptions(required_capacity={"slot": 1}, max_attempts=1,
                queue_timeout_ms=5_000, total_timeout_ms=120_000,
                preferred_attributes={"worker_id": preferred_worker} if preferred_worker else {}, spillover_after_ms=100)
            decoder = FrameDecoder()
            # Rotate before the 32MiB input replay budget; camera lifetime is
            # independent of one bounded execution's input history.
            with client.invoke_stream(VIDEO_INTERFACE, options=options) as execution:
                sent = 0
                for _ in range(120):
                    if args.max_frames and index >= args.max_frames:
                        ended = True
                        break
                    ok, frame = capture.read()
                    if not ok:
                        ended = True
                        break
                    payload = encode_jpeg(frame, args.jpeg_quality)
                    if len(payload) > 16 * 1024 * 1024:
                        raise ValueError("encoded frame exceeds the inline object budget")
                    if sent + len(payload) > 24 * 1024 * 1024:
                        # This captured frame is skipped at the batch boundary.
                        break
                    execution.send(payload, {"frame_index": str(index), "captured_at_ms": str(now_ms())})
                    sent += len(payload)
                    received = False
                    while not received:
                        event = next(execution)
                        if event["type"] == "assigned":
                            preferred_worker = event["worker_id"]
                        elif event["type"] == "output":
                            for image, metadata in decoder.feed(event["data"]):
                                latency = now_ms() - int(metadata["captured_at_ms"])
                                print(f"frame={metadata['frame_index']} e2e={latency}ms inference={metadata['inference_ms']}ms objects={metadata['objects']}")
                                if not args.headless:
                                    cv2.imshow("Anyserve video", decode_jpeg(image))
                                    ended = cv2.waitKey(1) & 0xFF == ord("q")
                                received = True
                        elif event["type"] == "finished":
                            raise RuntimeError(f"execution ended before its frame response: {event}")
                    index += 1
                    if ended:
                        break
                execution.finish_input()
                for event in execution:
                    if event["type"] == "output":
                        decoder.feed(event["data"])
                    elif event["type"] == "finished" and event["state"] != "succeeded":
                        raise RuntimeError(str(event.get("error")))
                decoder.finish()
    finally:
        capture.release()
        if not args.headless:
            cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
