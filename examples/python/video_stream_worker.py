from __future__ import annotations
import argparse
import time

from anyserve import serve, worker
from video_stream_common import VIDEO_INTERFACE, assemble_inputs, decode_jpeg, encode_jpeg, now_ms, pack_frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Run YOLO on live JPEG inputs over one outbound Anyserve session.")
    parser.add_argument("--endpoint", default="http://127.0.0.1:50052")
    parser.add_argument("--worker-id")
    parser.add_argument("--model", default="yolo11n.pt")
    parser.add_argument("--device", default="auto")
    parser.add_argument("--tracker", help="Optional Ultralytics tracker configuration")
    parser.add_argument("--conf", type=float, default=0.25)
    parser.add_argument("--iou", type=float, default=0.45)
    parser.add_argument("--imgsz", type=int, default=640)
    parser.add_argument("--jpeg-quality", type=int, default=80)
    args = parser.parse_args()
    from ultralytics import YOLO
    import torch
    device = args.device
    if device == "auto":
        device = "cuda:0" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
    model = YOLO(args.model)

    @worker(interface=VIDEO_INTERFACE, capacity={"slot": 1}, attributes={"device": device},
            streaming_input=True, pass_context=True)
    def detect(chunks, context):
        for payload, metadata in assemble_inputs(chunks):
            if context.cancelled:
                return
            frame = decode_jpeg(payload)
            started = time.perf_counter()
            options = {"source": frame, "device": device, "conf": args.conf,
                       "iou": args.iou, "imgsz": args.imgsz, "verbose": False}
            result = model.track(persist=True, tracker=args.tracker, **options)[0] if args.tracker else model.predict(**options)[0]
            inference_ms = (time.perf_counter() - started) * 1000
            boxes = result.boxes
            info = {**metadata, "job_id": context.job_id, "worker_completed_at_ms": now_ms(),
                    "inference_ms": round(inference_ms, 2), "objects": len(boxes) if boxes is not None else 0}
            yield pack_frame(encode_jpeg(result.plot(), args.jpeg_quality), info)

    serve(detect, args.endpoint, worker_id=args.worker_id)


if __name__ == "__main__":
    main()
