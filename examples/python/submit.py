from __future__ import annotations

import argparse

from anyserve import AnyserveClient, FRAME_DATA


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit a demo Anyserve job using the Python low-level SDK.",
    )
    parser.add_argument(
        "--endpoint",
        default="http://127.0.0.1:50052",
        help="Anyserve gRPC endpoint",
    )
    parser.add_argument(
        "--job-id",
        default=None,
        help="Optional job ID to reuse",
    )
    parser.add_argument(
        "--payload",
        default="hello from python example",
        help="UTF-8 payload to send to the worker",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = AnyserveClient(args.endpoint)
    payload = args.payload.encode("utf-8")

    job = client.submit_job(
        interface_name="demo.echo.v1",
        required_attributes={"runtime": "python"},
        required_capacity={"slot": 1},
        metadata={"example": "python"},
        job_id=args.job_id,
    )
    print(f"submitted job {job['job_id']}")

    stream = client.open_stream(job["job_id"], "input.default")
    client.push_frames(stream["stream_id"], [(FRAME_DATA, payload, {})])
    client.close_stream(stream["stream_id"])

    for event in client.watch_job(job["job_id"]):
        print(f"job event seq={event['sequence']} kind={event['kind']}")

    streams = client.list_streams(job["job_id"])
    output_stream = next(
        item for item in streams if item["stream_name"] == "output.default"
    )
    output_payload = b"".join(
        frame["payload"]
        for frame in client.pull_frames(output_stream["stream_id"], follow=False)
        if frame["kind"] == "data"
    )
    final_job = client.get_job(job["job_id"])

    print(f"output payload={output_payload.decode('utf-8', errors='replace')}")
    print(f"final job state={final_job['state']}")


if __name__ == "__main__":
    main()
