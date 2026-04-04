from __future__ import annotations

import argparse

from anyserve import serve, worker


@worker(
    interface="demo.echo.v1",
    attributes={"runtime": "python"},
    capacity={"slot": 1},
    codec="bytes",
)
def echo(payload: bytes) -> bytes:
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a demo Anyserve worker using the Python high-level API.",
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
        "--heartbeat-interval-secs",
        type=float,
        default=5,
        help="Seconds between heartbeats",
    )
    parser.add_argument(
        "--poll-interval-secs",
        type=float,
        default=1,
        help="Seconds between empty lease polls",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(f"starting python demo worker on {args.endpoint}")
    serve(
        echo,
        endpoint=args.endpoint,
        worker_id=args.worker_id,
        heartbeat_interval_secs=args.heartbeat_interval_secs,
        poll_interval_secs=args.poll_interval_secs,
    )


if __name__ == "__main__":
    main()
