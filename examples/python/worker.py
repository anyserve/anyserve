from __future__ import annotations
import argparse
from anyserve import serve, worker


@worker(interface="demo.echo.v1", max_active_leases=4)
def echo(payload: bytes) -> bytes:
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an outbound Anyserve echo worker.")
    parser.add_argument("--endpoint", default="http://127.0.0.1:50052")
    parser.add_argument("--worker-id")
    args = parser.parse_args()
    serve(echo, endpoint=args.endpoint, worker_id=args.worker_id)


if __name__ == "__main__":
    main()
