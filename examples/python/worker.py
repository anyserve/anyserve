from __future__ import annotations
import argparse
import signal
from anyserve import serve, worker


@worker(interface="demo.echo.v1", max_active_leases=4)
def echo(payload: bytes) -> bytes:
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an outbound Anyserve echo worker.")
    parser.add_argument("--endpoint", help="default: ANYSERVE_ENDPOINT or http://127.0.0.1:50052")
    parser.add_argument("--worker-id")
    args = parser.parse_args()
    # Container runtimes send SIGTERM; Python running as PID 1 needs a handler.
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        serve(echo, endpoint=args.endpoint, worker_id=args.worker_id)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
