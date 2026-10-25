from __future__ import annotations
import argparse
from anyserve import AnyserveClient, ExecutionOptions


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit a recoverable job with complete input.")
    parser.add_argument("--endpoint", help="default: ANYSERVE_ENDPOINT or http://127.0.0.1:50052")
    parser.add_argument("--job-id")
    parser.add_argument("--payload", default="hello from Python")
    parser.add_argument("--ephemeral", action="store_true")
    args = parser.parse_args()
    client = AnyserveClient(args.endpoint)
    job = client.submit("demo.echo.v1", args.payload.encode(), job_id=args.job_id,
                        durability="ephemeral" if args.ephemeral else "durable",
                        options=ExecutionOptions(total_timeout_ms=30_000))
    print(f"accepted {job['job_id']} ({job['durability']})")
    for output in client.result(job["job_id"]):
        if "inline" in output:
            print(output["inline"].decode("utf-8", errors="replace"))


if __name__ == "__main__":
    main()
