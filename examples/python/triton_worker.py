"""Serve one Triton model through anyserve.

The payload is a KServe v2 inference request (the JSON body Triton accepts on
``/v2/models/<name>/infer``) and the result is Triton's response body. The
worker does not interpret tensors, so any model works unchanged; anyserve adds
placement, deadlines and failover across hosts while Triton keeps model
loading, versions and dynamic batching.
"""
from __future__ import annotations
import argparse
import urllib.error
import urllib.request

from anyserve import WorkerError, serve, worker


def main() -> None:
    parser = argparse.ArgumentParser(description="Forward KServe v2 inference requests to a Triton model.")
    parser.add_argument("--endpoint", default="http://127.0.0.1:50052", help="anyserve control plane")
    parser.add_argument("--triton", default="http://127.0.0.1:8000", help="Triton HTTP endpoint")
    parser.add_argument("--model", required=True, help="Triton model name")
    parser.add_argument("--interface", help="default: triton/<model>")
    parser.add_argument("--concurrency", type=int, default=4, help="in-flight requests; match the model's instance count")
    parser.add_argument("--worker-id")
    args = parser.parse_args()
    model_url = f"{args.triton.rstrip('/')}/v2/models/{args.model}"
    with urllib.request.urlopen(f"{model_url}/ready", timeout=10) as response:
        if response.status != 200:
            raise SystemExit(f"model {args.model} is not ready: HTTP {response.status}")

    @worker(interface=args.interface or f"triton/{args.model}",
            attributes={"model": args.model}, max_active_leases=args.concurrency)
    def infer(payload: bytes) -> bytes:
        request = urllib.request.Request(f"{model_url}/infer", data=payload,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            # Triton rejected this request: running it again elsewhere would not help.
            raise WorkerError(f"triton returned HTTP {error.code}: {error.read().decode(errors='replace')[:200]}")
        except (urllib.error.URLError, TimeoutError) as error:
            # Triton itself is unreachable: another worker may still serve the job.
            raise WorkerError(f"triton unreachable: {error}", retryable=True)

    serve(infer, args.endpoint, worker_id=args.worker_id)


if __name__ == "__main__":
    main()
