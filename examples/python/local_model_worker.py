"""Backend-independent outbound worker that forwards payloads to a local HTTP model server ($MODEL_URL)."""
from __future__ import annotations
import os
import signal
import urllib.request
from anyserve import serve, worker

# Point MODEL_URL at any local HTTP inference endpoint (e.g. FastAPI, vLLM, Triton HTTP,
# or Ollama such as http://127.0.0.1:11434/api/generate).
MODEL_URL = os.environ.get("MODEL_URL", "http://127.0.0.1:8000/predict")
INTERFACE = os.environ.get("ANYSERVE_INTERFACE", "demo.local_model.v1")


@worker(interface=INTERFACE, max_active_leases=4)
def predict(payload: bytes) -> bytes:
    req = urllib.request.Request(MODEL_URL, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        serve(predict)
    except KeyboardInterrupt:
        pass
