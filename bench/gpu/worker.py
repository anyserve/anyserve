"""Application worker and the identical direct-ORT helpers for the GPU experiment."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import threading
import time

import numpy as np

MODEL_SHA = "c17eb830b28be4381ae141cf6eb477efeac424bfc18c867b964f5129e13ed083"
INTERFACE = "experiment.holdem-spin.v1"
_LOG_LOCK = threading.Lock()


def emit(kind, **fields):
    with _LOG_LOCK:
        print(json.dumps(dict(kind=kind, time_ns=time.time_ns(), **fields)), flush=True)


def samples():
    return np.random.default_rng(20261021).random((256, 288), dtype=np.float32)


def session(model):
    import onnxruntime as ort

    assert hashlib.sha256(Path(model).read_bytes()).hexdigest() == MODEL_SHA
    ort.preload_dlls(directory="")
    options = ort.SessionOptions()
    options.intra_op_num_threads = options.inter_op_num_threads = 1
    result = ort.InferenceSession(str(model), sess_options=options, providers=[
        ("CUDAExecutionProvider", {"device_id": 0, "gpu_mem_limit": 2 * 1024**3}),
    ])
    assert result.get_providers()[0] == "CUDAExecutionProvider", result.get_providers()
    assert result.get_inputs()[0].name == "input"
    return result


def infer(engine, value):
    result = engine.run(None, {"input": value.reshape(1, 288)})
    assert all(np.isfinite(item).all() for item in result), "nonfinite model output"
    return result


def encode(host, index, values):
    header = json.dumps(dict(host=host, index=index, arrays=[
        dict(dtype=str(item.dtype), shape=list(item.shape)) for item in values
    ]), separators=(",", ":")).encode()
    return struct.pack("<I", len(header)) + header + b"".join(item.tobytes() for item in values)


def decode(payload):
    length, = struct.unpack("<I", payload[:4])
    info = json.loads(payload[4:4 + length])
    offset, values = 4 + length, []
    for item in info["arrays"]:
        dtype = np.dtype(item["dtype"])
        count = int(np.prod(item["shape"]))
        values.append(np.frombuffer(payload, dtype, count, offset).reshape(item["shape"]))
        offset += count * dtype.itemsize
    assert offset == len(payload)
    return info, values


def main():
    from anyserve import WorkerConnection, serve, worker

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--host", choices=["a", "b"], required=True)
    parser.add_argument("--slots", type=int, required=True)
    parser.add_argument("--endpoint", default="http://10.0.3.60:50052")
    parser.add_argument("--token-file", type=Path, required=True)
    args = parser.parse_args()
    engine = session(args.model)
    pool = samples()
    answers = [infer(engine, value) for value in pool]
    np.savez(args.run_dir / f"{args.host}-goldens.npz", **{
        f"o{i}": np.stack([answer[i] for answer in answers]) for i in range(len(answers[0]))
    })
    gpu = subprocess.check_output([
        "nvidia-smi", "--query-compute-apps=pid,process_name,used_memory", "--format=csv,noheader"
    ], text=True)
    rows = [line for line in gpu.splitlines() if line.split(",", 1)[0].strip() == str(os.getpid())]
    assert rows, "worker PID absent from nvidia-smi compute processes"
    attributes = dict(host=args.host, run_id=args.run_id, model_sha256=MODEL_SHA)
    worker_id = f"{args.run_id}-{args.host}"
    token = args.token_file.read_text().strip()
    connection = WorkerConnection(args.endpoint, dict(
        worker_id=worker_id, interfaces=[INTERFACE], attributes=attributes,
        max_concurrency=args.slots,
    ), token=token)

    @worker(interface=INTERFACE, attributes=attributes, max_active_leases=args.slots, pass_context=True)
    def handle(payload, context):
        index, = struct.unpack("<I", payload[:4])
        fields = dict(job_id=context.job_id, attempt_id=context.attempt_id,
                      lease_id=context.lease_id, host=args.host, index=index)
        emit("start", **fields)
        value = np.frombuffer(payload[4:], dtype=np.float32).reshape(1, 288)
        output = encode(args.host, index, infer(engine, value))
        emit("finish", **fields)
        return output

    ready = dict(pid=os.getpid(), providers=engine.get_providers(), gpu_process=rows,
                 generation=connection.generation, model_sha256=MODEL_SHA,
                 input_sha256=hashlib.sha256(pool.tobytes()).hexdigest(),
                 numpy=np.__version__, slots=args.slots, worker_id=worker_id)
    (args.run_dir / f"{args.host}-ready.json").write_text(json.dumps(ready, indent=2) + "\n")
    emit("ready", **ready)
    serve(handle, connection=connection, worker_id=worker_id, token=token)


if __name__ == "__main__":
    main()
