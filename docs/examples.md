# Examples & Integrations (`anyserve.universe`)

Ready-to-run inference connectors and host-application integrations live in
[anyserve.universe](https://github.com/anyserve/anyserve.universe) and are
documented in full on the [AnyServe Docs website](https://anyserve.github.io/anyserve/examples/overview/).

| Example | Type | Interface(s) | What it demonstrates |
|---|---|---|---|
| [`openai/`](https://github.com/anyserve/anyserve.universe/tree/main/openai) | Inference worker + CLI + HTTP proxy | `openai/chat/<model>`, `completions`, `embeddings` | Put vLLM, Ollama, llama.cpp, SGLang, or embedding servers behind AnyServe with SSE streaming, cooperative cancellation, and a local OpenAI-compatible HTTP proxy (`proxy.py`) |
| [`triton-inference-server/`](https://github.com/anyserve/anyserve.universe/tree/main/triton-inference-server) | Inference worker + benchmark client | `triton/<model>` | Auto-discover `READY` Triton models over `/v2/repository/index`, forward KServe v2 payloads, and exercise dynamic batching with the included `batch_scorer` model repository |
| [`yolo/`](https://github.com/anyserve/anyserve.universe/tree/main/yolo) | Inference worker + image/video client | `yolo/<model>` | Serve Ultralytics YOLO with per-request `conf`/`labels` metadata, headless annotated image/MP4 output (`--out`), and frame deadline budgets |
| [`vla/`](https://github.com/anyserve/anyserve.universe/tree/main/vla) | Policy worker + probe/replay client | `vla/<model>` | Serve LeRobot SmolVLA 50-action chunks over AnyServe with `--probe` smoke testing and 30 Hz LeRobot episode replay |
| [`comfyui/`](https://github.com/anyserve/anyserve.universe/tree/main/comfyui) | ComfyUI custom node pack | calls `openai/chat/*`, `yolo/*`, or any interface | `AnyServeConfig`, `AnyServeOpenAIChat` (LLM/VLM), `AnyServeYOLODetect` (annotated `IMAGE` + inpainting `MASK`), and generic `AnyServeInvoke` + CPU Docker image and API workflow |
| [`ros2/`](https://github.com/anyserve/anyserve.universe/tree/main/ros2) | ROS 2 (`ament_python`) package | calls `yolo/*`, `vla/*` | `yolo_node` (`/camera/image_raw` → `vision_msgs/Detection2DArray` + `/detections/image`) and `vla_node` (cameras + `/joint_states` + `/vla/instruction` → 30 Hz `/joint_commands`) + ROS 2 Jazzy Docker image |

## Quickstart

```bash
# 1. Start an authenticated control plane
anyserve serve --console-listen 127.0.0.1:3001 --auto-token

# 2. Clone the examples and install the Python SDK
git clone https://github.com/anyserve/anyserve.universe.git
cd anyserve.universe
pip install anyserve-runtime
```

See the individual guides under [`website/src/content/docs/examples/`](../website/src/content/docs/examples/) or online at [https://anyserve.github.io/anyserve/examples/overview/](https://anyserve.github.io/anyserve/examples/overview/).
