"""Text-to-image backend (SD-Turbo via diffusers). Returns PNG bytes, so the
benchmark exercises long-running jobs with ~0.5 MB binary outputs."""
import io
import os
import threading

import torch
from diffusers import AutoPipelineForText2Image
from fastapi import FastAPI, Response
from pydantic import BaseModel

MODEL = os.environ.get("IMAGE_MODEL", "stabilityai/sd-turbo")
pipe = AutoPipelineForText2Image.from_pretrained(MODEL, torch_dtype=torch.float16).to("cuda")
pipe.set_progress_bar_config(disable=True)
# Diffusers pipelines are not thread-safe; the GPU runs one image at a time anyway.
lock = threading.Lock()
app = FastAPI()


class Prompt(BaseModel):
    prompt: str
    steps: int = 1
    size: int = 512


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/generate")
def generate(req: Prompt):
    with lock:
        image = pipe(req.prompt, num_inference_steps=req.steps, guidance_scale=0.0,
                     height=req.size, width=req.size).images[0]
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return Response(buf.getvalue(), media_type="image/png")
