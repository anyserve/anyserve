"""Stand-in RL policy server: an MLP actor mapping an observation vector to
action logits, sized like a small game/robot policy. Batch-1, CPU, so latency
reflects a typical per-step policy call rather than a GPU batch job."""
import os

import torch
from fastapi import FastAPI
from pydantic import BaseModel

OBS_DIM = int(os.environ.get("POLICY_OBS_DIM", "256"))
ACT_DIM = int(os.environ.get("POLICY_ACT_DIM", "64"))
HIDDEN = int(os.environ.get("POLICY_HIDDEN", "1024"))

torch.set_num_threads(int(os.environ.get("POLICY_THREADS", "4")))
torch.manual_seed(0)
model = torch.nn.Sequential(
    torch.nn.Linear(OBS_DIM, HIDDEN), torch.nn.ReLU(),
    torch.nn.Linear(HIDDEN, HIDDEN), torch.nn.ReLU(),
    torch.nn.Linear(HIDDEN, ACT_DIM),
).eval()

app = FastAPI()


class Step(BaseModel):
    obs: list[float]


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/act")
def act(step: Step):
    with torch.inference_mode():
        obs = torch.tensor(step.obs, dtype=torch.float32).reshape(1, -1)[:, :OBS_DIM]
        if obs.shape[1] < OBS_DIM:
            obs = torch.nn.functional.pad(obs, (0, OBS_DIM - obs.shape[1]))
        logits = model(obs)[0]
    return {"action": int(torch.argmax(logits)), "logits": [round(x, 4) for x in logits.tolist()]}
