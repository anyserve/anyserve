# Google Colab Qwen Examples

This directory contains Google Colab examples that run a small Hugging Face Qwen model with AnyServe.

It is the hosted-notebook variant of the local Ollama walkthrough in [../ollama/README.md](../ollama/README.md).

Use:

- [qwen3_anyserve.ipynb](qwen3_anyserve.ipynb)
- [qwen3_remote_worker.ipynb](qwen3_remote_worker.ipynb)

`qwen3_anyserve.ipynb` does this:

- loads `Qwen/Qwen3-0.6B` with `transformers`
- exposes a tiny OpenAI-compatible upstream on the notebook runtime
- starts `anyserve` in front of it
- publishes a temporary public URL with Cloudflare Tunnel so another person can call the AnyServe endpoint directly

`qwen3_remote_worker.ipynb` does this:

- loads `Qwen/Qwen3-0.6B` with `transformers`
- exposes a tiny OpenAI-compatible upstream on the notebook runtime
- starts only the worker process inside Colab
- connects that worker to an AnyServe deployment that already exists somewhere else
- lets another person call the existing AnyServe public URL directly

Notes:

- this example is chat-only
- the Hugging Face model ID is `Qwen/Qwen3-0.6B`
- the matching Ollama family in the local example is `qwen3:0.6b-fp16`
- the all-in-one notebook's public Cloudflare URL is temporary and tied to the lifetime of the notebook runtime

Remote deployment notes:

- the remote gRPC endpoint used by workers is not the same thing as the public HTTP gateway URL
- the remote gateway should already expose `Qwen/Qwen3-0.6B` in `[openai].models`
- model aliasing is not implemented yet, so the gateway model name should match exactly
