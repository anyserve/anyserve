# Ollama Example

This directory is the canonical walkthrough for the built-in OpenAI-compatible gateway and the built-in LLM worker in `anyserve` backed by Ollama.

It uses two files in this directory:

- `anyserve.toml`: starts the control plane and the built-in OpenAI-compatible gateway
- `worker.toml`: starts the built-in LLM worker and points it at an OpenAI-compatible upstream

If you want to point the same worker at SGLang or vLLM instead of Ollama, change `base_url`, `provider`, and the model list in `anyserve.toml`. The rest of the flow stays the same.

## Hosted Notebook Variant

If you want the same gateway shape inside Google Colab instead of a local Ollama process, use the separate example:

- [../google-colab/README.md](../google-colab/README.md)

That example uses a small Hugging Face Qwen model inside the notebook runtime and publishes the AnyServe endpoint through Cloudflare Tunnel.

## 1. Start Ollama

Install Ollama with Homebrew:

```bash
brew install ollama
```

Start the local Ollama server:

```bash
ollama serve
```

In another shell, pull the model used by the checked-in example:

```bash
ollama pull qwen3:0.6b-fp16
```

The checked-in worker config points at:

```text
http://127.0.0.1:11434/v1
```

## 2. Check or Adjust the Config

The example is already wired for a local Ollama instance:

- [worker.toml](worker.toml)
  - `base_url = "http://127.0.0.1:11434/v1"`
  - `provider = "ollama"`
  - `interfaces = ["llm.chat.v1", "llm.embed.v1"]`
- [anyserve.toml](anyserve.toml)
  - `[openai].models = ["qwen3:0.6b-fp16"]`
  - `[openai].chat_interface = "llm.chat.v1"`
  - `[openai].embeddings_interface = "llm.embed.v1"`

Those model names are returned by `GET /v1/models`. Right now they are not aliases. Use the real upstream model names.

## 3. Start AnyServe

```bash
mise exec -- cargo run -p anyserve -- serve --config examples/ollama/anyserve.toml
```

This starts:

- gRPC on `127.0.0.1:50052`
- the OpenAI-compatible gateway on `127.0.0.1:8080`

## 4. Start the Built-In LLM Worker

```bash
mise exec -- cargo run -p anyserve -- worker --config examples/ollama/worker.toml
```

The worker pulls jobs from AnyServe and forwards them to Ollama.

## 5. Call the OpenAI-Compatible Endpoint

List models:

```bash
curl http://127.0.0.1:8080/v1/models
```

Chat completion:

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "qwen3:0.6b-fp16",
    "messages": [{"role": "user", "content": "hello"}]
  }'
```

Streaming chat:

```bash
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "qwen3:0.6b-fp16",
    "stream": true,
    "messages": [{"role": "user", "content": "hello"}]
  }'
```

If you also want embeddings, pull an embedding-capable model first and add it to `[openai].models`. For example:

```bash
ollama pull nomic-embed-text
```

Then call:

```bash
curl http://127.0.0.1:8080/v1/embeddings \
  -H 'content-type: application/json' \
  -d '{
    "model": "nomic-embed-text",
    "input": "hello"
  }'
```

## Current Behavior

- The gateway is thin. It submits jobs into AnyServe and waits for the LLM worker.
- The worker mostly forwards raw OpenAI-style JSON to the upstream.
- `model` is passed through as-is.
- Streaming is real streaming and event-driven inside AnyServe, but it is still chunk-based forwarding from the upstream response, not a token-aware rewriter.
- There is no aliasing, routing, auth, or automatic model discovery yet.
