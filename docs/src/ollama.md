# Ollama

This page maps the checked-in LLM example onto a local Ollama instance.

Keep the detailed walkthrough in [examples/llm/README.md](../../examples/llm/README.md). This page stays short and Ollama-specific.

## Install Ollama

On macOS with Homebrew:

```bash
brew install ollama
```

## Start Ollama

Run the local server:

```bash
ollama serve
```

By default Ollama listens on `127.0.0.1:11434`.

## Pull a Model

The checked-in example files use this model:

```bash
ollama pull qwen3:0.6b-fp16
```

Verify it is available:

```bash
ollama list
```

## Start AnyServe with the Built-In OpenAI Gateway

Use the example config from the same checked-in example directory:

```bash
mise exec -- cargo run -p anyserve -- serve --config examples/llm/anyserve.toml
```

That starts:

- gRPC on `127.0.0.1:50052`
- the OpenAI-compatible gateway on `127.0.0.1:8080`

## Start the Built-In OpenAI Worker

Use the worker config in the same directory:

```bash
mise exec -- cargo run -p anyserve -- worker --config examples/llm/worker.toml
```

That worker points to:

```text
http://127.0.0.1:11434/v1
```

and registers itself with:

- `family = "llm"`
- `protocol = "openai-compatible"`
- `provider = "ollama"`

## Smoke Test

Health:

```bash
curl http://127.0.0.1:8080/healthz
```

Models:

```bash
curl http://127.0.0.1:8080/v1/models
```

Chat completion:

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "qwen3:0.6b-fp16",
    "messages": [{"role": "user", "content": "Say hello in one short sentence."}]
  }'
```

Streaming chat:

```bash
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "qwen3:0.6b-fp16",
    "stream": true,
    "messages": [{"role": "user", "content": "Count to three in one line."}]
  }'
```

## Streaming Semantics

The current streaming path is:

1. Ollama sends `text/event-stream`
2. the built-in worker reads upstream bytes as they arrive
3. the worker writes each chunk into `output.default`
4. the built-in OpenAI gateway reads those frames and relays them back to the HTTP client

So this is real streaming. It does not buffer the full completion before replying.

Two limits are still important:

- forwarding is chunk-based, not token-aware
- the gateway now wakes up on internal stream events instead of polling, but it still preserves upstream chunk boundaries rather than rebuilding token-level deltas

## Embeddings

The example config is set up for the chat model above. If you also want embeddings with Ollama, pull an embedding model and add it to `[openai].models`, for example:

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
