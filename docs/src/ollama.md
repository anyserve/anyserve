# Ollama

The full walkthrough lives in [examples/ollama/README.md](https://github.com/anyserve/anyserve/blob/main/examples/ollama/README.md).

This page only keeps the Ollama-specific notes that are useful outside the example:

- the checked-in example assumes a local Ollama server on `127.0.0.1:11434`
- `qwen3:0.6b-fp16` is the default chat model in the example config
- `nomic-embed-text` is the optional embedding model
- `examples/ollama/worker.toml` points the built-in worker at the OpenAI-compatible Ollama endpoint
