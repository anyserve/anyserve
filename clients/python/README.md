# AnyServe Python SDK

`anyserve-runtime` provides Python clients and workers backed by the native Rust
transport. Python 3.10+ is required; use the SDK and server from the same release.
Install from PyPI with `pip install anyserve-runtime` or from the repository root
with `pip install ./clients/python`.

- **Live Demo (no server setup needed)**: sign in with GitHub at
  [https://anyserve.arcosx.org/tokens](https://anyserve.arcosx.org/tokens) to mint isolated
  `client` / `worker` tokens and connect to `https://anyserve.arcosx.org` (gRPC) or
  `wss://anyserve.arcosx.org` (WebSocket).
- [Start a local server and worker](https://github.com/anyserve/anyserve#quick-start).
- [Client, streaming and worker API reference](https://github.com/anyserve/anyserve/blob/main/docs/python-sdk.md).
- [Build and test commands](https://github.com/anyserve/anyserve/blob/main/docs/contributing.md).
- [Echo worker](https://github.com/anyserve/anyserve/blob/main/examples/python/worker.py),
  [local HTTP model worker](https://github.com/anyserve/anyserve/blob/main/examples/python/local_model_worker.py),
  and [submitter](https://github.com/anyserve/anyserve/blob/main/examples/python/submit.py).
