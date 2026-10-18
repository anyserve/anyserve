# Request-path reference review

Reviewed on 2026-10-18. This companion to the
[runtime reference review](reference-architectures.md) covers request routing.
The mechanisms below are design inputs, not dependencies or copied source.

## NVIDIA Dynamo

Reference revision: `d846fdaed801007e3e1da8879665fcf7de60ec61`.

The [architecture description](https://github.com/ai-dynamo/dynamo/blob/d846fdaed801007e3e1da8879665fcf7de60ec61/docs/fern/pages/developer-guide/knowledge-base/overview.md)
separates request execution, deployment adaptation and state propagation.
Its [router configuration](https://github.com/ai-dynamo/dynamo/blob/d846fdaed801007e3e1da8879665fcf7de60ec61/docs/fern/pages/developer-guide/knowledge-base/modular-components/router/configuration-and-tuning.md)
distinguishes queue admission, candidate filtering and worker selection.

For Anyserve, eligibility must be established before preference/load scoring;
queue limits and cancellation belong to the scheduler's admission lifecycle.
Dropping an RPC is not sufficient to remove a queued job or undo a reservation.
This is also demonstrated by Dynamo's
[cancellation/admission race report](https://github.com/ai-dynamo/dynamo/issues/10878).
Anyserve should test that cancellation before dispatch cannot later consume a
worker slot. It should not adopt prefill/decode separation, NIXL, KV movement or
GPU placement as generic runtime responsibilities.

## SGLang Model Gateway

Reference revision: `4ec19ca27f08f7ce44bbe1da70ce964731940eb2`.

The [HTTP router](https://github.com/sgl-project/sglang/blob/4ec19ca27f08f7ce44bbe1da70ce964731940eb2/sgl-model-gateway/src/routers/http/router.rs)
keeps backend routing distinct from reliability components.
The [power-of-two policy](https://github.com/sgl-project/sglang/blob/4ec19ca27f08f7ce44bbe1da70ce964731940eb2/sgl-model-gateway/src/policies/power_of_two.rs)
compares eligible workers using consistent load signals and handles missing
observations. The [circuit breaker](https://github.com/sgl-project/sglang/blob/4ec19ca27f08f7ce44bbe1da70ce964731940eb2/sgl-model-gateway/src/core/circuit_breaker.rs)
uses monotonic elapsed time for recovery transitions.

Anyserve can use generic capacity/reservation load, health and locality hints
without interpreting tokens. Unknown backend measurements must not look like
zero load. HTTP cancellation and validation failures must not count as backend
health failures. Retry eligibility and circuit health remain distinct decisions.
Tokenizer, reasoning parser, tool execution and conversation-history integration
stay outside the generic core.

## vLLM Semantic Router

Reference revision: `fe3dfb68748a5f95c04dbd7f5ed191260f787ff3`.

The [component and protocol overview](https://github.com/vllm-project/semantic-router/blob/fe3dfb68748a5f95c04dbd7f5ed191260f787ff3/website/docs/overview/semantic-router-overview.md)
separates logical model/pool choice from selecting a physical inference replica.
It also distinguishes ingress/provider translation from policy evaluation.

Anyserve's stable interface name should identify a capability, with attributes
selecting a compatible worker pool. A policy extension can supply preferences,
but inference-based classification is not a mandatory step before dispatch.
The model engine remains responsible for its own batching and GPU scheduler.
Public API adapters should not leak provider-specific request structures into
generic job matching or persistence.

## LiteLLM

Reference revision: `9c958aff349680503a5d9785b3ef507eb9822002`.

The [request-lifecycle documentation](https://docs.litellm.ai/docs/proxy/architecture)
places noncritical usage/logging work after response delivery and distinguishes
retrying a deployment within a group from falling back between model groups.
The implementation is in
[`litellm/router.py`](https://github.com/BerriAI/litellm/blob/9c958aff349680503a5d9785b3ef507eb9822002/litellm/router.py).

Anyserve should have a single attempt budget and deadline across all adapters
and workers; nested independent retries can multiply execution. A completion
acknowledgement retry must resend the same result rather than invoke the model
again. Observability may be asynchronous; durable acceptance and terminal
confirmation must wait for their own persistence boundary. Broad provider SDK
translation and account/billing management are separate product concerns.

## Resulting requirements

- One explicit admission lifecycle, with bounded pending work and cancellation
  checked before reservation and before dispatch.
- One retry budget across lease expiry, worker failure and adapter behavior.
- Capability filtering, locality preferences and physical load selection are
  separate policies over small metadata, not input buffers.
- Live stream forwarding and control messages are bounded independently; large
  outputs cannot prevent heartbeat, cancellation or completion acknowledgements.
- Noncritical logging cannot introduce a mandatory database round trip per token.
- Benchmark both a mock backend and real backends. The mock isolates middleware
  cost; engine results measure useful inference throughput and latency.
