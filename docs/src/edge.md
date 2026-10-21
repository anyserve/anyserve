# Workers Across Hosts and Clusters

Workers establish outbound native Session and Attempt connections. They may run
in different hosts, networks or clusters, subject to network access to the
control-plane listener. Application backends remain local to each worker or are
called using application-owned clients.

```text
application -> native Client -> one control-plane Runtime
                                      ^          ^
                               Session/Attempt connections
                                      |          |
                                 worker A     worker B
                                      |          |
                                  backend A   backend B
```

Control and data use separate connections. Small inputs travel in Assign;
larger or live input arrives over Attempt. Opening attempts, acknowledgements
and optional journal commits add exchanges. The protocol makes no fixed
one-RTT or WAN-overhead promise.

## Advertise capabilities

For example, an application-owned Python worker can advertise its location and
model version while keeping its handler generic:

```python
from anyserve import serve, worker

@worker(
    interface="policy.evaluate.v1",
    attributes={"region": "near", "model": "v1"},
    capacity={"slot": 4},
    max_active_leases=4,
)
def evaluate(payload: bytes) -> bytes:
    # Replace this echo with the application's backend call.
    return payload

serve(evaluate, endpoint="https://center:50052", token="WORKER_TOKEN")
```

The control plane must have matching TLS and worker-role credentials configured.
Attributes are caller/worker-defined metadata. The Runtime does not load or
validate a model based on the `model` attribute.

## Express placement through the native API

```python
from anyserve import AnyserveClient, ExecutionOptions

client = AnyserveClient("https://center:50052", token="CLIENT_TOKEN")
outputs = client.execute(
    "policy.evaluate.v1", b"observation",
    options=ExecutionOptions(
        required_attributes={"model": "v1"},
        preferred_attributes={"region": "near"},
        required_capacity={"slot": 1},
        spillover_after_ms=20,
        queue_timeout_ms=100,
        total_timeout_ms=1000,
        max_attempts=3,
    ),
)
```

Required constraints always apply. Before the spillover hold expires, all
preferences must match. After it expires, partially matching workers may serve
the job, ranked by preference and load. With no hold interval, preferences rank
eligible workers immediately. Queue and total timeouts bound waiting.

A stable worker ID is an application routing attribute when the worker advertises
it. The Python SDK includes `worker_id` automatically; other workers must include
it in Hello attributes if callers need to prefer it. Reusing one worker does
not make its local application state durable. If a new worker is selected,
applications reconstruct any session state they require.

Backend batching, GPU memory, model versions, health checks and autoscaling remain
with the worker/backend and deployment platform. Use the management worker view
for connectivity and capacity; it does not independently prove backend correctness.
