# Create Gateway

This page defines the production gateway shape for Anyserve.

Use it as the contract for the endpoint, worker connection flow, dashboard, and routing surface.

## The Point

Do not lead with a generic control plane.

Lead with one concrete result:

`Create a production-grade inference endpoint in front of your own workers.`

The runtime can stay workload-neutral underneath. The production surface should not.

The production form is simple:

- user signs in to Anyserve or a self-hosted control plane
- user creates a gateway inside their own workspace
- user connects their own workers to that workspace
- user only sees their own workers
- application traffic always hits Anyserve first, never the worker directly

This matters because people adopt infrastructure when they can see how to put it in front of real traffic, not when they first meet the kernel internals.

## Default Template

The default template is:

`Single Region Quickstart`

Why this is the default:

- highest first-run success rate
- lowest debugging surface
- easiest mental model
- best first production path

What the template does:

- creates one gateway
- binds it to one default region
- scopes the gateway to one workspace
- targets one healthy worker pool owned by that workspace
- exposes one hosted inference endpoint with OpenAI-compatible traffic enabled by default
- issues one API key
- ships with one default model alias
- issues one worker connect token

What it does not do by default:

- multi-region routing
- edge-first fallback
- weighted traffic shifting
- advanced policy editing

Those become follow-up templates, not the first-run path.

## The First-Run Flow

The product flow should be:

1. User opens the site.
2. User clicks `Create Gateway`.
3. User picks `Single Region Quickstart`.
4. User enters a gateway name.
5. User chooses a default region.
6. User chooses a capacity source:
   - `My Anyserve Workers` (default)
   - `OpenAI-compatible upstream`
   - `Managed worker pool`
7. The platform provisions the gateway.
8. The user lands on a dashboard with:
   - endpoint
   - API key
   - worker connect token / connect command
   - default model alias
   - current region
   - their own worker health
   - copy-paste curl and SDK snippets

The first meaningful user action after creation is not "configure routing."

It is "send the first request."

## Create Page Information Architecture

The create page should have four blocks.

### 1. Hero

Purpose:
- explain the result in one sentence
- keep the promise concrete

Content:
- title: `Create Gateway`
- subtitle: `Get a production-grade inference endpoint in front of your own workers.`
- proof line: `Start in one region. Add failover and edge routing later.`

### 2. Template Picker

The first version should show three cards, but only one is default:

- `Single Region Quickstart`
  - recommended
  - lowest setup friction
  - best for first request
- `Multi-Region Auto Failover`
  - not default
  - adds region failover and health-aware routing
- `Edge First, Core Fallback`
  - not default
  - routes to edge capacity first, then falls back to a central pool

The first card should be preselected.

### 3. Gateway Form

Fields:

- `Gateway Name`
- `Default Region`
- `Capacity Source`
  - `My Anyserve Workers`
  - `OpenAI-compatible upstream`
  - `Managed worker pool`
- `Default Model Alias`
  - prefilled with `chat-default`

Advanced settings should be collapsed by default:

- idle timeout
- request timeout
- worker protocol
- region pinning

### 4. Live Preview

This panel matters. It turns infrastructure setup into a visible production shape.

Show:

- generated endpoint URL
- generated API key name
- generated worker connect token
- effective default region
- model alias mapping
- first curl snippet

The user should understand the outcome before clicking submit.

## Post-Create Dashboard Information Architecture

The first dashboard should have four modules above the fold.

### 1. Quick Start

This is the most important panel.

Show:

- endpoint
- API key
- worker connect command
- model alias
- curl example
- Python example
- JavaScript example

The curl example should work with zero edits except the API key.

### 2. My Workers

Show:

- only workers owned by the current user or workspace
- default region
- active workers in that region
- worker health
- last heartbeat
- capacity summary
- worker tags like region / provider / protocol

Do not expose lease internals here.
Show operational health, not runtime guts.

### 3. Recent Requests

Show:

- timestamp
- model alias
- chosen region
- chosen worker from the current user's pool
- latency
- status

The request table is where the user learns that scheduling is really happening.

### 4. Routing Summary

For the default template this should stay very simple:

- `mode: single_region`
- `default region: iad`
- `fallback: disabled`
- `policy: lowest-latency healthy worker in selected region`

If the product is honest, the default policy can be explained in one sentence.

## Workspace Model

This part should be explicit.

The system should not look like a public pool where users share anonymous workers.

It is:

- one Anyserve account
- one or more workspaces
- one or more gateways per workspace
- one or more workers connected by that workspace

Isolation rule:

- a gateway can only route to workers owned by the same workspace unless the user explicitly adds an external upstream

That rule is simple enough for users to trust.

## Worker Connection Model

The worker should connect outbound to the hosted control plane.

Do not require users to expose inbound ports just to participate.

The product shape should be:

1. user creates a gateway
2. Anyserve issues a worker connect token
3. user runs a connect command on their machine / node / edge box
4. worker registers itself to the workspace
5. the dashboard shows that worker under `My Workers`
6. gateway traffic can now be scheduled onto it

Suggested connect command:

```bash
anyserve worker connect \
  --cloud https://cloud.anyserve.run \
  --token wk_live_xxxxxxxxxxxx \
  --gateway gw_01jr9v4g0r2n8m0b0d4g6t8m9p \
  --region us-east-1
```

The important thing is not the exact CLI spelling.

The important thing is the network shape:

- worker dials out
- the hosted control plane stays the middle station
- the user never sends app traffic straight to the worker

## Request Path

This is the canonical request path:

```text
App / SDK
  -> Anyserve gateway endpoint
  -> gateway auth + workspace lookup
  -> routing decision inside that workspace
  -> one of the user's own workers or configured upstreams
  -> response streamed back through Anyserve
```

That means the website is a relay and control point.

It owns:

- auth
- tenant isolation
- request logging
- worker selection
- health checks
- fallback policy

It does not need to own the user's compute.

## SDK Story

There should be two SDK surfaces.

### 1. Management SDK

This is for provisioning and operations.

Examples:

- create gateway
- list gateways
- create API keys
- create worker connect tokens
- list my workers
- inspect recent requests

This should be an Anyserve SDK or CLI.

### 2. Inference SDK

This is for application traffic.

This should use existing OpenAI-compatible SDKs whenever possible.

Python example:

```python
from openai import OpenAI

client = OpenAI(
    api_key="any_live_xxxxxxxxxxxx",
    base_url="https://gw-01jr9v4g0r2n8m0b0d4g6t8m9p.anyserve.run/v1",
)

response = client.chat.completions.create(
    model="chat-default",
    messages=[{"role": "user", "content": "hello"}],
)
```

JavaScript example:

```js
import OpenAI from "openai";

const client = new OpenAI({
  apiKey: "any_live_xxxxxxxxxxxx",
  baseURL: "https://gw-01jr9v4g0r2n8m0b0d4g6t8m9p.anyserve.run/v1",
});

const response = await client.chat.completions.create({
  model: "chat-default",
  messages: [{ role: "user", content: "hello" }],
});
```

This is the cleanest split:

- management goes through Anyserve SDK / console
- inference goes through standard OpenAI SDKs pointed at Anyserve

## API Contract

The hosted control plane should expose a small, product-shaped API on top of the existing runtime.

Suggested endpoints:

### `POST /api/gateways`

Creates a gateway.

Request:

```json
{
  "name": "my-first-gateway",
  "template": "single_region_quickstart",
  "default_region": "us-east-1",
  "capacity_source": {
    "kind": "managed_worker_pool"
  },
  "default_model_alias": "chat-default"
}
```

Response:

```json
{
  "gateway_id": "gw_01jr9v4g0r2n8m0b0d4g6t8m9p",
  "name": "my-first-gateway",
  "status": "ready",
  "template": "single_region_quickstart",
  "dashboard_url": "https://console.anyserve.run/gateways/gw_01jr9v4g0r2n8m0b0d4g6t8m9p",
  "endpoint_url": "https://gw-01jr9v4g0r2n8m0b0d4g6t8m9p.anyserve.run/v1",
  "default_region": "us-east-1",
  "default_model_alias": "chat-default",
  "api_key": {
    "key_id": "key_01jr9v6d5e2k6z0m3j9s2x4w7q",
    "token": "any_live_xxxxxxxxxxxx"
  },
  "worker_connect": {
    "token_id": "wkt_01jr9v8n2a6e4r0m1y7q3s9t5u",
    "token": "wk_live_xxxxxxxxxxxx",
    "command": "anyserve worker connect --cloud https://cloud.anyserve.run --token wk_live_xxxxxxxxxxxx --gateway gw_01jr9v4g0r2n8m0b0d4g6t8m9p --region us-east-1"
  }
}
```

### `GET /api/gateways/{gateway_id}`

Returns the gateway summary used by the dashboard.

Suggested fields:

- metadata
- status
- endpoint URL
- default region
- template
- model aliases
- routing summary
- worker summary

### `GET /api/gateways/{gateway_id}/workers`

Returns worker health for the selected gateway.

Suggested fields:

- worker id
- region
- protocol
- health
- last heartbeat
- advertised capacity
- available capacity

The endpoint must only return workers visible to the current workspace.

### `GET /api/gateways/{gateway_id}/requests?limit=50`

Returns recent request activity for the dashboard table.

Suggested fields:

- request id
- created at
- model alias
- selected region
- selected worker
- duration ms
- result status

### `POST /api/gateways/{gateway_id}/keys`

Creates an additional key for the gateway.

That lets the product support team workflows without building org-scale auth on day one.

### `POST /api/gateways/{gateway_id}/worker-tokens`

Creates a new connect token for a worker that should belong to this gateway or workspace.

### `GET /api/workers`

Returns the current user's visible workers across the active workspace.

## Provisioning States

The control plane should surface a tiny state machine:

- `provisioning`
- `ready`
- `degraded`
- `error`

That is enough for v1.

Do not invent ten provisioning states to look enterprise.

## Mapping To The Existing Runtime

The mapping to Anyserve should stay straightforward:

- gateway = hosted control-plane object plus HTTP endpoint configuration
- model alias = interface routing entry plus upstream mapping
- worker summary = filtered view over matching worker supply inside the current workspace
- recent requests = request log derived from gateway traffic and job execution metadata

The runtime stays generic. The product surface becomes opinionated.

## Deferred Features

These should be explicitly deferred from the first product pass:

- billing
- enterprise RBAC
- full multi-tenant admin console
- advanced policy DSL
- token-level traffic shaping
- cross-region data replication UI
- cross-workspace worker sharing

The first version wins by being obvious, not by looking complete.

## Homepage Messaging

The homepage should not open with protocols, schedulers, or routing theory.

It should open with ownership and production shape.

Recommended default:

- eyebrow: `ANYSERVE`
- hero: `Hosted endpoint. Your workers.`
- subhead: `Anyserve hosts the gateway, auth, and routing. You connect your own workers and serve chat, embeddings, speech, vision, image, rerank, and custom inference through one endpoint.`
- primary CTA: `Create Gateway`
- secondary CTA: `Connect Worker`

That is the cleanest version because it says:

- who owns the endpoint
- who owns the workers
- what Anyserve does in the middle
- that the product is bigger than one protocol

## Value Props

The first three value blocks under the hero should be:

### 1. Hosted entry point

Suggested copy:

`Get a production-grade gateway, API key, dashboard, and request logs without exposing your workers directly.`

### 2. Your workers stay yours

Suggested copy:

`Connect your own workers to Anyserve. Route traffic through your workspace, not a public shared pool.`

### 3. More than one inference type

Suggested copy:

`Use one control plane for chat, embeddings, speech, vision, image, rerank, and custom inference protocols.`

This is where you expand beyond OpenAI-compatible chat.

Do not try to cram that into the hero.

## Supporting Line

Use this line near the hero, the CTA area, or the quick-start block:

`OpenAI-compatible when you want it. Not limited to OpenAI-style workloads.`

That line does two jobs:

- easy onboarding
- wider product boundary

## Hero Variants

Here are three usable homepage variants.

### Variant A, recommended

- hero: `Hosted endpoint. Your workers.`
- subhead: `Anyserve hosts the gateway, auth, and routing. You connect your own workers and serve chat, embeddings, speech, vision, image, rerank, and custom inference through one endpoint.`

Why this is best:

- shortest
- most ownable
- strongest trust boundary

### Variant B, more developer-facing

- hero: `Create a hosted inference gateway for your own workers.`
- subhead: `Use standard SDKs on the outside. Keep your workers and inference stack on the inside.`

Why use it:

- easier for SDK users
- more literal

### Variant C, more infra-facing

- hero: `One gateway for all your inference workers.`
- subhead: `Route requests through Anyserve, keep execution on your own workers, and support more than one inference type from day one.`

Why use it:

- stronger platform feel
- better if the audience is already infra-heavy

## Short Taglines

These are good for GitHub, social cards, headers, and production pages:

- `Hosted endpoint. Your workers.`
- `Hosted gateway. Your workers.`
- `One endpoint for your inference workers.`
- `Connect your workers. Serve any inference type.`
- `Your control plane for hosted inference entry points.`

## What To Avoid

Avoid these as primary homepage lines:

- `generic distributed execution`
- `global execution kernel`
- `flexible orchestration substrate`
- `edge-native orchestration fabric`
- `supports all inference types`

The last one is true, but too broad on its own.

Say the product first. Then say the breadth.
