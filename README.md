# Cloud Agent Runtime

Reference architecture and implementation experiments for **one cloud Agent per user/session**, with durable state, isolated execution and pluggable harnesses.

## System model

```text
Client
  |
Application API
  |  Auth / Tenant / Billing / Quota / RBAC
  v
Agent Control Plane
  |  Profile / Session / Run / Policy / Approval
  v
Harness Adapter
  |  Codex / Claude / OpenCode / custom harness
  v
Sandbox
  |  warm pool / snapshot / resume
  v
Capabilities
  |  MCP / REST / browser / code / data / A2A
  v
State + Evidence
  |  PostgreSQL / S3 / OTel
  v
Model / Compute
     hosted APIs or self-hosted vLLM / SGLang
```

## Goals

- durable session identity
- isolated and resumable sandboxes
- provider-neutral harness adapters
- capability binding and policy enforcement
- approval / budget gates for consequential actions
- evidence, traces and replay
- multi-tenant quota and SLO controls

## Non-goals

- reimplementing every Agent SDK
- putting business state into Kubernetes CRDs
- assuming one model provider or one sandbox runtime
- treating chat history as the entire session state

## First vertical slice

```text
Create Session
  -> Allocate / Resume Sandbox
  -> Start Run
  -> Harness calls MCP Tool
  -> Persist Artifact
  -> Emit Trace + Evidence
  -> Complete / Pause
  -> Resume later
```

## Status

Private incubation repository. The first milestone is a local reference runtime with explicit state contracts before Kubernetes-scale work.
