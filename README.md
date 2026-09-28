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

## Implemented durable slice

The repository now includes canonical Run/Session contracts, SQLite reference persistence, filesystem artifacts, sandbox lifecycle interfaces, a Temporal client/worker path, deterministic Workflow IDs, approval signal deduplication, pause/resume/complete/fail signals, `run_state` queries, and Temporal integration tests.

```text
Canonical Run ID
      ↓
Temporal Workflow
      ↓
pause / approval signal / resume
      ↓
provider-neutral runtime state
      ↓
complete + evidence refs
```

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
python examples/local_runtime.py
```

For the Temporal worker and durable workflow path, see `docs/temporal-worker.md` and `docs/temporal.md`.

## Status

Public pre-1.0 reference runtime. The local runtime and Temporal durable lifecycle are implemented; PostgreSQL/S3 adapters, production sandbox isolation, warm pools, tenant quotas, and Kubernetes/GPU scheduling integrations remain roadmap work.

This repository is the **Durable Action Plane** used by `agentic-aiops`; bounded decisions are provided by `agent-decision-lab`.

## Contributing and license

See `CONTRIBUTING.md`, `SECURITY.md`, and `CODE_OF_CONDUCT.md`.

Licensed under Apache License 2.0. See `LICENSE`.
