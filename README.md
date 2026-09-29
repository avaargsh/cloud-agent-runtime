# Cloud Agent Runtime — Lifecycle Binding Experiments

Thin integration experiments for **durable orchestration ↔ isolated Agent sandbox execution**.

> **Project direction (2026-09-29): do not build another Durable Agent Runtime.**
> Temporal / Restate should own durable execution. Kubernetes Agent Sandbox (or another sandbox provider) should own isolated execution. This repository now focuses only on the small binding/recovery gap between them.

## Boundary

```text
Application / Agent semantics
          |
          v
Temporal / Restate
  durable workflow state
          |
          | LifecycleBinding
          v
Kubernetes Agent Sandbox
  logical workspace identity
          |
          v
gVisor / Kata / Kubernetes compute
```

The useful contract is intentionally small:

- bind a durable workflow/run identity to a logical sandbox;
- release compute while preserving recoverable workspace state;
- restore/rebind after worker, pod, node or sandbox replacement;
- verify workspace/artifact/version/credential invariants before continuation;
- preserve idempotency so recovery does not repeat external side effects.

## Non-goals

This project does **not** aim to implement:

- a new durable workflow engine or event log;
- an AgentRun controller/state machine;
- retry, checkpoint, approval, cancellation or scheduling engines;
- a sandbox controller or warm-pool manager;
- a generic Agent control plane;
- PostgreSQL/S3 as a second source of truth for workflow lifecycle;
- Kueue/Volcano/GPU scheduling.

Those concerns belong to Temporal/Restate, Kubernetes Agent Sandbox, Kubernetes/cloud IAM, object storage and the surrounding application platform.

## Existing experiments

The repository contains earlier reference experiments for canonical Run/Session contracts, SQLite persistence, local sandbox lifecycle and a Temporal workflow bridge. They are retained as **experimental evidence**, not as the product boundary.

The Temporal path already demonstrates the key architectural point:

```text
canonical application Run ID
          ↓
Temporal Workflow
          ↓
wait / signal / retry / resume
          ↓
sandbox activity / binding
          ↓
artifact + evidence refs
```

Durability stays in Temporal. The next work is to make sandbox binding/recovery explicit without duplicating Temporal semantics.

## Target Golden Path

```text
Workflow starts
  -> bind logical sandbox
  -> exec
  -> persist artifact/workspace ref
  -> release compute
  -> wait for human (zero sandbox compute)
  -> allocate replacement sandbox
  -> restore + validate binding invariants
  -> exec
  -> complete
```

Acceptance must prove:

1. workflow identity survives worker restart;
2. sandbox compute can disappear while the workflow waits;
3. a replacement sandbox restores the expected workspace/artifacts;
4. credentials are reissued with the correct scope rather than blindly persisted;
5. candidate/release version is unchanged or explicitly migrated;
6. completed side effects are not repeated after recovery.

## Repository role in the open-source stack

- **agentic-aiops** — Agent-specific operational workflow: evidence → decision → policy → approval → action → verification.
- **agent-decision-lab** — bounded decision benchmark/gateway and confidence-gated escalation.
- **cloud-agent-runtime** — thin durable-workflow ↔ sandbox binding experiments only.
- **temp-runner** — disposable cross-repository E2E harness; no product code.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

For the existing Temporal experiment, see `docs/temporal-worker.md` and `docs/temporal.md`.

## Status

Public pre-1.0 architecture/compatibility experiment. The project is intentionally shrinking its ownership surface: **adopt durable execution and sandbox runtimes; implement only the binding semantics that cannot naturally live upstream.**

Licensed under Apache License 2.0. See `LICENSE`.
