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

- canonical session/run identity across provider lifecycles
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

## Implemented lifecycle-binding slice

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

## Durable sandbox recovery

Sandbox execution identity is explicitly disposable. Each Run persists a
`SandboxBinding` with provider identity, binding revision, snapshot reference,
replacement history, retryable cleanup refs and an idempotency key for rebind.

This makes the recovery contract survive a runtime-process restart:

```text
Run / Workflow identity stays stable
        |
Sandbox A -> snapshot/loss
        |
persisted SandboxBinding
        |
Sandbox B -> rebind/resume
        |
artifact/evidence refs preserved
```

Provider cleanup happens after the new binding is durable; failed cleanup remains
recorded and retryable rather than being hidden.

## Evidence archive boundary

The v3.2 evidence path is implemented as a separate execution-evidence boundary rather than a second tracing database:

```text
OTel exported span
      ↓
EvidenceEvent + provenance
      ↓
AppendOnlyEvidenceCollector
      ↓
EvidenceArchive
      ↓
content-addressed evidence object + immutable manifest
      ↓
digest-verified replay
```

Evidence can carry canonical Run/Action identity, OTel trace/span identity, policy and artifact digests, and generated/observed/authorized/executed/committed provenance. `S3ObjectStore` provides the optional production-facing adapter for AWS S3 and S3-compatible endpoints such as MinIO or Ceph RGW. The live lifecycle Golden Slice intentionally validates semantics with the in-memory ObjectStore implementation; S3 compatibility is covered by adapter contract tests.

See `docs/evidence-object-store.md`, `docs/s3-minio-evidence.md`, and `docs/agentos-v32-golden-slice.md`.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
python examples/local_runtime.py
python examples/otel_evidence_archive.py
```

For the Temporal worker and provider-owned durable workflow path, see `docs/temporal-worker.md` and `docs/temporal.md`.

## Status

Public pre-1.0 lifecycle-binding reference. Canonical identity, local and Kubernetes sandbox recovery primitives, a Temporal adapter, OTel-to-Evidence projection, content-addressed EvidenceArchive, S3/MinIO/Ceph-compatible object storage, and policy-digest evidence binding are implemented. Temporal owns durable orchestration and workflow history. PostgreSQL runtime-state persistence, production sandbox isolation, storage-side retention enforcement, warm pools, tenant quotas, and Kubernetes/GPU scheduling integrations remain roadmap work.

This repository is a thin **Run ↔ Workflow ↔ Sandbox lifecycle binding layer**. It does not own durable orchestration, placement policy, or sandbox implementation semantics. Bounded decisions are provided by `agent-decision-lab`.

## Contributing and license

See `CONTRIBUTING.md`, `SECURITY.md`, and `CODE_OF_CONDUCT.md`.

Licensed under Apache License 2.0. See `LICENSE`.


## P0 Run recovery acceptance boundary

This reference runtime distinguishes a confirmed provider result from an ambiguous remote outcome.

- The canonical Run ID is persisted before workflow start.
- Workflow start and sandbox retirement each use a deterministic operation ID scoped to the owning Run.
- When a start request may have reached Temporal but its acknowledgement is lost, the Run records `unknown`; recovery must query/attach to the deterministic Workflow ID and must not start a replacement workflow.
- Cleanup intent and each retry outcome are persisted with the same cleanup operation ID. A lost cleanup acknowledgement keeps the old sandbox reference in `pending_cleanup_refs` until a retry receives a verifiable acknowledgement.
- A fake/in-memory provider test is not evidence of an OS-level process-kill or a live Temporal server fault test. Those scenarios are tracked separately in `docs/recovery-acceptance.md`, where unexecuted rows remain `NOT RUN`.

Do not claim exactly-once external side effects unless the provider exposes a verifiable idempotency contract. Stable client operation identity alone does not prove provider-side deduplication.
