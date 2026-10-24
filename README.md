# Cloud Agent Runtime

Reference implementation experiments for **one cloud Agent per user/session**, with canonical identity, durable workflow coordination, isolated execution and pluggable harnesses.

## Architecture and ownership

```text
Client / Application API
  -> Auth, tenant/project scope, quota, RBAC and approval
  -> Canonical Agent / Session / Run state
  -> Temporal workflow (durable timers, signals, retries, history)
  -> Harness adapter
  -> Replaceable Sandbox / Workspace
  -> Tools and capabilities (MCP / REST / browser / code / data / A2A)
  -> Artifacts + OTel traces + immutable evidence
  -> Hosted models or self-hosted vLLM / SGLang
```

The runtime owns **lifecycle binding and canonical references**, not workflow-engine semantics, Agent harness semantics, compute placement, or sandbox implementation. Temporal owns durable orchestration mechanics; the GPU compute platform owns compute admission/allocation/release. Integration across those boundaries uses public contracts rather than importing another project's internals.

## Implemented lifecycle slice

The repository includes canonical Run/Session contracts, an in-memory and SQLite reference store, filesystem artifacts, sandbox provider interfaces, a Temporal client/worker path, deterministic Workflow IDs, approval signal deduplication, pause/resume/complete/fail signals, `run_state` queries, and Temporal integration tests.

```text
Canonical Run ID
   -> deterministic Temporal Workflow ID
   -> pause / approval / resume / terminal signal
   -> canonical runtime state + artifact/evidence references
```

## Temporal lifecycle safety

Approved signals resume a waiting Workflow, but a manual pause requires an explicit resume; late approvals cannot mutate a terminal Workflow. `TemporalRunBridge.complete()` treats only a terminal `succeeded` Workflow as successful and rejects failed, cancelled or unknown terminal outcomes. These guards were merged through [#55](https://github.com/avaargsh/cloud-agent-runtime/pull/55) and [#56](https://github.com/avaargsh/cloud-agent-runtime/pull/56).

See [Temporal Worker](docs/temporal-worker.md), [Temporal adapter](docs/temporal.md), and the [recovery evidence matrix](docs/recovery-matrix.md) for behavior, test links, and limitations. In particular, pre-upgrade production Workflow History replay is not yet proven.

## Recovery and persistence

A sandbox is replaceable execution capacity. Its binding, provider reference, snapshot reference, replacement history, pending cleanup references and rebind idempotency key are recorded against the canonical Run. A replacement becomes canonical only after its binding is durably stored; cleanup of the retired sandbox happens after that commit. Failed or ambiguous cleanup remains pending until verified and reconciled.

Identity, state, workspace and execution persistence are independent guarantees. A durable Run row is not proof that a workflow can resume; a workflow history is not proof that workspace bytes survived; a snapshot reference is not proof that the referenced object exists. Each claim needs evidence from its owning subsystem.

See:
- [Runtime recovery matrix](docs/recovery-matrix.md)
- [Persistence model](docs/persistence.md)
- [Sandbox recovery](docs/sandbox-recovery.md)
- [Temporal worker](docs/temporal-worker.md) and [Temporal adapter](docs/temporal.md)
- [Evidence archive](docs/evidence-object-store.md) and [S3/MinIO compatibility](docs/s3-minio-evidence.md)
- [AgentOS v3.2 Golden Slice](docs/agentos-v32-golden-slice.md)

## Compute execution adapter (experimental)

The [compute execution adapter](docs/compute-execution.md) adds profile-based routing: ordinary Worker/Sandbox/Workspace profiles stay on the existing runtime path without a Compute call; GPU Workloads use the same canonical Session/Run identities and a durably persisted, immutable `ComputeRequest` idempotency key before the injected client crosses the side-effect boundary.

This is **not yet a production GPU integration**. The authenticated public Management API client, authoritative profile resolver, real PostgreSQL/Control Plane receipt reconciliation, and GPU provider acceptance are separate blockers. A returned `REQUESTED` or HTTP ACK is never promoted to `ALLOCATED` without independent ownership evidence.

Focused contract tests: `pytest -q tests/test_compute_execution.py`.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
python examples/local_runtime.py
python examples/otel_evidence_archive.py
```

## Current status and acceptance boundary

This is a public pre-1.0 lifecycle-binding reference. Local lifecycle, SQLite persistence, Temporal adapter contracts, OTel-to-Evidence projection and content-addressed evidence archives have reference implementations. An optional S3-compatible object-store adapter is covered by adapter-contract tests. The Golden Slice uses controlled disposable/test infrastructure; it is not a universal production sign-off.

P0 Run recovery is governed by [Issue #30](https://github.com/avaargsh/cloud-agent-runtime/issues/30) and the recovery matrix. Do not infer production acceptance from a PR description or an old green check: record the exact commit SHA, workflow URL, Python version and artifact for each test. A test against Temporal's SDK test service is not a test against the deployed Temporal cluster. A disposable kind/Calico cluster is not the production sandbox or network-isolation sign-off. Production provider side-effect ACK-loss, deployed-cluster recovery and production credential revocation remain separate acceptance rows until actually exercised.

PostgreSQL runtime-state persistence, production credential authority integration, production sandbox isolation, storage-side retention enforcement, warm pools, tenant quotas, and Kubernetes/GPU scheduling integrations remain roadmap work.

## Contributing and license

See `CONTRIBUTING.md`, `SECURITY.md`, and `CODE_OF_CONDUCT.md`. Licensed under Apache License 2.0; see [LICENSE](LICENSE).


## P0 Run recovery acceptance boundary

The canonical Run ID and deterministic Workflow ID must remain stable across ambiguous start acknowledgements and Runtime/Worker restarts. Cleanup intent remains pending until provider state independently confirms the sandbox is gone. See [docs/recovery-matrix.md](docs/recovery-matrix.md) for scenarios and explicit production evidence boundaries.
