# P0 Run Recovery Acceptance Matrix

Scope: Issue #30 — canonical Run identity and recoverable execution. This document separates automated test evidence from production-provider acceptance.

## Evidence rules

- CI results are valid only for the exact checkout SHA shown by the workflow. Historical green results do not transfer to later commits.
- Report PR head SHA and the actual Actions checkout SHA separately when the workflow tests a synthetic merge commit.
- A timeout, lost response, connection reset, or caller death after request dispatch is an **ambiguous outcome** until the owning provider is queried authoritatively.
- Keep the original Run, deterministic Workflow ID, provider reference, and operation identity. Do not create a replacement workflow or sandbox solely to resolve ambiguity.
- Stable client operation IDs are useful for correlation and audit; they do not by themselves prove provider-side exactly-once behavior.
- Use **PASS** only for a test that actually ran and passed on the recorded SHA. A test double or SDK test service is limited-boundary evidence, not production proof. Use **NOT RUN** or **BLOCKED** for unexecuted external scenarios.

## Recovery matrix

| Scenario | Required invariant | Evidence boundary / status |
|---|---|---|
| StartWorkflow request reaches Temporal but ACK is dropped; caller is killed | Reopen the same store; reconcile the original Run and deterministic Workflow ID; exactly one start event; no duplicate start | Full local Temporal development server + HTTP/2 response-drop proxy exercise this client boundary; production Temporal remains separate |
| Caller dies after remote start but before local WorkflowRef bind | The same Run and operation identity survive process restart; recover by deterministic Workflow ID | Require process-level test and exact-SHA evidence |
| Provider status cannot be established | State remains unknown; observe/reconcile again; do not start another workflow | Fail-closed behavior must be covered by tests |
| Duplicate attach to running, terminal, or unknown workflow | Attach only after status is verified; terminal/unknown states fail closed | Temporal driver tests; live provider coverage separate |
| Wall-clock timeout or cancellation | Timeout is durable in workflow history; terminal state and stable event identity survive projection/restart; cancellation is never reported as success | Test-environment evidence is not a production rollout claim |
| Approval/signal is retried after an acknowledgement failure | Same approval/event identity and reason; duplicate delivery has one logical effect; conflicting retries fail closed. Late approval cannot overwrite terminal state; approval cannot implicitly override a manual pause. | Runtime/Temporal tests on exact SHA; production history replay requires separate verification |
| Completion is retried, or a signal races with cancellation/failure | Only `terminal=True` and `phase=succeeded` satisfy successful completion; failed/cancelled/unknown terminal phases raise a conflict rather than returning success. | Temporal bridge regression tests; no claim that prior completed result payload equals a retried payload |
| Kubernetes deletes a Pod but cleanup ACK is lost | Keep cleanup pending until independent observation confirms Gone; retry the same operation identity; record logical attempt evidence | Disposable kind can prove behavior against its Kubernetes API; not production-cluster acceptance |
| Cleanup provider is unknown or Gone cannot be independently verified | Do not call another provider; preserve pending reference and unknown/blocked state | Negative tests plus provider-specific acceptance |
| Sandbox allocation/binding or persistence fails | Retain allocation identity to verify/clean up; never silently lose an orphan or allocate a replacement before reconciliation | Unit/provider-boundary proof is not equivalent to production-provider proof |
| Worker process is killed while waiting and another Worker starts | Run/Session/Workflow identities and history remain stable; signal deduplication and replay yield consistent terminal evidence | Requires independent Worker process-replacement test on exact SHA |
| External business provider applies a side effect but ACK is lost | Reconcile authoritative provider state or use its documented idempotency contract before retrying mutation | NOT RUN until an actual target provider and its authoritative read/idempotency contract are available |
| Production Temporal accepts start but its response is dropped | Same assertions as SDK-service test against the deployed namespace and server-side history | NOT RUN unless explicitly executed against deployed Temporal |
| Cross-tenant sandbox network access / production credential revocation | Enforced network policy and authoritative credential/approval revocation, independently observed after restart | Disposable Calico/kind or synthetic authority adapter are integration evidence only; production acceptance remains separate |

## Invariants

1. `run_id` is platform-owned and never changes during sandbox recovery.
2. Workflow identity is a durable orchestration reference; sandbox identity is replaceable execution capacity.
3. A replacement sandbox becomes canonical only after its binding is durably stored.
4. Retired-sandbox cleanup happens after the replacement binding is durable.
5. Pending cleanup references are a durable retry queue, not best-effort telemetry.
6. Replaying the same idempotency/rebind key must not allocate a second replacement.
7. Remote side effects must be reconciled after ambiguous acknowledgement; do not infer success from a client timeout or a client operation ID.
8. Identity, state, workspace, and execution persistence are independent contracts and require separate evidence.

## Current evidence boundary (2026-10-24)

- PR [#55](https://github.com/avaargsh/cloud-agent-runtime/pull/55) was squash-merged as `3ade0a499bda867e86dc0e133fae3e513d0a2e11`; its head `19e9a552c9743c6c017f088394999bd003065eb3` passed the [Python 3.11/3.12 suite](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/40060623448) and [disposable Golden Slice](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/40060623421).
- PR [#56](https://github.com/avaargsh/cloud-agent-runtime/pull/56) was squash-merged as `b5b32ee410a58bcfc30da341c6928e8ace6f8d8a`; its head `0b1cd2b00980bde3b967760c69429a1b09afafb6` passed the [Python 3.11/3.12 suite](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/40060913065) and [disposable Golden Slice](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/40060913109).
- The combined main checkout `b5b32ee410a58bcfc30da341c6928e8ace6f8d8a` passed [Python 3.11/3.12 tests](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/40061222806). The preceding intermediate main checkout `3ade0a499bda867e86dc0e133fae3e513d0a2e11` had one [Python 3.12 subprocess-recovery failure](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/40061188191): SQLite reconciliation evidence was printed, but the subprocess exited with signal 11. That incident remains a test-stability risk and must not be erased by the later green run.
- CI uses a disposable **full local Temporal development server** for the process recovery / ACK-drop tests; other timer-oriented tests may still use the SDK's legacy time-skipping server. Disposable Kubernetes is separate. A historic production Workflow History replay across these implementation versions, deployed Temporal transport ACK-loss, external-provider side-effect ACK-loss, and production cluster/security enforcement are **NOT RUN / BLOCKED** until exercised in their target environments.

Issue #30 remains open until the scenario-by-scenario acceptance matrix and remaining provider-specific boundaries are explicitly resolved. A merged bug-fix PR or green unit/integration matrix is not a production sign-off.

**Python 3.12 subprocess-exit stability:** Intermediate and final main CI recorded exit -11 after successful SQLite `attached` output. For the non-timer R01/R02 tests, use the full local Temporal development server rather than the legacy Java time-skipping server; retain fatal-signal tracebacks and repeated real subprocess checks. Exact-head PASS is a test-harness mitigation, **not** proof of the native crash's root cause or production provider acceptance.
