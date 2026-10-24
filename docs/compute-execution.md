# Compute execution adapter (experimental)

Status: **adapter contract + deterministic routing prototype**. This is not yet a production Compute client and does not claim that GPU resources can be created in a deployed platform.

## Contract boundary

The adapter targets `agent.execution-profile/v0.1`, `compute.request/v0.1`, and `compute.allocation/v0.1` from the reviewed [Agent Control Plane contract](https://github.com/avaargsh/agent-control-plane/pull/229). The profile passed to `ExecutionProfileRouter` must already have been resolved and schema-validated from an authoritative source. Never pass a profile document supplied directly by a browser or an untrusted Agent.

- `worker`, `sandbox`, and `workspace` with `accelerator.mode=none` return through the existing runtime path and never call the Compute client.
- `gpu-workload` requires `accelerator.mode=required`, a project/pool scope, and an immutable `name@sha256:<digest>` image.
- The request correlates to the existing `agentId`, `sessionId`, and `runId`. The Workload ID is deterministic per Session/Run; it does not replace Run or Session identity.
- A cryptographically random 128-bit `requestId` and canonical `intentDigest` are stored on the canonical Run before the Compute client side effect. `RuntimeStore.persist_compute_request` serializes concurrent first writes and every retry must reuse the stored request/key.
- A changed logical intent for the same Run fails closed. It cannot silently change the pool, profile revision, image, command, or accelerator demand under the old idempotency key.
- The returned allocation is checked against request/project/pool/workload identity. The adapter rejects `ALLOCATED` without provider references, `OwnershipProven=TRUE`, and evidence; it rejects `RELEASED` without `CleanupComplete=TRUE` and evidence.

## Authority and recovery

The injected `ComputeResourceClient.submit_or_reconcile` is a protocol seam. A production implementation must use the authenticated public Management API, pass `requestId` unchanged as `Idempotency-Key`, query the owner-scoped operation receipt after ambiguous outcomes, compare its server-generated request digest/resulting generation with authorized Desired state, and then read independent provider observations. It must not call internal Agent endpoints as a fallback and must not translate an HTTP ACK into `ALLOCATED` or `RELEASED`.

`UNKNOWN` means the caller cannot yet prove the outcome; it is not a failure that authorizes a new key. The client must reconcile the original receipt before retrying a possible side effect. Exactly-once provider side effects are not claimed; the design relies on idempotent desired-state submission, current-generation/UID fencing, and independent observation.

## Validation

Run:

~~~bash
pip install -e ".[dev]"
pytest -q tests/test_compute_execution.py
pytest -q
~~~

The focused tests cover no-GPU Worker routing, rejection of accidental accelerator requirements on non-GPU profiles, GPU request identity, digest-pinned images, SQLite restart preserving the request ID, lost-ACK retry with the original key, concurrent first dispatches, immutable-intent conflicts, stale writes, and mismatched allocation responses.

## Explicit limits / next gate

- No real authenticated Management API HTTP adapter is implemented here yet. The injected client in tests is a contract test double, not evidence of an end-to-end GPU submission.
- No production identity/profile resolver, project RBAC resolver, quota enforcement, or credential broker is wired in this module. Those remain upstream authority and security acceptance requirements.
- The request uses `expectedGeneration=0` because the current public Management API accepts first-generation create; recreate after tombstone is not supported by this adapter.
- Run and Session continue to own agent lifecycle identity. ComputeAllocation remains an external, evidence-bearing projection rather than another copy of durable desired state.
