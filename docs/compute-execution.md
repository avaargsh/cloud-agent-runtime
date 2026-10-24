# Compute execution adapter (experimental)

Status: **profile routing + authenticated Management API adapter + durable create/release intents; deployed cross-repository acceptance remains outstanding**. The client is implemented, but this does not claim GPU resources can be created/released in a production deployment.

## Contract boundary

The adapter targets `agent.execution-profile/v0.1`, `compute.request/v0.1`, and `compute.allocation/v0.1` from the reviewed [Agent Control Plane contract](https://github.com/avaargsh/agent-control-plane/pull/229). The profile passed to `ExecutionProfileRouter` must already have been resolved and schema-validated from an authoritative source. Never pass a profile document supplied directly by a browser or an untrusted Agent.

- `worker`, `sandbox`, and `workspace` with `accelerator.mode=none` return through the existing runtime path and never call the Compute client.
- `gpu-workload` requires `accelerator.mode=required`, a project/pool scope, and an immutable `name@sha256:<digest>` image.
- The request correlates to the existing `agentId`, `sessionId`, and `runId`. The Workload ID is deterministic per Session/Run; it does not replace Run or Session identity.
- A cryptographically random 128-bit create `requestId` and canonical `intentDigest` are stored on the canonical Run before the Compute create side effect. `RuntimeStore.persist_compute_request` serializes concurrent first writes and every retry must reuse the stored request/key.
- Release has its own durable 128-bit `requestId` (`compute.release-request/v0.1`) that is distinct from the immutable create request ID. Runtime reads the authorized Desired Generation through the same frozen route before persisting a first release intent; it then issues public Management DELETE with `If-Match` and reuses the same release key across retry/restart.
- Create and release request intents are persisted atomically with stale-save protection in InMemory/SQLite. Changed release source identity or Generation fails closed instead of mutating the stored release intent.
- A changed logical intent for the same Run fails closed. It cannot silently change the pool, profile revision, image, command, or accelerator demand under the old idempotency key.
- The returned allocation is checked against request/project/pool/workload identity. The adapter rejects `ALLOCATED` without provider references, `OwnershipProven=TRUE`, and evidence; it rejects `RELEASED` without `CleanupComplete=TRUE` and evidence.

## Authority and recovery

`ManagementComputeClient` implements the public authenticated Management API path behind injected `AccessTokenProvider`, `ComputePoolBindingResolver`, and optional `ComputeAllocationObserver` authorities. Create and DELETE both use owner-scoped OperationReceipt recovery, exact server `requestDigest` verification and authorized Desired-state checks. Release validates the server generation before freezing a first release intent and submits DELETE with `If-Match: "<expectedGeneration>"`.

An accepted DELETE receipt or `deleting=true` produces at most `RELEASING`. `RELEASED` is allowed only when an independent observer confirms every Workload-owned provider object is absent, the current generation was observed, `CleanupComplete=TRUE` is present, and cleanup/finalization evidence exists. A same-name or label-only match is not ownership proof. The client never falls back to internal Agent endpoints.

`UNKNOWN` means the caller cannot yet prove the outcome; it is not a failure that authorizes a new key. The client must reconcile the original receipt before retrying a possible side effect. Exactly-once provider side effects are not claimed; the design relies on idempotent desired-state submission, current-generation/UID fencing, and independent observation.

## Validation

Run:

~~~bash
pip install -e ".[dev]"
pytest -q tests/test_compute_execution.py
pytest -q
~~~

The focused tests cover no-GPU Worker routing, rejection of accidental accelerator requirements on non-GPU profiles, GPU create identity, digest-pinned images, SQLite restart, lost-ACK create/release recovery with the original keys, concurrent first dispatches, immutable-intent conflicts, stale Generation rejection, receipt digest mismatch, and refusing `RELEASED` while Desired state still exists.

## Explicit limits / next gate

- The HTTP adapter is implemented and tested against a local protocol server, but this is not a real shared PostgreSQL + authenticated Management API + kind/Kueue end-to-end acceptance.
- No production identity/profile resolver, project RBAC resolver, quota enforcement, or credential broker is wired in this module. Those remain upstream authority and security acceptance requirements.
- Create uses `expectedGeneration=0` because the current public Management API accepts first-generation create; release uses the server-issued positive Generation. Recreate after tombstone is not supported by this adapter.
- Run and Session continue to own agent lifecycle identity. ComputeAllocation remains an external, evidence-bearing projection rather than another copy of durable desired state. The v0.1 contract keeps `ComputeAllocation.requestId` as the original create identity; release ID remains separately persisted on Run/receipt until an explicitly reviewed wire-version change is accepted.
- The additive release envelope is under review in [ACP PR #248](https://github.com/avaargsh/agent-control-plane/pull/248); its tracking blocker is [ACP #247](https://github.com/avaargsh/agent-control-plane/issues/247).
