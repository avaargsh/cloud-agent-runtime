# Durable Persistence and Event-Driven Execution Contract

Status: **contract proposal; implementation acceptance remains separate**.

This document freezes platform semantics for durable Agent work across runtime, workflow, workspace, and compute-provider lifecycles. It complements [cloud-agent-runtime issue #30](https://github.com/avaargsh/cloud-agent-runtime/issues/30) and [gpu-compute-platform issue #116](https://github.com/avaargsh/gpu-compute-platform/issues/116).

## Core rule

A Pod, VM, Worker, sandbox, or model-serving process is replaceable execution capacity. The platform must preserve the logical task, authority, recoverable state, and evidence—not a particular process.

Four kinds of persistence are independent and must never be inferred from one another.

| Contract | What stays durable | What may be replaced | Authoritative mechanism |
| --- | --- | --- | --- |
| Identity persistence | Agent, Session, Run, task, and parent/child relationships | Temporal execution Run ID, Pod/VM ID, Worker process, provider attempt ID | Canonical runtime/control-plane records |
| State persistence | Lifecycle status, event cursor, checkpoint/artifact references, approval outcome, retry/idempotency metadata | In-memory objects and caches | Transactional RuntimeStore plus transition evidence |
| Workspace persistence | User-requested files, source changes, checkpoints, and generated artifacts | Container filesystem, ephemeral volume, sandbox image | Durable workspace/artifact backend plus content digest and version |
| Execution persistence | Pending workflow decisions, durable timers, waits, signal deduplication, retry state, and terminal result | Workflow Worker and individual Activity process | Temporal history plus idempotent Activities and reconciliation |

A snapshot reference is not proof that the referenced bytes exist. A durable Run row is not proof that its workflow can resume. A replayable workflow is not proof that a container filesystem survived. Every claim needs evidence from the owning subsystem.

## Ownership boundaries

- **Canonical runtime / control plane:** owns stable domain identities, tenant and project scope, Run/task status, approvals, desired execution intent, idempotency keys, and subsystem correlations.
- **Temporal:** owns durable orchestration mechanics—workflow history, timers, waits, signal handling, child-workflow coordination, and retry scheduling. Temporal IDs are references, not canonical Agent or Run identities. Do not put secrets, large payloads, or the application database into workflow history.
- **Workspace and artifact storage:** owns bytes, versions, retention policy, and integrity verification. Runtime records references and digests, not large file payloads.
- **GPU compute platform:** owns ComputePool/Workload intent, quota and admission, Provider Adapter dispatch, observed allocation state, release/finalization, and provider evidence. The current Kueue-only Golden Path remains the admission path.
- **Harness / Agent process:** executes a bounded attempt and may be killed at any point. It must not be the source of truth for identity, approvals, billing, or recovery decisions.

Do not import GPU platform internals into the Agent Runtime. Use a public, versioned compute request/allocation contract and the platform's authorized API. Conversely, the GPU platform must not become an Agent task scheduler or own workflow semantics.

## Event and task contract

Every externally meaningful transition uses a durable, versioned event envelope. At minimum, the canonical event records:

- event ID and schema version;
- event type and actor type/ID;
- tenant and project scope;
- Agent, Session, Run, and task IDs where applicable;
- correlation ID and causation ID;
- idempotency key or stable domain event ID;
- occurrence and recording timestamps;
- previous and next state, or a typed outcome;
- references to approval, artifact, compute operation, provider observation, trace, and evidence records as applicable.

Event payloads must avoid credentials and unbounded data. Put large content in durable storage and retain an authorized reference plus content digest. Duplicate delivery is expected; consumers deduplicate by stable event identity and must not repeat a logical side effect.

### Triggers and multi-Agent coordination

- **External events:** persist the event or inbox record before acknowledging receipt; dispatch through the durable workflow path. Retried webhook/message delivery must resolve to the original logical operation.
- **Schedules and timers:** use durable workflow timers or a scheduler that persists intended firing identity. A repeated firing must not create a second logical task. Define missed-fire and catch-up policy explicitly.
- **Human approval:** persist the request, authorized approver, scope, expiry, and decision before resuming the workflow. A signal is a wake-up mechanism, not the approval system of record. Reject decisions for the wrong Run/task, expired approvals, replayed request IDs, or terminal work.
- **Subtasks / child Agents:** assign stable child task and Run IDs and explicit parent-child links. Bound depth, concurrency, timeout, delegation scope, and budget. Propagate cancellation only under an explicit policy; one child failure must not accidentally mark siblings successful.
- **Retries:** separate a retry of the same external operation from a deliberate new attempt. The same operation retry retains the idempotency key; a new attempt receives a new attempt identity while preserving its parent logical task identity.

The existing Temporal adapter is the first workflow integration to harden. Do not add a second workflow engine unless a documented evaluation proves a requirement Temporal cannot meet.

## Decouple waiting work from GPU allocation

A Temporal Task Queue is a Worker dispatch mechanism; Kueue is a compute quota/admission mechanism. They are different queues with different semantics.

1. A workflow decides **whether and when work is eligible to run**.
2. A compute request expresses **what capacity is needed** and carries tenant/project/Run/task correlation.
3. The GPU platform admits and reconciles the request through its canonical Workload lifecycle and registered Provider Adapter.
4. Runtime waits on observed allocation/execution evidence; it must not treat HTTP success, a submitted Workload, or an admission receipt as GPU readiness or task success.
5. When work enters an external wait (approval, timer, or event) and no GPU is needed, checkpoint required state and artifacts, then release the active allocation. Do not hold expensive capacity merely to keep a workflow alive.
6. When the trigger arrives, request capacity again under a new allocation generation/attempt as appropriate, while retaining Agent/Session/Run/task identity and workspace references.
7. Release is complete only after independent provider observation confirms workload-owned resources are gone and release/finalization evidence is durably recorded.

For one logical compute operation, the original request ID and idempotency key must survive timeout, lost ACK, caller restart, and reconciler takeover. A deliberate re-acquisition is a new demand operation, not a retry of the released allocation.

### Cost attribution

Every compute request, allocation, release, and usage record must carry stable attribution to tenant, project, Agent, Session, Run, task, ComputePool, provider, and allocation generation/attempt. Record usage source, measurement interval, currency/unit, and whether the value is measured or estimated. Waiting time without allocated capacity must not be billed as active GPU usage by inference. Missing usage evidence is **unknown**, not zero. Duplicate provider observations must not double count usage; correction records must be auditable.

## Workspace contract

Workspace durability is independent from sandbox durability. Before a Run can be recovered onto a replacement Pod/VM, each artifact required to continue must be stored outside the disposable execution filesystem or referenced by a separately durable volume.

A workspace checkpoint must include a stable workspace identity, version/revision, storage reference, content digest, creation cause, and owning tenant/project/Agent/Session/Run scope. Recovery must verify existence, authorization, and digest before exposing content to a replacement sandbox. Distinguish:
- ephemeral scratch that is intentionally disposable;
- restorable checkpoint data required to resume an operation;
- user-owned deliverables that must survive Run completion and sandbox deletion.

Credential material is not a workspace artifact. Restore workload identity and short-lived credentials through the credential/authorization path rather than copying secrets into a workspace snapshot.

## Failure and recovery acceptance matrix

| Failure injection | Required result | Evidence required |
| --- | --- | --- |
| API process is killed after Run persistence but before workflow-start ACK | Restart reuses the original Run ID and deterministic Workflow ID; it attaches or reconciles rather than creating a second logical Run | persisted Run, workflow describe/result, stable IDs, duplicate-start assertion |
| Workflow start is accepted remotely but the caller loses the response | Retry uses the same deterministic identity; a terminal or running workflow is not silently reused for a new logical Run | original operation identity and retry provenance |
| Worker exits during a long wait | Workflow resumes from durable history; timer/approval/event is not lost or applied twice | Temporal history/replay plus state-store reconciliation |
| Duplicate or out-of-order event arrives | Deduplication and transition rules leave one valid outcome; terminal state remains immutable | event IDs, transition audit, negative assertion |
| Approval is delayed, duplicated, expired, or belongs to another task | Only a valid, correctly scoped decision resumes work | persisted approval and authorization decision |
| Subtask fails or parent is cancelled | Parent/child outcomes follow the documented propagation policy; every started child is terminal or recoverably pending | parent-child links and per-child transition history |
| GPU request loses ACK after provider accepted create | Same request converges on the same provider resource; no duplicate logical allocation | original request ID, provider identity/generation, observation evidence |
| Run waits without needing GPU | Active workload resources are released and independently observed gone; workflow remains durable | checkpoint digest, release operation, provider deletion and finalization evidence |
| External event resumes the Run | Same logical Run/task continues and a new allocation attempt may be acquired only when needed | event causation, unchanged logical IDs, allocation attempt/generation |
| Pod/VM is destroyed | Replacement execution sees verified required workspace/artifact content, while sandbox identity may change | old/new sandbox refs, digest verification, cleanup evidence |
| Release succeeds remotely but ACK or database commit is lost | Cleanup is retryable and converges to Gone; ownership is not finalized before required evidence is recorded | retry history, Gone observation, audit and tombstone/finalization state |
| A usage report is repeated or arrives late | Billing stays deduplicated and corrections are traceable; missing measurement remains unknown | attribution tuple, usage source, dedup key, correction record |

## Delivery order

1. **P0 — Freeze identity, state, idempotency, and lifecycle semantics.** Finish the recovery matrix in issue #30 first, including caller/process restart, lost ACK, cleanup recovery, unknown provider status, and terminal identity reuse.
2. **P0 — Prove the compute Golden Path.** Preserve the GPU platform boundary: authenticated request, accepted receipt, Kueue lifecycle, independent observation, release, and recovery on the exact tested SHA. Keep provider selection immutable and Kueue as the only registered production path until another provider independently passes acceptance.
3. **P1 — Durable event primitives.** Add event inbox/deduplication, durable timers, approval resumption, and child-task coordination on the existing Temporal integration. Keep application state in RuntimeStore; do not use workflow history as the application database.
4. **P1 — Durable workspace recovery.** Establish workspace storage API, checkpoint/version/digest contract, authorization checks, and replacement-Pod recovery test. Local filesystem storage remains a development adapter, not proof of cross-host durability.
5. **P1 — Cross-platform compute bridge.** Add a versioned adapter from workflow demand to public ComputeRequest/ComputeAllocation contracts; handle wait, acquire, observe, release, restart, and reconciliation without importing provider internals.
6. **P2 — End-to-end cost and audit.** Join request, allocation, provider observation, artifact digest, trace, and usage evidence using stable IDs. Mark unverified production dependencies as blocked, not passed.

## Merge gates and non-claims

- Unit tests are necessary but insufficient. Recovery requires Temporal integration tests, explicit process-kill/restart tests, durable-store restart, and provider-side lost-ACK/reconcile tests.
- GPU release and workspace persistence require a disposable real cluster/storage acceptance path; mocked provider tests do not prove NetworkPolicy, CSI/PVC, object-store retention, or actual device release.
- Every acceptance report names the exact commit SHA, commands, results, and unavailable checks. Skipped/unavailable environments are **NOT RUN / BLOCKED**, never PASS.
- This contract does not claim PostgreSQL RuntimeStore, durable cross-host workspace storage, production billing, or complete real-provider crash recovery until those implementations and evidence exist.
- Do not add a custom scheduler, duplicate GPU provider lifecycle, new Agent SDK, or second workflow engine as a shortcut.
