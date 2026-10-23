# P0 Run Recovery Acceptance Matrix

**Scope:** [Issue #30](https://github.com/avaargsh/cloud-agent-runtime/issues/30), Run identity and recoverable execution only. This matrix is the durable scenario contract; current execution results are in the exact-SHA GitHub Actions artifacts linked from the issue and pull request.

## Evidence rules

- A passing test result applies only to the commit SHA executed by that workflow. For pull requests, record both the PR head SHA and the Actions checkout SHA; the latter may be a synthetic merge commit.
- The dedicated `p0-run-recovery-acceptance` workflow emits an artifact per Python version. The artifact must include exact checkout SHA, Python version, command/job status, and, when executed, the machine-readable Temporal ACK-drop evidence JSON.
- An assertion backed only by an in-memory driver is contract/unit evidence, not proof of a remote provider effect.
- A timeout, reset, lost response, or process death after a remote call was sent is **ambiguous** unless provider state is authoritatively observed. Keep the original Run, operation identity and resource references; do not create a replacement Run or Workflow to resolve ambiguity.
- Client operation IDs correlate retries and audit records. They do not by themselves prove provider-side exactly-once behavior.
- Status labels: **PASS** only when the stated test actually ran and all its assertions passed at the recorded SHA; **PARTIAL** when only a test double/limited integration boundary is covered; **NOT RUN** when not executed; **BLOCKED** when the required external provider/target environment is unavailable. No status is inherited automatically by later commits.

## Scenario matrix

| ID | Scenario / fault boundary | Required invariant and evidence | Acceptance boundary |
|---|---|---|---|
| R01 | Temporal accepts StartWorkflow; response frames are dropped by a TCP/HTTP2 proxy; caller is blocked then SIGKILLed; a new process opens SQLite | Proxy log proves request-forward and response-drop; caller exit is SIGKILL; same Run ID, stable `agent-run-start:<run_id>` operation ID and deterministic Workflow ID are recovered; history has exactly one WorkflowExecutionStarted event; duplicate start is rejected | Dedicated ACK-drop integration against Temporal SDK test service. Must not be called production Temporal acceptance. |
| R02 | Caller dies after provider accepts start but before local WorkflowRef is durably bound | New process reopens the same store and reconciles by original Run ID/Workflow ID; no second start; audit has durable pre-call intent and post-restart attach evidence | Separate caller-process + SQLite + Temporal integration test |
| R03 | Provider Describe/query cannot establish whether a start was accepted | Run stays `unknown`; recovery records a failed closed observation; no replacement workflow is created | Unit/driver tests are necessary but cannot replace the R01/R02 remote fault experiment |
| R04 | Duplicate start attaches to a running Workflow; deterministic ID already points to a terminal Workflow; provider status is absent | Running execution can be attached only after status verification; terminal/unknown status fails closed | Temporal driver tests on exact SHA |
| R05 | Wall-clock budget timeout fires while Run is active | Timeout is owned by Temporal durable history; stable timeout event ID is recorded; terminal state is projected back to canonical Run and survives restart/reconciliation | Temporal workflow/runtime integration tests on exact SHA |
| R06 | Cancellation request or approval/signal is retried after response failure | Cancellation intent is persisted before signalling; same event ID, reason and evidence are reused; duplicate signal effects/transitions are deduplicated | Runtime/Temporal integration tests; record any live worker-process replay test separately |
| R07 | Kubernetes accepts Pod deletion, but caller observes a timeout/lost ACK | Keep the sandbox ref pending and retain the same `sandbox-cleanup:<run_id>:<sandbox_ref>` operation ID until independently observed Gone; after Runtime/SQLite/Provider reconstruction, retry the same cancellation intent; evidence records `intent_persisted → unknown → intent_persisted → observed_gone → acknowledged` with logical attempt numbers `1,1,2,2,2` | Disposable-kind test exercises real Kubernetes API side effect + injected post-effect timeout; not production-cluster acceptance |
| R08 | Cleanup reference belongs to unknown/different Provider, or independent provider read cannot establish Gone | Do not call the wrong Provider or drop the pending reference; persist a blocked/unknown recovery record; retry must not silently claim cleanup | Negative/unit tests; separately mark live provider behavior |
| R09 | Sandbox allocation succeeds but binding/persistence fails | Retain enough allocation identity to verify/clean up without allocating a second sandbox; persist recovery evidence if the outcome is ambiguous | Unit tests may cover rollback/best-effort cleanup; durable external provider proof stays PARTIAL/NOT RUN unless executed against that provider |
| R10 | Pause/resume persistence fails | Durable Run state and provider state cannot silently diverge; original binding/reference remains available for later recovery | Unit tests are not process/provider-level failure proof |
| R11 | Worker A process is SIGKILLed while Workflow is waiting; signals are recorded while no Worker polls; Worker B restarts | Same Workflow/Run/Session identities and history survive replay; duplicate signals have one domain effect; final result and transition history are consistent | Separate OS-process Worker A/B integration test; record both Python-version outcomes |
| R12 | Reconstruct Runtime and SQLite store after ambiguous start or cleanup | Canonical identities, operation ID, recovery status, attempts and evidence survive serialization/reopen; retry does not allocate/start a replacement | SQLite reconstruction tests; each failure injection remains a separate row |
| R13 | External business Provider applies a non-sandbox side effect but ACK is lost | Retry uses the same provider operation identity; authoritative provider read or documented idempotency contract determines the outcome before another mutation | **NOT RUN** until an actual target Provider and its read/idempotency contract are available |
| R14 | Live deployed Temporal cluster response is dropped after server-side acceptance | Same assertions as R01, using the target Temporal service/namespace and exported provider history | **NOT RUN** unless run against the deployed Temporal environment |

## Per-run evidence checklist

For each run, retain these fields in the CI artifact or linked issue comment:

- PR head SHA and exact Actions checkout SHA, workflow run URL, Python version, command and final job result.
- Canonical Run ID, Session ID, deterministic Workflow ID, start/cleanup operation IDs and provider execution identity where the provider exposes it.
- Proxy event records proving request-forward / response-drop, caller exit signal, reconciliation result, WorkflowExecutionStarted count and duplicate-start rejection for R01.
- Cleanup intent/result records with the same operation ID, logical attempt number, independent Gone observation, and the pending-reference state before/after retry.
- Explicit `NOT RUN` or `BLOCKED` for every scenario that the workflow did not execute or that requires an unavailable production provider/cluster.

## Current production boundary

A Temporal SDK test server with a TCP proxy and an OS-level caller SIGKILL proves the injected client fault against that test service. A disposable-kind Kubernetes API test proves the sandbox adapter's cleanup recovery against that disposable cluster. Neither proves behavior against the deployed production Temporal cluster, production sandbox provider, or production network-isolation policies.

Issue #30 remains open until all code/CI rows have exact-SHA evidence and production-specific rows are either separately executed or explicitly accepted as blocked by the required provider/cluster contract. P1 credential broker/security workspace work and P2 multi-Agent/GPU scheduling are outside this matrix.
