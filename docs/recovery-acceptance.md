# P0 Recovery Acceptance Matrix

Scope: Issue #30 only. Do not use this matrix to claim production acceptance from mocks or Temporal's embedded test service.

## Evidence rules

- Every executed result must name the exact commit SHA, workflow run URL, Python version, command, and evidence artifact.
- The embedded Temporal SDK test service can validate request/response-drop mechanics and client recovery logic. It does not prove behavior against the deployed Temporal cluster.
- A remote call timeout, connection reset, missing ACK, or caller death after send means the outcome is ambiguous until authoritative provider state resolves it.
- Never create a replacement Run or Workflow to resolve ambiguity. Use the original Run ID, stable operation ID, and deterministic Workflow ID.
- Stable client operation IDs are audit/correlation identities, not proof of provider-side exactly-once behavior.
- If the server/provider state cannot be observed, leave the Run/operation unknown and retry observation—not the side effect.

## Scenario matrix

| ID | Scenario | Required assertion | Status / evidence |
|---|---|---|---|
| R01 | Run is durable; StartWorkflow request is forwarded; ACK frames dropped; caller is SIGKILLed | After a new process opens the same SQLite store, one original Run is attached to the original Workflow ID; provider history shows one start event; duplicate start is rejected | **PENDING exact-head CI** on the current PR #41 head `34f14e4ac710e5e32fadb8f52598fb03701fbe25`; source test is `tests/test_temporal_ack_drop_proxy.py`. Do not count as PASS until artifact exists. |
| R02 | ACK ambiguous; reopen same SQLite state | Persisted Run ID and start operation ID remain identical; recovery attaches by deterministic ID | **PENDING exact-head CI** on `3c3860ff1e78d9018ad808080705f68236cc4f55`. |
| R03 | Provider cannot verify status after ACK loss | Persist `unknown`; do not issue another Workflow start | **PENDING exact-head CI** on `3c3860ff1e78d9018ad808080705f68236cc4f55`. |
| R04 | Duplicate attach to a running workflow | Return the same deterministic Workflow ID and execution identity | **PENDING exact-head CI**. |
| R05 | Deterministic ID already refers to a terminal workflow | Fail closed; do not report a terminal workflow as running | **PENDING exact-head CI**. |
| R06 | Provider executes a side effect but ACK is lost | Same stable provider operation identity on retry; authoritative provider inspection resolves outcome | **NOT RUN** — no real external side-effect provider fixture/service is available in this CI report. |
| R07 | Sandbox cleanup executes but cleanup ACK is lost | Stable cleanup operation ID; retain pending cleanup reference until authoritative acknowledgement/inspection; append retry provenance | **PARTIAL** — deterministic provider-boundary tests exist, but live production-provider ACK-loss is **NOT RUN**. |
| R08 | Cleanup provider/status is unknown | Do not invoke a different provider or discard the sandbox reference; operation stays unknown/pending | **PENDING exact-head CI**; live-provider verification **NOT RUN**. |
| R09 | Sandbox allocation succeeds, bind fails | Preserve allocation identity and prove cleanup/reconciliation after restart | **NOT RUN** for durable external provider semantics; a best-effort in-memory cleanup is not sufficient. |
| R10 | Pause/resume persistence failure | Record the failure boundary and reconcile store/workflow state before accepting new mutations | **NOT RUN** for process/provider-level injection. |
| R11 | Worker OS process is SIGKILLed while waiting / signal delivery, then a separate Worker process restarts | Same Workflow ID and execution; duplicate signals are processed once; transition history remains consistent | **FAIL on PR #46 head `59966c9503019552fea39f655bf8fc6f0b4deca5`**: full pytest run [39955957118](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39955957118) reports 148 passed, 1 failed, 1 skipped. The failing test waited for the workflow to reach `waiting` but it remained `running`; do not accept this test until fixed and rerun. |
| R12 | In-memory and SQLite stores preserve recovery operation/state semantics | Equivalent intent and operation identities survive serialization/reopen | **PENDING exact-head CI** on PR #41 SHA above. |
| R13 | Separate caller process dies after Temporal accepts start but before local WorkflowRef bind; a new process reopens SQLite | Same Run ID and deterministic Workflow ID; one start event / duplicate start rejection | Test exists in `tests/test_temporal_process_recovery.py`; **PENDING exact-head CI** for PR #41 SHA above. This does not prove response-drop injection. |
| R14 | TCP proxy forwards `StartWorkflowExecution`, then drops response frames; caller remains blocked, is SIGKILLed, and a new process reconciles | Proxy log proves forward + drop; process exit is SIGKILL; recovered identity is unchanged; history contains one start event | Test exists in `tests/test_temporal_ack_drop_proxy.py`; **PENDING exact-head CI**. Uses the Temporal SDK test service; production Temporal acceptance remains separate. |

## Exact SHA evidence ledger

| Commit SHA | Standard full pytest | Dedicated P0 recovery tests | Worker SIGKILL recovery | Temporal Start ACK-drop proxy | Live external provider side-effect / cleanup ACK loss |
|---|---|---|---|---|---|
| `3c3860ff1e78d9018ad808080705f68236cc4f55` (PR #41 current head at the time of inspection) | **NO RUN RECORDED** for this SHA | **NO RUN RECORDED** for this SHA | Included in dedicated test command; exact-head result not recorded | Included in dedicated test command; exact-head result not recorded | **NOT RUN** |
| `59966c9503019552fea39f655bf8fc6f0b4deca5` (PR #46 current head at inspection) | **FAIL**, [run 39955957118](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39955957118), 148 passed / 1 failed / 1 skipped | Not the same test scope | **FAIL**, same run; see failure above | **NOT RUN** in #46 test command | kind cleanup workflow was still in progress at inspection; production provider **NOT RUN** |
| `65f3329e9edc8ba91b404babeab21b78714c84b6` (historical PR #41 head) | Historical current-base blocker noted in comments; do not substitute for current head | **PASS**, [run 39952876316](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39952876316) | The separate caller SIGKILL + SQLite reopen test passed in the focused run | **NOT RUN** at that revision | **NOT RUN** |
| `972f5314927131c6c8b5d402bc6305e7942d8a53` (older historical revision) | **BLOCKED** at that revision by missing source modules; subsequent PR #45 restored those modules | Historical focused workflow passed; not a current-head result | **NOT RUN** | **NOT RUN** | **NOT RUN** |

### Historical evidence is not current-head evidence

Earlier successful runs on older PR #41 commits (including [39952876316](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39952876316)) are historical evidence only. As of inspection, GitHub Actions returned no workflow run associated with PR #41 current head `3c3860ff1e78d9018ad808080705f68236cc4f55`. Do not promote an older green check to current-head PASS.

## Required ACK-drop experiment

For R01/R14 the test must prove all of these in one exact-SHA run:

1. Persist one canonical Run and operation intent to durable storage before the RPC.
2. Forward the HTTP/2 `StartWorkflowExecution` request to Temporal.
3. Capture structured evidence that response frames for that RPC are dropped before the caller receives an ACK.
4. SIGKILL the still-blocked caller process and assert the exit signal.
5. Start a separate caller process against the same SQLite file.
6. Attach / reconcile only through the original deterministic Workflow ID.
7. Assert one Workflow Execution Started event and provider rejection of a duplicate start.
8. Upload machine-readable proxy events and recovery evidence with tested SHA, Run ID, operation ID, Workflow ID, execution run ID, SIGKILL result, and action-run URL.

If any prerequisite or assertion is missing, status remains **NOT RUN** or **BLOCKED**, not PASS.

## Sandbox cleanup evidence boundary

The external provider must expose a stable idempotency identity or an authoritative read operation that resolves whether the cleanup has committed. For ambiguous cleanup:

- Keep the original sandbox reference in the durable pending list.
- Reuse the same `sandbox-cleanup:<run_id>:<sandbox_ref>` operation ID.
- Persist attempt number, error/timeout and subsequent acknowledgement/inspection evidence.
- Do not allocate a replacement sandbox merely to hide uncertain cleanup.
- Do not report a live production-provider guarantee based on an in-memory test, fake driver, or kind fixture alone.

Issue #30 stays open until the current PR-head matrix is green and live-provider rows are separately executed or explicitly accepted as blocked by the provider contract.


## Ledger update after documentation/workflow correction

- Updated recovery report blob SHA: `45fed897ff7ba4ce9a44e0e76191686593200328`.
- Updated focused workflow blob SHA: `ef0249fa26e081e55ffc3402cbdeefa6dc58124b`.
- The commit created by these sequential edits is `34f14e4ac710e5e32fadb8f52598fb03701fbe25`; fresh exact-head CI is required and had not appeared at the time of this write.
- PR #46 Worker A SIGKILL / Worker B replay test failure remains independently recorded on `59966c9503019552fea39f655bf8fc6f0b4deca5`. The test currently never transitions from `running` to the awaited `waiting` state before SIGKILL. It is a failing test, not accepted recovery evidence.
