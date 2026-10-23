# P0 Recovery Acceptance Matrix

Scope: Issue #30 only. This document separates code-level deterministic tests from real process/provider failure evidence.

Evidence rules:
- A passing unit or Temporal test-environment test is not proof of OS-level process restart against a live provider.
- Record the exact tested commit SHA, workflow run URL, and artifact/log reference for every executed row.
- If a provider outcome cannot be confirmed, keep ownership and status unresolved. Do not mint a new Run ID or start a replacement workflow.
- Provider-side exactly-once behavior requires a provider idempotency contract; deterministic client keys alone do not establish it.

| ID | Scenario | Expected invariant | Evidence status |
|---|---|---|---|
| R01 | Run persisted; StartWorkflow is forwarded; response frames are dropped; caller SIGKILLed | Same Run ID and deterministic Workflow ID; no blind second start | TCP/gRPC proxy test added in `tests/test_temporal_ack_drop_proxy.py`; exact-head CI pending; test-server scope only |
| R02 | Reconstruct runtime with same SQLite store after ambiguous start | Same Run ID recovered; query/attach by deterministic Workflow ID | Implemented test; CI pending for this PR |
| R03 | Provider status query fails/unavailable after lost ACK | Run remains `unknown`; no duplicate workflow creation | Implemented fail-closed unit test; CI pending for this PR |
| R04 | Duplicate attach to running workflow | Attach original workflow identity only | Existing Temporal driver test; exact-head CI pending |
| R05 | Duplicate attach to terminal workflow | Fail closed; do not represent terminal execution as active | Existing Temporal driver test; exact-head CI pending |
| R06 | Provider completed mutation but response/ACK was lost; retry with same operation ID | Keep ownership unresolved until provider acknowledgement or authoritative status verifies result | PARTIAL — deterministic cleanup ACK-loss test only; live provider NOT RUN |
| R07 | Sandbox cleanup fails and is retried | Same Run-scoped cleanup operation ID, retry provenance retained, ref remains pending until acknowledged | Cleanup lost-ACK and retry tests exist in `tests/test_runtime.py`; exact-head CI pending |
| R08 | Unknown cleanup provider / provider status | Keep old sandbox ref pending; record blocked/unknown evidence; never call the wrong provider | Negative test added in `tests/test_runtime.py`; exact-head CI pending |
| R09 | Allocation succeeds, bind fails | Allocation identity retained sufficiently to retry/verify cleanup | PARTIAL — in-memory allocation/bind failure cleanup tests exist in `tests/test_sandbox_recovery.py`; durable provider intent and live-provider proof NOT RUN |
| R10 | Pause/resume persistence failure | Store state and provider execution do not silently diverge | Unit tests exist in `tests/test_sandbox_recovery.py`; exact-head CI pending; process/provider-level fault proof NOT RUN |
| R11 | Real Worker SIGKILL and restart while workflow is waiting / during signal delivery | Same workflow history, no duplicate execution, state reconciliation verified | NOT RUN — requires dedicated integration harness |
| R12 | In-memory vs SQLite store semantic parity for recovery metadata | Same recovery state and operation ID after store reopen | Implemented SQLite recovery test; CI pending |
| R13 | Caller subprocess is SIGKILLed after Temporal accepts start but before canonical Run stores WorkflowRef; new process reopens SQLite and reconciles | Same Run ID + deterministic Workflow ID; one start event and duplicate-start rejection | Covered by `tests/test_temporal_process_recovery.py`; exact-head CI pending. This scenario does not drop the response frames. |\n| R14 | TCP proxy forwards StartWorkflowExecution and drops its response HEADERS/DATA; caller remains blocked, is SIGKILLed, and a new process reconciles | Proxy logs show request forwarded and response dropped; the original Run/Workflow IDs survive; history has one start event and duplicate start is rejected | Test added in `tests/test_temporal_ack_drop_proxy.py`; exact-head CI pending. Uses the Temporal SDK test service, not production-provider acceptance. |

## Exact-head evidence log

| Commit SHA | Python 3.11 pytest | Python 3.12 pytest | Live process kill | Live Temporal lost-ACK | Artifact / logs |
|---|---|---|---|---|---|
| `972f5314927131c6c8b5d402bc6305e7942d8a53` | BLOCKED — pre-existing `main` collection error: `cloud_agent_runtime.authoritative_identity` missing | BLOCKED — same collection error | NOT RUN | NOT RUN | [P0 focused acceptance workflow PASS on 3.11/3.12](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39951572280); [full pytest blocked](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39951572565); [AgentOS golden not counted](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39951572638). `tests/test_authoritative_identity.py` exists on `main` but imports missing `authoritative_identity.py` and `identity.py`; this branch did not implement P1 identity/credential modules. Live process/provider failure rows remain NOT RUN. |

## Required commands for process-level experiment

The process-level scenario must run a separate caller process against a live Temporal test service and durable SQLite file:

1. Caller writes one canonical Run and stable start operation ID to SQLite.
2. Caller sends start request to Temporal.
3. Inject a transport/proxy fault after the server accepted the request but before the client receives the response, then SIGKILL the caller.
4. Restart caller using the same SQLite file; call `reconcile_run(original_run_id)`.
5. Assert exactly one Temporal Workflow Execution exists for the deterministic Workflow ID and the recovered `WorkflowRef` refers to it.
6. Persist the report with Run ID, operation ID, Workflow ID, process exit signal, Temporal execution IDs/count, restart PID, exact commit SHA and CI artifact URL.

Do not mark R01 passed when only a fake driver or in-process exception is used.


## Caller process test harness

`cloud-agent-runtime-recovery-probe` is a small separate-process helper. It writes an intent record before requesting Temporal and supports a later `reconcile` process that can only attach by the stable Workflow ID. It is a harness component, not evidence by itself.

A live ACK-loss experiment must be run against a disposable Temporal service via a TCP/gRPC fault proxy. The fault proxy must confirm it forwarded the start request and then drop/reset the response before the caller receives it. Run the probe as a subprocess, record its PID, kill it with SIGKILL while the response is blocked, then invoke `reconcile` from a new process against the same state file. The provider's workflow execution list must show exactly one Workflow ID and the returned workflow ref must match it.

Until the new proxy test passes on the exact current SHA and its evidence artifact is reviewed, R01 remains `PENDING CI`; production Temporal/provider ACK-loss is not accepted by this test alone. R11 (Worker restart while waiting/during signal delivery) remains `NOT RUN`. No CI result from the ordinary pytest matrix may be substituted for that evidence.


## Scope distinction: live process kill vs network ACK drop

The dedicated process test launches a separate Python caller against Temporal's local test server. It persists the Run and operation intent in SQLite, waits until Temporal accepts the deterministic Workflow ID, then sends SIGKILL to the caller before the WorkflowRef can be committed to the RuntimeStore. A different process reopens the SQLite file, reconciles by the original Run ID, and checks that Temporal lists exactly one execution.

This proves process death after server-side workflow acceptance and before local binding persistence. The new R14 test adds a TCP proxy that parses HTTP/2 request headers, forwards StartWorkflowExecution, drops that RPC's response frames, and captures evidence. It must pass on the exact current SHA before being accepted. The SDK test server is a controlled integration environment, not production Temporal or external-provider acceptance.
