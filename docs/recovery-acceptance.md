# P0 Recovery Acceptance Matrix

Scope: Issue #30 only. This document separates code-level deterministic tests from real process/provider failure evidence.

Evidence rules:
- A passing unit or Temporal test-environment test is not proof of OS-level process restart against a live provider.
- Record the exact tested commit SHA, workflow run URL, and artifact/log reference for every executed row.
- If a provider outcome cannot be confirmed, keep ownership and status unresolved. Do not mint a new Run ID or start a replacement workflow.
- Provider-side exactly-once behavior requires a provider idempotency contract; deterministic client keys alone do not establish it.

| ID | Scenario | Expected invariant | Evidence status |
|---|---|---|---|
| R01 | Run persisted; provider may accept start; ACK lost; caller process SIGKILLed | Same Run ID and deterministic Workflow ID; no blind second start | NOT RUN — requires OS process kill with live Temporal service |
| R02 | Reconstruct runtime with same SQLite store after ambiguous start | Same Run ID recovered; query/attach by deterministic Workflow ID | Implemented test; CI pending for this PR |
| R03 | Provider status query fails/unavailable after lost ACK | Run remains `unknown`; no duplicate workflow creation | Implemented fail-closed unit test; CI pending for this PR |
| R04 | Duplicate attach to running workflow | Attach original workflow identity only | Existing Temporal driver test; exact-head CI pending |
| R05 | Duplicate attach to terminal workflow | Fail closed; do not represent terminal execution as active | Existing Temporal driver test; exact-head CI pending |
| R06 | Provider completed mutation but response/ACK was lost; retry with same operation ID | Keep ownership unresolved until provider acknowledgement or authoritative status verifies result | PARTIAL — deterministic cleanup ACK-loss test only; live provider NOT RUN |
| R07 | Sandbox cleanup fails and is retried | Same Run-scoped cleanup operation ID, retry provenance retained, ref remains pending until acknowledged | Implemented unit test; CI pending for this PR |
| R08 | Unknown cleanup provider / provider status | Keep old sandbox ref pending; record blocked/unknown evidence | Code path added; dedicated negative test NOT RUN |
| R09 | Allocation succeeds, bind fails | Allocation identity retained sufficiently to retry/verify cleanup | NOT RUN in this PR; existing best-effort cleanup needs durable allocation intent |
| R10 | Pause/resume persistence failure | Store state and provider execution do not silently diverge | NOT RUN in this PR |
| R11 | Real Worker SIGKILL and restart while workflow is waiting / during signal delivery | Same workflow history, no duplicate execution, state reconciliation verified | NOT RUN — requires dedicated integration harness |
| R12 | In-memory vs SQLite store semantic parity for recovery metadata | Same recovery state and operation ID after store reopen | Implemented SQLite recovery test; CI pending |

## Exact-head evidence log

| Commit SHA | Python 3.11 pytest | Python 3.12 pytest | Live process kill | Live Temporal lost-ACK | Artifact / logs |
|---|---|---|---|---|---|
| `278354933f36b785d67f63c8b92345ed4731cfdf` | PASS — full pytest matrix | PASS — full pytest matrix | NOT RUN | NOT RUN | [P0 acceptance workflow](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39951104588); [full pytest matrix](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39951104580). Artifacts: [Python 3.11 report](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39951104588), [Python 3.12 report](https://github.com/avaargsh/cloud-agent-runtime/actions/runs/39951104588). Live process/provider failure rows remain NOT RUN. |

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

Until that exact harness has been executed and the report plus temporal history exported, R01 and R11 remain `NOT RUN`. No CI result from the ordinary pytest matrix may be substituted for that evidence.
