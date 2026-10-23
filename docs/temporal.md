# Temporal Workflow Driver

The production-shaped runtime path is asynchronous.

```text
Canonical Session / Run
        |
        v
AsyncAgentRuntime
        |
        v
TemporalWorkflowDriver
        |
        v
Temporal Workflow Execution
```

## Identity

The platform owns the canonical `run_id`.

Temporal owns its workflow/run identity.

The default mapping is deterministic:

```text
runtime run_id = 123
Temporal workflow_id = agent-run-123
Temporal run_id = provider ref only
```

A Temporal ID is never used as the platform's primary Run identity.

## Start and lost-ACK recovery

The asynchronous runtime accepts an optional caller-supplied `run_id`. An API
or coordinator that must recover after losing the response **must persist and
reuse that same Run ID** on the retry; do not mint a new ID to get around an
ambiguous result.

Before the first side-effecting `StartWorkflow` RPC, the runtime durably writes
the canonical Run and a provider reference containing the deterministic
Workflow ID. The default mapping is `agent-run-<runtime_run_id>`. The request
passes one input object containing:

- `runtime_run_id`
- `session_id`

A timeout or connection error after the RPC may mean Temporal accepted the
workflow but the client did not receive the ACK. That result is UNKNOWN, not
proof of rejection. The Run stays in `created` with the original Workflow
reference and its sandbox is retained. Retrying `start_run(session_id=...,
run_id=<original>)` must reissue the same deterministic Workflow ID; Temporal's
`WorkflowAlreadyStartedError` path describes and attaches to a live execution.
The retry must not allocate a second sandbox or Workflow. Once the reference is
confirmed and durably saved, the canonical Run advances to `running`.

The same pending reference survives reconstruction from SQLite. This is an
at-least-once request/reconciliation contract, not a claim of exactly-once
remote side effects. A known terminal Workflow conflict or an inaccessible
provider must fail closed; do not silently create a different Workflow ID.

## Approval

When the runtime enters `WAITING_APPROVAL`, the authoritative approval state remains in runtime state.

Resolving the approval sends an `approval_resolved` Signal to the Temporal Workflow so durable control flow can resume.

## Failure behavior

A transport error during workflow start is ambiguous because the server may
have accepted the request before the ACK was lost. The asynchronous path keeps
the durable Run in `created`, preserves its deterministic Workflow reference,
retains the sandbox and propagates the error to the caller. Recovery reuses the
same Run ID and Workflow ID. Cleanup is not inferred from a timeout; it needs a
separate, explicit lifecycle decision and provider evidence.

A failure before the first durable Run write has no Workflow side effect. A
failure saving the post-start status leaves the earlier durable `created`
record available for the same start-or-attach recovery. Do not convert UNKNOWN
to FAILED or delete a possibly active sandbox based only on the RPC exception.

## Install

```bash
pip install -e ".[temporal]"
```

The adapter follows the Temporal Python client model of asynchronous `start_workflow()` and Workflow-handle `signal()` operations.
