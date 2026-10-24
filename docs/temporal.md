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

## Start

The driver starts a configured Workflow Type on a configured Task Queue and passes one input object containing:

- `runtime_run_id`
- `session_id`

## Approval

When the runtime enters `WAITING_APPROVAL`, the authoritative approval state remains in runtime state.

Resolving the approval sends an `approval_resolved` Signal to the Temporal Workflow so durable control flow can resume.

## Completion confirmation

`TemporalRunBridge.complete()` signals `complete_run` and waits for an observed successful terminal Workflow result. It returns normally only when `terminal=True` **and** `phase="succeeded"`. This applies to retries after a prior completion, a race in which the Workflow closes during signalling, and asynchronous confirmation after the signal is accepted.

Terminal `failed`, `cancelled`, or unrecognized phases raise `TemporalWorkflowConflict` instead of being misreported as successful completion. If a live Workflow never confirms completion before the configured timeout, `complete()` raises `TimeoutError`; a signal acknowledgement alone does not prove successful execution. A pre-existing `succeeded` state is treated as an idempotently satisfied postcondition; the bridge does not compare a retry's result payload or evidence references to the prior completed result.

`TemporalRunBridge.get_run_status()` is a read operation; callers should use its `phase`, `terminal` and `error` fields rather than equating any terminal state with success.

## Worker replay and deployment compatibility

Temporal persists signal and timer history; Worker replacement is covered by repository integration tests, but successful replay **between previous and new Workflow code versions** has not been established using exported historical production executions. Before rolling out a new Workflow implementation to running production executions, replay representative pre-upgrade histories with Temporal's Python SDK `Replayer` or configure an appropriate Worker Versioning strategy. Do not equate a green in-process test or disposable Golden Slice with production history compatibility.

## Failure behavior

If workflow start fails after a local sandbox has been allocated:

1. Run is marked `FAILED`.
2. The sandbox is terminated.
3. The original provider error is propagated.

This avoids leaving a Run marked active when no durable workflow exists.

## Install

```bash
pip install -e ".[temporal]"
```

The adapter follows the Temporal Python client model of asynchronous `start_workflow()` and Workflow-handle `signal()` operations.
