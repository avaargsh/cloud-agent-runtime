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
