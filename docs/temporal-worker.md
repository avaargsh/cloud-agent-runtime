# Temporal Worker Reference

The runtime now includes a concrete Temporal Workflow definition and Worker entry point.

The design keeps a strict ownership boundary:

```text
Runtime DB
  = canonical Session / Run / Approval state

Temporal
  = durable control flow, waits, retries and signals

Sandbox
  = execution environment state

Artifact / Evidence Store
  = durable payloads and audit evidence
```

## Workflow

`AgentRunWorkflow` is registered explicitly with the stable name:

`AgentRunWorkflow`

The client adapter can therefore start it by name without making Python class names part of the external contract.

The workflow currently understands these Signals:

- `approval_resolved`
- `pause_run`
- `resume_run`
- `complete_run`
- `fail_run`

It exposes the `run_state` Query for operational inspection.

## Approval behavior

An approved event resumes the workflow phase.

A denied approval transitions the durable workflow to a terminal failed state.

Evidence references carried with the approval are retained in workflow state.

## Start a Worker

```bash
pip install -e ".[temporal]"
cloud-agent-runtime-worker \
  --target localhost:7233 \
  --namespace default \
  --task-queue agent-runs
```

Environment variables are also supported:

- `TEMPORAL_ADDRESS`
- `TEMPORAL_NAMESPACE`
- `TEMPORAL_TASK_QUEUE`

## Current boundary

The workflow intentionally does not store model prompts, tool secrets or large artifacts in Workflow history.

External data should be represented by references so Temporal history remains an orchestration log rather than the application database.
