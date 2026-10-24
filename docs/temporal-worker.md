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
- `cancel_run`
- `wait_for_input`

It exposes the `run_state` Query for operational inspection.

## Approval and terminal behavior

- An approved `approval_resolved` signal resumes a **waiting** Workflow to `running`. While manually `paused`, the approval decision and evidence are retained, but the Workflow remains paused until an explicit `resume_run` signal.
- A denied approval transitions a nonterminal Workflow to terminal `failed`, retaining the decision reason and evidence.
- Approval signals arriving **after a terminal Run** (`succeeded`, `failed`, or `cancelled`) are ignored; they cannot rewrite phase, result, error, or audit evidence.
- Repeated nonterminal approval decisions with the same `approval_id` are deduplicated by the Workflow. Canonical approval authorization and durable decision state remain the Runtime/control-plane responsibility.

A `run_state` Query exposes both `phase` and `terminal`; consumers must check the actual phase, not just the terminal boolean.

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
