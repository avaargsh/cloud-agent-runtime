# Sandbox replacement recovery

The canonical Run must outlive any individual sandbox instance.

```text
Run R
  -> Workflow W
  -> Sandbox A
       -> durable Artifact/Evidence refs
       -> snapshot S
       -> lost/terminated
  -> Sandbox B restored from S
  -> same Run R
  -> same Workflow W
  -> complete
```

## Invariants

- `run_id` is platform-owned and does not change when execution moves.
- `workflow_ref` is continuation identity and does not change during sandbox replacement.
- `sandbox_ref` is provider identity and is expected to change.
- durable artifact/evidence refs belong to the Run and survive sandbox replacement.
- snapshot refs are recovery inputs, not canonical Run identity.

## Current proof

`tests/test_sandbox_recovery.py` exercises the provider-neutral reference runtime with an in-memory sandbox provider. It proves identity and durable-reference semantics without claiming production isolation.

The next integration step is to repeat the same contract against a disposable Kubernetes sandbox provider and inject actual sandbox loss.
