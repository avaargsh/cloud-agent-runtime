# Runtime Recovery Matrix

Issue: #30

The canonical `Run` is the durable identity. Workflow and sandbox provider
operations may fail before commit, after commit, or after the remote side effect
commits but before the caller receives an acknowledgement.

This matrix is the minimum correctness boundary before adding more runtime
providers or scale features.

| Failure point | Durable state after failure | Retry rule | Current proof |
| --- | --- | --- | --- |
| sandbox bind fails after allocation | no new Run binding | terminate allocated sandbox; retry may allocate again | `test_start_run_cleans_allocated_sandbox_when_bind_fails` |
| initial Run persistence fails after sandbox bind | no durable Run | terminate owned sandbox | `test_start_run_cleans_bound_sandbox_when_persistence_fails` |
| workflow start fails | Run is durable and FAILED | do not fabricate workflow ownership | `test_start_run_cleans_sandbox_when_workflow_start_fails` |
| workflow starts but workflow-ref persistence fails | Run is FAILED; workflow ref cleared only after confirmed termination | retain workflow ref if termination is uncertain/failed | sync + async binding-persistence tests |
| pause snapshot succeeds but Run persistence fails | Run remains RUNNING | restore cached Run/sandbox state; retry pause | `test_failed_pause_persistence_restores_running_cache_and_is_retryable` |
| replacement sandbox allocation/bind fails | old binding remains canonical | clean orphan replacement; retry rebind | rebind bind-failure tests |
| replacement binding persistence fails | old binding remains canonical | terminate unbound replacement; retry rebind | `test_failed_rebind_restores_old_binding_and_cache` |
| old sandbox cleanup fails | new binding is canonical; old ref remains in `pending_cleanup_refs` | retry cleanup without allocating another replacement | `test_rebind_replay_retries_pending_cleanup_without_new_replacement` |
| old sandbox terminate succeeds but cleanup-state persistence fails | new binding is canonical; durable pending cleanup still names old ref | same rebind key retries cleanup only; provider termination must be idempotent | `test_rebind_replay_recovers_when_cleanup_commit_fails_after_termination` |
| process restarts with paused Run + snapshot | same Run/workflow identity survives | reconstruct provider binding from durable store and resume into replacement sandbox | `test_sqlite_restart_preserves_binding_and_recovers_same_run` |
| duplicate approval delivery after durable approval commit | approval outcome already durable | identical retry may resend signal; conflicting retry fails | approval retry tests |

## Invariants

1. `run_id` is platform-owned and never changes during sandbox recovery.
2. `workflow_ref` owns continuation and is not replaced by sandbox rebind.
3. A replacement sandbox becomes canonical only after its binding is durably
   stored.
4. Retired sandbox cleanup happens after the replacement binding is durable.
5. `pending_cleanup_refs` is a durable retry queue, not best-effort telemetry.
6. Replaying the same `rebind_key` must never allocate another replacement.
7. Provider termination used by cleanup must be idempotent because the remote
   terminate may commit before cleanup-state persistence succeeds.
8. Ambiguous remote workflow ownership must remain explicit; it must not be
   converted into a synthetic successful binding.

## Remaining acceptance work

- exercise the same matrix against the Kubernetes sandbox provider
- add a PostgreSQL store adapter and rerun store-boundary failure injection
- [x] emit recovery operation identity/provenance into OTel/evidence records
- prove Temporal duplicate/terminal attach semantics in the live Golden Stack


## Recovery evidence attributes

Recovery spans that cross a retry/rebind boundary use stable semantic attributes:

- `agent.operation.id`: identity of the current remote mutation attempt
- `agent.retry.key`: caller-visible idempotency/rebind key
- `agent.recovery.original_operation_id`: operation identity being recovered or superseded
- `agent.recovery.reason`: machine-readable recovery cause

The OTel evidence projection promotes these fields into the immutable EvidenceEvent payload so recovery provenance is explicit during audit/replay instead of being hidden only inside arbitrary span attributes.
