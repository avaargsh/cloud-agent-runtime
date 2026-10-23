# Persistence

The reference runtime supports a durable local SQLite store in addition to the in-memory store. The RuntimeStore interface keeps lifecycle code independent of the storage implementation.

## Independent persistence guarantees

Identity, state, workspace and execution persistence are separate contracts:

- **Identity:** canonical Agent, Session, Run and task relationships survive process/provider replacement.
- **State:** lifecycle transitions, approvals, provider references, operation IDs, retry metadata and evidence references survive store reopen.
- **Workspace:** required files, checkpoints and user deliverables live in a durable workspace/artifact backend, not only inside a disposable sandbox.
- **Execution:** pending waits, timers, signals, retry state and workflow result survive Worker replacement through Temporal history and deterministic reconciliation.

One does not imply another. A Run row is not proof that the workflow resumes; a snapshot reference is not proof the bytes exist; a replayable workflow is not proof that workspace data was retained.

## SQLite reference adapter

`SQLiteStore` persists JSON representations of Session identity/status, provider/state references, CapabilityBindings, Run lifecycle, sandbox/snapshot references, WorkflowRef, ArtifactRef/EvidenceRef, approvals, budget and metadata.

SQLite is intended for local development and contract/failure-injection tests. It is not a substitute for a multi-replica production database with operational backup, restore, migration, high-availability and concurrency acceptance.

## Artifact store

`LocalArtifactStore` provides a filesystem-backed reference implementation:

```text
bytes/text -> SHA-256 -> durable file -> ArtifactRef
```

The runtime stores references instead of copying large artifact payloads into transactional rows. Production workspace durability still requires a separately operated storage backend, access control, retention policy and digest verification.
