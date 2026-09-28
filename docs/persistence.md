# Persistence

The reference runtime now supports a durable local SQLite store in addition to the in-memory store.

## Why a store interface

Canonical Agent state must survive:

- API process restarts,
- harness/provider changes,
- sandbox pause/resume,
- long approval waits,
- durable workflow execution.

The runtime therefore depends on a small `RuntimeStore` interface rather than a process-local dictionary.

## SQLite reference adapter

`SQLiteStore` persists JSON representations of:

- Session identity and status,
- provider/state references,
- CapabilityBindings,
- Run lifecycle,
- sandbox and snapshot references,
- WorkflowRef,
- ArtifactRef / EvidenceRef,
- approvals,
- budget,
- metadata.

This adapter is intentionally small and suitable for local development and contract testing.

A production implementation can replace it with PostgreSQL while preserving the runtime-facing store interface.

## Artifact store

`LocalArtifactStore` provides a filesystem-backed reference implementation:

```text
bytes/text
   |
SHA-256
   |
durable file
   |
ArtifactRef
```

The application stores references in Run state instead of copying large artifact payloads into transactional rows.
