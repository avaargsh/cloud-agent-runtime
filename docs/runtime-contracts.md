# Runtime Contracts

## Durable state

The runtime separates references from payloads.

### ArtifactRef

Points to generated files, patches, reports, model outputs or sandbox exports.

### EvidenceRef

Points to immutable/replayable evidence such as traces, tool results, logs or evaluation artifacts.

### Approval

Represents an explicit wait state for consequential actions.

### Budget

Bounds execution using deterministic limits:

- tool calls,
- model tokens,
- cost,
- wall-clock time.

### CapabilityBinding

Binds a runtime session/release to a named external capability such as MCP, REST, browser or data service.

## Sandbox lifecycle

```text
Allocate -> Bind -> Execute -> Snapshot/Pause -> Resume -> Terminate
```

Snapshot/Resume is modeled explicitly so durable Agent state does not depend on a long-lived process.

## Workflow driver

The runtime exposes a provider-neutral workflow driver contract. Temporal can implement this contract without making Temporal IDs the canonical Session/Run identity.


## Sandbox recovery and placement migration

Sandbox identity is execution-scoped, not Run-scoped.

A snapshot preserves recoverable execution state but does not require the restored
sandbox to reuse the previous sandbox ID. This allows the control plane to move a
Run between cells or replace a failed sandbox without changing the canonical Run
or durable Workflow identity.

```text
Run ID / WorkflowRef   stable
        |
        +-- old SandboxRef -- snapshot --> terminated
        |
        +-- new SandboxRef <-- restore --- snapshot
```

Placement policy is intentionally outside this runtime. A control plane may decide
to `rebind` or `drain-rebind`; the runtime only exposes the recovery primitive.


## Durable SandboxBinding

A canonical Run persists the provider binding needed to recover after the runtime
process itself restarts. The in-process Sandbox object is only a cache.

```text
Run
  └── SandboxBinding
       ├── provider
       ├── sandbox_id / sandbox_ref
       ├── revision
       ├── snapshot_ref
       ├── previous_refs
       ├── pending_cleanup_refs
       └── last_rebind_key
```

Replacement follows a durable-before-cleanup rule:

1. allocate or restore Sandbox B;
2. persist Run -> Sandbox B as the new binding revision;
3. record Sandbox A in pending cleanup;
4. terminate Sandbox A;
5. clear the pending cleanup reference after provider acknowledgement.

If step 2 fails, Sandbox B is best-effort terminated because it never became the
durable binding. If step 4 fails, the Run remains bound to B and cleanup can be
retried from persisted state.

A caller may supply a `rebind_key`. Replaying the same key is a no-op once that
replacement has been durably committed, preventing retry from allocating Sandbox C.

The ownership rule is therefore explicit:

```text
Canonical Run lifetime        != Sandbox lifetime
Temporal Workflow lifetime    != Sandbox lifetime
Sandbox provider identity     may change
Run / Workflow identity       remains stable
Artifact / Evidence refs      remain durable
```
