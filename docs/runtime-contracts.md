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
