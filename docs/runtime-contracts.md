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
