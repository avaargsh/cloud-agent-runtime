# State Model

## Why multiple stores

Agent systems produce different classes of state with different consistency and retention requirements.

### Transactional state
Examples:

- Session / Run metadata
- status
- provider refs
- approvals
- quotas

Suggested store: PostgreSQL or equivalent.

### Artifact state
Examples:

- files
- patches
- generated reports
- sandbox snapshots
- large evidence payloads

Suggested store: S3-compatible object storage.

### Telemetry
Examples:

- traces
- spans
- logs
- metrics
- tool-call timing

Suggested interface: OpenTelemetry.

### Provider state
Examples:

- model-provider thread ID
- Temporal workflow ID
- sandbox-provider ID

Provider state is referenced, not treated as the platform's canonical identity.

## State references

The runtime should pass references instead of copying large state into every request.

```text
Session
  ├── workflowRef
  ├── sandboxRef
  ├── memoryRef
  ├── artifactRefs[]
  └── evidenceRefs[]
```
