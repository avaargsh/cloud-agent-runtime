# Evidence Object Store Boundary

AgentOS v3.2 uses object storage as the evidence archive boundary.

Design:

```
Execution
   |
   v
EvidenceEvent
   |
   v
Object Store (S3 / MinIO)
   |
   v
EvidenceManifest
```

The runtime does not maintain a second trace database. It stores immutable
object references and content digests.

## Storage model

Hot execution state:

- Run identity
- Session identity
- Binding state
- Workflow references

Evidence archive:

- JSON event bundles
- artifact references
- policy decisions
- tool receipts
- provenance records

Production deployments should enable:

- object versioning
- retention policy / object lock
- independent digest checkpoint storage

This keeps Evidence separate from Harness logs and allows an external auditor
to verify execution history.
