# Evidence Object Store Boundary

AgentOS v3.2 uses S3-compatible object storage as the evidence archive boundary.

```text
OTel exported span
      |
      v
OTelEvidenceAdapter
      |
      v
EvidenceEvent
      |
      v
AppendOnlyEvidenceCollector
      |
      v
EvidenceArchive
      |
      +--> evidence/<run>/<content-digest>.json
      |
      +--> evidence-manifests/<run>/<manifest-digest>.json
```

The runtime does not maintain a second trace database. OpenTelemetry remains the
telemetry substrate; the adapter only projects selected exported span facts into
the evidence contract.

## Trust boundary

Harness-local traces are not authoritative evidence. The archive records
execution-external provenance and persists immutable, content-addressed objects.

Every archive write produces two objects:

1. the evidence record bundle;
2. an immutable manifest containing the evidence object key and digest.

Replay verifies both layers. Manifest bytes are checked against the digest
encoded in the manifest key, then evidence bytes are checked against the digest
inside the manifest.

For production S3, MinIO, or Ceph RGW deployments, enable:

- bucket versioning;
- object lock / retention;
- restricted delete permissions;
- an independently retained trusted manifest or chain-head checkpoint.

A digest chain or content-addressed object name detects mutation, but it does
not by itself make a storage administrator trustworthy.

## Stored evidence

Evidence objects may contain:

- OTel trace/span identity;
- Run and Action identity;
- policy digest;
- artifact digest;
- tool receipts;
- generated/observed/authorized/executed/committed provenance;
- append-only chain linkage.

Hot Run, Session, Workflow, and Sandbox state stays in the runtime state store.
Evidence payloads stay in the object archive.
