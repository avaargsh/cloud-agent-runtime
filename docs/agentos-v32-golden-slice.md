# AgentOS v3.2 Golden Slice

The Golden Slice proves the four v3.2 gaps without introducing another runtime.

```text
Canonical Run / Session
        |
        v
Temporal Workflow
        |
        v
Kubernetes Sandbox A
        |
        | checkpoint + real Pod loss
        v
Kubernetes Sandbox B
        |
        v
OTel span snapshot
        |
        v
EvidenceEvent + provenance
        |
        v
content-addressed object + manifest
        |
        v
replay + Temporal completion
```

## Invariants

The live kind + Temporal proof requires:

- Run identity unchanged;
- Session identity unchanged;
- Temporal Workflow identity unchanged;
- Temporal execution Run ID unchanged;
- Sandbox identity changed;
- old Pod converges to deletion;
- replacement Pod becomes Ready;
- Evidence carries OTel trace/span identity and provenance;
- manifest replay verifies content digests;
- the Temporal result references the archived Evidence manifest.

The workflow intentionally uses the in-memory implementation of the ObjectStore
contract so the Golden Slice tests lifecycle semantics independently from a
specific object-storage deployment. The S3-compatible adapter has its own
contract tests and can be pointed at AWS S3, MinIO, or Ceph RGW.

This separation prevents a storage emulator from becoming part of the AgentOS
runtime contract.
