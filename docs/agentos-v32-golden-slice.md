# AgentOS v3.2 Golden Slice

The Golden Slice proves the four v3.2 gaps without introducing another runtime.

```text
CapabilityIntent
        |
        v
compiled policy digest
        |
        v
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
        | agent.policy.digest
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
- an external `AGENTOS_POLICY_DIGEST` is required and validated;
- Evidence carries the exact policy digest plus OTel trace/span identity;
- manifest replay returns the same policy digest;
- manifest replay verifies content digests;
- the Temporal result references the archived Evidence manifest and policy digest.

The standalone runtime workflow uses a deterministic policy-digest fixture so it
can test lifecycle/evidence semantics without depending on another repository.

The cross-repository AgentOS proof replaces that fixture with the SHA-256 digest
of the actual Rego compiled by `agent-control-plane` from CapabilityIntent.

The workflow intentionally uses the in-memory implementation of the ObjectStore
contract so the Golden Slice tests lifecycle semantics independently from a
specific object-storage deployment. The S3-compatible adapter has its own
contract tests and can be pointed at AWS S3, MinIO, or Ceph RGW.

This separation prevents a storage emulator, policy engine, or workflow engine
from becoming part of the AgentOS runtime contract.
