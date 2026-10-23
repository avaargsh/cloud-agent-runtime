# Roadmap

## v0.1 — State contracts
- [x] Session and Run models
- [x] Provider/state references
- [x] Minimal local lifecycle
- [x] ArtifactRef / EvidenceRef types
- [x] Approval / budget model
- [x] Capability binding model
- [x] Workflow reference contract

## v0.2 — Local runtime
- [x] SQLite reference store
- [ ] PostgreSQL store adapter
- [x] Local filesystem artifact store
- [ ] Production-grade S3-compatible storage and retention enforcement
- [x] Sandbox provider interface
- [x] In-memory sandbox allocate/bind/snapshot/resume
- [ ] Harness provider interface
- [ ] Production OTel collector/export configuration

## v0.3 — Durable workflow
- [x] Async runtime path
- [x] Temporal Python client adapter
- [x] Deterministic Temporal Workflow ID from canonical Run ID
- [x] WorkflowAlreadyStarted attach/reuse baseline
- [x] Approval resolution signal and deduplication baseline
- [x] Provider-neutral sync/async workflow contracts
- [x] Run pause / resume
- [x] Retry / idempotency baseline
- [x] Approval wait states
- [x] Sandbox snapshot / resume contracts
- [x] Temporal Worker reference workflow and CLI
- [x] Signals and run_state query
- [x] Temporal test-environment integration
- [ ] Complete exact-SHA recovery acceptance matrix across process/worker restart and ambiguous provider ACK
- [ ] Live provider acceptance for side-effect idempotency and cleanup reconciliation

## v0.4 — Security and Kubernetes scale
- [ ] Production authoritative credential/approval service and revocation fan-out
- [ ] Production-enforced sandbox network isolation and workspace destruction acceptance
- [ ] Warm sandbox pool
- [ ] Tenant quota and SLO controls
- [ ] Kueue / Volcano / GPU / DRA integration through public compute contracts
- [ ] Self-hosted vLLM / SGLang model provider

## Governance

The project is a lifecycle-binding layer. Temporal owns durable orchestration; the GPU compute platform owns capacity admission, allocation and release; the harness executes bounded attempts. Do not fold those responsibilities into a new runtime abstraction without evidence that an existing interface cannot satisfy the requirement. Track implementation status separately from contract proposals and production acceptance.
