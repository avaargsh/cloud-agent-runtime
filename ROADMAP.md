# Roadmap

## v0.1 — State contracts
- [x] Session model
- [x] Run model
- [x] provider/state references
- [x] minimal local lifecycle
- [x] ArtifactRef / EvidenceRef types
- [x] approval / budget model
- [x] capability binding model
- [x] workflow reference contract

## v0.2 — Local runtime
- [x] SQLite reference store
- [ ] PostgreSQL store adapter
- [x] local filesystem artifact store
- [ ] S3-compatible object-store adapter
- [x] sandbox provider interface
- [x] in-memory sandbox allocate/bind/snapshot/resume
- [ ] harness provider interface
- [x] capability binding contract
- [ ] OpenTelemetry traces

## v0.3 — Durable workflow
- [x] async runtime path
- [x] Temporal Python client adapter
- [x] deterministic Temporal Workflow ID from canonical Run ID
- [x] approval resolution signal
- [x] provider-neutral sync/async workflow contracts
- [x] run pause / resume
- [ ] retry / idempotency policy
- [x] approval wait states
- [x] sandbox snapshot / resume
- [x] Temporal Worker reference workflow
- [x] stable AgentRunWorkflow name
- [x] approval / pause / resume / complete / fail signals
- [x] run_state query
- [x] worker CLI
- [x] Temporal test environment integration
- [x] Worker + Signal + Query + Result integration test

## v0.4 — Kubernetes scale
- [ ] warm sandbox pool
- [ ] tenant quota
- [ ] Kueue / Volcano integration
- [ ] GPU / DRA bindings
- [ ] self-hosted vLLM / SGLang model provider
