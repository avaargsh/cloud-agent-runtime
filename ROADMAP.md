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
- [ ] SQLite/PostgreSQL store adapter
- [ ] object-store artifact adapter
- [x] sandbox provider interface
- [x] in-memory sandbox allocate/bind/snapshot/resume
- [ ] harness provider interface
- [x] capability binding contract
- [ ] OpenTelemetry traces

## v0.3 — Durable workflow
- [ ] Temporal-backed Run lifecycle
- [x] provider-neutral workflow driver
- [x] run pause / resume
- [ ] retry and idempotency
- [x] approval wait states
- [x] sandbox snapshot / resume

## v0.4 — Kubernetes scale
- [ ] warm sandbox pool
- [ ] tenant quota
- [ ] Kueue / Volcano integration
- [ ] GPU / DRA bindings
- [ ] self-hosted vLLM / SGLang model provider
