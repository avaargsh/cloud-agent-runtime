# Roadmap

## v0.1 — State contracts
- [x] Session model
- [x] Run model
- [x] provider/state references
- [x] minimal local lifecycle
- [ ] ArtifactRef / EvidenceRef types
- [ ] approval / budget model
- [ ] capability binding model

## v0.2 — Local runtime
- [ ] SQLite/PostgreSQL store adapter
- [ ] object-store artifact adapter
- [ ] sandbox provider interface
- [ ] harness provider interface
- [ ] MCP capability binding
- [ ] OpenTelemetry traces

## v0.3 — Durable workflow
- [ ] Temporal-backed Run lifecycle
- [ ] pause / resume
- [ ] retry and idempotency
- [ ] approval wait states
- [ ] sandbox snapshot / resume

## v0.4 — Kubernetes scale
- [ ] warm sandbox pool
- [ ] tenant quota
- [ ] Kueue / Volcano integration
- [ ] GPU / DRA bindings
- [ ] self-hosted vLLM / SGLang model provider
