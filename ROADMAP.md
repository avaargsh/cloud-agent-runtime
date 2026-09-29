# Roadmap

## Direction reset — 2026-09-29

**Decision:** do not build a standalone Durable Agent Runtime.

Temporal / Restate own durable execution. Kubernetes Agent Sandbox or equivalent providers own sandbox execution. This repository is narrowed to the integration seam.

## Keep — proven reference experiments

- [x] Temporal Python client/worker experiment
- [x] deterministic mapping from application Run ID to Temporal Workflow ID
- [x] attach/reuse semantics for an existing Workflow
- [x] signal/query integration tests
- [x] artifact/evidence reference types
- [x] sandbox provider interface
- [x] local sandbox allocate/bind/snapshot/resume experiment

These remain useful as compatibility fixtures; they are not a second production lifecycle engine.

## P0 — LifecycleBinding contract

- [ ] define `LifecycleBinding` with workflow ref, logical sandbox ref, workspace ref and release/candidate version
- [ ] define explicit `bind / release / restore / validate` operations
- [ ] make credentials references/leases, never durable secret payloads
- [ ] specify idempotency key propagation across workflow activity → sandbox execution
- [ ] specify artifact/workspace integrity checks
- [ ] define recovery reasons and evidence emitted on rebind

## P0 — Recovery Golden Path

- [ ] start Temporal workflow
- [ ] allocate/bind sandbox and execute first step
- [ ] persist workspace/artifact reference
- [ ] terminate/release sandbox compute
- [ ] wait for approval with zero sandbox compute
- [ ] allocate a replacement sandbox
- [ ] restore workspace and validate binding invariants
- [ ] continue the same Temporal workflow
- [ ] prove the first side effect is not repeated
- [ ] complete with evidence refs

## P1 — Provider adapters

- [ ] Kubernetes Agent Sandbox adapter
- [ ] retain local adapter for tests
- [ ] evaluate Restate adapter only after the binding contract is stable
- [ ] upstream provider-specific improvements where practical

## Explicitly cancelled

- [x] ~~PostgreSQL lifecycle store~~ — no second workflow source of truth
- [x] ~~custom durable event log~~
- [x] ~~AgentRun controller/state machine~~
- [x] ~~retry/checkpoint/approval engine~~
- [x] ~~workflow scheduler~~
- [x] ~~sandbox controller / warm-pool controller~~
- [x] ~~tenant quota implementation~~
- [x] ~~Kueue / Volcano integration~~
- [x] ~~GPU / DRA bindings~~
- [x] ~~self-hosted vLLM / SGLang provider~~

## Exit criterion

If the P0 binding contract collapses into a few provider-specific helper functions with no reusable cross-provider semantics, upstream/integrate those helpers and archive this repository rather than expanding it back into a platform.
