# AgentOS v3.2 execution contracts

This repository owns execution primitives, not AgentOS desired-state policy.

The control-plane repository defines portable v3.2 contracts. This runtime
implements the provider-side behavior needed to prove those contracts without
becoming a second control plane.

## Sandbox replacement

A Run and its Workflow identity outlive an individual sandbox. A sandbox may be
terminated, restored from a snapshot, and rebound without changing the canonical
Run or Workflow reference. Rebinding no longer resurrects a terminated sandbox
just to terminate it a second time.

## Tool LIMBO contract

Side-effect consistency belongs to the execution contract, not to model memory.

The reference executor uses:

- an idempotency key derived from Run and Action identity;
- bounded retry;
- verification before retry when delivery outcome is ambiguous;
- fail-closed validation when a side-effecting retry has no idempotency support.

A lost ACK after a committed write is therefore resolved by read-after-write
verification before another mutation is issued.

## External evidence collector

Evidence is collected outside the harness. Each record carries provenance roles
for generation, observation, authorization, execution and commit, and records
form an append-only digest chain.

The digest chain detects mutation and ordering changes. Audit across a trust
boundary additionally requires an independently retained head digest (or a
future signature/transparency-log integration); the hash chain by itself is not
claimed to make an untrusted collector trustworthy.

## Boundary

This runtime does not own placement policy, release governance, CapabilityIntent
compilation, long-term memory, or a new durable workflow engine. Temporal and
sandbox providers remain external implementations behind the runtime contracts.
