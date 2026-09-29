# Architecture

## One seam, not another runtime

```text
Agent / Application semantics
          |
          v
Durable Orchestrator
Temporal / Restate
          |
          | LifecycleBinding
          v
Sandbox Provider
Kubernetes Agent Sandbox / local test adapter
          |
          v
Isolated compute
gVisor / Kata / Kubernetes
```

There are three different lifetimes and they must not be collapsed into one "Agent Runtime":

```text
Business / Agent Run     hours / days
Durable workflow         hours / days
Logical sandbox          minutes / hours, recoverable
Compute allocation       seconds / minutes
```

## Ownership

### Durable orchestrator owns

- workflow history and durable state;
- retry and idempotent activity semantics;
- timers and long waits;
- human approval waits/signals;
- cancellation and resume;
- workflow versioning/replay semantics.

### Sandbox provider owns

- isolated execution;
- sandbox allocation/termination;
- workspace/snapshot mechanisms;
- runtime isolation and compute lifecycle;
- warm-pool/scale-to-zero mechanisms when supported.

### This repository owns only the seam

`LifecycleBinding` records enough information to safely continue a durable workflow in a replacement sandbox:

- durable workflow reference;
- logical sandbox/workspace reference;
- artifact/evidence references;
- release/candidate version;
- credential lease/reference metadata;
- idempotency/operation identity;
- recovery reason and validation evidence.

## Recovery invariant

A rebind is allowed only when the adapter can establish:

```text
expected workspace
AND expected artifacts
AND expected release/candidate version
AND valid credential scope
AND no completed side effect will be replayed
```

Failure to establish an invariant must stop continuation and emit evidence; it must not silently create a fresh sandbox and pretend it is the old execution environment.

## Identity

Application-level Session/Run IDs may remain useful correlation identities, but this repository does not make them a second durable lifecycle state machine. Provider IDs are references:

```text
Application Run ID -> Temporal/Restate execution ref
Application Run ID -> logical sandbox binding ref
```

## Policy boundary

Authorization, budget and irreversible-action policy remain application/platform concerns. The binding adapter may carry references required to enforce them, but it does not become a policy engine.

## Legacy experiments

The current `runtime.py`, `async_runtime.py`, local stores and Temporal workflow code predate this boundary reset. They are retained while the P0 binding contract is extracted. New features must not extend them into a competing durable runtime.
