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


### Opt-in verified-outcome gate

The legacy \`verification_mode="read-after-write"\` remains unchanged for
compatibility: it uses a read callback to reconcile an ambiguous ACK.

A side-effecting operation can instead select
\`verification_mode="verified-outcome"\`, provide an immutable, scoped
\`expected_target_ref\` (including resource UID/generation where relevant), and
supply a **read-only independent** verifier. This stricter path:

1. derives a stable operation key from the Run and Action;
2. calls the side-effect provider once;
3. calls the observer even when the provider returns a successful ACK;
4. accepts only a typed \`VerifiedToolOutcome\` with exact operation key and
   target reference, \`status="succeeded"\` and a nonempty evidence reference;
5. returns a \`ToolExecutionReceipt.verification_ref\` for downstream audit.

If the provider ACK is lost, verification may close the operation without
reinvoking it. If verification is unknown, negative, mismatched or unavailable,
the call **fails closed**; the caller must reconcile the original operation
before it may claim success. No blind mutation retry is attempted in this mode,
regardless of \`max_attempts\`.

The typed outcome is **not** a cryptographic proof or an authorization grant.
A production observer must validate the authoritative external state and the
trusted proof channel (for example, the frozen ACP independent proof plus
out-of-band digest/signing policy), bind principal/policy/operation/target
identities, and reject stale evidence. A caller that simply echoes a provider
ACK into \`VerifiedToolOutcome\` does not satisfy this trust requirement.

The change does not itself mark canonical Run/Temporal workflows complete,
revoke credentials, enforce cross-tenant policy, or claim live Kubernetes
acceptance. Focused tests use a synthetic observer to falsify the contract;
production integration remains an explicit cross-repository acceptance gate.


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
