# Workload identity and sandbox security contract

## Security boundary

Tenant-facing APIs must construct **AuthenticatedPrincipal** only after validating the caller's authentication token (for example, signature, issuer, audience, expiry, and applicable OIDC claims). Do not accept **tenant_id**, **agent_id**, **release_id**, workload identity claims, workspace refs, or approval receipts from a request body. The principal object is a trusted adapter input, not a self-authenticating token.

Expose **AuthorizedAgentRuntime** to tenant-facing handlers. Keep the wrapped **AgentRuntime**, its store, provider clients, credential broker administration, and approval resolver out of tenant-controlled code. The facade resolves canonical Session and Run rows before authorizing access. A Run's tenant, Agent, Release and workspace are derived from persisted Session / SandboxBinding state, never from caller-supplied Run fields. The dependent immutable-identity change in PR #36 is required to protect the same invariant across durable-store updates.

Credential binding configuration is server-owned. A request selects a configured binding ID and a subset of scopes; the caller cannot set the audience, workspace, tenant, Agent, Session, or Release. Scopes must be allowed by both the configured CredentialBinding and the Session CapabilityBinding mode. Unknown capability modes grant no scopes.

## Credential lifetime, approval, replay and revocation

- Credentials have a bounded TTL. The effective expiry is no later than the configured binding TTL, requested TTL, workload identity expiry, or approval expiry.
- Read-only scopes may be issued only when the Session has the matching read capability.
- Scopes ending in **:write**, **:delete**, **:admin**, or **:mutate** require an approval receipt from the configured trusted ApprovalResolver. The receipt is bound to tenant, Agent, Session, Run, CredentialBinding, audience, scope set, approver, and expiry.
- A credential issuance request ID is one-shot per Run and binding. Replaying it is rejected rather than minting another token.
- Credentials can be revoked individually by the broker, by Run, or by approval. The facade revokes a Run's credentials before pause/resume/rebind/completion/cancellation. Approval revocation is fanned out by a trusted administrative event to the approval authority and broker; it is deliberately not a tenant-facing facade method. Resumed/replaced workspaces must obtain new leases.
- The in-memory broker is a deterministic reference/test adapter. Its replay ledger is process-local and its opaque tokens are not upstream provider credentials. Production requires a durable broker adapter (for example, Vault/OIDC/STS), durable request-ID consumption, an authenticated token-exchange/validation boundary, and a trusted approval service. Never persist access-token strings in Run metadata or logs.

## Sandbox and workspace policy

The Kubernetes reference provider creates one disposable Pod per sandbox. It sets restricted Pod security context, runs as a non-root UID, drops Linux capabilities, disallows privilege escalation, makes the image root filesystem read-only, disables automatic ServiceAccount token mounting, and supplies bounded **emptyDir** mounts at **/workspace** and **/tmp**.

Each Pod receives a NetworkPolicy selecting only that sandbox label with empty ingress and egress rule sets. This is deny-all by default; no DNS or external API access is enabled. A provider must fail closed if the API rejects the policy. Actual packet enforcement depends on a NetworkPolicy-enforcing CNI; synthetic manifest tests do not prove live packet isolation.

Workspace retention is **ephemeral by default**: no PVC, hostPath, or cross-Run volume is mounted. Termination deletes the Pod and its NetworkPolicy; the local ephemeral workspace is not a durable source of truth. The current Kubernetes snapshot reference is a lifecycle marker, not a filesystem snapshot, so Pause/Resume must not be interpreted as preserving workspace files. Durable artifacts/checkpoints must be exported to the separately authorized artifact/state store before termination.

Persistent workspaces are not supported by this provider yet. A future persistent-volume path must be an explicit policy opt-in and bind the volume to tenant + Agent + Session + Run, reject caller-selected PVC names, define encryption/key scope and retention, prevent cross-tenant reattachment, and verify data/snapshot lineage. PVC deletion, snapshot expiration, temporary-file shredding limits, and cleanup after lost acknowledgements require their own provider acceptance tests before persistent mode can be enabled.

## Negative-first acceptance matrix

| Threat | Required expected result |
| --- | --- |
| Principal from another tenant/Agent supplies a Session or Run ID | Generic authorization denial; no workspace or credential returned |
| Caller supplies a forged workspace ref or selects a provider/PVC | No such input on the secured Run/credential API; provider binding is resolved from persisted state |
| Run parameter attempts to widen scopes or change audience | Broker rejects; audience is fixed by server-owned binding |
| Read-only capability requests write/admin scope | Denied |
| Missing, expired, mismatched, or revoked approval | Denied; any issued approval-backed lease is invalidated on revocation |
| Credential issuance request ID replay | Denied; no second lease minted |
| Expired, revoked, wrong-audience, or over-scoped bearer token | Validation denied |
| Run pauses, completes, is cancelled, or replaces workspace | Existing Run leases are revoked; replacement needs new leases |
| Sandbox Pod starts or its policy cannot be created | Allocation fails closed; provider tries to remove a policy left by failed Pod creation |
| Sandbox is terminated | Pod and NetworkPolicy deletion are retried idempotently; no PVC or hostPath survives because none is created |

## Evidence boundary

The Python tests exercise authorization and broker semantics with synthetic principals, approvals, clock, and provider fakes. Kubernetes manifest tests validate emitted policy shape only. They do not prove OIDC verification, durable broker replay protection, real CNI packet filtering, upstream token exchange, actual workspace deletion on a cluster, or production readiness. Those remain separate acceptance gates.
