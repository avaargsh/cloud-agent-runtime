# Authoritative workload credential and approval integration

Status: **Runtime adapter plus paired ACP authority implementation are available in Draft PRs #52 and #237; source-level acceptance is green, production deployment remains blocked.** The Runtime client in this PR is not itself a credential issuer and does not create real upstream credentials.

## Trust boundaries

- Tenant-facing code must enter through AuthorizedAgentRuntime. The caller cannot submit tenant, Agent, Session, Release, Run or workspace claims to select the principal; the facade reads canonical Session/Run/SandboxBinding rows.
- The production AuthoritativeCredentialBroker requires an injected WorkloadAssertionIssuer backed by KMS or a workload identity service. The Agent never sees its signing key. The assertion is audience-restricted to urn:cloud-agent-runtime:credential-authority, short-lived, one-purpose, and binds the canonical Run/Session/tenant/Agent/Release, current sandbox binding, credential binding, audience, scopes, request ID and expiry. The authority MUST verify the assertion signature and claims before issuing a token.
- All service calls use an injected server-side service credential and an HTTPS transport with certificate/hostname verification, a bounded timeout, a response-size cap, and redirects disabled. The client does not automatically retry an issuance POST.
- The opaque token is returned only to the intended in-process caller; it is not stored in Run metadata or logs. Resource servers MUST consult current authority/introspection or use provider-recognized revocation/fencing. An offline self-contained JWT cannot be made immediately invalid by a revocation API alone.

## Versioned service contract

The paired ACP authority implementation and PostgreSQL/HTTPS/cross-repository integration tests are in PR #237. This contract remains versioned here to define caller behavior; current-head CI is necessary but not evidence of production deployment. JSON responses must be objects, and all times are Unix seconds in UTC.

### POST /v1/workload-credentials:issue

Request fields: request_id, workload_assertion, binding_id, audience, scopes, requested_ttl_seconds, and optional approval_id.

Authority requirements:
1. Verify the signed workload assertion against an operator-pinned issuer/key and expected audience. Reject untrusted keys, wrong issuer/audience, expired assertion, future iat, excessive lifetime, wrong tenant/Agent/Session/Run binding, stale sandbox revision, or mismatched credential binding.
2. Resolve the configured CredentialBinding on the server. Never trust the client's requested Audience or Scope without intersecting it with current capability/tenant policy.
3. For write/delete/admin/mutate scopes, load current approval from the authoritative approval store inside the issuance decision. Bind it to tenant, Agent, Session, Run, credential binding, audience, exact scope set, approver, expiry, and revocation/version. A client-supplied approval ID or old receipt is not authorization by itself.
4. Consume request_id atomically and durably. A duplicate ID must not create a second lease; return HTTP 409. The idempotency key is bound to the signed Run and credential binding.
5. Store only a token digest where possible. Return credential with credential_id, access_token, audience, exact scopes, issued_at, expires_at, run_id, session_id, binding_id, sandbox_id, and sandbox_revision. Expiry cannot exceed requested TTL, configured maximum, assertion expiry or current approval expiry.

### POST /v1/workload-credentials:introspect

Request: access_token, expected audience, required_scopes.

Return credential.active, authoritative audience/scopes/expiry, and the canonical workload_identity. Every introspection must check expiry, individual/Run/approval revocation, current approval state and credential registry version. If any source is unknown or unavailable, return inactive/deny. Never trust the token's unverified claims or accept a stale cached PERMIT after revocation.

### Credential revocation

- POST /v1/workload-credentials:revoke — request credential_id; return revoked: bool.
- POST /v1/workload-credentials:revoke-run — request run_id; return non-negative revoked_count. Bind the caller to a trusted Runtime service identity.
- POST /v1/workload-credentials:revoke-approval — requires the dedicated `approval.revoker` role; request approval_id and stable event_id; return non-negative revoked_count. This operation must be idempotent by event ID. The `credential.revoker` role must not revoke approval state.

Run pause/resume/rebind/completion/cancellation revoke Run leases. A new sandbox revision gets new workload assertions and leases. The previous token must fail introspection after revocation even if it has not reached its nominal expiry.

### GET /v1/authorization/approvals/{approval_id}

Return state: ACTIVE and a grant containing the exact immutable fields expected by ApprovalGrant, including expiration. Denied, pending, revoked, not found, expired or malformed results map to no approval. Service outage/malformed responses raise an error and credential issuance fails closed. The client reads this endpoint for preflight but the broker must independently recheck the current approval during issue and introspection.

### POST /v1/authorization/approvals/{approval_id}:revoke

Request: a unique event_id from a trusted, authenticated approval-outbox delivery. Return state: REVOKED and a positive monotonic version. Duplicate event delivery is idempotent and cannot resurrect an approval. The revocation handler first records revocation at the authority, then fans out to credential leases using the same event ID. If fan-out fails, the durable consumer retries; the authority must already deny issuance and introspection for the revoked approval.

## Failure semantics and operational requirements

- 401/403 is DENY. Timeouts, TLS errors, 5xx, invalid JSON, missing fields, unexpected scopes/audience, identity mismatch and invalid expiry never fall back to the in-memory broker.
- HTTP 409 on the issuance endpoint maps to replay rejection; issuance POSTs are not blindly retried after ambiguous timeout. The caller reconciles the same request ID via the authority's audit/query mechanism.
- The revocation event consumer must authenticate the source (signature or mutually authenticated trusted outbox); a non-empty source string is not authentication. The library requires an injected ApprovalRevocationEventVerifier.
- The authority service needs a transactional durable database/ledger, uniqueness constraints, revocation status/version, an outbox, least-privilege DB roles, audit evidence and backup/restore tests. Its operator keys and policy store are not controlled by Agent Runtime.
- The paired ACP PR #237 includes the service, schema, validated operator configuration, signed assertion verifier, durable replay handling, transactional approval-revocation outbox, and PostgreSQL/HTTPS/cross-repository integration tests. Production GO still requires deployment evidence for signer/key lifecycle, service authentication and mTLS, trusted revocation delivery, and protected-resource online introspection/fencing.

## Current ACP dependency

The existing ACP current-effect PostgreSQL reader is a narrow, opt-in authority for Kubernetes Deployment effects; its own documentation says it does not supply a general authorization API or signer issuer. It cannot be reused as a credential broker. This contract deliberately preserves that ownership boundary and leaves the production readiness gate open until the ACP authority implementation is deployed and exercised.
