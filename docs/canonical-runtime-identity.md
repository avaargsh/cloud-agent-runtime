# Canonical Runtime Run/Session identity authority

Status: **implementation slice; production credential enablement remains blocked until the deployed service identity and transport are verified**.

## Decision: one authority, two composition modes

RuntimeStore remains the sole authority for current Run, Session and Sandbox binding state. Do not create an identity cache, second Run ledger, or write endpoint.

### Same-process composition (preferred when services share a trusted process)

Inject `RuntimeStoreRunAuthorityReader` with the same `RuntimeStore` used by `AgentRuntime`. Each `get_run_binding(run_id)` reads the Run and its owning Session from the store at call time and returns `CanonicalRuntimeRunBinding`. It never accepts tenant, Agent, Session, Release, Run, workspace or Sandbox claims from an incoming credential request.

The adapter requires a persisted `SandboxBinding` whose `sandbox_ref`, provider, Sandbox ID and positive revision agree. It does not synthesize a revision from the convenience `Run.sandbox_ref` field. Older or partially migrated records without a canonical binding fail closed until repaired by the normal Runtime lifecycle.

### Separate-process composition

Mount `RuntimeCanonicalIdentityWSGI` behind a trusted HTTPS service ingress and configure its `RuntimeIdentityServiceAuthenticator` to require the `runtime.identity.reader` service role. Production deployments must authenticate the calling workload (for example, ingress mTLS identity mapped to that role), restrict network reachability, and protect/rotate service credentials. The WSGI component is only an application handler: `wsgi.url_scheme == https` is defense in depth, **not** TLS or mTLS by itself.

The read-only route is:

```http
GET /v1/runtime/canonical-runs/{run_id}
```

The client is `HttpsRuntimeStoreRunAuthorityReader`, used with the existing `HttpsJsonAuthorityTransport` configured for verified HTTPS, a least-privilege service credential and a bounded timeout. It issues no writes and does not retry. The contract is versioned so clients reject incompatible responses instead of guessing.

## Response contract

HTTP 200 JSON:

```json
{
  "contract_version": "runtime.canonical-run.v1",
  "tenant_id": "tenant-…",
  "agent_id": "agent-…",
  "release_id": "release-…",
  "session_id": "session-…",
  "run_id": "run-…",
  "sandbox_id": "sandbox-…",
  "sandbox_revision": 3,
  "workspace_ref": "sandbox://kubernetes/sandbox-…",
  "status": "running",
  "session_status": "active"
}
```

Values above illustrate the schema; a real response contains the values read from RuntimeStore. Identity fields come from the persisted owning Session and Run, and the Sandbox fields come from the persisted `Run.sandbox_binding`. No bearer token, assertion, file content or mutable authorization policy is returned.

| Condition | Local reader | Read-only endpoint |
|---|---|---|
| Run and Session found; canonical records valid | Return current binding | `200` with versioned binding |
| Run or owning Session does not exist | `RuntimeIdentityNotFoundError` | `404` with a sanitized error |
| SandboxBinding missing, revision invalid, IDs/refs disagree, unknown status or malformed record | Protocol failure | `503`, no record details |
| RuntimeStore or authentication dependency fails | Unavailable error | `503`, no record details |
| Caller not authorized or transport is not marked HTTPS by the trusted ingress | Not applicable | `403` |
| Method other than GET | Not applicable | `405` |

Responses include `Cache-Control: no-store`; the endpoint has no mutation route. Unknown Run/Session and unavailability must never fall back to assertion/request claims or an in-memory identity.

## Credential-authority rules

- On **issuance**, the authority verifies the signed workload assertion and compares its tenant, Agent, Release, Session, Run, Sandbox ID/revision and workspace ref against this live reader result. A mismatch or missing Run denies issuance; a reader outage must not produce a lease.
- On **every introspection**, the authority re-reads this binding. Non-running Run, non-active Session, missing Run, changed Sandbox ID/revision/ref or mismatch returns `active: false` (or a fail-closed unavailable result that callers must treat as DENY). No stale permit or offline bearer-token grace may bypass this check.
- Runtime's status names are normalized to lowercase to interoperate with the ACP `CanonicalRunBinding` protocol. Unknown status is a protocol failure, not an implicit ACTIVE/RUNNING state.
- The caller's authenticated service identity authorizes access to the read-only route; a non-empty string or caller-supplied role field is not authentication.
- The runtime API does not prove the assertion signer/KMS, credentials broker, TLS/mTLS ingress, approval event publisher or protected-resource fencing are correctly deployed. Those are separate deployment gates.

## Validation

`tests/test_canonical_runtime_identity.py` covers direct `InMemoryStore` and restarted `SQLiteStore` reads, sandbox rebind/revision changes, unknown Run/Session, paused/terminal state projection, malformed/missing SandboxBinding, unavailable storage, HTTPS and role gates, read-only HTTP semantics, schema version/shape checks and remote-reader failure mapping.

The ACP cross-repository suite must additionally run this reader against the real RuntimeStore classes and assert that forged signed claims cannot override canonical state. Green Runtime unit tests alone are not the cross-repository Golden Path.
