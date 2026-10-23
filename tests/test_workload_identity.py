from cloud_agent_runtime import (
    AgentRuntime,
    AuthorizedAgentRuntime,
    CapabilityBinding,
    CredentialBinding,
    CredentialRequest,
    IdentityAuthorizationError,
    InMemoryApprovalResolver,
    InMemoryCredentialBroker,
    InMemorySandboxProvider,
    InMemoryWorkflowDriver,
)
from cloud_agent_runtime.identity import (
    ApprovalGrant,
    AuthenticatedPrincipal,
    CredentialExpiredError,
    CredentialReplayError,
)


class FakeClock:
    def __init__(self, now=1_000.0):
        self.now = now

    def __call__(self):
        return self.now


def make_boundary(*, clock=None, mode="read-write"):
    clock = clock or FakeClock()
    runtime = AgentRuntime(
        sandbox_provider=InMemorySandboxProvider(),
        workflow_driver=InMemoryWorkflowDriver(),
    )
    session = runtime.create_session(
        agent_id="agent-a",
        release_id="release-a-v1",
        tenant_id="tenant-a",
        capabilities=[
            CapabilityBinding(
                name="github",
                kind="http",
                provider="github",
                mode=mode,
            )
        ],
    )
    binding = CredentialBinding(
        binding_id="github-api",
        capability="github",
        audience="https://api.github.com",
        allowed_scopes=("github:read", "github:write"),
        max_ttl_seconds=90,
    )
    broker = InMemoryCredentialBroker(clock=clock)
    approvals = InMemoryApprovalResolver()
    secure = AuthorizedAgentRuntime(
        runtime,
        credential_bindings={binding.binding_id: binding},
        credential_broker=broker,
        approval_resolver=approvals,
        clock=clock,
    )
    principal = AuthenticatedPrincipal(
        issuer="https://issuer.example.test",
        subject="user-agent-a",
        tenant_id="tenant-a",
        agent_id="agent-a",
    )
    return clock, runtime, session, binding, broker, approvals, secure, principal


def test_run_creation_and_lookup_reject_cross_tenant_or_forged_session():
    _, runtime, session, _, _, _, secure, principal = make_boundary()
    other = runtime.create_session(
        agent_id="agent-b",
        release_id="release-b-v1",
        tenant_id="tenant-b",
    )
    attacker = AuthenticatedPrincipal(
        issuer=principal.issuer,
        subject="attacker",
        tenant_id="tenant-b",
        agent_id="agent-b",
    )

    try:
        secure.start_run(attacker, session_id=session.session_id)
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("cross-tenant start unexpectedly succeeded")
    assert runtime.store.runs == {}

    run = secure.start_run(principal, session_id=session.session_id)
    try:
        secure.get_run(attacker, run_id=run.run_id)
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("cross-tenant Run lookup unexpectedly succeeded")

    try:
        secure.start_run(principal, session_id=other.session_id)
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("forged session parameter unexpectedly succeeded")


def test_credential_audience_and_scopes_come_from_server_binding():
    _, _, session, _, broker, _, secure, principal = make_boundary(mode="read-only")
    run = secure.start_run(principal, session_id=session.session_id)

    lease = secure.issue_credential(
        principal,
        run_id=run.run_id,
        request=CredentialRequest(
            request_id="req-read-1",
            binding_id="github-api",
            scopes=("github:read",),
            ttl_seconds=30,
        ),
    )
    identity = broker.validate(
        lease.access_token,
        audience="https://api.github.com",
        required_scopes=("github:read",),
    )
    assert identity.run_id == run.run_id
    assert identity.session_id == session.session_id
    assert identity.tenant_id == "tenant-a"
    assert identity.agent_id == "agent-a"
    assert identity.workspace_ref == run.sandbox_ref
    assert lease.expires_at - lease.issued_at <= 30

    for audience, scopes in (
        ("https://attacker.example.test", ("github:read",)),
        ("https://api.github.com", ("github:write",)),
    ):
        try:
            broker.validate(
                lease.access_token,
                audience=audience,
                required_scopes=scopes,
            )
        except IdentityAuthorizationError:
            pass
        else:
            raise AssertionError("audience/scope tampering unexpectedly succeeded")


def test_write_credentials_require_exact_live_approval_and_expire_with_it():
    clock, _, session, _, broker, approvals, secure, principal = make_boundary()
    run = secure.start_run(principal, session_id=session.session_id)
    grant = ApprovalGrant(
        approval_id="approval-run-a",
        tenant_id=session.tenant_id,
        agent_id=session.agent_id,
        session_id=session.session_id,
        run_id=run.run_id,
        binding_id="github-api",
        audience="https://api.github.com",
        scopes=("github:write",),
        approved_by="security-reviewer",
        issued_at=clock(),
        expires_at=clock() + 10,
    )
    approvals.add(grant)
    lease = secure.issue_credential(
        principal,
        run_id=run.run_id,
        request=CredentialRequest(
            request_id="req-write-1",
            binding_id="github-api",
            scopes=("github:write",),
            ttl_seconds=60,
            approval_id=grant.approval_id,
        ),
    )
    assert lease.expires_at == grant.expires_at

    clock.now += 11
    try:
        broker.validate(
            lease.access_token,
            audience="https://api.github.com",
            required_scopes=("github:write",),
        )
    except CredentialExpiredError:
        pass
    else:
        raise AssertionError("expired approval-backed credential remained valid")


def test_approval_and_run_revocation_invalidate_issued_leases():
    clock, _, session, _, broker, approvals, secure, principal = make_boundary()
    run = secure.start_run(principal, session_id=session.session_id)
    grant = ApprovalGrant(
        approval_id="approval-revocable",
        tenant_id=session.tenant_id,
        agent_id=session.agent_id,
        session_id=session.session_id,
        run_id=run.run_id,
        binding_id="github-api",
        audience="https://api.github.com",
        scopes=("github:write",),
        approved_by="security-reviewer",
        issued_at=clock(),
        expires_at=clock() + 60,
    )
    approvals.add(grant)
    lease = secure.issue_credential(
        principal,
        run_id=run.run_id,
        request=CredentialRequest(
            request_id="req-write-revoke",
            binding_id="github-api",
            scopes=("github:write",),
            ttl_seconds=60,
            approval_id=grant.approval_id,
        ),
    )
    secure.revoke_approval(grant.approval_id)
    try:
        broker.validate(lease.access_token, audience=lease.audience)
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("approval revocation did not invalidate the lease")

    read_lease = secure.issue_credential(
        principal,
        run_id=run.run_id,
        request=CredentialRequest(
            request_id="req-read-revoke",
            binding_id="github-api",
            scopes=("github:read",),
            ttl_seconds=60,
        ),
    )
    secure.pause_run(principal, run_id=run.run_id)
    try:
        broker.validate(read_lease.access_token, audience=read_lease.audience)
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("pause did not revoke Run credentials")


def test_replayed_issuance_request_and_cross_run_approval_fail_closed():
    clock, _, session, _, _, approvals, secure, principal = make_boundary()
    run = secure.start_run(principal, session_id=session.session_id)
    grant = ApprovalGrant(
        approval_id="approval-wrong-run",
        tenant_id=session.tenant_id,
        agent_id=session.agent_id,
        session_id=session.session_id,
        run_id="another-run",
        binding_id="github-api",
        audience="https://api.github.com",
        scopes=("github:write",),
        approved_by="security-reviewer",
        issued_at=clock(),
        expires_at=clock() + 60,
    )
    approvals.add(grant)
    request = CredentialRequest(
        request_id="request-one-shot",
        binding_id="github-api",
        scopes=("github:read",),
        ttl_seconds=30,
    )
    secure.issue_credential(principal, run_id=run.run_id, request=request)
    try:
        secure.issue_credential(principal, run_id=run.run_id, request=request)
    except CredentialReplayError:
        pass
    else:
        raise AssertionError("replayed request ID unexpectedly minted another lease")

    try:
        secure.issue_credential(
            principal,
            run_id=run.run_id,
            request=CredentialRequest(
                request_id="request-wrong-approval",
                binding_id="github-api",
                scopes=("github:write",),
                ttl_seconds=30,
                approval_id=grant.approval_id,
            ),
        )
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("approval bound to another Run unexpectedly succeeded")


def test_configured_binding_not_granted_to_session_cannot_be_used():
    _, _, session, _, _, _, secure, principal = make_boundary()
    run = secure.start_run(principal, session_id=session.session_id)
    try:
        secure.issue_credential(
            principal,
            run_id=run.run_id,
            request=CredentialRequest(
                request_id="req-unknown-binding",
                binding_id="internal-admin",
                scopes=("admin:write",),
            ),
        )
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("unconfigured credential binding unexpectedly succeeded")
