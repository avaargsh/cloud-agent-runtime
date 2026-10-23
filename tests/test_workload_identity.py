from cloud_agent_runtime import (
    AgentRuntime,
    CapabilityBinding,
    InMemorySandboxProvider,
    InMemoryWorkflowDriver,
)
from cloud_agent_runtime.authorized_runtime import AuthorizedAgentRuntime
from cloud_agent_runtime.identity import (
    ApprovalGrant,
    AuthenticatedPrincipal,
    CredentialBinding,
    CredentialExpiredError,
    CredentialReplayError,
    CredentialRequest,
    IdentityAuthorizationError,
    InMemoryApprovalResolver,
    InMemoryCredentialBroker,
    WorkloadIdentity,
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

    try:
        secure.issue_credential(
            principal,
            run_id=run.run_id,
            request=CredentialRequest(
                request_id="req-readonly-write-escalation",
                binding_id="github-api",
                scopes=("github:write",),
            ),
        )
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("read-only capability minted a write credential")


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
    # Trusted revocation event fan-out: it updates the authoritative resolver
    # and invalidates the broker's already-issued approval-bound leases.
    approvals.revoke(grant.approval_id)
    broker.revoke_approval(grant.approval_id)
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

def test_secured_rebind_rejects_caller_selected_snapshot_reference():
    _, _, session, _, _, _, secure, principal = make_boundary()
    run = secure.start_run(principal, session_id=session.session_id)

    try:
        secure.rebind_run_sandbox(
            principal,
            run_id=run.run_id,
            snapshot_ref="sandbox://kubernetes/another-agents-workspace",
        )
    except TypeError:
        pass
    else:
        raise AssertionError("caller-selected snapshot reference was accepted")

def test_same_tenant_agent_mismatch_and_mutated_run_cannot_rebind_workspace():
    _, runtime, session, _, broker, _, secure, principal = make_boundary()
    another_session = runtime.create_session(
        agent_id="agent-b",
        release_id="release-b-v1",
        tenant_id=session.tenant_id,
    )
    other_agent = AuthenticatedPrincipal(
        issuer=principal.issuer,
        subject="agent-b-subject",
        tenant_id=session.tenant_id,
        agent_id="agent-b",
    )

    try:
        secure.start_run(other_agent, session_id=session.session_id)
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("another Agent in the same tenant accessed this Session")

    another_run = secure.start_run(other_agent, session_id=another_session.session_id)
    try:
        secure.get_run(principal, run_id=another_run.run_id)
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("Agent read another Agent's Run in the same tenant")

    run = secure.start_run(principal, session_id=session.session_id)
    canonical = runtime.store.get_run(run.run_id)
    assert canonical.sandbox_ref is not None
    run.sandbox_ref = "sandbox://kubernetes/forged-workspace"
    run.sandbox_binding.sandbox_id = "forged-workspace"

    lease = secure.issue_credential(
        principal,
        run_id=run.run_id,
        request=CredentialRequest(
            request_id="req-canonical-workspace",
            binding_id="github-api",
            scopes=("github:read",),
        ),
    )
    identity = broker.validate(
        lease.access_token,
        audience="https://api.github.com",
        required_scopes=("github:read",),
    )
    assert identity.workspace_ref == canonical.sandbox_ref
    assert identity.sandbox_id == canonical.sandbox_binding.sandbox_id

def test_inactive_session_cannot_rebind_or_resume_a_run():
    _, runtime, session, _, _, _, secure, principal = make_boundary()
    run = secure.start_run(principal, session_id=session.session_id)
    canonical_workspace = runtime.store.get_run(run.run_id).sandbox_ref
    runtime.pause_session(session.session_id)

    for operation in (
        lambda: secure.rebind_run_sandbox(principal, run_id=run.run_id),
        lambda: secure.resume_run(principal, run_id=run.run_id),
    ):
        try:
            operation()
        except IdentityAuthorizationError:
            pass
        else:
            raise AssertionError("inactive Session allowed Run recovery")
    assert runtime.store.get_run(run.run_id).sandbox_ref == canonical_workspace

def test_unknown_scope_verbs_require_approval_in_reference_broker():
    clock = FakeClock()
    binding = CredentialBinding(
        binding_id="github-owner",
        capability="github",
        audience="https://api.github.com",
        allowed_scopes=("github:owner",),
        max_ttl_seconds=30,
    )
    identity = WorkloadIdentity(
        issuer="runtime-test",
        subject="run:run-unknown-scope",
        tenant_id="tenant-a",
        agent_id="agent-a",
        release_id="release-a",
        session_id="session-a",
        run_id="run-unknown-scope",
        sandbox_id="sandbox-a",
        sandbox_revision=1,
        workspace_ref="sandbox://kubernetes/sandbox-a",
        binding_id=binding.binding_id,
        capability=binding.capability,
        audience=binding.audience,
        allowed_scopes=("github:owner",),
        issued_at=clock(),
        expires_at=clock() + 60,
    )
    broker = InMemoryCredentialBroker(clock=clock)
    try:
        broker.issue(
            identity,
            binding,
            CredentialRequest(
                request_id="unknown-owner-scope",
                binding_id=binding.binding_id,
                scopes=("github:owner",),
                ttl_seconds=20,
            ),
        )
    except IdentityAuthorizationError:
        pass
    else:
        raise AssertionError("unknown non-read scope bypassed the approval gate")

