import ssl

import pytest

from cloud_agent_runtime.authoritative_identity import (
    ApprovalRevocationEvent,
    ApprovalRevocationFanout,
    AuthorityNotFoundError,
    AuthorityProtocolError,
    AuthoritativeApprovalResolver,
    AuthoritativeCredentialBroker,
    HttpsJsonAuthorityTransport,
)
from cloud_agent_runtime.identity import (
    ApprovalGrant,
    CredentialBinding,
    CredentialRequest,
    IdentityAuthorizationError,
    WorkloadIdentity,
)


class FakeClock:
    def __init__(self, now=10_000.0):
        self.now = now

    def __call__(self):
        return self.now


class FakeAssertionIssuer:
    def __init__(self):
        self.calls = []

    def issue_assertion(self, identity, *, audience, binding, request, expires_at):
        self.calls.append({
            "identity": identity,
            "audience": audience,
            "binding": binding,
            "request": request,
            "expires_at": expires_at,
        })
        return "signed-short-lived-workload-assertion"


class FakeTransport:
    def __init__(self):
        self.calls = []
        self.responses = {}
        self.errors = {}

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        error = self.errors.get((method, path))
        if error:
            raise error
        response = self.responses.get((method, path), {})
        return response(body) if callable(response) else response


def identity(now=10_000.0):
    return WorkloadIdentity(
        issuer="runtime-issuer",
        subject="run:run-a",
        tenant_id="tenant-a",
        agent_id="agent-a",
        release_id="release-a",
        session_id="session-a",
        run_id="run-a",
        sandbox_id="sandbox-a",
        sandbox_revision=3,
        workspace_ref="sandbox://kubernetes/sandbox-a",
        binding_id="github-binding",
        capability="github",
        audience="https://api.github.com",
        allowed_scopes=("github:read", "github:write"),
        issued_at=now,
        expires_at=now + 120,
    )


def binding():
    return CredentialBinding(
        binding_id="github-binding",
        capability="github",
        audience="https://api.github.com",
        allowed_scopes=("github:read", "github:write"),
        max_ttl_seconds=90,
    )


def approval(now=10_000.0, *, expires=10_030.0, agent_id="agent-a"):
    return ApprovalGrant(
        approval_id="approval-a",
        tenant_id="tenant-a",
        agent_id=agent_id,
        session_id="session-a",
        run_id="run-a",
        binding_id="github-binding",
        audience="https://api.github.com",
        scopes=("github:write",),
        approved_by="human:security",
        issued_at=now,
        expires_at=expires,
    )


def broker_response(*, scopes=None, audience="https://api.github.com",
                    run_id="run-a", session_id="session-a",
                    binding_id="github-binding", expires_at=10_020.0):
    return {
        "credential": {
            "credential_id": "cred-001",
            "access_token": "opaque-provider-token",
            "audience": audience,
            "scopes": list(scopes or ("github:read",)),
            "issued_at": 10_000.0,
            "expires_at": expires_at,
            "run_id": run_id,
            "session_id": session_id,
            "binding_id": binding_id,
            "sandbox_id": "sandbox-a",
            "sandbox_revision": 3,
        }
    }


def identity_payload():
    value = identity()
    return {
        "issuer": value.issuer,
        "subject": value.subject,
        "tenant_id": value.tenant_id,
        "agent_id": value.agent_id,
        "release_id": value.release_id,
        "session_id": value.session_id,
        "run_id": value.run_id,
        "sandbox_id": value.sandbox_id,
        "workspace_ref": value.workspace_ref,
        "binding_id": value.binding_id,
        "capability": value.capability,
        "audience": value.audience,
        "allowed_scopes": list(value.allowed_scopes),
        "issued_at": value.issued_at,
        "expires_at": value.expires_at,
    }


def make_broker(transport=None, clock=None):
    return AuthoritativeCredentialBroker(
        transport=transport or FakeTransport(),
        assertion_issuer=FakeAssertionIssuer(),
        clock=clock or FakeClock(),
    )


def test_issue_sends_signed_assertion_not_caller_claims_and_bounds_lease():
    clock = FakeClock()
    transport = FakeTransport()
    assertion = FakeAssertionIssuer()
    transport.responses[("POST", "/v1/workload-credentials:issue")] = broker_response(
        expires_at=10_025.0
    )
    broker = AuthoritativeCredentialBroker(
        transport=transport, assertion_issuer=assertion, clock=clock
    )
    request = CredentialRequest(
        request_id="one-shot-1",
        binding_id="github-binding",
        scopes=("github:read",),
        ttl_seconds=25,
    )

    lease = broker.issue(identity(), binding(), request)

    method, path, body = transport.calls[0]
    assert (method, path) == ("POST", "/v1/workload-credentials:issue")
    assert body["workload_assertion"] == "signed-short-lived-workload-assertion"
    assert body["request_id"] == "one-shot-1"
    assert body["audience"] == "https://api.github.com"
    assert body["scopes"] == ["github:read"]
    assert "tenant_id" not in body
    assert "agent_id" not in body
    assert "workspace_ref" not in body
    assert len(assertion.calls) == 1
    assert assertion.calls[0]["identity"].run_id == "run-a"
    assert lease.access_token == "opaque-provider-token"
    assert lease.expires_at <= 10_025.0
    assert lease.run_id == "run-a"
    assert transport.calls[0][2]["workload_assertion"] == "signed-short-lived-workload-assertion"


@pytest.mark.parametrize(
    "override",
    [
        {"audience": "https://evil.example"},
        {"scopes": ("github:read", "github:write")},
        {"run_id": "run-other"},
        {"session_id": "session-other"},
        {"binding_id": "binding-other"},
        {"expires_at": 10_091.0},
    ],
)
def test_issue_rejects_authority_response_that_widens_or_rebinds(override):
    transport = FakeTransport()
    result = {
        "audience": "https://api.github.com",
        "scopes": ("github:read",),
        "run_id": "run-a",
        "session_id": "session-a",
        "binding_id": "github-binding",
        "expires_at": 10_020.0,
    }
    result.update(override)
    transport.responses[("POST", "/v1/workload-credentials:issue")] = broker_response(**result)
    request = CredentialRequest(
        request_id="strict-one-shot",
        binding_id="github-binding",
        scopes=("github:read",),
        ttl_seconds=20,
    )
    with pytest.raises(AuthorityProtocolError):
        make_broker(transport).issue(identity(), binding(), request)


def test_issue_requires_exact_live_approval_for_write_and_passes_only_approval_id():
    transport = FakeTransport()
    assertion = FakeAssertionIssuer()
    transport.responses[("POST", "/v1/workload-credentials:issue")] = broker_response(
        scopes=("github:write",), expires_at=10_010.0
    )
    broker = AuthoritativeCredentialBroker(
        transport=transport, assertion_issuer=assertion, clock=FakeClock()
    )
    request = CredentialRequest(
        request_id="write-one-shot",
        binding_id="github-binding",
        scopes=("github:write",),
        ttl_seconds=60,
        approval_id="approval-a",
    )
    with pytest.raises(IdentityAuthorizationError):
        broker.issue(identity(), binding(), request)
    with pytest.raises(IdentityAuthorizationError):
        broker.issue(identity(), binding(), request, approval(agent_id="other-agent"))
    lease = broker.issue(identity(), binding(), request, approval())
    body = transport.calls[-1][2]
    assert body["approval_id"] == "approval-a"
    assert "approval" not in body
    assert lease.expires_at <= approval().expires_at


def test_introspection_fails_closed_on_inactive_wrong_audience_or_scope():
    clock = FakeClock()
    transport = FakeTransport()
    broker = make_broker(transport, clock)
    path = "/v1/workload-credentials:introspect"
    transport.responses[("POST", path)] = {
        "credential": {
            "active": True,
            "audience": "https://api.github.com",
            "scopes": ["github:read"],
            "expires_at": 10_020.0,
            "workload_identity": identity_payload(),
        }
    }
    assert broker.validate(
        "opaque-provider-token",
        audience="https://api.github.com",
        required_scopes=("github:read",),
    ).run_id == "run-a"
    for record, audience_value, scopes in (
        ({"active": False}, "https://api.github.com", ()),
        ({
            "active": True,
            "audience": "https://evil.example",
            "scopes": ["github:read"],
            "expires_at": 10_020.0,
            "workload_identity": identity_payload(),
        }, "https://evil.example", ()),
        ({
            "active": True,
            "audience": "https://api.github.com",
            "scopes": ["github:read"],
            "expires_at": 10_020.0,
            "workload_identity": identity_payload(),
        }, "https://api.github.com", ("github:write",)),
    ):
        transport.responses[("POST", path)] = {"credential": record}
        with pytest.raises(IdentityAuthorizationError):
            broker.validate(
                "opaque-provider-token",
                audience=audience_value,
                required_scopes=scopes,
            )


def test_revocation_contract_requires_valid_acknowledgements():
    transport = FakeTransport()
    broker = make_broker(transport)
    transport.responses[("POST", "/v1/workload-credentials:revoke")] = {"revoked": True}
    transport.responses[("POST", "/v1/workload-credentials:revoke-run")] = {
        "revoked_count": 2
    }
    transport.responses[("POST", "/v1/workload-credentials:revoke-approval")] = {
        "revoked_count": 1
    }
    assert broker.revoke_credential("cred-001") is True
    assert broker.revoke_run("run-a", sandbox_revision=3) == 2
    assert transport.calls[-2] == (
        "POST",
        "/v1/workload-credentials:revoke-run",
        {"run_id": "run-a", "sandbox_revision": 3},
    )
    assert broker.revoke_approval("approval-a") == 1
    transport.responses[("POST", "/v1/workload-credentials:revoke-run")] = {
        "revoked_count": -1
    }
    with pytest.raises(AuthorityProtocolError):
        broker.revoke_run("run-a", sandbox_revision=3)


def test_authoritative_approval_resolver_reloads_live_state_and_checks_expiry():
    clock = FakeClock()
    transport = FakeTransport()
    resolver = AuthoritativeApprovalResolver(transport, clock=clock)
    path = "/v1/authorization/approvals/approval-a"
    transport.responses[("GET", path)] = {
        "state": "ACTIVE",
        "grant": {
            "approval_id": "approval-a",
            "tenant_id": "tenant-a",
            "agent_id": "agent-a",
            "session_id": "session-a",
            "run_id": "run-a",
            "binding_id": "github-binding",
            "audience": "https://api.github.com",
            "scopes": ["github:write"],
            "approved_by": "human:security",
            "issued_at": 9_999.0,
            "expires_at": 10_010.0,
        },
    }
    assert resolver.get_approved_grant("approval-a").run_id == "run-a"
    clock.now = 10_011.0
    assert resolver.get_approved_grant("approval-a") is None
    transport.errors[("GET", path)] = AuthorityNotFoundError(404)
    assert resolver.get_approved_grant("approval-a") is None


def test_run_revocation_requires_a_canonical_positive_sandbox_revision():
    broker = make_broker(FakeTransport())
    with pytest.raises(ValueError, match="sandbox_revision"):
        broker.revoke_run("run-a", sandbox_revision=0)


def test_revocation_fanout_revokes_authority_before_existing_leases():
    transport = FakeTransport()
    approval_path = "/v1/authorization/approvals/approval-a:revoke"
    transport.responses[("POST", approval_path)] = {"state": "REVOKED", "version": 8}
    transport.responses[("POST", "/v1/workload-credentials:revoke-approval")] = {
        "revoked_count": 3
    }
    approvals = AuthoritativeApprovalResolver(transport)
    broker = make_broker(transport)

    class TrustedOutboxVerifier:
        def verify(self, event):
            return event.source == "trusted-approval-outbox"

    fanout = ApprovalRevocationFanout(
        approvals=approvals,
        broker=broker,
        event_verifier=TrustedOutboxVerifier(),
    )

    assert fanout.handle(ApprovalRevocationEvent(
        event_id="event-1",
        approval_id="approval-a",
        source="trusted-approval-outbox",
    )) == 3
    assert transport.calls[0] == (
        "POST", approval_path, {"event_id": "event-1"}
    )
    assert transport.calls[1] == (
        "POST", "/v1/workload-credentials:revoke-approval",
        {"approval_id": "approval-a", "event_id": "event-1"},
    )


def test_revocation_fanout_surfaces_broker_failure_for_retry():
    transport = FakeTransport()
    transport.responses[("POST", "/v1/authorization/approvals/approval-a:revoke")] = {
        "state": "REVOKED", "version": 8
    }
    transport.errors[("POST", "/v1/workload-credentials:revoke-approval")] = (
        RuntimeError("broker down")
    )
    class TrustedOutboxVerifier:
        def verify(self, event):
            return event.source == "trusted-approval-outbox"

    fanout = ApprovalRevocationFanout(
        approvals=AuthoritativeApprovalResolver(transport),
        broker=make_broker(transport),
        event_verifier=TrustedOutboxVerifier(),
    )
    with pytest.raises(RuntimeError, match="broker down"):
        fanout.handle(ApprovalRevocationEvent(
            event_id="event-1",
            approval_id="approval-a",
            source="trusted-approval-outbox",
        ))


def test_production_transport_requires_verified_https_and_bounded_timeout():
    with pytest.raises(ValueError, match="HTTPS"):
        HttpsJsonAuthorityTransport(
            base_url="http://identity.example",
            service_token_provider=lambda: "secret",
        )
    insecure = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    insecure.check_hostname = False
    insecure.verify_mode = ssl.CERT_NONE
    with pytest.raises(ValueError, match="TLS"):
        HttpsJsonAuthorityTransport(
            base_url="https://identity.example",
            service_token_provider=lambda: "secret",
            ssl_context=insecure,
        )
    with pytest.raises(ValueError, match="timeout"):
        HttpsJsonAuthorityTransport(
            base_url="https://identity.example",
            service_token_provider=lambda: "secret",
            timeout_seconds=90,
        )

def test_untrusted_revocation_event_is_denied_before_any_authority_mutation():
    transport = FakeTransport()

    class RejectAllVerifier:
        def verify(self, event):
            return False

    fanout = ApprovalRevocationFanout(
        approvals=AuthoritativeApprovalResolver(transport),
        broker=make_broker(transport),
        event_verifier=RejectAllVerifier(),
    )
    with pytest.raises(IdentityAuthorizationError, match="authentication failed"):
        fanout.handle(ApprovalRevocationEvent(
            event_id="untrusted-event",
            approval_id="approval-a",
            source="caller-controlled",
        ))
    assert transport.calls == []

def test_run_revocation_fences_old_sandbox_assertions_but_allows_new_revision():
    clock = FakeClock()
    transport = FakeTransport()
    assertion = FakeAssertionIssuer()
    broker = AuthoritativeCredentialBroker(
        transport=transport,
        assertion_issuer=assertion,
        clock=clock,
    )
    request1 = CredentialRequest(
        request_id="revision-3-before-fence",
        binding_id="github-binding",
        scopes=("github:read",),
        ttl_seconds=20,
    )
    transport.responses[("POST", "/v1/workload-credentials:issue")] = broker_response()
    broker.issue(identity(), binding(), request1)
    broker.revoke_run("run-a", sandbox_revision=3)

    stale = identity()
    with pytest.raises(IdentityAuthorizationError):
        broker.issue(
            stale,
            binding(),
            CredentialRequest(
                request_id="revision-3-after-fence",
                binding_id="github-binding",
                scopes=("github:read",),
                ttl_seconds=20,
            ),
        )
    newer = WorkloadIdentity(
        **{
            **identity_payload(),
            "sandbox_id": "sandbox-b",
            "sandbox_revision": 4,
            "workspace_ref": "sandbox://kubernetes/sandbox-b",
        }
    )
    transport.responses[("POST", "/v1/workload-credentials:issue")] = broker_response(
        expires_at=10_020.0
    )
    transport.responses[("POST", "/v1/workload-credentials:issue")]["credential"].update(
        {"sandbox_id": "sandbox-b", "sandbox_revision": 4}
    )
    lease = broker.issue(
        newer,
        binding(),
        CredentialRequest(
            request_id="revision-4-after-fence",
            binding_id="github-binding",
            scopes=("github:read",),
            ttl_seconds=20,
        ),
    )
    assert lease.run_id == "run-a"

