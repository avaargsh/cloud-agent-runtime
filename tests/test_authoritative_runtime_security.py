import pytest

from cloud_agent_runtime import (
    CapabilityBinding,
    CredentialBinding,
    InMemorySandboxProvider,
    InMemoryWorkflowDriver,
)
from cloud_agent_runtime.authoritative_identity import (
    ApprovalRevocationEvent,
    ApprovalRevocationFanout,
    AuthoritativeApprovalResolver,
    AuthoritativeCredentialBroker,
    HttpsJsonAuthorityTransport,
)
from cloud_agent_runtime.authoritative_runtime_security import (
    AuthoritativeRuntimeSecurity,
    RoleRoutedAuthorityTransport,
    build_authoritative_runtime_security,
)
from cloud_agent_runtime.identity import (
    ApprovalGrant,
    AuthenticatedPrincipal,
    CredentialRequest,
    IdentityAuthorizationError,
)
from cloud_agent_runtime.runtime import AgentRuntime


class RecordingTransport:
    def __init__(self, name, response=None):
        self.name = name
        self.response = response or {}
        self.calls = []

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        return self.response


class FakeAssertionIssuer:
    def issue_assertion(self, identity, *, audience, binding, request, expires_at):
        return "signed-assertion-placeholder"


class FakeEventVerifier:
    def __init__(self, accepted=True):
        self.accepted = accepted
        self.calls = []

    def verify(self, event):
        self.calls.append(event)
        return self.accepted


def test_role_router_routes_to_distinct_least_privilege_transports():
    issue = RecordingTransport("issuer")
    introspect = RecordingTransport("introspector")
    revoke = RecordingTransport("revoker")
    router = RoleRoutedAuthorityTransport(
        issuer=issue,
        introspector=introspect,
        revoker=revoke,
    )

    router.request("POST", "/v1/workload-credentials:issue", {"request_id": "r1"})
    router.request("POST", "/v1/workload-credentials:introspect", {"access_token": "t"})
    router.request("POST", "/v1/workload-credentials:revoke-run", {"run_id": "run1"})
    assert len(issue.calls) == 1
    assert len(introspect.calls) == 1
    assert len(revoke.calls) == 1

    with pytest.raises(RuntimeError, match="not assigned"):
        router.request("POST", "/v1/unknown-privileged-route", {})


def test_role_router_rejects_transport_reuse():
    shared = RecordingTransport("shared")
    with pytest.raises(ValueError, match="must be distinct"):
        RoleRoutedAuthorityTransport(
            issuer=shared,
            introspector=shared,
            revoker=RecordingTransport("revoker"),
        )


def _runtime():
    return AgentRuntime(
        sandbox_provider=InMemorySandboxProvider(),
        workflow_driver=InMemoryWorkflowDriver(),
    )


def test_production_builder_requires_distinct_role_token_providers():
    providers = [lambda: f"token-{index}" for index in range(4)]
    bundle = build_authoritative_runtime_security(
        _runtime(),
        authority_base_url="https://identity.example.test",
        credential_bindings={
            "github-api": CredentialBinding(
                binding_id="github-api",
                capability="github",
                audience="https://api.github.com",
                allowed_scopes=("github:read",),
                max_ttl_seconds=60,
            )
        },
        assertion_issuer=FakeAssertionIssuer(),
        approval_event_verifier=FakeEventVerifier(),
        issue_service_token_provider=providers[0],
        introspection_service_token_provider=providers[1],
        credential_revocation_token_provider=providers[2],
        approval_authority_token_provider=providers[3],
    )
    assert isinstance(bundle, AuthoritativeRuntimeSecurity)
    assert isinstance(bundle.runtime._credential_broker, AuthoritativeCredentialBroker)
    assert isinstance(bundle.approval_resolver, AuthoritativeApprovalResolver)
    assert isinstance(bundle.approval_revocation_fanout, ApprovalRevocationFanout)
    assert isinstance(bundle.runtime._credential_broker.transport, RoleRoutedAuthorityTransport)


def test_production_builder_rejects_reused_token_provider_and_plain_http():
    shared = lambda: "same-service-token"
    providers = [lambda: "issue", lambda: "introspect", lambda: "revoke"]
    common = dict(
        runtime=_runtime(),
        credential_bindings={},
        assertion_issuer=FakeAssertionIssuer(),
        approval_event_verifier=FakeEventVerifier(),
    )
    duplicate = shared
    with pytest.raises(ValueError, match="distinct token provider"):
        build_authoritative_runtime_security(
            **common,
            authority_base_url="https://identity.example.test",
            issue_service_token_provider=duplicate,
            introspection_service_token_provider=duplicate,
            credential_revocation_token_provider=providers[1],
            approval_authority_token_provider=providers[2],
        )
    with pytest.raises(ValueError, match="requires HTTPS"):
        build_authoritative_runtime_security(
            **common,
            authority_base_url="http://identity.example.test",
            issue_service_token_provider=providers[0],
            introspection_service_token_provider=providers[1],
            credential_revocation_token_provider=providers[2],
            approval_authority_token_provider=lambda: "approval",
        )


def test_approval_event_fanout_authenticates_and_revokes_authority_first():
    order = []
    event = ApprovalRevocationEvent(
        event_id="evt-44",
        approval_id="approval-44",
        source="trusted-outbox",
    )

    class ApprovalService:
        def revoke(self, approval_id, *, event_id):
            order.append(("authority", approval_id, event_id))
            return 12

    class Broker:
        def revoke_approval(self, approval_id, *, event_id=None):
            order.append(("broker", approval_id, event_id))
            return 2

    verifier = FakeEventVerifier(accepted=True)
    fanout = ApprovalRevocationFanout(
        approvals=ApprovalService(),
        broker=Broker(),
        event_verifier=verifier,
    )
    assert fanout.handle(event) == 2
    assert order == [
        ("authority", "approval-44", "evt-44"),
        ("broker", "approval-44", "evt-44"),
    ]

    rejected = ApprovalRevocationFanout(
        approvals=ApprovalService(),
        broker=Broker(),
        event_verifier=FakeEventVerifier(accepted=False),
    )
    order.clear()
    with pytest.raises(IdentityAuthorizationError, match="authentication failed"):
        rejected.handle(event)
    assert order == []


def test_approval_fanout_propagates_broker_failure_for_durable_retry():
    order = []
    event = ApprovalRevocationEvent(
        event_id="evt-retry",
        approval_id="approval-retry",
        source="trusted-outbox",
    )

    class ApprovalService:
        def revoke(self, approval_id, *, event_id):
            order.append("authority")
            return 1

    class BrokenBroker:
        def revoke_approval(self, approval_id, *, event_id=None):
            order.append("broker")
            raise RuntimeError("temporary downstream outage")

    fanout = ApprovalRevocationFanout(
        approvals=ApprovalService(),
        broker=BrokenBroker(),
        event_verifier=FakeEventVerifier(),
    )
    with pytest.raises(RuntimeError, match="temporary downstream outage"):
        fanout.handle(event)
    assert order == ["authority", "broker"]
