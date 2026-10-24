from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Mapping

from .authorized_runtime import AuthorizedAgentRuntime
from .authoritative_identity import (
    ApprovalRevocationEvent,
    ApprovalRevocationEventVerifier,
    ApprovalRevocationFanout,
    AuthoritativeApprovalResolver,
    AuthoritativeCredentialBroker,
    HttpsJsonAuthorityTransport,
    JsonAuthorityTransport,
    WorkloadAssertionIssuer,
)
from .identity import CredentialBinding
from .runtime import AgentRuntime


class RoleRoutedAuthorityTransport:
    """Route authority calls through distinct, least-privilege service tokens.

    The existing broker contract has one transport interface. This router keeps
    that interface while enforcing a separate HTTPS transport/service identity
    for issuance, introspection, and credential revocation. Unknown operations
    fail closed instead of using a default credential.
    """

    def __init__(
        self,
        *,
        issuer: JsonAuthorityTransport,
        introspector: JsonAuthorityTransport,
        revoker: JsonAuthorityTransport,
    ) -> None:
        if len({id(issuer), id(introspector), id(revoker)}) != 3:
            raise ValueError("issue, introspection, and revocation transports must be distinct")
        self._issuer = issuer
        self._introspector = introspector
        self._revoker = revoker

    def request(self, method, path, body=None):
        if method == "POST" and path == "/v1/workload-credentials:issue":
            return self._issuer.request(method, path, body)
        if method == "POST" and path == "/v1/workload-credentials:introspect":
            return self._introspector.request(method, path, body)
        if method == "POST" and path in {
            "/v1/workload-credentials:revoke",
            "/v1/workload-credentials:revoke-run",
            "/v1/workload-credentials:revoke-approval",
        }:
            return self._revoker.request(method, path, body)
        raise RuntimeError("authoritative request is not assigned to a service role")


class RoleRoutedApprovalTransport:
    """Separate read-only approval lookup from approval-authority revocation."""

    def __init__(
        self,
        *,
        reader: JsonAuthorityTransport,
        revoker: JsonAuthorityTransport,
    ) -> None:
        if reader is revoker:
            raise ValueError("approval reader and approval revoker transports must be distinct")
        self._reader = reader
        self._revoker = revoker

    def request(self, method, path, body=None):
        if method == "GET" and path.startswith("/v1/authorization/approvals/") and not path.endswith(":revoke"):
            return self._reader.request(method, path, body)
        if method == "POST" and path.startswith("/v1/authorization/approvals/") and path.endswith(":revoke"):
            return self._revoker.request(method, path, body)
        raise RuntimeError("approval request is not assigned to a service role")


@dataclass(frozen=True)
class AuthoritativeRuntimeSecurity:
    """Production-configuration bundle; it is not proof the authority is deployed."""

    runtime: AuthorizedAgentRuntime
    credential_broker: AuthoritativeCredentialBroker
    approval_resolver: AuthoritativeApprovalResolver
    approval_revocation_fanout: ApprovalRevocationFanout

    def handle_approval_revocation(self, event: ApprovalRevocationEvent) -> int:
        """Entry point for an authenticated, durable approval-outbox consumer.

        The consumer must ACK its own durable inbox only after this returns. If
        downstream fan-out fails, propagate the failure and retry the same event
        ID; do not acknowledge it or generate a replacement event ID.
        """
        return self.approval_revocation_fanout.handle(event)


def build_authoritative_runtime_security(
    runtime: AgentRuntime,
    *,
    authority_base_url: str,
    credential_bindings: Mapping[str, CredentialBinding],
    assertion_issuer: WorkloadAssertionIssuer,
    approval_event_verifier: ApprovalRevocationEventVerifier,
    issue_service_token_provider: Callable[[], str],
    introspection_service_token_provider: Callable[[], str],
    credential_revocation_token_provider: Callable[[], str],
    approval_reader_token_provider: Callable[[], str],
    approval_revocation_token_provider: Callable[[], str],
    issuer: str = "cloud-agent-runtime",
    timeout_seconds: float = 3.0,
    clock: Callable[[], float] = time.time,
) -> AuthoritativeRuntimeSecurity:
    """Compose fail-closed Runtime adapters with separate authority service roles.

    Operator configuration must supply five distinct service credentials:
    issuer, introspector, credential revoker, approval reader, and approval
    revoker. This
    function uses verified TLS, bounded responses/timeouts, and disabled
    redirects for every HTTPS client. The remote API itself must implement the
    versioned contract in docs/authoritative-identity-integration.md.

    This does not create the ACP server, provision trust keys, or make an
    offline token immediately revocable. Production remains disabled until the
    paired ACP authority, KMS/SPIFFE assertion issuer, authenticated approval
    outbox, durable database and live acceptance are deployed and verified.
    """
    providers = (
        issue_service_token_provider,
        introspection_service_token_provider,
        credential_revocation_token_provider,
        approval_reader_token_provider,
        approval_revocation_token_provider,
    )
    if len({id(provider) for provider in providers}) != 5:
        raise ValueError("each authority service role requires a distinct token provider")
    if not authority_base_url.startswith("https://"):
        raise ValueError("authoritative identity requires HTTPS")

    issue_transport = HttpsJsonAuthorityTransport(
        base_url=authority_base_url,
        service_token_provider=issue_service_token_provider,
        timeout_seconds=timeout_seconds,
    )
    introspection_transport = HttpsJsonAuthorityTransport(
        base_url=authority_base_url,
        service_token_provider=introspection_service_token_provider,
        timeout_seconds=timeout_seconds,
    )
    revocation_transport = HttpsJsonAuthorityTransport(
        base_url=authority_base_url,
        service_token_provider=credential_revocation_token_provider,
        timeout_seconds=timeout_seconds,
    )
    approval_reader_transport = HttpsJsonAuthorityTransport(
        base_url=authority_base_url,
        service_token_provider=approval_reader_token_provider,
        timeout_seconds=timeout_seconds,
    )
    approval_revocation_transport = HttpsJsonAuthorityTransport(
        base_url=authority_base_url,
        service_token_provider=approval_revocation_token_provider,
        timeout_seconds=timeout_seconds,
    )
    routed_approval_transport = RoleRoutedApprovalTransport(
        reader=approval_reader_transport,
        revoker=approval_revocation_transport,
    )

    routed_broker_transport = RoleRoutedAuthorityTransport(
        issuer=issue_transport,
        introspector=introspection_transport,
        revoker=revocation_transport,
    )
    broker = AuthoritativeCredentialBroker(
        transport=routed_broker_transport,
        assertion_issuer=assertion_issuer,
        clock=clock,
    )
    approvals = AuthoritativeApprovalResolver(routed_approval_transport, clock=clock)
    fanout = ApprovalRevocationFanout(
        approvals=approvals,
        broker=broker,
        event_verifier=approval_event_verifier,
    )
    facade = AuthorizedAgentRuntime(
        runtime,
        credential_bindings=credential_bindings,
        credential_broker=broker,
        approval_resolver=approvals,
        issuer=issuer,
        clock=clock,
    )
    return AuthoritativeRuntimeSecurity(
        runtime=facade,
        credential_broker=broker,
        approval_resolver=approvals,
        approval_revocation_fanout=fanout,
    )
