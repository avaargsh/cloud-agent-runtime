from __future__ import annotations

import json
import ssl
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    HTTPSHandler,
    Request,
    build_opener,
)

from .identity import (
    ApprovalGrant,
    ApprovalResolver,
    CredentialBinding,
    CredentialBroker,
    CredentialExpiredError,
    CredentialLease,
    CredentialReplayError,
    CredentialRequest,
    IdentityAuthorizationError,
    WorkloadIdentity,
)


class AuthorityUnavailableError(RuntimeError):
    """The authoritative identity service cannot be reached safely."""


class AuthorityProtocolError(RuntimeError):
    """The authoritative identity service returned an invalid response."""


class AuthorityRequestError(RuntimeError):
    """The authority denied or failed a request; response bodies are not echoed."""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"authoritative identity request failed (HTTP {status})")


class AuthorityNotFoundError(AuthorityRequestError):
    """The requested authoritative approval does not exist."""


class JsonAuthorityTransport(Protocol):
    """Minimal injectable HTTP transport used by the authority adapters."""

    def request(
        self,
        method: str,
        path: str,
        body: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        ...


class WorkloadAssertionIssuer(Protocol):
    """KMS/SPIFFE-backed signer for short-lived broker-audience assertions.

    The implementation must not expose signing keys to the Agent or accept
    identity claims directly from an Agent request. It must sign the canonical
    identity and requested binding/scope context with a short expiry.
    """

    def issue_assertion(
        self,
        identity: WorkloadIdentity,
        *,
        audience: str,
        binding: CredentialBinding,
        request: CredentialRequest,
        expires_at: float,
    ) -> str:
        ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Prevent forwarding service bearer tokens to redirect targets.
        return None


class HttpsJsonAuthorityTransport:
    """Bounded HTTPS JSON transport with verified TLS and no redirects."""

    _MAX_RESPONSE_BYTES = 1_048_576

    def __init__(
        self,
        *,
        base_url: str,
        service_token_provider: Callable[[], str],
        timeout_seconds: float = 3.0,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("authority base_url must be an absolute HTTPS origin/path")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or timeout_seconds <= 0
            or timeout_seconds > 15
        ):
            raise ValueError("authority timeout must be in (0, 15] seconds")
        context = ssl_context or ssl.create_default_context()
        if context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname:
            raise ValueError("authority TLS must verify the peer certificate and hostname")
        self._base_url = base_url.rstrip("/")
        self._token_provider = service_token_provider
        self._timeout = float(timeout_seconds)
        self._opener = build_opener(
            _NoRedirect(),
            HTTPSHandler(context=context),
        )

    def request(
        self,
        method: str,
        path: str,
        body: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError("authority path must be an absolute service-relative path")
        token = self._token_provider()
        if not isinstance(token, str) or not token.strip():
            raise AuthorityUnavailableError("authoritative service credential is unavailable")

        data = (
            json.dumps(
                body,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            if body is not None
            else None
        )
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = Request(
            self._base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                effective = urlsplit(response.geturl())
                base = urlsplit(self._base_url)
                if (
                    effective.scheme != "https"
                    or effective.hostname != base.hostname
                    or effective.port != base.port
                ):
                    raise AuthorityProtocolError("authority redirected outside its configured origin")
                raw = response.read(self._MAX_RESPONSE_BYTES + 1)
                status = response.status
        except HTTPError as exc:
            if exc.code == 404 and method == "GET" and path.startswith("/v1/authorization/approvals/"):
                raise AuthorityNotFoundError(404) from None
            if exc.code == 409 and method == "POST" and path == "/v1/workload-credentials:issue":
                raise CredentialReplayError("authoritative request ID was already consumed") from None
            if exc.code in (401, 403):
                raise IdentityAuthorizationError("authoritative identity request was denied") from None
            raise AuthorityRequestError(exc.code) from None
        except URLError as exc:
            raise AuthorityUnavailableError("authoritative identity service unavailable") from None
        except TimeoutError:
            raise AuthorityUnavailableError("authoritative identity request timed out") from None

        if status < 200 or status >= 300:
            raise AuthorityRequestError(status)
        if len(raw) > self._MAX_RESPONSE_BYTES:
            raise AuthorityProtocolError("authoritative response exceeds size limit")
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise AuthorityProtocolError("authoritative response is not valid JSON") from None
        if not isinstance(payload, dict):
            raise AuthorityProtocolError("authoritative response must be a JSON object")
        return payload


def _as_string(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise AuthorityProtocolError(f"authoritative field {key} is missing or invalid")
    return value


def _as_number(data: Mapping[str, Any], key: str) -> float:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AuthorityProtocolError(f"authoritative field {key} is missing or invalid")
    return float(value)


def _decode_identity(data: Any) -> WorkloadIdentity:
    if not isinstance(data, dict):
        raise AuthorityProtocolError("authoritative workload identity is missing")
    try:
        string_fields = (
            "issuer", "subject", "tenant_id", "agent_id", "release_id",
            "session_id", "run_id", "sandbox_id", "workspace_ref",
            "binding_id", "capability", "audience",
        )
        values = {name: _as_string(data, name) for name in string_fields}
        raw_scopes = data.get("allowed_scopes")
        if (
            not isinstance(raw_scopes, list)
            or not raw_scopes
            or any(not isinstance(scope, str) or not scope.strip() for scope in raw_scopes)
            or len(set(raw_scopes)) != len(raw_scopes)
        ):
            raise AuthorityProtocolError("authoritative identity scopes are invalid")
        values["allowed_scopes"] = tuple(raw_scopes)
        values["issued_at"] = _as_number(data, "issued_at")
        values["expires_at"] = _as_number(data, "expires_at")
        identity = WorkloadIdentity(**values)
    except TypeError:
        raise AuthorityProtocolError("authoritative workload identity shape is invalid") from None
    if identity.expires_at <= identity.issued_at:
        raise AuthorityProtocolError("authoritative workload identity lifetime is invalid")
    return identity


def _identity_payload(identity: WorkloadIdentity) -> dict[str, Any]:
    return {
        "issuer": identity.issuer,
        "subject": identity.subject,
        "tenant_id": identity.tenant_id,
        "agent_id": identity.agent_id,
        "release_id": identity.release_id,
        "session_id": identity.session_id,
        "run_id": identity.run_id,
        "sandbox_id": identity.sandbox_id,
        "workspace_ref": identity.workspace_ref,
        "binding_id": identity.binding_id,
        "capability": identity.capability,
        "audience": identity.audience,
        "allowed_scopes": list(identity.allowed_scopes),
        "issued_at": identity.issued_at,
        "expires_at": identity.expires_at,
    }


def _decode_grant(data: Any, *, now: float) -> ApprovalGrant | None:
    if not isinstance(data, dict) or data.get("state") != "ACTIVE":
        return None
    grant_data = data.get("grant")
    if not isinstance(grant_data, dict):
        raise AuthorityProtocolError("active approval has no grant payload")
    raw_scopes = grant_data.get("scopes")
    if (
        not isinstance(raw_scopes, list)
        or not raw_scopes
        or any(not isinstance(scope, str) or not scope.strip() for scope in raw_scopes)
        or len(set(raw_scopes)) != len(raw_scopes)
    ):
        raise AuthorityProtocolError("approval scopes are invalid")
    grant = ApprovalGrant(
        approval_id=_as_string(grant_data, "approval_id"),
        tenant_id=_as_string(grant_data, "tenant_id"),
        agent_id=_as_string(grant_data, "agent_id"),
        session_id=_as_string(grant_data, "session_id"),
        run_id=_as_string(grant_data, "run_id"),
        binding_id=_as_string(grant_data, "binding_id"),
        audience=_as_string(grant_data, "audience"),
        scopes=tuple(raw_scopes),
        approved_by=_as_string(grant_data, "approved_by"),
        issued_at=_as_number(grant_data, "issued_at"),
        expires_at=_as_number(grant_data, "expires_at"),
    )
    if grant.issued_at > now or grant.expires_at <= now:
        return None
    return grant


@dataclass(frozen=True)
class AuthoritativeCredentialBroker(CredentialBroker):
    """Client for the versioned, operator-owned Runtime Credential Authority API.

    The service contract is specified in docs/authoritative-identity-integration.md.
    This adapter deliberately does not mint provider tokens locally. It sends a
    signed, short-lived workload assertion to the authority, which must enforce
    durable request-ID consumption, exact approval binding, and revocation.
    """

    transport: JsonAuthorityTransport
    assertion_issuer: WorkloadAssertionIssuer
    clock: Callable[[], float] = time.time
    assertion_audience: str = "urn:cloud-agent-runtime:credential-authority"

    def issue(
        self,
        identity: WorkloadIdentity,
        binding: CredentialBinding,
        request: CredentialRequest,
        approval: ApprovalGrant | None = None,
    ) -> CredentialLease:
        now = self.clock()
        requested = set(request.scopes)
        if identity.expires_at <= now:
            raise CredentialExpiredError("workload identity has expired")
        if (
            identity.binding_id != binding.binding_id
            or identity.capability != binding.capability
            or identity.audience != binding.audience
            or request.binding_id != binding.binding_id
            or not requested
            or requested - set(identity.allowed_scopes)
            or requested - set(binding.allowed_scopes)
        ):
            raise IdentityAuthorizationError("credential binding or scopes do not match canonical identity")

        needs_approval = any(
            scope.rsplit(":", 1)[-1].lower() in {"write", "delete", "admin", "mutate"}
            for scope in requested
        )
        if request.approval_id is not None and not needs_approval:
            raise IdentityAuthorizationError("approval IDs are accepted only for approval-scoped credentials")
        if needs_approval:
            if approval is None or (
                approval.tenant_id != identity.tenant_id
                or approval.agent_id != identity.agent_id
                or approval.session_id != identity.session_id
                or approval.run_id != identity.run_id
                or approval.binding_id != binding.binding_id
                or approval.audience != binding.audience
                or not requested.issubset(set(approval.scopes))
                or approval.expires_at <= now
            ):
                raise IdentityAuthorizationError("an exact, live approval is required")
        if approval is not None and request.approval_id != approval.approval_id:
            raise IdentityAuthorizationError("approval reference mismatch")

        expires_at = min(
            now + request.ttl_seconds,
            now + binding.max_ttl_seconds,
            identity.expires_at,
            approval.expires_at if needs_approval and approval is not None else float("inf"),
        )
        if expires_at <= now:
            raise CredentialExpiredError("no positive credential lifetime remains")
        assertion = self.assertion_issuer.issue_assertion(
            identity,
            audience=self.assertion_audience,
            binding=binding,
            request=request,
            expires_at=expires_at,
        )
        if not isinstance(assertion, str) or not assertion.strip():
            raise AuthorityProtocolError("workload assertion issuer returned no assertion")

        response = self.transport.request(
            "POST",
            "/v1/workload-credentials:issue",
            {
                "request_id": request.request_id,
                "workload_assertion": assertion,
                "binding_id": binding.binding_id,
                "audience": binding.audience,
                "scopes": list(request.scopes),
                "requested_ttl_seconds": request.ttl_seconds,
                "approval_id": request.approval_id if needs_approval else None,
            },
        )
        issued = response.get("credential")
        if not isinstance(issued, dict):
            raise AuthorityProtocolError("authoritative credential response is missing")
        credential_id = _as_string(issued, "credential_id")
        token = _as_string(issued, "access_token")
        audience = _as_string(issued, "audience")
        raw_scopes = issued.get("scopes")
        if (
            audience != binding.audience
            or not isinstance(raw_scopes, list)
            or not all(isinstance(scope, str) for scope in raw_scopes)
            or set(raw_scopes) != requested
            or len(raw_scopes) != len(set(raw_scopes))
        ):
            raise AuthorityProtocolError("authoritative credential audience or scopes mismatch")
        issued_at = _as_number(issued, "issued_at")
        token_expires_at = _as_number(issued, "expires_at")
        if issued_at > now or token_expires_at <= now or token_expires_at > expires_at:
            raise AuthorityProtocolError("authoritative credential lifetime violates the requested bound")
        if (
            _as_string(issued, "run_id") != identity.run_id
            or _as_string(issued, "session_id") != identity.session_id
            or _as_string(issued, "binding_id") != binding.binding_id
        ):
            raise AuthorityProtocolError("authoritative credential identity binding mismatch")
        return CredentialLease(
            credential_id=credential_id,
            access_token=token,
            audience=audience,
            scopes=tuple(sorted(raw_scopes)),
            issued_at=issued_at,
            expires_at=token_expires_at,
            run_id=identity.run_id,
            session_id=identity.session_id,
            binding_id=binding.binding_id,
        )

    def validate(
        self,
        access_token: str,
        *,
        audience: str,
        required_scopes: tuple[str, ...] = (),
    ) -> WorkloadIdentity:
        if not isinstance(access_token, str) or not access_token:
            raise IdentityAuthorizationError("credential is missing")
        response = self.transport.request(
            "POST",
            "/v1/workload-credentials:introspect",
            {
                "access_token": access_token,
                "audience": audience,
                "required_scopes": list(required_scopes),
            },
        )
        record = response.get("credential")
        if not isinstance(record, dict) or record.get("active") is not True:
            raise IdentityAuthorizationError("credential is unknown, expired, or revoked")
        actual_audience = _as_string(record, "audience")
        scopes = record.get("scopes")
        expires_at = _as_number(record, "expires_at")
        if (
            actual_audience != audience
            or not isinstance(scopes, list)
            or any(not isinstance(scope, str) for scope in scopes)
            or len(scopes) != len(set(scopes))
            or set(required_scopes) - set(scopes)
            or expires_at <= self.clock()
        ):
            raise IdentityAuthorizationError("credential audience, scope, expiry, or revocation check failed")
        identity = _decode_identity(record.get("workload_identity"))
        if (
            identity.audience != actual_audience
            or identity.expires_at < expires_at
            or identity.expires_at <= self.clock()
            or set(scopes) - set(identity.allowed_scopes)
        ):
            raise IdentityAuthorizationError("credential identity binding is inconsistent with the active lease")
        return identity

    def revoke_credential(self, credential_id: str) -> bool:
        response = self.transport.request(
            "POST",
            "/v1/workload-credentials:revoke",
            {"credential_id": credential_id},
        )
        revoked = response.get("revoked")
        if type(revoked) is not bool:
            raise AuthorityProtocolError("credential revocation acknowledgement is invalid")
        return revoked

    def revoke_run(self, run_id: str) -> int:
        response = self.transport.request(
            "POST",
            "/v1/workload-credentials:revoke-run",
            {"run_id": run_id},
        )
        return _revoked_count(response)

    def revoke_approval(
        self,
        approval_id: str,
        *,
        event_id: str | None = None,
    ) -> int:
        body: dict[str, Any] = {"approval_id": approval_id}
        if event_id is not None:
            if not event_id.strip():
                raise ValueError("event_id must be non-empty when supplied")
            body["event_id"] = event_id
        response = self.transport.request(
            "POST",
            "/v1/workload-credentials:revoke-approval",
            body,
        )
        return _revoked_count(response)


def _revoked_count(response: Mapping[str, Any]) -> int:
    count = response.get("revoked_count")
    if type(count) is not int or count < 0:
        raise AuthorityProtocolError("revocation count is invalid")
    return count


class AuthoritativeApprovalResolver(ApprovalResolver):
    """Read current approval state from the authoritative API on every issuance."""

    def __init__(
        self,
        transport: JsonAuthorityTransport,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._transport = transport
        self._clock = clock

    def get_approved_grant(self, approval_id: str) -> ApprovalGrant | None:
        if not isinstance(approval_id, str) or not approval_id.strip():
            return None
        try:
            payload = self._transport.request(
                "GET",
                "/v1/authorization/approvals/" + quote(approval_id, safe=""),
            )
        except AuthorityNotFoundError:
            return None
        grant = _decode_grant(payload, now=self._clock())
        if grant is not None and grant.approval_id != approval_id:
            raise AuthorityProtocolError("authoritative approval ID does not match the requested ID")
        return grant

    def revoke(self, approval_id: str, *, event_id: str) -> int:
        """Apply a trusted, uniquely identified revocation event to the authority."""
        if not approval_id.strip() or not event_id.strip():
            raise ValueError("approval_id and event_id are required")
        response = self._transport.request(
            "POST",
            "/v1/authorization/approvals/" + quote(approval_id, safe="") + ":revoke",
            {"event_id": event_id},
        )
        version = response.get("version")
        if type(version) is not int or version <= 0 or response.get("state") != "REVOKED":
            raise AuthorityProtocolError("authoritative approval revocation acknowledgement is invalid")
        return version


@dataclass(frozen=True)
class ApprovalRevocationEvent:
    event_id: str
    approval_id: str
    source: str


class ApprovalRevocationEventVerifier(Protocol):
    """Authenticates a signed event or trusted outbox/mTLS delivery envelope."""

    def verify(self, event: ApprovalRevocationEvent) -> bool:
        ...


class ApprovalRevocationFanout:
    """Fan out trusted approval revocation to authority and broker fail-closed.

    A trusted verifier is mandatory; source text alone is not authentication.
    The authority is revoked first. If broker fan-out fails, the exception is
    surfaced for retry; the authority API must already deny issuance and
    introspection for that approval while fan-out converges.
    """

    def __init__(
        self,
        *,
        approvals: AuthoritativeApprovalResolver,
        broker: CredentialBroker,
        event_verifier: ApprovalRevocationEventVerifier,
    ) -> None:
        self._approvals = approvals
        self._broker = broker
        self._event_verifier = event_verifier

    def handle(self, event: ApprovalRevocationEvent) -> int:
        if (
            not isinstance(event.event_id, str)
            or not event.event_id.strip()
            or not isinstance(event.approval_id, str)
            or not event.approval_id.strip()
            or not isinstance(event.source, str)
            or not event.source.strip()
        ):
            raise ValueError("complete trusted approval revocation event is required")
        try:
            verified = self._event_verifier.verify(event)
        except Exception:
            raise IdentityAuthorizationError("approval revocation event authentication failed") from None
        if verified is not True:
            raise IdentityAuthorizationError("approval revocation event authentication failed")
        self._approvals.revoke(event.approval_id, event_id=event.event_id)
        # Do not swallow this failure. The caller's durable inbox must retry;
        # introspection at the authority must deny revoked-approval leases.
        return self._broker.revoke_approval(
            event.approval_id,
            event_id=event.event_id,
        )
