from __future__ import annotations

import hashlib
import secrets
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Protocol
from uuid import uuid4


class IdentityAuthorizationError(PermissionError):
    """A principal, scope, audience, approval, or identity binding was denied."""


class CredentialExpiredError(IdentityAuthorizationError):
    """The credential is expired or no longer authorized."""


class CredentialReplayError(IdentityAuthorizationError):
    """A credential issuance request ID has already been consumed."""


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    """Claims produced by trusted authentication middleware, never request JSON."""

    issuer: str
    subject: str
    tenant_id: str
    agent_id: str

    def __post_init__(self) -> None:
        if any(
            not value.strip()
            for value in (self.issuer, self.subject, self.tenant_id, self.agent_id)
        ):
            raise ValueError("principal identity fields must be non-empty")


@dataclass(frozen=True)
class CredentialBinding:
    """Server-owned mapping from a session capability to one external audience."""

    binding_id: str
    capability: str
    audience: str
    allowed_scopes: tuple[str, ...]
    max_ttl_seconds: int = 120

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (self.binding_id, self.capability, self.audience)
        ):
            raise ValueError("credential binding identifiers and audience are required")
        if not self.allowed_scopes or any(not scope.strip() for scope in self.allowed_scopes):
            raise ValueError("credential binding must define non-empty scopes")
        if len(set(self.allowed_scopes)) != len(self.allowed_scopes):
            raise ValueError("credential binding scopes must be unique")
        if (
            isinstance(self.max_ttl_seconds, bool)
            or not isinstance(self.max_ttl_seconds, int)
            or self.max_ttl_seconds <= 0
        ):
            raise ValueError("max_ttl_seconds must be a positive integer")


@dataclass(frozen=True)
class CredentialRequest:
    """A request for a subset of a server-configured credential binding."""

    request_id: str
    binding_id: str
    scopes: tuple[str, ...]
    ttl_seconds: int = 60
    approval_id: str | None = None

    def __post_init__(self) -> None:
        if not self.request_id.strip() or not self.binding_id.strip():
            raise ValueError("request_id and binding_id are required")
        if not self.scopes or any(not scope.strip() for scope in self.scopes):
            raise ValueError("credential request must specify non-empty scopes")
        if len(set(self.scopes)) != len(self.scopes):
            raise ValueError("credential request scopes must be unique")
        if (
            isinstance(self.ttl_seconds, bool)
            or not isinstance(self.ttl_seconds, int)
            or self.ttl_seconds <= 0
        ):
            raise ValueError("ttl_seconds must be a positive integer")


@dataclass(frozen=True)
class WorkloadIdentity:
    """Canonical, short-lived identity derived from stored Session and Run state."""

    issuer: str
    subject: str
    tenant_id: str
    agent_id: str
    release_id: str
    session_id: str
    run_id: str
    sandbox_id: str
    sandbox_revision: int
    workspace_ref: str
    binding_id: str
    capability: str
    audience: str
    allowed_scopes: tuple[str, ...]
    issued_at: float
    expires_at: float


@dataclass(frozen=True)
class ApprovalGrant:
    """Trusted approval receipt bound to one run, binding, audience, and scope set."""

    approval_id: str
    tenant_id: str
    agent_id: str
    session_id: str
    run_id: str
    binding_id: str
    audience: str
    scopes: tuple[str, ...]
    approved_by: str
    issued_at: float
    expires_at: float


@dataclass(frozen=True)
class CredentialLease:
    """Secret-bearing broker response. Never serialize or log this object."""

    credential_id: str
    access_token: str = field(repr=False)
    audience: str = ""
    scopes: tuple[str, ...] = ()
    issued_at: float = 0.0
    expires_at: float = 0.0
    run_id: str = ""
    session_id: str = ""
    binding_id: str = ""


class ApprovalResolver(Protocol):
    """Adapter to an authoritative approval service such as Agent Control Plane."""

    def get_approved_grant(self, approval_id: str) -> ApprovalGrant | None:
        ...


class CredentialBroker(Protocol):
    """Broker boundary; production implementations should use a trusted STS/Vault."""

    def issue(
        self,
        identity: WorkloadIdentity,
        binding: CredentialBinding,
        request: CredentialRequest,
        approval: ApprovalGrant | None = None,
    ) -> CredentialLease:
        ...

    def validate(
        self,
        access_token: str,
        *,
        audience: str,
        required_scopes: tuple[str, ...] = (),
    ) -> WorkloadIdentity:
        ...

    def revoke_credential(self, credential_id: str) -> bool:
        ...

    def revoke_run(self, run_id: str) -> int:
        ...

    def revoke_approval(
        self,
        approval_id: str,
        *,
        event_id: str | None = None,
    ) -> int:
        ...


@dataclass
class _CredentialRecord:
    credential_id: str
    token_digest: str
    identity: WorkloadIdentity
    audience: str
    scopes: tuple[str, ...]
    issued_at: float
    expires_at: float
    request_key: tuple[str, str, str]
    approval_id: str | None
    revoked: bool = False


def _scope_needs_approval(scope: str) -> bool:
    return scope.rsplit(":", 1)[-1].lower() in {
        "write",
        "delete",
        "admin",
        "mutate",
    }


class InMemoryApprovalResolver:
    """Test/development approval source. It is not a production approval store."""

    def __init__(self) -> None:
        self._grants: dict[str, ApprovalGrant] = {}
        self._revoked: set[str] = set()
        self._lock = threading.RLock()

    def add(self, grant: ApprovalGrant) -> None:
        with self._lock:
            if grant.approval_id in self._grants:
                raise ValueError("approval ID already exists")
            self._grants[grant.approval_id] = grant

    def get_approved_grant(self, approval_id: str) -> ApprovalGrant | None:
        with self._lock:
            if approval_id in self._revoked:
                return None
            return self._grants.get(approval_id)

    def revoke(self, approval_id: str) -> bool:
        with self._lock:
            existed = approval_id in self._grants
            self._revoked.add(approval_id)
            return existed


class InMemoryCredentialBroker:
    """Strict in-memory reference broker for tests; leases are never persisted."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._clock = clock
        self._records: dict[str, _CredentialRecord] = {}
        self._token_index: dict[str, str] = {}
        self._consumed_request_ids: set[tuple[str, str, str]] = set()
        self._revoked_approvals: set[str] = set()
        self._lock = threading.RLock()

    def issue(
        self,
        identity: WorkloadIdentity,
        binding: CredentialBinding,
        request: CredentialRequest,
        approval: ApprovalGrant | None = None,
    ) -> CredentialLease:
        now = self._clock()
        if identity.expires_at <= now:
            raise CredentialExpiredError("workload identity has expired")
        if (
            identity.binding_id != binding.binding_id
            or identity.capability != binding.capability
            or identity.audience != binding.audience
            or request.binding_id != binding.binding_id
        ):
            raise IdentityAuthorizationError("credential binding does not match workload identity")

        requested = set(request.scopes)
        if not requested or requested - set(identity.allowed_scopes):
            raise IdentityAuthorizationError("requested scope is outside the workload grant")
        if requested - set(binding.allowed_scopes):
            raise IdentityAuthorizationError("requested scope is outside the configured credential binding")

        needs_approval = any(_scope_needs_approval(scope) for scope in requested)
        if needs_approval:
            if approval is None:
                raise IdentityAuthorizationError("write-capable credentials require an approved grant")
            if (
                approval.tenant_id != identity.tenant_id
                or approval.agent_id != identity.agent_id
                or approval.session_id != identity.session_id
                or approval.run_id != identity.run_id
                or approval.binding_id != binding.binding_id
                or approval.audience != binding.audience
                or not requested.issubset(set(approval.scopes))
                or not approval.approved_by.strip()
                or approval.issued_at > now
                or approval.expires_at <= now
            ):
                raise IdentityAuthorizationError("approval is stale or not bound to this exact workload")

        ttl = min(request.ttl_seconds, binding.max_ttl_seconds)
        expires_at = min(now + ttl, identity.expires_at)
        if needs_approval and approval is not None:
            expires_at = min(expires_at, approval.expires_at)
        if expires_at <= now:
            raise CredentialExpiredError("no positive credential lifetime remains")

        request_key = (identity.run_id, binding.binding_id, request.request_id)
        with self._lock:
            # Recheck expiry and approval revocation inside the same lock that
            # consumes the request ID, so a concurrent revoke cannot race issue.
            now = self._clock()
            if identity.expires_at <= now or expires_at <= now:
                raise CredentialExpiredError("identity, approval, or credential has expired")
            if needs_approval and approval is not None and (
                approval.expires_at <= now
                or approval.approval_id in self._revoked_approvals
            ):
                raise IdentityAuthorizationError("approval has been revoked or expired")
            if request_key in self._consumed_request_ids:
                raise CredentialReplayError("credential issuance request ID has already been consumed")
            self._consumed_request_ids.add(request_key)
            credential_id = str(uuid4())
            token = secrets.token_urlsafe(32)
            digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
            record = _CredentialRecord(
                credential_id=credential_id,
                token_digest=digest,
                identity=identity,
                audience=binding.audience,
                scopes=tuple(sorted(requested)),
                issued_at=now,
                expires_at=expires_at,
                request_key=request_key,
                approval_id=approval.approval_id if needs_approval and approval else None,
            )
            self._records[credential_id] = record
            self._token_index[digest] = credential_id

        return CredentialLease(
            credential_id=credential_id,
            access_token=token,
            audience=binding.audience,
            scopes=record.scopes,
            issued_at=now,
            expires_at=expires_at,
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
        digest = hashlib.sha256(access_token.encode("utf-8")).hexdigest()
        now = self._clock()
        with self._lock:
            credential_id = self._token_index.get(digest)
            record = self._records.get(credential_id or "")
            if record is None:
                raise IdentityAuthorizationError("credential is unknown")
            if record.revoked:
                raise IdentityAuthorizationError("credential has been revoked")
            if record.expires_at <= now:
                raise CredentialExpiredError("credential has expired")
            if record.approval_id in self._revoked_approvals:
                raise IdentityAuthorizationError("credential approval has been revoked")
            if audience != record.audience:
                raise IdentityAuthorizationError("credential audience mismatch")
            if set(required_scopes) - set(record.scopes):
                raise IdentityAuthorizationError("credential scope mismatch")
            return record.identity

    def revoke_credential(self, credential_id: str) -> bool:
        with self._lock:
            record = self._records.get(credential_id)
            if record is None:
                return False
            record.revoked = True
            return True

    def revoke_run(self, run_id: str) -> int:
        with self._lock:
            count = 0
            for record in self._records.values():
                if record.identity.run_id == run_id and not record.revoked:
                    record.revoked = True
                    count += 1
            return count

    def revoke_approval(
        self,
        approval_id: str,
        *,
        event_id: str | None = None,
    ) -> int:
        with self._lock:
            self._revoked_approvals.add(approval_id)
            count = 0
            for record in self._records.values():
                if record.approval_id == approval_id and not record.revoked:
                    record.revoked = True
                    count += 1
            return count
