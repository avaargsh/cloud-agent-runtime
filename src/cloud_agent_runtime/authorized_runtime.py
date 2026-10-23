from __future__ import annotations

import time
from typing import Mapping

from .contracts import Budget
from .identity import (
    ApprovalResolver,
    AuthenticatedPrincipal,
    CredentialBinding,
    CredentialBroker,
    CredentialLease,
    CredentialRequest,
    IdentityAuthorizationError,
    InMemoryCredentialBroker,
    WorkloadIdentity,
)
from .models import Run, RunStatus, Session, SessionStatus
from .runtime import AgentRuntime


_READ_ONLY_MODES = {"read-only", "readonly"}
_READ_WRITE_MODES = {"read-write", "write"}
_ADMIN_MODES = {"admin"}


def _scopes_allowed_by_mode(capability: str, mode: str) -> set[str]:
    normalized = mode.strip().lower()
    if normalized in _READ_ONLY_MODES:
        return {f"{capability}:read"}
    if normalized in _READ_WRITE_MODES:
        return {f"{capability}:read", f"{capability}:write"}
    if normalized in _ADMIN_MODES:
        return {
            f"{capability}:read",
            f"{capability}:write",
            f"{capability}:delete",
            f"{capability}:admin",
        }
    return set()


class AuthorizedAgentRuntime:
    """Tenant-facing authorization facade around the internal AgentRuntime.

    The principal must come from validated authentication middleware (for
    example, verified OIDC claims). Never construct it from caller-controlled
    JSON. A service must not expose the wrapped AgentRuntime or its store.
    """

    def __init__(
        self,
        runtime: AgentRuntime,
        *,
        credential_bindings: Mapping[str, CredentialBinding],
        credential_broker: CredentialBroker | None = None,
        approval_resolver: ApprovalResolver | None = None,
        issuer: str = "cloud-agent-runtime",
        clock=time.time,
    ) -> None:
        self._runtime = runtime
        self._credential_bindings = dict(credential_bindings)
        for key, binding in self._credential_bindings.items():
            if key != binding.binding_id:
                raise ValueError("credential binding map key must equal binding_id")
        self._credential_broker = credential_broker or InMemoryCredentialBroker(clock=clock)
        self._approval_resolver = approval_resolver
        self._issuer = issuer
        self._clock = clock

    def _owned_session(
        self,
        principal: AuthenticatedPrincipal,
        session_id: str,
    ) -> Session:
        try:
            session = self._runtime.store.get_session(session_id)
        except KeyError:
            raise IdentityAuthorizationError("resource not found or not authorized") from None
        if (
            session.tenant_id != principal.tenant_id
            or session.agent_id != principal.agent_id
        ):
            raise IdentityAuthorizationError("resource not found or not authorized")
        if session.status != SessionStatus.ACTIVE:
            raise IdentityAuthorizationError("session is not active")
        return session

    def _owned_run(
        self,
        principal: AuthenticatedPrincipal,
        run_id: str,
    ) -> tuple[Run, Session]:
        try:
            run = self._runtime.store.get_run(run_id)
            session = self._runtime.store.get_session(run.session_id)
        except KeyError:
            raise IdentityAuthorizationError("resource not found or not authorized") from None
        if (
            session.tenant_id != principal.tenant_id
            or session.agent_id != principal.agent_id
        ):
            raise IdentityAuthorizationError("resource not found or not authorized")
        return run, session

    def start_run(
        self,
        principal: AuthenticatedPrincipal,
        *,
        session_id: str,
        budget: Budget | None = None,
    ) -> Run:
        """Start only under an owned session; caller cannot supply identity/workspace refs."""
        self._owned_session(principal, session_id)
        return self._runtime.start_run(session_id=session_id, budget=budget)

    def get_run(self, principal: AuthenticatedPrincipal, *, run_id: str) -> Run:
        run, _ = self._owned_run(principal, run_id)
        return run

    def _workload_identity(
        self,
        run: Run,
        session: Session,
        binding: CredentialBinding,
    ) -> WorkloadIdentity:
        if run.status != RunStatus.RUNNING:
            raise IdentityAuthorizationError("credentials are available only to running runs")
        persisted = run.sandbox_binding
        if (
            persisted is None
            or run.sandbox_ref is None
            or persisted.sandbox_ref != run.sandbox_ref
            or not persisted.sandbox_id
            or not persisted.provider
            or run.sandbox_ref != f"sandbox://{persisted.provider}/{persisted.sandbox_id}"
        ):
            raise IdentityAuthorizationError("run has no valid canonical workspace binding")

        capability = session.capability_bindings.get(binding.capability)
        if capability is None:
            raise IdentityAuthorizationError("credential capability is not granted to this session")
        mode_scopes = _scopes_allowed_by_mode(binding.capability, capability.mode)
        allowed_scopes = tuple(sorted(set(binding.allowed_scopes) & mode_scopes))
        if not allowed_scopes:
            raise IdentityAuthorizationError("session capability mode grants no configured credential scope")

        now = self._clock()
        return WorkloadIdentity(
            issuer=self._issuer,
            subject=f"run:{run.run_id}",
            tenant_id=session.tenant_id,
            agent_id=session.agent_id,
            release_id=session.release_id,
            session_id=session.session_id,
            run_id=run.run_id,
            sandbox_id=persisted.sandbox_id,
            workspace_ref=persisted.sandbox_ref,
            binding_id=binding.binding_id,
            capability=binding.capability,
            audience=binding.audience,
            allowed_scopes=allowed_scopes,
            issued_at=now,
            expires_at=now + binding.max_ttl_seconds,
        )

    def issue_credential(
        self,
        principal: AuthenticatedPrincipal,
        *,
        run_id: str,
        request: CredentialRequest,
    ) -> CredentialLease:
        run, session = self._owned_run(principal, run_id)
        binding = self._credential_bindings.get(request.binding_id)
        if binding is None:
            raise IdentityAuthorizationError("credential binding is not configured")
        identity = self._workload_identity(run, session, binding)

        approval = None
        if request.approval_id is not None:
            if self._approval_resolver is None:
                raise IdentityAuthorizationError("no trusted approval resolver is configured")
            approval = self._approval_resolver.get_approved_grant(request.approval_id)
            if approval is None:
                raise IdentityAuthorizationError("approval is missing, denied, or revoked")

        return self._credential_broker.issue(identity, binding, request, approval)

    def revoke_run_credentials(
        self,
        principal: AuthenticatedPrincipal,
        *,
        run_id: str,
    ) -> int:
        self._owned_run(principal, run_id)
        return self._credential_broker.revoke_run(run_id)

    def pause_run(self, principal: AuthenticatedPrincipal, *, run_id: str) -> Run:
        self._owned_run(principal, run_id)
        self._credential_broker.revoke_run(run_id)
        return self._runtime.pause_run(run_id)

    def resume_run(self, principal: AuthenticatedPrincipal, *, run_id: str) -> Run:
        self._owned_run(principal, run_id)
        self._credential_broker.revoke_run(run_id)
        return self._runtime.resume_run(run_id)

    def rebind_run_sandbox(
        self,
        principal: AuthenticatedPrincipal,
        *,
        run_id: str,
        snapshot_ref: str | None = None,
        rebind_key: str | None = None,
    ) -> Run:
        self._owned_run(principal, run_id)
        self._credential_broker.revoke_run(run_id)
        return self._runtime.rebind_run_sandbox(
            run_id,
            snapshot_ref=snapshot_ref,
            rebind_key=rebind_key,
        )

    def cleanup_retired_sandboxes(
        self,
        principal: AuthenticatedPrincipal,
        *,
        run_id: str,
    ) -> Run:
        self._owned_run(principal, run_id)
        return self._runtime.cleanup_retired_sandboxes(run_id)

    def request_approval(
        self,
        principal: AuthenticatedPrincipal,
        *,
        run_id: str,
        action: str,
        evidence_refs: list[str] | tuple[str, ...] | None = None,
    ):
        self._owned_run(principal, run_id)
        return self._runtime.request_approval(
            run_id,
            action=action,
            evidence_refs=evidence_refs,
        )

    def complete_run(
        self,
        principal: AuthenticatedPrincipal,
        *,
        run_id: str,
        artifact_refs=None,
        evidence_refs=None,
    ) -> Run:
        self._owned_run(principal, run_id)
        result = self._runtime.complete_run(
            run_id,
            artifact_refs=artifact_refs,
            evidence_refs=evidence_refs,
        )
        self._credential_broker.revoke_run(run_id)
        return result

    def cancel_run(
        self,
        principal: AuthenticatedPrincipal,
        *,
        run_id: str,
        reason: str = "cancelled by caller",
        event_id: str | None = None,
        evidence_refs=None,
    ) -> Run:
        self._owned_run(principal, run_id)
        self._credential_broker.revoke_run(run_id)
        cancel = getattr(self._runtime, "cancel_run", None)
        if cancel is None:
            raise RuntimeError("wrapped runtime does not implement cancellation")
        return cancel(
            run_id,
            reason=reason,
            event_id=event_id,
            evidence_refs=evidence_refs,
        )

    def revoke_approval(self, approval_id: str) -> int:
        """Called by a trusted approval-revocation event, not by tenant requests."""
        if self._approval_resolver is not None:
            revoke = getattr(self._approval_resolver, "revoke", None)
            if revoke is not None:
                revoke(approval_id)
        return self._credential_broker.revoke_approval(approval_id)
