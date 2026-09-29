from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Protocol, Sequence

from .sandbox import Sandbox, SandboxProvider


class BindingValidationError(RuntimeError):
    """Recovery cannot prove that a replacement sandbox is safe to continue."""


class BindingPhase(str, Enum):
    BOUND = "bound"
    RELEASED = "released"
    RESTORED = "restored"


@dataclass(frozen=True)
class LifecycleBinding:
    workflow_ref: str
    logical_sandbox_ref: str
    sandbox_provider: str
    session_id: str
    release_id: str
    workspace_ref: str | None = None
    artifact_refs: tuple[str, ...] = ()
    credential_ref: str | None = None
    idempotency_key: str | None = None
    physical_sandbox_ref: str | None = None
    phase: BindingPhase = BindingPhase.BOUND

    def released(
        self,
        *,
        workspace_ref: str,
        artifact_refs: Sequence[str] = (),
    ) -> "LifecycleBinding":
        return replace(
            self,
            workspace_ref=workspace_ref,
            artifact_refs=tuple(artifact_refs),
            physical_sandbox_ref=None,
            phase=BindingPhase.RELEASED,
        )


@dataclass(frozen=True)
class RestoreRequest:
    workflow_ref: str
    session_id: str
    release_id: str
    credential_ref: str | None = None
    idempotency_key: str | None = None


class LifecycleBindingAdapter(Protocol):
    def bind(
        self,
        *,
        workflow_ref: str,
        session_id: str,
        release_id: str,
        credential_ref: str | None = None,
        idempotency_key: str | None = None,
    ) -> LifecycleBinding:
        ...

    def release(
        self,
        binding: LifecycleBinding,
        *,
        artifact_refs: Sequence[str] = (),
    ) -> LifecycleBinding:
        ...

    def restore(
        self,
        binding: LifecycleBinding,
        request: RestoreRequest,
    ) -> LifecycleBinding:
        ...


@dataclass
class SandboxLifecycleBindingAdapter:
    """Thin adapter: orchestration stays outside; sandbox lifecycle stays provider-owned."""

    provider: SandboxProvider

    def bind(
        self,
        *,
        workflow_ref: str,
        session_id: str,
        release_id: str,
        credential_ref: str | None = None,
        idempotency_key: str | None = None,
    ) -> LifecycleBinding:
        sandbox = self.provider.bind(self.provider.allocate(), session_id=session_id)
        return LifecycleBinding(
            workflow_ref=workflow_ref,
            logical_sandbox_ref=f"sandbox://{session_id}",
            physical_sandbox_ref=sandbox.sandbox_id,
            sandbox_provider=sandbox.provider,
            session_id=session_id,
            release_id=release_id,
            credential_ref=credential_ref,
            idempotency_key=idempotency_key,
        )

    def release(
        self,
        binding: LifecycleBinding,
        *,
        artifact_refs: Sequence[str] = (),
    ) -> LifecycleBinding:
        if binding.physical_sandbox_ref is None:
            raise BindingValidationError("binding has no active physical sandbox")
        sandbox = Sandbox(
            sandbox_id=binding.physical_sandbox_ref,
            provider=binding.sandbox_provider,
            status=self._bound_status(),
            session_id=binding.session_id,
        )
        snapshotted = self.provider.snapshot(sandbox)
        if snapshotted.snapshot_ref is None:
            raise BindingValidationError("sandbox provider returned no workspace snapshot")
        self.provider.terminate(snapshotted)
        return binding.released(
            workspace_ref=snapshotted.snapshot_ref,
            artifact_refs=artifact_refs,
        )

    def restore(
        self,
        binding: LifecycleBinding,
        request: RestoreRequest,
    ) -> LifecycleBinding:
        self.validate(binding, request)
        assert binding.workspace_ref is not None
        sandbox = self.provider.resume(
            binding.workspace_ref,
            session_id=binding.session_id,
        )
        if sandbox.provider != binding.sandbox_provider:
            raise BindingValidationError("sandbox provider mismatch after restore")
        if sandbox.session_id != binding.session_id:
            raise BindingValidationError("sandbox session mismatch after restore")
        return replace(
            binding,
            physical_sandbox_ref=sandbox.sandbox_id,
            credential_ref=request.credential_ref,
            phase=BindingPhase.RESTORED,
        )

    @staticmethod
    def validate(binding: LifecycleBinding, request: RestoreRequest) -> None:
        if binding.phase != BindingPhase.RELEASED:
            raise BindingValidationError("only a released binding can be restored")
        if binding.workspace_ref is None:
            raise BindingValidationError("released binding has no workspace reference")
        if request.workflow_ref != binding.workflow_ref:
            raise BindingValidationError("workflow identity mismatch")
        if request.session_id != binding.session_id:
            raise BindingValidationError("session identity mismatch")
        if request.release_id != binding.release_id:
            raise BindingValidationError("release/candidate version mismatch")
        if binding.credential_ref is not None and request.credential_ref is None:
            raise BindingValidationError("credential must be reissued before restore")
        if (
            binding.idempotency_key is not None
            and request.idempotency_key != binding.idempotency_key
        ):
            raise BindingValidationError("idempotency key mismatch")

    @staticmethod
    def _bound_status():
        from .sandbox import SandboxStatus
        return SandboxStatus.BOUND
