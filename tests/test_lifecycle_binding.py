import pytest

from cloud_agent_runtime.lifecycle_binding import (
    BindingPhase,
    BindingValidationError,
    RestoreRequest,
    SandboxLifecycleBindingAdapter,
)
from cloud_agent_runtime.sandbox import InMemorySandboxProvider


def test_release_then_restore_preserves_logical_identity_and_invariants():
    adapter = SandboxLifecycleBindingAdapter(InMemorySandboxProvider())
    binding = adapter.bind(
        workflow_ref="temporal://run-381",
        session_id="session-1",
        release_id="candidate-v7",
        credential_ref="lease://initial",
        idempotency_key="op-1",
    )
    first_physical = binding.physical_sandbox_ref

    released = adapter.release(
        binding,
        artifact_refs=("artifact://sha256/a",),
    )

    assert released.phase == BindingPhase.RELEASED
    assert released.physical_sandbox_ref is None
    assert released.workspace_ref is not None
    assert released.logical_sandbox_ref == binding.logical_sandbox_ref

    restored = adapter.restore(
        released,
        RestoreRequest(
            workflow_ref="temporal://run-381",
            session_id="session-1",
            release_id="candidate-v7",
            credential_ref="lease://reissued",
            idempotency_key="op-1",
        ),
    )

    assert restored.phase == BindingPhase.RESTORED
    assert restored.logical_sandbox_ref == binding.logical_sandbox_ref
    assert restored.workspace_ref == released.workspace_ref
    assert restored.artifact_refs == ("artifact://sha256/a",)
    assert restored.credential_ref == "lease://reissued"
    assert restored.physical_sandbox_ref == first_physical


@pytest.mark.parametrize(
    ("restore_request", "message"),
    [
        (
            RestoreRequest("temporal://other", "session-1", "candidate-v7", "lease://new", "op-1"),
            "workflow identity mismatch",
        ),
        (
            RestoreRequest("temporal://run-381", "session-1", "candidate-v8", "lease://new", "op-1"),
            "release/candidate version mismatch",
        ),
        (
            RestoreRequest("temporal://run-381", "session-1", "candidate-v7", None, "op-1"),
            "credential must be reissued",
        ),
        (
            RestoreRequest("temporal://run-381", "session-1", "candidate-v7", "lease://new", "op-2"),
            "idempotency key mismatch",
        ),
    ],
)
def test_restore_fails_closed_when_recovery_invariant_is_not_proven(restore_request, message):
    adapter = SandboxLifecycleBindingAdapter(InMemorySandboxProvider())
    binding = adapter.bind(
        workflow_ref="temporal://run-381",
        session_id="session-1",
        release_id="candidate-v7",
        credential_ref="lease://initial",
        idempotency_key="op-1",
    )
    released = adapter.release(binding)

    with pytest.raises(BindingValidationError, match=message):
        adapter.restore(released, restore_request)


class WrongIdentityProvider(InMemorySandboxProvider):
    def __init__(self, *, provider_name: str | None = None, session_id: str | None = None):
        super().__init__()
        self._provider_name = provider_name
        self._session_id = session_id

    def resume(self, snapshot_ref: str, *, session_id: str):
        sandbox = super().resume(snapshot_ref, session_id=session_id)
        if self._provider_name is not None:
            sandbox = sandbox.__class__(
                sandbox_id=sandbox.sandbox_id,
                provider=self._provider_name,
                status=sandbox.status,
                snapshot_ref=sandbox.snapshot_ref,
                session_id=sandbox.session_id,
            )
        if self._session_id is not None:
            sandbox = sandbox.__class__(
                sandbox_id=sandbox.sandbox_id,
                provider=sandbox.provider,
                status=sandbox.status,
                snapshot_ref=sandbox.snapshot_ref,
                session_id=self._session_id,
            )
        return sandbox


@pytest.mark.parametrize(
    ("provider", "message"),
    [
        (
            WrongIdentityProvider(provider_name="other-provider"),
            "sandbox provider mismatch after restore",
        ),
        (
            WrongIdentityProvider(session_id="other-session"),
            "sandbox session mismatch after restore",
        ),
    ],
)
def test_restore_rejects_replacement_sandbox_with_wrong_identity(provider, message):
    adapter = SandboxLifecycleBindingAdapter(provider)
    binding = adapter.bind(
        workflow_ref="temporal://run-381",
        session_id="session-1",
        release_id="candidate-v7",
        credential_ref="lease://initial",
        idempotency_key="op-1",
    )
    released = adapter.release(binding)

    with pytest.raises(BindingValidationError, match=message):
        adapter.restore(
            released,
            RestoreRequest(
                workflow_ref="temporal://run-381",
                session_id="session-1",
                release_id="candidate-v7",
                credential_ref="lease://reissued",
                idempotency_key="op-1",
            ),
        )
