import pytest

from cloud_agent_runtime import (
    AgentRuntime,
    InMemorySandboxProvider,
    InMemoryWorkflowDriver,
    SQLiteStore,
)


class FailOnceTerminateProvider(InMemorySandboxProvider):
    def __init__(self) -> None:
        self.failures_remaining = 1

    def terminate(self, sandbox):
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise RuntimeError("simulated provider cleanup failure")
        return super().terminate(sandbox)


class FailingResumeProvider(InMemorySandboxProvider):
    def resume(self, snapshot_ref: str, *, session_id: str):
        raise RuntimeError("simulated restore failure")


def test_run_survives_sandbox_replacement_with_durable_refs():
    runtime = AgentRuntime(
        sandbox_provider=InMemorySandboxProvider(),
        workflow_driver=InMemoryWorkflowDriver(),
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="checkout-sre-golden-v1",
        tenant_id="golden-demo",
    )
    run = runtime.start_run(session_id=session.session_id)

    canonical_run_id = run.run_id
    workflow_ref = run.workflow_ref
    sandbox_a = run.sandbox_ref

    run.artifact_refs.append("artifact://golden/run-001/diagnostic.json")
    run.evidence_refs.append("evidence://sha256/frozen-before-rebind")
    runtime.store.save_run(run)

    active = runtime._sandboxes[run.run_id]
    snapshotted = runtime.sandbox_provider.snapshot(active)
    assert snapshotted.snapshot_ref is not None
    active.status = active.status.BOUND

    rebound = runtime.rebind_run_sandbox(
        run.run_id,
        snapshot_ref=snapshotted.snapshot_ref,
        rebind_key="replace-after-loss-1",
    )

    assert rebound.run_id == canonical_run_id
    assert rebound.workflow_ref == workflow_ref
    assert rebound.sandbox_ref != sandbox_a
    assert rebound.sandbox_binding is not None
    assert rebound.sandbox_binding.revision == 2
    assert sandbox_a in rebound.sandbox_binding.previous_refs
    assert rebound.sandbox_binding.pending_cleanup_refs == []
    assert rebound.artifact_refs == [
        "artifact://golden/run-001/diagnostic.json"
    ]
    assert rebound.evidence_refs == [
        "evidence://sha256/frozen-before-rebind"
    ]

    completed = runtime.complete_run(
        canonical_run_id,
        evidence_refs=["evidence://sha256/after-rebind"],
    )
    assert completed.run_id == canonical_run_id
    assert completed.workflow_ref == workflow_ref
    assert completed.sandbox_ref == rebound.sandbox_ref


def test_sqlite_restart_preserves_binding_and_recovers_same_run(tmp_path):
    db = tmp_path / "runtime.db"
    provider = InMemorySandboxProvider()
    workflow = InMemoryWorkflowDriver()

    first_store = SQLiteStore(db)
    first = AgentRuntime(
        store=first_store,
        sandbox_provider=provider,
        workflow_driver=workflow,
    )
    session = first.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )
    run = first.start_run(session_id=session.session_id)
    original_run_id = run.run_id
    original_workflow = run.workflow_ref
    original_sandbox = run.sandbox_ref

    run.artifact_refs.append("artifact://checkpoint")
    run.evidence_refs.append("evidence://before-restart")
    first.store.save_run(run)

    paused = first.pause_run(run.run_id)
    snapshot_ref = paused.sandbox_snapshot_ref
    assert snapshot_ref is not None
    first_store.close()

    second_store = SQLiteStore(db)
    restarted = AgentRuntime(
        store=second_store,
        sandbox_provider=provider,
        workflow_driver=workflow,
    )
    recovered_before = restarted.store.get_run(original_run_id)
    assert recovered_before.sandbox_binding is not None
    assert recovered_before.sandbox_binding.snapshot_ref == snapshot_ref

    resumed = restarted.resume_run(original_run_id)

    assert resumed.run_id == original_run_id
    assert resumed.workflow_ref == original_workflow
    assert resumed.sandbox_ref != original_sandbox
    assert resumed.sandbox_binding is not None
    assert resumed.sandbox_binding.revision == 2
    assert original_sandbox in resumed.sandbox_binding.previous_refs
    assert resumed.artifact_refs == ["artifact://checkpoint"]
    assert resumed.evidence_refs == ["evidence://before-restart"]
    second_store.close()


def test_rebind_key_makes_recovery_idempotent():
    runtime = AgentRuntime(
        sandbox_provider=InMemorySandboxProvider(),
        workflow_driver=InMemoryWorkflowDriver(),
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )
    run = runtime.start_run(session_id=session.session_id)

    first = runtime.rebind_run_sandbox(
        run.run_id,
        rebind_key="recovery-attempt-42",
    )
    first_ref = first.sandbox_ref
    first_revision = first.sandbox_binding.revision

    replay = runtime.rebind_run_sandbox(
        run.run_id,
        rebind_key="recovery-attempt-42",
    )

    assert replay.sandbox_ref == first_ref
    assert replay.sandbox_binding.revision == first_revision


def test_failed_restore_does_not_change_durable_binding():
    healthy = InMemorySandboxProvider()
    runtime = AgentRuntime(
        sandbox_provider=healthy,
        workflow_driver=InMemoryWorkflowDriver(),
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )
    run = runtime.start_run(session_id=session.session_id)
    paused = runtime.pause_run(run.run_id)
    original_ref = paused.sandbox_ref
    original_binding = paused.sandbox_binding

    runtime.sandbox_provider = FailingResumeProvider()
    with pytest.raises(RuntimeError, match="simulated restore failure"):
        runtime.resume_run(run.run_id)

    persisted = runtime.store.get_run(run.run_id)
    assert persisted.status.value == "paused"
    assert persisted.sandbox_ref == original_ref
    assert persisted.sandbox_binding == original_binding


def test_failed_old_sandbox_cleanup_is_recorded_and_retryable():
    provider = FailOnceTerminateProvider()
    runtime = AgentRuntime(
        sandbox_provider=provider,
        workflow_driver=InMemoryWorkflowDriver(),
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )
    run = runtime.start_run(session_id=session.session_id)
    old_ref = run.sandbox_ref

    rebound = runtime.rebind_run_sandbox(
        run.run_id,
        rebind_key="recovery-with-cleanup-retry",
    )

    assert rebound.sandbox_binding is not None
    assert rebound.sandbox_binding.pending_cleanup_refs == [old_ref]

    cleaned = runtime.cleanup_retired_sandboxes(run.run_id)
    assert cleaned.sandbox_binding.pending_cleanup_refs == []
