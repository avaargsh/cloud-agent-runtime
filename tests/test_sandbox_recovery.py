import pytest

from cloud_agent_runtime import (
    AgentRuntime,
    InMemorySandboxProvider,
    InMemoryWorkflowDriver,
    SQLiteStore,
)
from cloud_agent_runtime.store import InMemoryStore


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



class FailingRunStore(InMemoryStore):
    def save_run(self, run):
        raise RuntimeError("simulated run persistence failure")


class FailOnRunSaveNumberStore(InMemoryStore):
    def __init__(self, fail_on: int):
        super().__init__()
        self.fail_on = fail_on
        self.save_count = 0

    def save_run(self, run):
        self.save_count += 1
        if self.save_count == self.fail_on:
            raise RuntimeError("simulated run persistence failure")
        return super().save_run(run)


class ToggleFailRunStore(InMemoryStore):
    def __init__(self):
        super().__init__()
        self.fail_runs = False

    def save_run(self, run):
        if self.fail_runs:
            raise RuntimeError("simulated run persistence failure")
        return super().save_run(run)


class FailingWorkflowDriver:
    name = "failing-workflow"

    def start_run(self, *, runtime_run_id: str, session_id: str):
        raise RuntimeError("simulated workflow start failure")

    def signal(self, workflow, *, name: str, payload: dict):
        return None


class TrackingProvider(InMemorySandboxProvider):
    def __init__(self):
        self.allocated = []
        self.terminated = []

    def allocate(self):
        sandbox = super().allocate()
        self.allocated.append(sandbox)
        return sandbox

    def terminate(self, sandbox):
        self.terminated.append(sandbox.sandbox_id)
        return super().terminate(sandbox)


class BindFailProvider(TrackingProvider):
    def bind(self, sandbox, *, session_id: str):
        raise RuntimeError("simulated bind failure")


class CleanupFailProvider(TrackingProvider):
    def terminate(self, sandbox):
        self.terminated.append(sandbox.sandbox_id)
        raise RuntimeError("simulated cleanup failure")


def test_start_run_cleans_allocated_sandbox_when_bind_fails():
    provider = BindFailProvider()
    runtime = AgentRuntime(
        sandbox_provider=provider,
        workflow_driver=None,
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )

    with pytest.raises(RuntimeError, match="simulated bind failure"):
        runtime.start_run(session_id=session.session_id)

    assert len(provider.allocated) == 1
    assert provider.terminated == [
        provider.allocated[0].sandbox_id
    ]
    assert provider.allocated[0].status.value == "terminated"
    assert runtime._sandboxes == {}


def test_start_run_cleans_bound_sandbox_when_persistence_fails():
    provider = TrackingProvider()
    runtime = AgentRuntime(
        store=FailingRunStore(),
        sandbox_provider=provider,
        workflow_driver=None,
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )

    with pytest.raises(
        RuntimeError,
        match="simulated run persistence failure",
    ):
        runtime.start_run(session_id=session.session_id)

    assert len(provider.allocated) == 1
    assert provider.terminated == [
        provider.allocated[0].sandbox_id
    ]
    assert provider.allocated[0].status.value == "terminated"
    assert runtime._sandboxes == {}


def test_cleanup_failure_does_not_mask_persistence_failure():
    provider = CleanupFailProvider()
    runtime = AgentRuntime(
        store=FailingRunStore(),
        sandbox_provider=provider,
        workflow_driver=None,
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )

    with pytest.raises(
        RuntimeError,
        match="simulated run persistence failure",
    ):
        runtime.start_run(session_id=session.session_id)

    assert len(provider.allocated) == 1
    assert provider.terminated == [
        provider.allocated[0].sandbox_id
    ]
    assert runtime._sandboxes == {}



def test_start_run_cleans_sandbox_when_workflow_start_fails():
    provider = TrackingProvider()
    runtime = AgentRuntime(
        sandbox_provider=provider,
        workflow_driver=FailingWorkflowDriver(),
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )

    with pytest.raises(
        RuntimeError,
        match="simulated workflow start failure",
    ):
        runtime.start_run(session_id=session.session_id)

    assert len(provider.allocated) == 1
    assert provider.terminated == [
        provider.allocated[0].sandbox_id
    ]
    assert provider.allocated[0].status.value == "terminated"
    assert runtime._sandboxes == {}
    runs = list(runtime.store.runs.values())
    assert len(runs) == 1
    assert runs[0].status.value == "failed"
    assert runs[0].workflow_ref is None


def test_failed_rebind_restores_old_binding_and_cache():
    store = ToggleFailRunStore()
    provider = CleanupFailProvider()
    runtime = AgentRuntime(
        store=store,
        sandbox_provider=provider,
        workflow_driver=InMemoryWorkflowDriver(),
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )
    run = runtime.start_run(session_id=session.session_id)
    original_ref = run.sandbox_ref
    original_binding = run.sandbox_binding
    original_cache = runtime._sandboxes[run.run_id]

    store.fail_runs = True
    with pytest.raises(
        RuntimeError,
        match="simulated run persistence failure",
    ):
        runtime.rebind_run_sandbox(
            run.run_id,
            rebind_key="failed-rebind",
        )

    persisted = store.get_run(run.run_id)
    assert persisted.sandbox_ref == original_ref
    assert persisted.sandbox_binding == original_binding
    assert runtime._sandboxes[run.run_id] is original_cache
    assert provider.terminated[-1] != original_cache.sandbox_id


def test_failed_resume_restores_paused_status_and_binding():
    store = ToggleFailRunStore()
    provider = TrackingProvider()
    runtime = AgentRuntime(
        store=store,
        sandbox_provider=provider,
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
    original_cache = runtime._sandboxes[run.run_id]

    store.fail_runs = True
    with pytest.raises(
        RuntimeError,
        match="simulated run persistence failure",
    ):
        runtime.resume_run(run.run_id)

    persisted = store.get_run(run.run_id)
    assert persisted.status.value == "paused"
    assert persisted.sandbox_ref == original_ref
    assert persisted.sandbox_binding == original_binding
    assert runtime._sandboxes[run.run_id] is original_cache



class TerminateFailWorkflowDriver(InMemoryWorkflowDriver):
    def terminate_run(self, workflow, *, reason):
        raise RuntimeError("simulated workflow termination failure")


def test_workflow_binding_persistence_failure_terminates_workflow():
    store = FailOnRunSaveNumberStore(fail_on=2)
    provider = TrackingProvider()
    workflow = InMemoryWorkflowDriver()
    runtime = AgentRuntime(
        store=store,
        sandbox_provider=provider,
        workflow_driver=workflow,
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )

    with pytest.raises(
        RuntimeError,
        match="simulated run persistence failure",
    ):
        runtime.start_run(session_id=session.session_id)

    runs = list(store.runs.values())
    assert len(runs) == 1
    assert runs[0].status.value == "failed"
    assert runs[0].workflow_ref is None
    assert len(workflow.terminated) == 1
    assert workflow.terminated[0][1] == "runtime binding persistence failed"
    assert provider.terminated == [
        provider.allocated[0].sandbox_id
    ]
    assert runtime._sandboxes == {}


def test_termination_failure_retains_workflow_ref_for_recovery():
    store = FailOnRunSaveNumberStore(fail_on=2)
    provider = TrackingProvider()
    workflow = TerminateFailWorkflowDriver()
    runtime = AgentRuntime(
        store=store,
        sandbox_provider=provider,
        workflow_driver=workflow,
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )

    with pytest.raises(
        RuntimeError,
        match="simulated run persistence failure",
    ):
        runtime.start_run(session_id=session.session_id)

    runs = list(store.runs.values())
    assert len(runs) == 1
    assert runs[0].status.value == "failed"
    assert runs[0].workflow_ref is not None
    assert runtime._sandboxes == {}



def test_failed_pause_persistence_restores_running_cache_and_is_retryable():
    store = ToggleFailRunStore()
    provider = TrackingProvider()
    runtime = AgentRuntime(
        store=store,
        sandbox_provider=provider,
        workflow_driver=InMemoryWorkflowDriver(),
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="release-v1",
        tenant_id="tenant-a",
    )
    run = runtime.start_run(session_id=session.session_id)
    cached = runtime._sandboxes[run.run_id]
    original_ref = run.sandbox_ref

    store.fail_runs = True
    with pytest.raises(
        RuntimeError,
        match="simulated run persistence failure",
    ):
        runtime.pause_run(run.run_id)

    persisted = store.get_run(run.run_id)
    assert persisted.status.value == "running"
    assert persisted.sandbox_ref == original_ref
    assert persisted.sandbox_snapshot_ref is None
    assert persisted.sandbox_binding is not None
    assert persisted.sandbox_binding.snapshot_ref is None
    assert runtime._sandboxes[run.run_id] is cached
    assert cached.status.value == "bound"
    assert cached.snapshot_ref is None

    store.fail_runs = False
    paused = runtime.pause_run(run.run_id)

    assert paused.status.value == "paused"
    assert paused.sandbox_snapshot_ref is not None
    assert runtime._sandboxes[run.run_id].status.value == "paused"



def test_rebind_cleans_allocated_sandbox_when_bind_fails():
    healthy = TrackingProvider()
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
    original_ref = run.sandbox_ref
    original_binding = run.sandbox_binding
    original_cache = runtime._sandboxes[run.run_id]

    failing = BindFailProvider()
    runtime.sandbox_provider = failing

    with pytest.raises(RuntimeError, match="simulated bind failure"):
        runtime.rebind_run_sandbox(
            run.run_id,
            rebind_key="bind-failure-recovery",
        )

    assert len(failing.allocated) == 1
    assert failing.terminated == [
        failing.allocated[0].sandbox_id
    ]
    assert failing.allocated[0].status.value == "terminated"

    persisted = runtime.store.get_run(run.run_id)
    assert persisted.sandbox_ref == original_ref
    assert persisted.sandbox_binding == original_binding
    assert runtime._sandboxes[run.run_id] is original_cache
