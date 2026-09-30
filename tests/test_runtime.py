import pytest

from cloud_agent_runtime import (
    AgentRuntime,
    ApprovalStatus,
    Budget,
    CapabilityBinding,
    InMemorySandboxProvider,
    InMemoryWorkflowDriver,
    RunStatus,
    SessionStatus,
)


def runtime() -> AgentRuntime:
    return AgentRuntime(
        sandbox_provider=InMemorySandboxProvider(),
        workflow_driver=InMemoryWorkflowDriver(),
    )


def test_session_and_run_lifecycle() -> None:
    rt = runtime()

    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
        capabilities=[
            CapabilityBinding(
                name="repo",
                kind="mcp",
                provider="github",
                mode="read-write",
            )
        ],
    )
    assert session.status == SessionStatus.ACTIVE
    assert "repo" in session.capability_bindings

    run = rt.start_run(
        session_id=session.session_id,
        budget=Budget(
            max_tool_calls=20,
            max_model_tokens=50_000,
        ),
    )
    assert run.status == RunStatus.RUNNING
    assert run.sandbox_ref is not None
    assert run.workflow_ref is not None

    completed = rt.complete_run(
        run.run_id,
        artifact_refs=["artifact://patch.diff"],
        evidence_refs=["trace://run-1"],
    )
    assert completed.status == RunStatus.SUCCEEDED
    assert completed.artifact_refs == ["artifact://patch.diff"]


def test_paused_session_rejects_new_run() -> None:
    rt = runtime()
    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    rt.pause_session(session.session_id)

    with pytest.raises(ValueError):
        rt.start_run(session_id=session.session_id)


def test_resume_session() -> None:
    rt = runtime()
    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    rt.pause_session(session.session_id)
    resumed = rt.resume_session(session.session_id)

    assert resumed.status == SessionStatus.ACTIVE


def test_run_snapshot_and_resume() -> None:
    rt = runtime()
    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    run = rt.start_run(session_id=session.session_id)

    paused = rt.pause_run(run.run_id)
    assert paused.status == RunStatus.PAUSED
    assert paused.sandbox_snapshot_ref is not None

    resumed = rt.resume_run(run.run_id)
    assert resumed.status == RunStatus.RUNNING
    assert resumed.sandbox_ref is not None


def test_approval_wait_state() -> None:
    rt = runtime()
    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    run = rt.start_run(session_id=session.session_id)

    approval = rt.request_approval(
        run.run_id,
        action="merge pull request",
    )

    assert rt.store.get_run(run.run_id).status == RunStatus.WAITING_APPROVAL
    assert approval.status == ApprovalStatus.PENDING

    resolved = rt.resolve_approval(
        run.run_id,
        approval.approval_id,
        approved=True,
        actor="user@example",
    )

    assert resolved.status == ApprovalStatus.APPROVED
    assert rt.store.get_run(run.run_id).status == RunStatus.RUNNING


def test_denied_approval_fails_run() -> None:
    rt = runtime()
    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    run = rt.start_run(session_id=session.session_id)

    approval = rt.request_approval(
        run.run_id,
        action="delete production namespace",
    )

    rt.resolve_approval(
        run.run_id,
        approval.approval_id,
        approved=False,
        actor="user@example",
        reason="too risky",
    )

    assert rt.store.get_run(run.run_id).status == RunStatus.FAILED


def test_sandbox_rebind_preserves_run_and_workflow_identity() -> None:
    rt = runtime()
    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    run = rt.start_run(session_id=session.session_id)
    original_run_id = run.run_id
    original_workflow_ref = run.workflow_ref
    original_sandbox_ref = run.sandbox_ref

    current = rt._sandboxes[run.run_id]
    snapshotted = rt.sandbox_provider.snapshot(current)
    snapshot_ref = snapshotted.snapshot_ref
    assert snapshot_ref is not None

    # Rebind is a running-run execution replacement. Paused recovery is
    # intentionally owned by resume_run().
    current.status = current.status.BOUND
    rebound = rt.rebind_run_sandbox(
        run.run_id,
        snapshot_ref=snapshot_ref,
    )

    assert rebound.run_id == original_run_id
    assert rebound.workflow_ref == original_workflow_ref
    assert rebound.sandbox_ref != original_sandbox_ref
    assert rebound.sandbox_snapshot_ref == snapshot_ref



def test_paused_run_must_use_resume_not_rebind() -> None:
    rt = runtime()
    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    run = rt.start_run(session_id=session.session_id)
    rt.pause_run(run.run_id)

    with pytest.raises(
        ValueError,
        match="use resume_run for paused recovery",
    ):
        rt.rebind_run_sandbox(run.run_id)



class FailOnceApprovalSignalDriver(InMemoryWorkflowDriver):
    def __init__(self):
        super().__init__()
        self.signal_attempts = 0
        self.delivered = []

    def signal(self, workflow, *, name: str, payload: dict):
        self.signal_attempts += 1
        if self.signal_attempts == 1:
            raise RuntimeError("simulated workflow signal failure")
        self.delivered.append(
            (workflow.workflow_id, name, dict(payload))
        )


def test_approval_resolution_retry_redelivers_same_durable_signal() -> None:
    driver = FailOnceApprovalSignalDriver()
    rt = AgentRuntime(
        sandbox_provider=InMemorySandboxProvider(),
        workflow_driver=driver,
    )
    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    run = rt.start_run(session_id=session.session_id)
    approval = rt.request_approval(
        run.run_id,
        action="restart workload",
        evidence_refs=["evidence://approval/input"],
    )

    with pytest.raises(
        RuntimeError,
        match="simulated workflow signal failure",
    ):
        rt.resolve_approval(
            run.run_id,
            approval.approval_id,
            approved=True,
            actor="operator@example",
            reason="approved after review",
        )

    persisted = rt.store.get_run(run.run_id)
    assert persisted.status == RunStatus.RUNNING
    assert persisted.approvals[0].status == ApprovalStatus.APPROVED

    retried = rt.resolve_approval(
        run.run_id,
        approval.approval_id,
        approved=True,
        actor="operator@example",
        reason="approved after review",
    )

    assert retried.status == ApprovalStatus.APPROVED
    assert driver.signal_attempts == 2
    assert driver.delivered[0][2] == {
        "approval_id": approval.approval_id,
        "approved": True,
        "actor": "operator@example",
        "reason": "approved after review",
        "evidence_refs": ["evidence://approval/input"],
    }


def test_approval_resolution_retry_rejects_conflicting_outcome() -> None:
    rt = runtime()
    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    run = rt.start_run(session_id=session.session_id)
    approval = rt.request_approval(
        run.run_id,
        action="restart workload",
    )
    rt.resolve_approval(
        run.run_id,
        approval.approval_id,
        approved=True,
        actor="operator@example",
    )

    with pytest.raises(
        ValueError,
        match="different outcome",
    ):
        rt.resolve_approval(
            run.run_id,
            approval.approval_id,
            approved=False,
            actor="operator@example",
        )


def test_approval_resolution_retry_rejects_changed_audit_metadata() -> None:
    rt = runtime()
    session = rt.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    run = rt.start_run(session_id=session.session_id)
    approval = rt.request_approval(
        run.run_id,
        action="restart workload",
    )
    rt.resolve_approval(
        run.run_id,
        approval.approval_id,
        approved=True,
        actor="operator@example",
        reason="reviewed",
    )

    with pytest.raises(
        ValueError,
        match="metadata differs",
    ):
        rt.resolve_approval(
            run.run_id,
            approval.approval_id,
            approved=True,
            actor="other@example",
            reason="reviewed",
        )
