import pytest

from cloud_agent_runtime import AgentRuntime, RunStatus, SessionStatus


def test_session_and_run_lifecycle() -> None:
    runtime = AgentRuntime()

    session = runtime.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    assert session.status == SessionStatus.ACTIVE

    run = runtime.start_run(
        session_id=session.session_id,
        sandbox_ref="sandbox://warm-1",
    )
    assert run.status == RunStatus.RUNNING

    completed = runtime.complete_run(
        run.run_id,
        artifact_refs=["artifact://patch.diff"],
        evidence_refs=["trace://run-1"],
    )
    assert completed.status == RunStatus.SUCCEEDED
    assert completed.artifact_refs == ["artifact://patch.diff"]


def test_paused_session_rejects_new_run() -> None:
    runtime = AgentRuntime()
    session = runtime.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    runtime.pause_session(session.session_id)

    with pytest.raises(ValueError):
        runtime.start_run(session_id=session.session_id)


def test_resume_session() -> None:
    runtime = AgentRuntime()
    session = runtime.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )
    runtime.pause_session(session.session_id)
    resumed = runtime.resume_session(session.session_id)

    assert resumed.status == SessionStatus.ACTIVE
