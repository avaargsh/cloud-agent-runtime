from cloud_agent_runtime.temporal_workflow import (
    AgentRunWorkflow,
    AgentRunWorkflowState,
)


def initialized_workflow() -> AgentRunWorkflow:
    instance = AgentRunWorkflow()
    instance._state = AgentRunWorkflowState(
        runtime_run_id="run-1",
        session_id="session-1",
    )
    return instance


def test_approval_signal_preserves_evidence() -> None:
    workflow = initialized_workflow()

    workflow.approval_resolved(
        {
            "approval_id": "approval-1",
            "approved": True,
            "actor": "operator@example",
            "evidence_refs": [
                "evidence://aiops/e-1",
                "evidence://aiops/e-2",
            ],
        }
    )

    state = workflow.run_state()

    assert state["phase"] == "running"
    assert state["evidence_refs"] == [
        "evidence://aiops/e-1",
        "evidence://aiops/e-2",
    ]
    assert len(state["approval_events"]) == 1


def test_denied_approval_is_terminal() -> None:
    workflow = initialized_workflow()

    workflow.approval_resolved(
        {
            "approval_id": "approval-1",
            "approved": False,
            "reason": "blast radius too large",
        }
    )

    state = workflow.run_state()

    assert state["phase"] == "failed"
    assert state["terminal"] is True
    assert state["error"] == "blast radius too large"


def test_pause_resume_and_complete() -> None:
    workflow = initialized_workflow()

    workflow.pause_run()
    assert workflow.run_state()["phase"] == "paused"

    workflow.resume_run()
    assert workflow.run_state()["phase"] == "running"

    workflow.complete_run(
        {
            "result": {"summary": "done"},
            "evidence_refs": [
                "evidence://run/post-check",
            ],
        }
    )

    state = workflow.run_state()
    assert state["phase"] == "succeeded"
    assert state["terminal"] is True
    assert "evidence://run/post-check" in state[
        "evidence_refs"
    ]
