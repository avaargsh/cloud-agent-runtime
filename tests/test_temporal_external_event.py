from cloud_agent_runtime.event_inbox import EventEnvelope
from cloud_agent_runtime.temporal_workflow import AgentRunWorkflow, AgentRunWorkflowState


def workflow_for(run_id="run-a"):
    instance = AgentRunWorkflow()
    instance._state = AgentRunWorkflowState(
        runtime_run_id=run_id, session_id="session-a", phase="waiting"
    )
    return instance


def event_payload(event_id="provider-event-1"):
    return EventEnvelope(
        event_id=event_id,
        tenant_id="tenant-a",
        run_id="run-a",
        event_type="human.reviewed",
        correlation_id="task-1",
        evidence_refs=("evidence://decision/1",),
    ).to_dict()


def test_external_event_is_audited_once_without_approval_side_effects():
    instance = workflow_for()
    payload = event_payload()
    instance.external_event_received(payload)
    instance.external_event_received(payload)
    state = instance.run_state()
    assert state["external_events"] == [payload]
    assert state["phase"] == "waiting"
    assert state["approval_events"] == []
    assert state["transition_events"] == []
    assert state["evidence_refs"] == []


def test_foreign_run_and_late_terminal_events_cannot_mutate_workflow():
    instance = workflow_for()
    wrong_run = {**event_payload(), "run_id": "run-b"}
    instance.external_event_received(wrong_run)
    instance.external_event_received({**event_payload(), "event_id": ""})
    instance.external_event_received({**event_payload(), "schema_version": 2})
    assert instance.run_state()["external_events"] == []

    instance.external_event_received(event_payload())
    instance.complete_run({"result": {"done": True}})
    terminal = instance.run_state()
    instance.external_event_received(event_payload("late-event"))
    assert instance.run_state() == terminal


def test_conflicting_duplicate_event_id_does_not_mutate_prior_evidence():
    instance = workflow_for()
    instance.external_event_received(event_payload())
    instance.external_event_received({**event_payload(), "event_type": "forged"})
    assert instance.run_state()["external_events"] == [event_payload()]


def test_event_history_saturation_does_not_falsely_acknowledge():
    instance = workflow_for()
    instance._state.external_events = [
        {"event_id": f"event-{n}"} for n in range(1000)
    ]
    instance.external_event_received(event_payload("event-over-limit"))
    assert len(instance.run_state()["external_events"]) == 1000
