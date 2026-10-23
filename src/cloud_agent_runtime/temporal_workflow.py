from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from temporalio import workflow


@dataclass
class AgentRunWorkflowState:
    runtime_run_id: str
    session_id: str
    phase: str = "running"
    paused: bool = False
    terminal: bool = False
    approval_events: list[dict[str, Any]] = field(default_factory=list)
    processed_approval_ids: set[str] = field(default_factory=set)
    evidence_refs: list[str] = field(default_factory=list)
    result: dict[str, Any] | None = None
    error: str | None = None
    transition_events: list[dict[str, Any]] = field(default_factory=list)
    timeout_seconds: float | None = None

    def record_transition(self, *, event_id: str, event_type: str,
                          evidence_refs: list[str] | None = None) -> None:
        if any(event["event_id"] == event_id for event in self.transition_events):
            return
        refs = list(evidence_refs or [])
        self.transition_events.append({
            "event_id": event_id,
            "event_type": event_type,
            "phase": self.phase,
            "evidence_refs": refs,
        })
        for ref in refs:
            if ref not in self.evidence_refs:
                self.evidence_refs.append(ref)


@workflow.defn(name="AgentRunWorkflow")
class AgentRunWorkflow:
    """Durable control flow for one canonical Run.

    Workflow history is the durable source for orchestration transitions.
    Canonical product state is reconciled separately by AsyncAgentRuntime.
    """

    def __init__(self) -> None:
        self._state: AgentRunWorkflowState | None = None

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        timeout_value = payload.get("timeout_seconds")
        timeout_seconds = None if timeout_value is None else float(timeout_value)
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._state = AgentRunWorkflowState(
            runtime_run_id=str(payload["runtime_run_id"]),
            session_id=str(payload["session_id"]),
            timeout_seconds=timeout_seconds,
        )
        self._state.record_transition(
            event_id=f"run-started:{self._state.runtime_run_id}",
            event_type="run_started",
        )
        try:
            if timeout_seconds is None:
                await workflow.wait_condition(
                    lambda: self._state is not None and self._state.terminal
                )
            else:
                await workflow.wait_condition(
                    lambda: self._state is not None and self._state.terminal,
                    timeout=timeout_seconds,
                )
        except TimeoutError:
            assert self._state is not None
            if not self._state.terminal:
                self._state.phase = "failed"
                self._state.error = "run wall-clock budget exceeded"
                self._state.terminal = True
                self._state.record_transition(
                    event_id=f"run-timeout:{self._state.runtime_run_id}",
                    event_type="run_timed_out",
                )

        assert self._state is not None
        return {
            "runtime_run_id": self._state.runtime_run_id,
            "session_id": self._state.session_id,
            "phase": self._state.phase,
            "result": self._state.result,
            "error": self._state.error,
            "terminal": self._state.terminal,
            "evidence_refs": list(self._state.evidence_refs),
            "transition_events": list(self._state.transition_events),
        }

    @workflow.signal(name="approval_resolved")
    def approval_resolved(self, payload: dict[str, Any]) -> None:
        if self._state is None or self._state.terminal:
            return
        approval_id = str(payload.get("approval_id") or "")
        if approval_id and approval_id in self._state.processed_approval_ids:
            return
        event = dict(payload)
        self._state.approval_events.append(event)
        if approval_id:
            self._state.processed_approval_ids.add(approval_id)
        refs = [str(ref) for ref in event.get("evidence_refs", [])]
        if event.get("approved") is False:
            self._state.phase = "failed"
            self._state.error = str(event.get("reason") or "approval denied")
            self._state.terminal = True
            event_type = "approval_denied"
        else:
            self._state.phase = "running"
            event_type = "approval_granted"
        self._state.record_transition(
            event_id=f"approval:{approval_id}" if approval_id else f"approval-event:{len(self._state.approval_events)}",
            event_type=event_type,
            evidence_refs=refs,
        )

    @workflow.signal(name="pause_run")
    def pause_run(self, payload: dict[str, Any] | None = None) -> None:
        if self._state is None or self._state.terminal:
            return
        self._state.paused = True
        self._state.phase = "paused"
        self._state.record_transition(
            event_id=str((payload or {}).get("event_id") or f"pause:{self._state.runtime_run_id}"),
            event_type="run_paused",
        )

    @workflow.signal(name="resume_run")
    def resume_run(self, payload: dict[str, Any] | None = None) -> None:
        if self._state is None or self._state.terminal:
            return
        self._state.paused = False
        self._state.phase = "running"
        self._state.record_transition(
            event_id=str((payload or {}).get("event_id") or f"resume:{self._state.runtime_run_id}"),
            event_type="run_resumed",
        )

    @workflow.signal(name="complete_run")
    def complete_run(self, payload: dict[str, Any]) -> None:
        if self._state is None or self._state.terminal:
            return
        self._state.phase = "succeeded"
        self._state.result = dict(payload.get("result") or {})
        self._state.terminal = True
        refs = [str(ref) for ref in payload.get("evidence_refs", [])]
        self._state.record_transition(
            event_id=str(payload.get("event_id") or f"run-completed:{self._state.runtime_run_id}"),
            event_type="run_completed",
            evidence_refs=refs,
        )

    @workflow.signal(name="fail_run")
    def fail_run(self, payload: dict[str, Any]) -> None:
        if self._state is None or self._state.terminal:
            return
        self._state.phase = "failed"
        self._state.error = str(payload.get("error") or "runtime failure")
        self._state.terminal = True
        self._state.record_transition(
            event_id=str(payload.get("event_id") or f"run-failed:{self._state.runtime_run_id}"),
            event_type="run_failed",
            evidence_refs=[str(ref) for ref in payload.get("evidence_refs", [])],
        )

    @workflow.query(name="run_state")
    def run_state(self) -> dict[str, Any]:
        if self._state is None:
            return {"phase": "initializing", "terminal": False}
        return {
            "runtime_run_id": self._state.runtime_run_id,
            "session_id": self._state.session_id,
            "phase": self._state.phase,
            "paused": self._state.paused,
            "terminal": self._state.terminal,
            "approval_events": list(self._state.approval_events),
            "evidence_refs": list(self._state.evidence_refs),
            "error": self._state.error,
            "transition_events": list(self._state.transition_events),
            "timeout_seconds": self._state.timeout_seconds,
        }
