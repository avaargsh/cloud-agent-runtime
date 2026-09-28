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
    approval_events: list[dict[str, Any]] = field(
        default_factory=list
    )
    evidence_refs: list[str] = field(default_factory=list)
    result: dict[str, Any] | None = None
    error: str | None = None


@workflow.defn(name="AgentRunWorkflow")
class AgentRunWorkflow:
    """Durable control-flow shell for a canonical runtime Run.

    The runtime database remains the source of truth for product state.
    Temporal owns durable waiting/retry/control-flow semantics.
    """

    def __init__(self) -> None:
        self._state: AgentRunWorkflowState | None = None

    @workflow.run
    async def run(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        self._state = AgentRunWorkflowState(
            runtime_run_id=str(
                payload["runtime_run_id"]
            ),
            session_id=str(payload["session_id"]),
        )

        await workflow.wait_condition(
            lambda: (
                self._state is not None
                and self._state.terminal
            )
        )

        assert self._state is not None
        return {
            "runtime_run_id": (
                self._state.runtime_run_id
            ),
            "session_id": self._state.session_id,
            "phase": self._state.phase,
            "result": self._state.result,
            "error": self._state.error,
            "evidence_refs": list(
                self._state.evidence_refs
            ),
        }

    @workflow.signal(name="approval_resolved")
    def approval_resolved(
        self,
        payload: dict[str, Any],
    ) -> None:
        if self._state is None:
            return

        event = dict(payload)
        self._state.approval_events.append(event)

        for ref in event.get(
            "evidence_refs",
            [],
        ):
            if ref not in self._state.evidence_refs:
                self._state.evidence_refs.append(ref)

        if event.get("approved") is False:
            self._state.phase = "failed"
            self._state.error = (
                event.get("reason")
                or "approval denied"
            )
            self._state.terminal = True
        else:
            self._state.phase = "running"

    @workflow.signal(name="pause_run")
    def pause_run(
        self,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if self._state is None or self._state.terminal:
            return
        self._state.paused = True
        self._state.phase = "paused"

    @workflow.signal(name="resume_run")
    def resume_run(
        self,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if self._state is None or self._state.terminal:
            return
        self._state.paused = False
        self._state.phase = "running"

    @workflow.signal(name="complete_run")
    def complete_run(
        self,
        payload: dict[str, Any],
    ) -> None:
        if self._state is None or self._state.terminal:
            return

        self._state.phase = "succeeded"
        self._state.result = dict(
            payload.get("result") or {}
        )

        for ref in payload.get(
            "evidence_refs",
            [],
        ):
            if ref not in self._state.evidence_refs:
                self._state.evidence_refs.append(ref)

        self._state.terminal = True

    @workflow.signal(name="fail_run")
    def fail_run(
        self,
        payload: dict[str, Any],
    ) -> None:
        if self._state is None or self._state.terminal:
            return

        self._state.phase = "failed"
        self._state.error = str(
            payload.get("error")
            or "runtime failure"
        )
        self._state.terminal = True

    @workflow.query(name="run_state")
    def run_state(self) -> dict[str, Any]:
        if self._state is None:
            return {
                "phase": "initializing",
            }

        return {
            "runtime_run_id": (
                self._state.runtime_run_id
            ),
            "session_id": self._state.session_id,
            "phase": self._state.phase,
            "paused": self._state.paused,
            "terminal": self._state.terminal,
            "approval_events": list(
                self._state.approval_events
            ),
            "evidence_refs": list(
                self._state.evidence_refs
            ),
            "error": self._state.error,
        }
