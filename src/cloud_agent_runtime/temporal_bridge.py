from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from .temporal_driver import TemporalWorkflowDriver
from .workflow import WorkflowRef

@dataclass(frozen=True)
class TemporalRunStatus:
    runtime_run_id: str
    workflow_id: str
    phase: str
    paused: bool
    terminal: bool
    evidence_refs: tuple[str, ...]
    error: str | None = None

@dataclass
class TemporalRunBridge:
    """Minimal durable bridge for external control planes.

    Domain systems own evidence, decisions and actions. This bridge only owns
    canonical workflow identity, durable signals, status and evidence refs.
    """

    driver: TemporalWorkflowDriver

    async def start_or_attach(self, *, runtime_run_id: str, session_id: str) -> WorkflowRef:
        return await self.driver.start_run(runtime_run_id=runtime_run_id, session_id=session_id)

    async def signal_approval(self, workflow: WorkflowRef, *, approval_id: str, approved: bool, evidence_refs: Sequence[str] = (), reason: str | None = None) -> None:
        payload: dict[str, Any] = {"approval_id": approval_id, "approved": approved, "evidence_refs": list(evidence_refs)}
        if reason is not None:
            payload["reason"] = reason
        await self.driver.signal(workflow, name="approval_resolved", payload=payload)

    async def complete(self, workflow: WorkflowRef, *, result: dict[str, Any] | None = None, evidence_refs: Sequence[str] = ()) -> None:
        await self.driver.signal(workflow, name="complete_run", payload={"result": dict(result or {}), "evidence_refs": list(evidence_refs)})

    async def get_run_status(self, workflow: WorkflowRef) -> TemporalRunStatus:
        state = await self.driver.query(workflow, name="run_state")
        return TemporalRunStatus(runtime_run_id=str(state.get("runtime_run_id", "")), workflow_id=workflow.workflow_id, phase=str(state.get("phase", "unknown")), paused=bool(state.get("paused", False)), terminal=bool(state.get("terminal", False)), evidence_refs=tuple(str(item) for item in state.get("evidence_refs", [])), error=state.get("error"))
