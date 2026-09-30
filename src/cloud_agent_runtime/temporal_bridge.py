from __future__ import annotations

import asyncio
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
    completion_timeout_seconds: float = 5.0
    completion_poll_interval_seconds: float = 0.05

    async def start_or_attach(self, *, runtime_run_id: str, session_id: str) -> WorkflowRef:
        return await self.driver.start_run(runtime_run_id=runtime_run_id, session_id=session_id)

    def reference(self, *, runtime_run_id: str) -> WorkflowRef:
        """Return the canonical workflow identity without creating or attaching.

        Read/query and idempotent post-completion retries use this path so a
        terminal workflow remains observable without weakening start semantics.
        """
        return WorkflowRef(
            provider=self.driver.name,
            workflow_id=self.driver.workflow_id_for(runtime_run_id),
        )

    async def signal_approval(self, workflow: WorkflowRef, *, approval_id: str, approved: bool, evidence_refs: Sequence[str] = (), reason: str | None = None) -> None:
        payload: dict[str, Any] = {"approval_id": approval_id, "approved": approved, "evidence_refs": list(evidence_refs)}
        if reason is not None:
            payload["reason"] = reason
        await self.driver.signal(workflow, name="approval_resolved", payload=payload)

    async def complete(self, workflow: WorkflowRef, *, result: dict[str, Any] | None = None, evidence_refs: Sequence[str] = ()) -> None:
        # Completion is an idempotent bridge operation. A caller may retry after
        # the Temporal workflow has already consumed the first complete_run signal
        # and closed. In that case a second signal is invalid at the Temporal API
        # boundary, but the requested post-condition is already satisfied.
        state = await self.driver.query(workflow, name="run_state")
        if bool(state.get("terminal", False)):
            return

        try:
            await self.driver.signal(
                workflow,
                name="complete_run",
                payload={"result": dict(result or {}), "evidence_refs": list(evidence_refs)},
            )
        except Exception:
            # Close can race the pre-signal query. Confirm the durable terminal
            # state before deciding whether the signal error is safe to absorb.
            state = await self.driver.query(workflow, name="run_state")
            if bool(state.get("terminal", False)):
                return
            raise

        # Temporal signal delivery is durable but workflow handling is asynchronous.
        # Do not return completion to the caller until the workflow has observed the
        # signal and published its terminal state.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, self.completion_timeout_seconds)
        while True:
            state = await self.driver.query(workflow, name="run_state")
            if bool(state.get("terminal", False)):
                return
            if loop.time() >= deadline:
                raise TimeoutError(
                    f"workflow {workflow.workflow_id} did not acknowledge completion "
                    f"within {self.completion_timeout_seconds:.1f}s"
                )
            await asyncio.sleep(max(0.0, self.completion_poll_interval_seconds))

    async def get_run_status(self, workflow: WorkflowRef) -> TemporalRunStatus:
        state = await self.driver.query(workflow, name="run_state")
        return TemporalRunStatus(runtime_run_id=str(state.get("runtime_run_id", "")), workflow_id=workflow.workflow_id, phase=str(state.get("phase", "unknown")), paused=bool(state.get("paused", False)), terminal=bool(state.get("terminal", False)), evidence_refs=tuple(str(item) for item in state.get("evidence_refs", [])), error=state.get("error"))
