from __future__ import annotations

from typing import Protocol

from .workflow import WorkflowRef


class AsyncWorkflowDriver(Protocol):
    name: str

    async def start_run(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
        timeout_seconds: float | None = None,
    ) -> WorkflowRef:
        ...

    async def signal(
        self,
        workflow: WorkflowRef,
        *,
        name: str,
        payload: dict,
    ) -> None:
        ...

    async def terminate_run(
        self,
        workflow: WorkflowRef,
        *,
        reason: str,
    ) -> None:
        ...


class InMemoryAsyncWorkflowDriver:
    name = "in-memory-async"

    def __init__(self) -> None:
        self.signals: list[tuple[str, str, dict]] = []
        self.terminated: list[tuple[str, str]] = []
        self.started: list[dict[str, object]] = []

    async def start_run(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
        timeout_seconds: float | None = None,
    ) -> WorkflowRef:
        self.started.append({
            "runtime_run_id": runtime_run_id,
            "session_id": session_id,
            "timeout_seconds": timeout_seconds,
        })
        return WorkflowRef(
            provider=self.name,
            workflow_id=f"workflow-{runtime_run_id}",
            run_id=runtime_run_id,
        )

    async def signal(
        self,
        workflow: WorkflowRef,
        *,
        name: str,
        payload: dict,
    ) -> None:
        self.signals.append((workflow.workflow_id, name, dict(payload)))

    async def terminate_run(
        self,
        workflow: WorkflowRef,
        *,
        reason: str,
    ) -> None:
        self.terminated.append((workflow.workflow_id, reason))
