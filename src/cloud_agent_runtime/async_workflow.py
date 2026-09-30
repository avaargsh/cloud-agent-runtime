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

    async def cancel_run(
        self,
        workflow: WorkflowRef,
    ) -> None:
        ...


class InMemoryAsyncWorkflowDriver:
    name = "in-memory-async"

    def __init__(self) -> None:
        self.signals: list[tuple[str, str, dict]] = []
        self.cancelled: list[str] = []

    async def start_run(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
    ) -> WorkflowRef:
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
        self.signals.append(
            (
                workflow.workflow_id,
                name,
                dict(payload),
            )
        )


    async def cancel_run(
        self,
        workflow: WorkflowRef,
    ) -> None:
        self.cancelled.append(workflow.workflow_id)
