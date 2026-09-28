from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class WorkflowRef:
    provider: str
    workflow_id: str
    run_id: str | None = None


class WorkflowDriver(Protocol):
    name: str

    def start_run(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
    ) -> WorkflowRef:
        ...

    def signal(
        self,
        workflow: WorkflowRef,
        *,
        name: str,
        payload: dict,
    ) -> None:
        ...


class InMemoryWorkflowDriver:
    name = "in-memory"

    def start_run(
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

    def signal(
        self,
        workflow: WorkflowRef,
        *,
        name: str,
        payload: dict,
    ) -> None:
        return None
