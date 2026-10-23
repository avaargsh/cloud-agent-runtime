from __future__ import annotations

from typing import Protocol

from .workflow import WorkflowRef


class WorkflowStartOutcomeUnknown(RuntimeError):
    """The provider may have accepted a start, but the caller lacks an ACK."""

    def __init__(self, operation_id: str, workflow_id: str, message: str):
        super().__init__(message)
        self.operation_id = operation_id
        self.workflow_id = workflow_id


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

    async def reconcile_start(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
        operation_id: str,
        timeout_seconds: float | None = None,
    ) -> WorkflowRef | None:
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
        operation_id: str | None = None,
    ) -> None:
        ...


class InMemoryAsyncWorkflowDriver:
    name = "in-memory-async"

    def __init__(self) -> None:
        self.signals: list[tuple[str, str, dict]] = []
        self.terminated: list[tuple[str, str]] = []
        self.started: list[dict[str, object]] = []
        self.start_operation_ids: list[str] = []
        self.workflows: dict[str, WorkflowRef] = {}
        self.start_outcome_unknown_once = False
        self.terminate_after_effect_unknown_once = False

    async def start_run(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
        timeout_seconds: float | None = None,
    ) -> WorkflowRef:
        from .temporal_driver import deterministic_start_operation_id

        operation_id = deterministic_start_operation_id(runtime_run_id)
        self.started.append({
            "runtime_run_id": runtime_run_id,
            "session_id": session_id,
            "timeout_seconds": timeout_seconds,
            "operation_id": operation_id,
        })
        self.start_operation_ids.append(operation_id)
        ref = self.workflows.setdefault(
            f"agent-run-{runtime_run_id}",
            WorkflowRef(
                provider=self.name,
                workflow_id=f"agent-run-{runtime_run_id}",
                run_id=runtime_run_id,
            ),
        )
        if self.start_outcome_unknown_once:
            self.start_outcome_unknown_once = False
            raise WorkflowStartOutcomeUnknown(
                operation_id,
                ref.workflow_id,
                "simulated process lost acknowledgement after provider accepted workflow",
            )
        return ref

    async def reconcile_start(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
        operation_id: str,
        timeout_seconds: float | None = None,
    ) -> WorkflowRef | None:
        from .temporal_driver import deterministic_start_operation_id

        if operation_id != deterministic_start_operation_id(runtime_run_id):
            raise ValueError("start operation identity does not match canonical Run")
        return self.workflows.get(f"agent-run-{runtime_run_id}")

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
        operation_id: str | None = None,
    ) -> None:
        self.terminated.append((workflow.workflow_id, reason))
        if self.terminate_after_effect_unknown_once:
            self.terminate_after_effect_unknown_once = False
            raise RuntimeError(
                f"simulated termination ACK lost for {operation_id or workflow.workflow_id}"
            )
