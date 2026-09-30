from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .workflow import WorkflowRef


_TERMINAL_WORKFLOW_STATUSES = {
    "COMPLETED",
    "FAILED",
    "CANCELED",
    "CANCELLED",
    "TERMINATED",
    "CONTINUED_AS_NEW",
    "CONTINUEDASNEW",
    "TIMED_OUT",
    "TIMEDOUT",
}


class TemporalWorkflowConflict(RuntimeError):
    """Canonical Runtime Run points at a terminal Temporal execution."""


def _workflow_status_name(status: Any) -> str:
    if status is None:
        return ""
    name = getattr(status, "name", None)
    if isinstance(name, str) and name:
        return name.upper()
    value = str(status).upper()
    if "." in value:
        value = value.rsplit(".", 1)[-1]
    if value.startswith("WORKFLOW_EXECUTION_STATUS_"):
        value = value.removeprefix("WORKFLOW_EXECUTION_STATUS_")
    return value


def _workflow_is_terminal(status: Any) -> bool:
    return _workflow_status_name(status) in _TERMINAL_WORKFLOW_STATUSES


@dataclass
class TemporalWorkflowDriver:
    """Async adapter over a Temporal Python SDK Client."""

    client: Any
    task_queue: str
    workflow_type: str = "AgentRunWorkflow"
    workflow_id_prefix: str = "agent-run"
    name: str = "temporal"

    @classmethod
    async def connect(
        cls,
        target: str,
        *,
        task_queue: str,
        namespace: str = "default",
        workflow_type: str = "AgentRunWorkflow",
        workflow_id_prefix: str = "agent-run",
        **connect_kwargs: Any,
    ) -> "TemporalWorkflowDriver":
        try:
            from temporalio.client import Client
        except ImportError as exc:
            raise RuntimeError(
                'install Temporal support with: '
                'pip install -e ".[temporal]"'
            ) from exc

        client = await Client.connect(
            target,
            namespace=namespace,
            **connect_kwargs,
        )
        return cls(
            client=client,
            task_queue=task_queue,
            workflow_type=workflow_type,
            workflow_id_prefix=workflow_id_prefix,
        )

    def workflow_id_for(
        self,
        runtime_run_id: str,
    ) -> str:
        return (
            f"{self.workflow_id_prefix}-"
            f"{runtime_run_id}"
        )

    async def start_run(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
    ) -> WorkflowRef:
        workflow_id = self.workflow_id_for(
            runtime_run_id
        )

        try:
            from temporalio.common import WorkflowIDReusePolicy
            from temporalio.exceptions import WorkflowAlreadyStartedError
        except ImportError as exc:
            raise RuntimeError(
                'install Temporal support with: '
                'pip install -e ".[temporal]"'
            ) from exc

        try:
            handle = await self.client.start_workflow(
                self.workflow_type,
                args=[
                    {
                        "runtime_run_id": runtime_run_id,
                        "session_id": session_id,
                    }
                ],
                id=workflow_id,
                task_queue=self.task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            )
        except WorkflowAlreadyStartedError:
            # Canonical runtime_run_id maps to exactly one Temporal workflow
            # identity. Reuse is only valid while the existing execution is
            # nonterminal; attaching a terminal execution would make a new
            # Runtime Run look active while continuation has already ended.
            handle = self.client.get_workflow_handle(
                workflow_id
            )
            describe = getattr(handle, "describe", None)
            if describe is None:
                raise TemporalWorkflowConflict(
                    "cannot verify existing Temporal workflow status: "
                    f"{workflow_id}"
                )
            description = await describe()
            status = getattr(description, "status", None)
            if isinstance(description, dict):
                status = description.get("status")
            if not status:
                raise TemporalWorkflowConflict(
                    "existing Temporal workflow status is unavailable: "
                    f"{workflow_id}"
                )
            if _workflow_is_terminal(status):
                raise TemporalWorkflowConflict(
                    "existing Temporal workflow is terminal: "
                    f"{workflow_id} status={_workflow_status_name(status)}"
                )

        return WorkflowRef(
            provider=self.name,
            workflow_id=workflow_id,
            run_id=getattr(
                handle,
                "first_execution_run_id",
                None,
            ),
        )

    async def signal(
        self,
        workflow: WorkflowRef,
        *,
        name: str,
        payload: dict,
    ) -> None:
        if workflow.run_id is not None:
            handle = self.client.get_workflow_handle(
                workflow.workflow_id,
                run_id=workflow.run_id,
            )
        else:
            handle = self.client.get_workflow_handle(
                workflow.workflow_id,
            )

        await handle.signal(name, payload)

    async def terminate_run(
        self,
        workflow: WorkflowRef,
        *,
        reason: str,
    ) -> None:
        if workflow.run_id is not None:
            handle = self.client.get_workflow_handle(
                workflow.workflow_id,
                run_id=workflow.run_id,
            )
        else:
            handle = self.client.get_workflow_handle(
                workflow.workflow_id,
            )
        await handle.terminate(reason=reason)

    async def query(
        self,
        workflow: WorkflowRef,
        *,
        name: str,
        args: object | None = None,
    ) -> Any:
        if workflow.run_id is not None:
            handle = self.client.get_workflow_handle(
                workflow.workflow_id,
                run_id=workflow.run_id,
            )
        else:
            handle = self.client.get_workflow_handle(
                workflow.workflow_id,
            )

        if args is None:
            return await handle.query(name)
        return await handle.query(name, args)
