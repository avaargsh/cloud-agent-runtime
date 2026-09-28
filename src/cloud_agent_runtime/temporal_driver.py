from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .workflow import WorkflowRef


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
                'install Temporal support with: pip install -e ".[temporal]"'
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

    def workflow_id_for(self, runtime_run_id: str) -> str:
        return f"{self.workflow_id_prefix}-{runtime_run_id}"

    async def start_run(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
    ) -> WorkflowRef:
        workflow_id = self.workflow_id_for(runtime_run_id)

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
