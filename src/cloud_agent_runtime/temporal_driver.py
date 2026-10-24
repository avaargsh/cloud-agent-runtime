from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any

from .workflow import WorkflowRef


def deterministic_start_operation_id(runtime_run_id: str) -> str:
    """Stable provider-start operation identity, independent of attempts."""
    if not runtime_run_id or not runtime_run_id.strip():
        raise ValueError("runtime_run_id must be non-empty")
    return f"agent-run-start:{runtime_run_id}"


def deterministic_cleanup_operation_id(run_id: str, sandbox_ref: str) -> str:
    """Stable cleanup identity bound to its owner Run and sandbox reference."""
    if not run_id or not run_id.strip():
        raise ValueError("run_id must be non-empty")
    if not sandbox_ref or not sandbox_ref.strip():
        raise ValueError("sandbox_ref must be non-empty")
    return f"sandbox-cleanup:{run_id}:{sandbox_ref}"


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
    value = name.upper() if isinstance(name, str) and name else str(status).upper()
    if "." in value:
        value = value.rsplit(".", 1)[-1]
    if value.startswith("WORKFLOW_EXECUTION_STATUS_"):
        value = value.removeprefix("WORKFLOW_EXECUTION_STATUS_")
    return value


def _execution_run_id(description: Any) -> str | None:
    """Return the concrete execution run ID from a verified describe response."""
    execution = (
        description.get("execution")
        if isinstance(description, dict)
        else getattr(description, "execution", None)
    )
    run_id = (
        execution.get("run_id")
        if isinstance(execution, dict)
        else getattr(execution, "run_id", None)
    )
    return run_id if isinstance(run_id, str) and run_id.strip() else None


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
        timeout_seconds: float | None = None,
    ) -> WorkflowRef:
        if timeout_seconds is not None and (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be a finite positive number")
        workflow_id = self.workflow_id_for(
            runtime_run_id
        )

        description = None
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
                        "start_operation_id": deterministic_start_operation_id(runtime_run_id),
                        **(
                            {
                                "timeout_seconds": timeout_seconds,
                                "timeout_event_id": f"timeout:{runtime_run_id}",
                            }
                            if timeout_seconds is not None
                            else {}
                        ),
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

        resolved_run_id = (
            getattr(handle, "first_execution_run_id", None)
            or getattr(handle, "result_run_id", None)
            or _execution_run_id(description)
        )
        return WorkflowRef(
            provider=self.name,
            workflow_id=workflow_id,
            run_id=resolved_run_id,
        )

    async def reconcile_start(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
        operation_id: str,
        timeout_seconds: float | None = None,
    ) -> WorkflowRef | None:
        expected_operation_id = deterministic_start_operation_id(runtime_run_id)
        if operation_id != expected_operation_id:
            raise ValueError("start operation identity does not match canonical Run")
        workflow_id = self.workflow_id_for(runtime_run_id)
        handle = self.client.get_workflow_handle(workflow_id)
        describe = getattr(handle, "describe", None)
        if describe is None:
            raise TemporalWorkflowConflict(
                f"cannot reconcile workflow start without status lookup: {workflow_id}"
            )
        try:
            description = await describe()
        except Exception as exc:
            raise TemporalWorkflowConflict(
                f"workflow start outcome remains unknown for {workflow_id}"
            ) from exc
        status = getattr(description, "status", None)
        if isinstance(description, dict):
            status = description.get("status")
        if not status:
            raise TemporalWorkflowConflict(
                f"workflow start outcome remains unknown; status unavailable: {workflow_id}"
            )
        if _workflow_is_terminal(status):
            # The canonical Run may still have an ambiguous start outcome,
            # but a terminal execution must never be projected as an active
            # attachment. A separate terminal-result reconciliation path may
            # update the canonical terminal state; start recovery fails closed.
            raise TemporalWorkflowConflict(
                "existing Temporal workflow is terminal: "
                f"{workflow_id} status={_workflow_status_name(status)}"
            )
        return WorkflowRef(
            provider=self.name,
            workflow_id=workflow_id,
            # get_workflow_handle has no run ID unless one was supplied. After
            # an ambiguous start ACK, persist the execution observed by describe
            # instead of leaving the canonical WorkflowRef with a null run ID.
            run_id=(
                getattr(handle, "first_execution_run_id", None)
                or _execution_run_id(description)
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

        if name == "run_state":
            # A terminal execution no longer needs a live Worker to serve a
            # query. Read its persisted result directly, avoiding a query that
            # can wait on a workflow task which will never be polled again.
            try:
                description = await handle.describe()
                status = getattr(description, "status", None)
                if isinstance(description, dict):
                    status = description.get("status")
                if _workflow_is_terminal(status):
                    result = await handle.result()
                    if isinstance(result, dict) and "phase" in result:
                        phase = str(result["phase"])
                        return {
                            **result,
                            "paused": phase == "paused",
                            "terminal": True,
                            "approval_events": list(
                                result.get("approval_events", [])
                            ),
                        }
            except Exception:
                # The live query below may still work if describe/result is
                # temporarily unavailable or the execution is nonterminal.
                pass

        try:
            if args is None:
                return await handle.query(name)
            return await handle.query(name, args)
        except Exception:
            if name != "run_state":
                raise

            # Close can race the first describe/query. Re-check durable status
            # before deciding whether the query error should be propagated.
            description = await handle.describe()
            status = getattr(description, "status", None)
            if isinstance(description, dict):
                status = description.get("status")
            if not _workflow_is_terminal(status):
                raise

            result = await handle.result()
            if not isinstance(result, dict) or "phase" not in result:
                raise
            phase = str(result["phase"])
            return {
                **result,
                "paused": phase == "paused",
                "terminal": True,
                "approval_events": list(result.get("approval_events", [])),
            }
