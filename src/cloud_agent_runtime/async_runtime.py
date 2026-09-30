from __future__ import annotations

from .async_workflow import AsyncWorkflowDriver
from .contracts import Approval, Budget
from .models import Run, RunStatus
from .runtime import AgentRuntime
from .sandbox import SandboxProvider
from .store import RuntimeStore


class AsyncAgentRuntime(AgentRuntime):
    """AgentRuntime variant for async durable-workflow providers."""

    def __init__(
        self,
        store: RuntimeStore | None = None,
        *,
        sandbox_provider: SandboxProvider | None = None,
        workflow_driver: AsyncWorkflowDriver,
    ) -> None:
        super().__init__(
            store=store,
            sandbox_provider=sandbox_provider,
            workflow_driver=None,
        )
        self.async_workflow_driver = workflow_driver

    async def _best_effort_cancel_async_workflow(
        self,
        workflow,
    ) -> bool:
        cancel = getattr(
            self.async_workflow_driver,
            "cancel_run",
            None,
        )
        if cancel is None:
            return False
        try:
            await cancel(workflow)
            return True
        except Exception:
            return False

    async def start_run(
        self,
        *,
        session_id: str,
        sandbox_ref: str | None = None,
        budget: Budget | None = None,
    ) -> Run:
        # super().start_run persists the canonical Run and Sandbox binding
        # before this async workflow provider creates an external side effect.
        run = super().start_run(
            session_id=session_id,
            sandbox_ref=sandbox_ref,
            budget=budget,
        )

        try:
            workflow_ref = (
                await self.async_workflow_driver.start_run(
                    runtime_run_id=run.run_id,
                    session_id=session_id,
                )
            )
        except Exception:
            run.status = RunStatus.FAILED
            self._best_effort_save_failed_run(run)
            current = self._sandboxes.pop(
                run.run_id,
                None,
            )
            if current is not None:
                self._best_effort_terminate(current)
            raise

        run.workflow_ref = workflow_ref
        try:
            self.store.save_run(run)
        except Exception:
            cancelled = (
                await self._best_effort_cancel_async_workflow(
                    workflow_ref
                )
            )
            run.status = RunStatus.FAILED
            if cancelled:
                run.workflow_ref = None
            self._best_effort_save_failed_run(run)

            current = self._sandboxes.pop(
                run.run_id,
                None,
            )
            if current is not None:
                self._best_effort_terminate(current)
            raise

        return run

    async def resolve_approval(
        self,
        run_id: str,
        approval_id: str,
        *,
        approved: bool,
        actor: str,
        reason: str | None = None,
    ) -> Approval:
        approval = super().resolve_approval(
            run_id,
            approval_id,
            approved=approved,
            actor=actor,
            reason=reason,
        )

        run = self.store.get_run(run_id)
        if run.workflow_ref is not None:
            await self.async_workflow_driver.signal(
                run.workflow_ref,
                name="approval_resolved",
                payload={
                    "approval_id": approval_id,
                    "approved": approved,
                    "actor": actor,
                    "reason": reason,
                    "evidence_refs": list(
                        approval.evidence_refs
                    ),
                },
            )

        return approval
