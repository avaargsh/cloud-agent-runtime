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

    async def start_run(
        self,
        *,
        session_id: str,
        sandbox_ref: str | None = None,
        budget: Budget | None = None,
    ) -> Run:
        run = super().start_run(
            session_id=session_id,
            sandbox_ref=sandbox_ref,
            budget=budget,
        )

        try:
            run.workflow_ref = (
                await self.async_workflow_driver.start_run(
                    runtime_run_id=run.run_id,
                    session_id=session_id,
                )
            )
            self.store.save_run(run)
            return run
        except Exception:
            run.status = RunStatus.FAILED
            self.store.save_run(run)

            if (
                self.sandbox_provider is not None
                and run.run_id in self._sandboxes
            ):
                self.sandbox_provider.terminate(
                    self._sandboxes[run.run_id]
                )
            raise

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
