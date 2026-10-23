from __future__ import annotations

from .async_workflow import AsyncWorkflowDriver
from .contracts import Approval, ApprovalStatus, Budget
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

    async def _best_effort_terminate_async_workflow(
        self,
        workflow,
    ) -> bool:
        terminate = getattr(
            self.async_workflow_driver,
            "terminate_run",
            None,
        )
        if terminate is None:
            return False
        try:
            await terminate(
                workflow,
                reason="runtime binding persistence failed",
            )
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
        max_wall_seconds = budget.max_wall_seconds if budget is not None else None
        if max_wall_seconds is not None and (
            isinstance(max_wall_seconds, bool)
            or not isinstance(max_wall_seconds, int)
            or max_wall_seconds <= 0
        ):
            raise ValueError("budget.max_wall_seconds must be a positive integer")

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
                    **(
                        {"timeout_seconds": float(max_wall_seconds)}
                        if max_wall_seconds is not None
                        else {}
                    ),
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
            terminated = (
                await self._best_effort_terminate_async_workflow(
                    workflow_ref
                )
            )
            run.status = RunStatus.FAILED
            if terminated:
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

    async def reconcile_run(self, run_id: str) -> Run:
        """Reconcile canonical Run state with its durable Temporal workflow state.

        Temporal owns timeout/waiting execution. This method projects terminal
        workflow outcomes back into the canonical Run store without permitting
        a stale workflow observation to resurrect a terminal Run.
        """
        run = self.store.get_run(run_id)
        if run.workflow_ref is None:
            return run

        query = getattr(self.async_workflow_driver, "query", None)
        if query is None:
            raise RuntimeError(
                "workflow driver does not support querying durable Run state"
            )
        state = await query(run.workflow_ref, name="run_state")
        phase = str(state.get("phase", ""))
        status_by_phase = {
            "running": RunStatus.RUNNING,
            "waiting": RunStatus.WAITING_APPROVAL,
            "paused": RunStatus.PAUSED,
            "succeeded": RunStatus.SUCCEEDED,
            "failed": RunStatus.FAILED,
            "cancelled": RunStatus.CANCELLED,
        }
        target_status = status_by_phase.get(phase)
        if target_status is None:
            return run

        terminal_statuses = {
            RunStatus.SUCCEEDED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
        }
        if run.status in terminal_statuses:
            if target_status not in terminal_statuses:
                # A stale observation must never reopen a terminal Run.
                return run
            if run.status != target_status:
                raise RuntimeError(
                    "canonical Run and Temporal workflow have conflicting "
                    f"terminal states: {run.status.value} vs {target_status.value}"
                )

        changed = False
        temporal_transitions = state.get("transition_events", [])
        last_transition = (
            temporal_transitions[-1]
            if temporal_transitions
            else {}
        )
        if run.status != target_status:
            temporal_event_id = (
                last_transition.get("event_id")
                or last_transition.get("transition_id")
                or f"{run.workflow_ref.workflow_id}:{phase}"
            )
            if last_transition.get("event_type") == "run_timed_out":
                event_type = "workflow_run_timed_out"
            elif target_status == RunStatus.SUCCEEDED:
                event_type = "workflow_succeeded"
            elif target_status == RunStatus.FAILED:
                event_type = "workflow_failed"
            elif target_status == RunStatus.CANCELLED:
                event_type = "workflow_cancelled"
            elif target_status == RunStatus.PAUSED:
                event_type = "workflow_paused"
            elif target_status == RunStatus.WAITING_APPROVAL:
                event_type = "workflow_waiting"
            else:
                event_type = "workflow_reconciled"

            self._transition_run(
                run,
                to_status=target_status,
                event_type=event_type,
                event_id=str(temporal_event_id),
                evidence_refs=[
                    str(ref)
                    for ref in last_transition.get("evidence_refs", [])
                ],
            )
            changed = True

        for reference in state.get("evidence_refs", []):
            reference = str(reference)
            if reference not in run.evidence_refs:
                run.evidence_refs.append(reference)
                changed = True

        error = state.get("error")
        if target_status in {RunStatus.FAILED, RunStatus.CANCELLED} and error:
            metadata = {**dict(run.metadata), "terminal_reason": str(error)}
            if dict(run.metadata) != metadata:
                run.metadata = metadata
                changed = True

        if changed:
            self.store.save_run(run)
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
                    "approved": (
                        approval.status
                        == ApprovalStatus.APPROVED
                    ),
                    "actor": approval.actor,
                    "reason": approval.reason,
                    "evidence_refs": list(
                        approval.evidence_refs
                    ),
                },
            )

        return approval
