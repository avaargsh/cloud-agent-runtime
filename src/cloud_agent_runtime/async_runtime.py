from __future__ import annotations

from .async_workflow import AsyncWorkflowDriver, WorkflowStartOutcomeUnknown
from .contracts import Approval, ApprovalStatus, Budget
from .models import Run, RunStatus
from .runtime import AgentRuntime
from .sandbox import SandboxProvider
from .store import RuntimeStore
from .temporal_driver import deterministic_start_operation_id


class RunRecoveryConflict(RuntimeError):
    """Run cannot be resumed safely because provider outcome is unknown."""


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

    def _record_recovery(
        self,
        run: Run,
        *,
        operation_id: str,
        outcome: str,
        attempt: int,
        detail: str,
    ) -> None:
        run.recovery_operation_id = operation_id
        run.recovery_state = outcome
        run.recovery_attempts = max(run.recovery_attempts, attempt)
        run.recovery_evidence.append({
            "operation_id": operation_id,
            "outcome": outcome,
            "attempt": str(attempt),
            "detail": detail,
        })
        self.store.save_run(run)

    async def start_run(
        self,
        *,
        session_id: str,
        sandbox_ref: str | None = None,
        budget: Budget | None = None,
        run_id: str | None = None,
    ) -> Run:
        """Create a durable Run or reconcile a retry with the same caller identity."""
        max_wall_seconds = budget.max_wall_seconds if budget is not None else None
        if max_wall_seconds is not None and (
            isinstance(max_wall_seconds, bool)
            or not isinstance(max_wall_seconds, int)
            or max_wall_seconds <= 0
        ):
            raise ValueError("budget.max_wall_seconds must be a positive integer")

        existing: Run | None = None
        if run_id is not None:
            if not run_id.strip():
                raise ValueError("run_id must not be empty")
            try:
                existing = self.store.get_run(run_id)
            except KeyError:
                existing = None

        if existing is not None:
            if existing.session_id != session_id:
                raise ValueError("run_id is already bound to a different Session")
            if sandbox_ref is not None and existing.sandbox_ref != sandbox_ref:
                raise ValueError("run_id replay changed the sandbox reference")
            if budget is not None and existing.budget != budget:
                raise ValueError("run_id replay changed the Run budget")
            terminal_statuses = {
                RunStatus.SUCCEEDED,
                RunStatus.FAILED,
                RunStatus.CANCELLED,
            }
            if existing.status in terminal_statuses:
                return existing
            if existing.recovery_state in {"start_pending", "unknown", "intent_persisted"}:
                # Ambiguous retries attach/observe only. They never issue a fresh
                # StartWorkflow request merely because the first ACK was lost.
                return await self.reconcile_run(existing.run_id)
            if existing.recovery_state == "attached" and existing.workflow_ref is not None:
                return existing
            if existing.recovery_state == "none" and existing.workflow_ref is None:
                session = self.store.get_session(session_id)
                if session.status.value != "active":
                    raise ValueError("runs can only start on an active session")
                run = existing
            else:
                raise RunRecoveryConflict(
                    f"Run {existing.run_id} has inconsistent recovery metadata; refusing duplicate start"
                )
        else:
            # AgentRuntime commits the canonical Run and its transition evidence
            # before this async adapter crosses the remote side-effect boundary.
            run = super().start_run(
                session_id=session_id,
                sandbox_ref=sandbox_ref,
                budget=budget,
                run_id=run_id,
            )

        operation_id = deterministic_start_operation_id(run.run_id)
        run.recovery_operation_id = operation_id
        run.recovery_state = "start_pending"
        run.recovery_attempts = 0
        workflow_id_for = getattr(self.async_workflow_driver, "workflow_id_for", None)
        workflow_id = (
            workflow_id_for(run.run_id)
            if callable(workflow_id_for)
            else f"agent-run-{run.run_id}"
        )
        run.recovery_evidence.append({
            "operation_id": operation_id,
            "outcome": "intent_persisted",
            "attempt": "0",
            "detail": (
                "canonical Run persisted before provider start; "
                f"workflow_id={workflow_id}"
            ),
        })
        # This commit is a hard precondition: if it fails, do not call Temporal.
        self.store.save_run(run)

        start_kwargs = {
            "runtime_run_id": run.run_id,
            "session_id": session_id,
        }
        if max_wall_seconds is not None:
            start_kwargs["timeout_seconds"] = float(max_wall_seconds)

        try:
            workflow_ref = await self.async_workflow_driver.start_run(**start_kwargs)
        except WorkflowStartOutcomeUnknown as exc:
            self._record_recovery(
                run,
                operation_id=operation_id,
                outcome="unknown",
                attempt=1,
                detail=f"provider may have accepted start; workflow_id={exc.workflow_id}; {exc}",
            )
            raise
        except Exception as exc:
            # RPC errors and connection resets are ambiguous: a server-side
            # execution may exist even though the caller observed no ACK.
            # Preserve the Run and sandbox; reconcile by deterministic ID later.
            self._record_recovery(
                run,
                operation_id=operation_id,
                outcome="unknown",
                attempt=1,
                detail=f"start outcome could not be established: {type(exc).__name__}: {exc}",
            )
            raise

        run.workflow_ref = workflow_ref
        self._record_recovery(
            run,
            operation_id=operation_id,
            outcome="attached",
            attempt=1,
            detail=(
                f"provider acknowledged workflow_id={workflow_ref.workflow_id}; "
                f"provider={workflow_ref.provider}"
            ),
        )
        return self.store.get_run(run.run_id)

    async def _reconcile_start(self, run: Run) -> Run:
        """Attach to the deterministic existing workflow without creating one."""
        operation_id = deterministic_start_operation_id(run.run_id)
        if run.recovery_operation_id not in (None, operation_id):
            raise RunRecoveryConflict(
                "persisted start operation does not match canonical Run"
            )

        reconcile = getattr(self.async_workflow_driver, "reconcile_start", None)
        if not callable(reconcile):
            self._record_recovery(
                run,
                operation_id=operation_id,
                outcome="unknown",
                attempt=run.recovery_attempts + 1,
                detail="provider has no start reconciliation API; no replacement workflow was started",
            )
            raise RunRecoveryConflict(
                f"cannot verify provider workflow state for Run {run.run_id}"
            )

        timeout_seconds = (
            float(run.budget.max_wall_seconds)
            if run.budget is not None and run.budget.max_wall_seconds is not None
            else None
        )
        try:
            workflow_ref = await reconcile(
                runtime_run_id=run.run_id,
                session_id=run.session_id,
                operation_id=operation_id,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:
            self._record_recovery(
                run,
                operation_id=operation_id,
                outcome="unknown",
                attempt=run.recovery_attempts + 1,
                detail=f"provider reconciliation failed closed: {type(exc).__name__}: {exc}",
            )
            raise RunRecoveryConflict(
                f"provider reconciliation failed for Run {run.run_id}; no duplicate was started"
            ) from exc

        if workflow_ref is None:
            self._record_recovery(
                run,
                operation_id=operation_id,
                outcome="unknown",
                attempt=run.recovery_attempts + 1,
                detail="provider could not confirm the deterministic Workflow ID; no duplicate was started",
            )
            raise RunRecoveryConflict(
                f"provider cannot prove workflow existence or absence for Run {run.run_id}"
            )

        run.workflow_ref = workflow_ref
        self._record_recovery(
            run,
            operation_id=operation_id,
            outcome="attached",
            attempt=run.recovery_attempts + 1,
            detail=f"reconciled existing workflow_id={workflow_ref.workflow_id}; provider={workflow_ref.provider}",
        )
        return self.store.get_run(run.run_id)

    async def reconcile_run(self, run_id: str) -> Run:
        """Reconcile canonical Run state with its durable Temporal workflow state.

        Temporal owns timeout/waiting execution. This method projects terminal
        workflow outcomes back into the canonical Run store without permitting
        a stale workflow observation to resurrect a terminal Run.
        """
        run = self.store.get_run(run_id)
        start_operation_id = deterministic_start_operation_id(run.run_id)
        if run.workflow_ref is None or (
            run.recovery_operation_id == start_operation_id
            and run.recovery_state in {"start_pending", "unknown", "intent_persisted"}
        ):
            run = await self._reconcile_start(run)

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
