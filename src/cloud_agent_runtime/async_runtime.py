from __future__ import annotations

from .async_workflow import AsyncWorkflowDriver, WorkflowStartOutcomeUnknown
from .contracts import Approval, ApprovalStatus, Budget
from .models import Run, RunStatus, SessionStatus
from .runtime import AgentRuntime
from .sandbox import SandboxProvider
from .store import RuntimeStore
from .temporal_driver import deterministic_start_operation_id
from .workflow import WorkflowRef


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
        workflow: WorkflowRef,
        *,
        operation_id: str,
    ) -> bool:
        terminate = getattr(self.async_workflow_driver, "terminate_run", None)
        if terminate is None:
            return False
        try:
            await terminate(
                workflow,
                reason="runtime binding persistence failed",
                operation_id=operation_id,
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
        """Create or replay a Run using caller-stable identity when supplied.

        If a prior attempt may have reached the Workflow provider, this method
        reconciles the original deterministic provider identity; it never
        creates a second Run or starts a replacement Workflow for the same ID.
        """
        if budget is not None:
            timeout = budget.max_wall_seconds
            if timeout is not None and (
                isinstance(timeout, bool)
                or not isinstance(timeout, int)
                or timeout <= 0
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
            if existing.status in {RunStatus.SUCCEEDED, RunStatus.FAILED}:
                return existing
            if existing.recovery_state in {
                "start_pending",
                "unknown",
                "intent_persisted",
            }:
                # Attach/observe only. In an ambiguous state, a retry must not
                # issue another start merely because the first ACK was lost.
                return await self.reconcile_run(existing.run_id)
            if (
                existing.recovery_state == "attached"
                and existing.workflow_ref is not None
            ):
                return existing
            if (
                existing.recovery_state == "none"
                and existing.workflow_ref is None
            ):
                # The provider call is forbidden until recovery intent commits.
                # Therefore a durable record with no intent proves the first
                # attempt did not send the Workflow start request.
                session = self.store.get_session(session_id)
                if session.status != SessionStatus.ACTIVE:
                    raise ValueError("runs can only start on an active session")
                run = existing
            else:
                raise RunRecoveryConflict(
                    f"Run {existing.run_id} has inconsistent recovery metadata; refusing duplicate start"
                )
        else:
            # Canonical Run identity and sandbox binding are persisted before
            # any remote workflow start is requested.
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
        run.recovery_evidence.append({
            "operation_id": operation_id,
            "outcome": "intent_persisted",
            "attempt": "0",
            "detail": (
                "canonical run persisted before provider start; "
                f"workflow_id=agent-run-{run.run_id}"
            ),
        })
        self.store.save_run(run)

        start_kwargs = {
            "runtime_run_id": run.run_id,
            "session_id": session_id,
        }
        if budget is not None and budget.max_wall_seconds is not None:
            start_kwargs["timeout_seconds"] = float(budget.max_wall_seconds)

        try:
            workflow_ref = await self.async_workflow_driver.start_run(**start_kwargs)
        except WorkflowStartOutcomeUnknown as exc:
            self._record_recovery(
                run,
                operation_id=operation_id,
                outcome="unknown",
                attempt=1,
                detail=(
                    f"provider may have accepted start; workflow_id={exc.workflow_id}; "
                    f"{exc}"
                ),
            )
            raise
        except Exception as exc:
            # A timeout, connection reset or process cancellation may happen
            # after the provider accepted the request. Without a provider ACK
            # or query, do not translate this ambiguity into FAILED.
            self._record_recovery(
                run,
                operation_id=operation_id,
                outcome="unknown",
                attempt=1,
                detail=(
                    f"start outcome could not be established: "
                    f"{type(exc).__name__}: {exc}"
                ),
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

    async def reconcile_run(self, run_id: str) -> Run:
        """Attach an uncertain canonical Run to its original workflow or stay unknown."""
        run = self.store.get_run(run_id)
        operation_id = deterministic_start_operation_id(run.run_id)
        if run.recovery_operation_id not in (None, operation_id):
            raise RunRecoveryConflict(
                "persisted start operation does not match canonical Run"
            )
        if run.workflow_ref is not None and run.recovery_state == "attached":
            return run

        reconcile = getattr(self.async_workflow_driver, "reconcile_start", None)
        if reconcile is None:
            self._record_recovery(
                run,
                operation_id=operation_id,
                outcome="unknown",
                attempt=run.recovery_attempts + 1,
                detail="provider does not expose start reconciliation; no new workflow was created",
            )
            raise RunRecoveryConflict(
                f"cannot verify provider workflow state for Run {run_id}; Run remains unknown"
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
                f"provider reconciliation failed for Run {run_id}; no duplicate was started"
            ) from exc

        if workflow_ref is None:
            self._record_recovery(
                run,
                operation_id=operation_id,
                outcome="unknown",
                attempt=run.recovery_attempts + 1,
                detail="provider did not confirm an execution for deterministic workflow identity; no duplicate was started",
            )
            raise RunRecoveryConflict(
                f"provider cannot prove workflow existence or absence for Run {run_id}"
            )

        run.workflow_ref = workflow_ref
        self._record_recovery(
            run,
            operation_id=operation_id,
            outcome="attached",
            attempt=run.recovery_attempts + 1,
            detail=(
                f"reconciled existing workflow_id={workflow_ref.workflow_id}; "
                f"provider={workflow_ref.provider}"
            ),
        )
        return self.store.get_run(run_id)

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
                    "approved": approval.status == ApprovalStatus.APPROVED,
                    "actor": approval.actor,
                    "reason": approval.reason,
                    "evidence_refs": list(approval.evidence_refs),
                },
            )
        return approval
