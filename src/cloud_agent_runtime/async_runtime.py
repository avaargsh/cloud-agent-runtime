from __future__ import annotations

from uuid import uuid4

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
        run_id: str | None = None,
    ) -> Run:
        """Create or recover one durable Run using its original identity.

        Callers that need safe replay after losing the response must supply a
        stable run_id before the first call. Workflow identity is persisted
        before the Temporal start RPC, so an ambiguous outcome can be retried
        against the same provider identity after reconstructing this runtime.
        """
        if run_id is None:
            run_id = str(uuid4())
        elif not run_id.strip():
            raise ValueError("run_id must not be empty")

        try:
            run = self.store.get_run(run_id)
        except KeyError:
            run = None

        if run is not None:
            if run.session_id != session_id:
                raise ValueError(
                    "run_id is already bound to a different Session"
                )
            if sandbox_ref is not None and run.sandbox_ref != sandbox_ref:
                raise ValueError(
                    "run_id replay changed the sandbox reference"
                )
            if budget is not None and run.budget != budget:
                raise ValueError("run_id replay changed the Run budget")

            if run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED}:
                # Replaying a terminal create request returns the canonical
                # record; it must never create a new Workflow execution.
                return run
            if run.status not in {RunStatus.CREATED, RunStatus.RUNNING}:
                # A live Run with an established Workflow binding is not a
                # request to create/reattach a new execution. Its durable
                # lifecycle continues through the normal control methods.
                return run
            if (
                run.status == RunStatus.RUNNING
                and run.workflow_ref is not None
            ):
                return run

            expected_ref = self.async_workflow_driver.reference_for_run(
                run_id
            )
            if run.workflow_ref is not None and (
                run.workflow_ref.provider != expected_ref.provider
                or run.workflow_ref.workflow_id != expected_ref.workflow_id
            ):
                raise ValueError(
                    "persisted Workflow identity conflicts with deterministic Run mapping"
                )
            # Upgrade a pre-contract pending record as well as recovering a
            # crash between canonical Run persistence and Workflow start.
            if run.status != RunStatus.CREATED or run.workflow_ref is None:
                run.status = RunStatus.CREATED
                run.workflow_ref = expected_ref
                self.store.save_run(run)
        else:
            expected_ref = self.async_workflow_driver.reference_for_run(
                run_id
            )
            # This first durable write contains BOTH the canonical Run and the
            # deterministic Workflow reference. No provider side effect occurs
            # before this write commits.
            run = super().start_run(
                session_id=session_id,
                sandbox_ref=sandbox_ref,
                budget=budget,
                run_id=run_id,
                _initial_status=RunStatus.CREATED,
                _initial_workflow_ref=expected_ref,
            )

        try:
            workflow_ref = await self.async_workflow_driver.start_run(
                runtime_run_id=run_id,
                session_id=session_id,
            )
        except Exception:
            # A transport error can occur after Temporal accepted StartWorkflow.
            # The provider outcome is therefore UNKNOWN. Leave the durable Run
            # pending with its original deterministic Workflow ID and retain
            # the Sandbox; do not mark FAILED or tear down a possibly live Run.
            run.status = RunStatus.CREATED
            run.workflow_ref = expected_ref
            raise

        run.workflow_ref = workflow_ref
        run.status = RunStatus.RUNNING
        try:
            self.store.save_run(run)
        except Exception:
            # The provider may already be running. The previously committed
            # CREATED record still contains the deterministic Workflow ID, so
            # the next caller can safely start-or-attach to that same ID.
            run.status = RunStatus.CREATED
            run.workflow_ref = expected_ref
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
