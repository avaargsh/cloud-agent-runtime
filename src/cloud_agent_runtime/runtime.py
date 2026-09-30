from __future__ import annotations

from uuid import uuid4

from .contracts import (
    Approval,
    ApprovalStatus,
    ArtifactRef,
    Budget,
    CapabilityBinding,
    EvidenceRef,
)
from .models import Run, RunStatus, Session, SessionStatus
from .sandbox import Sandbox, SandboxProvider, SandboxStatus
from .store import InMemoryStore, RuntimeStore
from .workflow import WorkflowDriver


class AgentRuntime:
    """Reference Session/Run state machine with pluggable workflow and sandbox."""

    def __init__(
        self,
        store: RuntimeStore | None = None,
        *,
        sandbox_provider: SandboxProvider | None = None,
        workflow_driver: WorkflowDriver | None = None,
    ) -> None:
        self.store = store or InMemoryStore()
        self.sandbox_provider = sandbox_provider
        self.workflow_driver = workflow_driver
        self._sandboxes: dict[str, Sandbox] = {}

    def create_session(
        self,
        *,
        agent_id: str,
        release_id: str,
        tenant_id: str,
        capabilities: list[CapabilityBinding] | None = None,
    ) -> Session:
        session = Session(
            session_id=str(uuid4()),
            agent_id=agent_id,
            release_id=release_id,
            tenant_id=tenant_id,
            capability_bindings={
                item.name: item
                for item in (capabilities or [])
            },
        )
        self.store.save_session(session)
        return session

    def start_run(
        self,
        *,
        session_id: str,
        sandbox_ref: str | None = None,
        budget: Budget | None = None,
    ) -> Run:
        session = self.store.get_session(session_id)
        if session.status != SessionStatus.ACTIVE:
            raise ValueError("runs can only start on an active session")

        run_id = str(uuid4())
        workflow_ref = None

        if self.workflow_driver is not None:
            workflow_ref = self.workflow_driver.start_run(
                runtime_run_id=run_id,
                session_id=session_id,
            )

        run = Run(
            run_id=run_id,
            session_id=session_id,
            status=RunStatus.RUNNING,
            sandbox_ref=sandbox_ref,
            workflow_ref=workflow_ref,
            budget=budget,
        )

        if sandbox_ref is None and self.sandbox_provider is not None:
            sandbox = self.sandbox_provider.allocate()
            sandbox = self.sandbox_provider.bind(
                sandbox,
                session_id=session_id,
            )
            self._sandboxes[run_id] = sandbox
            run.sandbox_ref = (
                f"sandbox://{sandbox.provider}/{sandbox.sandbox_id}"
            )

        self.store.save_run(run)
        return run

    def request_approval(
        self,
        run_id: str,
        *,
        action: str,
        evidence_refs: list[str] | tuple[str, ...] | None = None,
    ) -> Approval:
        run = self.store.get_run(run_id)
        if run.status != RunStatus.RUNNING:
            raise ValueError("approval can only be requested by a running run")

        approval = Approval(
            approval_id=str(uuid4()),
            action=action,
            evidence_refs=tuple(evidence_refs or ()),
        )
        run.approvals.append(approval)
        run.status = RunStatus.WAITING_APPROVAL
        self.store.save_run(run)
        return approval

    def resolve_approval(
        self,
        run_id: str,
        approval_id: str,
        *,
        approved: bool,
        actor: str,
        reason: str | None = None,
    ) -> Approval:
        run = self.store.get_run(run_id)

        approval = next(
            (
                item
                for item in run.approvals
                if item.approval_id == approval_id
            ),
            None,
        )
        if approval is None:
            raise KeyError(f"unknown approval: {approval_id}")
        if approval.status != ApprovalStatus.PENDING:
            raise ValueError("approval is already resolved")

        approval.status = (
            ApprovalStatus.APPROVED
            if approved
            else ApprovalStatus.DENIED
        )
        approval.actor = actor
        approval.reason = reason

        run.status = (
            RunStatus.RUNNING
            if approved
            else RunStatus.FAILED
        )
        self.store.save_run(run)

        if self.workflow_driver is not None and run.workflow_ref is not None:
            self.workflow_driver.signal(
                run.workflow_ref,
                name="approval_resolved",
                payload={
                    "approval_id": approval_id,
                    "approved": approved,
                    "actor": actor,
                    "reason": reason,
                    "evidence_refs": list(approval.evidence_refs),
                },
            )

        return approval

    def pause_run(self, run_id: str) -> Run:
        run = self.store.get_run(run_id)
        if run.status != RunStatus.RUNNING:
            raise ValueError("only a running run can be paused")

        if self.sandbox_provider is not None and run_id in self._sandboxes:
            sandbox = self.sandbox_provider.snapshot(
                self._sandboxes[run_id]
            )
            run.sandbox_snapshot_ref = sandbox.snapshot_ref

        run.status = RunStatus.PAUSED
        self.store.save_run(run)
        return run

    def resume_run(self, run_id: str) -> Run:
        run = self.store.get_run(run_id)
        if run.status != RunStatus.PAUSED:
            raise ValueError("only a paused run can be resumed")

        if (
            self.sandbox_provider is not None
            and run.sandbox_snapshot_ref is not None
        ):
            sandbox = self.sandbox_provider.resume(
                run.sandbox_snapshot_ref,
                session_id=run.session_id,
            )
            self._sandboxes[run_id] = sandbox
            run.sandbox_ref = (
                f"sandbox://{sandbox.provider}/{sandbox.sandbox_id}"
            )

        run.status = RunStatus.RUNNING
        self.store.save_run(run)
        return run

    def rebind_run_sandbox(
        self,
        run_id: str,
        *,
        snapshot_ref: str | None = None,
    ) -> Run:
        """Replace sandbox execution while preserving canonical Run/Workflow identity."""
        run = self.store.get_run(run_id)
        if run.status != RunStatus.RUNNING:
            raise ValueError(
                "sandbox rebind requires a running run; "
                "use resume_run for paused recovery"
            )
        if self.sandbox_provider is None:
            raise ValueError("sandbox provider is required for rebind")

        previous = self._sandboxes.get(run_id)
        restore_ref = snapshot_ref or run.sandbox_snapshot_ref

        if restore_ref is not None:
            sandbox = self.sandbox_provider.resume(
                restore_ref,
                session_id=run.session_id,
            )
            run.sandbox_snapshot_ref = restore_ref
        else:
            sandbox = self.sandbox_provider.allocate()
            sandbox = self.sandbox_provider.bind(
                sandbox,
                session_id=run.session_id,
            )

        self._sandboxes[run_id] = sandbox
        run.sandbox_ref = (
            f"sandbox://{sandbox.provider}/{sandbox.sandbox_id}"
        )
        self.store.save_run(run)

        if (
            previous is not None
            and previous is not sandbox
            and previous.status != SandboxStatus.TERMINATED
        ):
            self.sandbox_provider.terminate(previous)

        return run

    def pause_session(self, session_id: str) -> Session:
        session = self.store.get_session(session_id)
        session.status = SessionStatus.PAUSED
        self.store.save_session(session)
        return session

    def resume_session(self, session_id: str) -> Session:
        session = self.store.get_session(session_id)
        if session.status == SessionStatus.CLOSED:
            raise ValueError("closed session cannot be resumed")
        session.status = SessionStatus.ACTIVE
        self.store.save_session(session)
        return session

    def complete_run(
        self,
        run_id: str,
        *,
        artifact_refs: list[ArtifactRef | str] | None = None,
        evidence_refs: list[EvidenceRef | str] | None = None,
    ) -> Run:
        run = self.store.get_run(run_id)
        if run.status != RunStatus.RUNNING:
            raise ValueError("only running runs can complete")

        run.status = RunStatus.SUCCEEDED
        run.artifact_refs.extend(artifact_refs or [])
        run.evidence_refs.extend(evidence_refs or [])
        self.store.save_run(run)
        return run
