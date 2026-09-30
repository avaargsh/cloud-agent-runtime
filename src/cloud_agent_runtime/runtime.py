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
from .models import (
    Run,
    RunStatus,
    SandboxBinding,
    Session,
    SessionStatus,
)
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

    @staticmethod
    def _sandbox_ref(sandbox: Sandbox) -> str:
        return f"sandbox://{sandbox.provider}/{sandbox.sandbox_id}"

    @staticmethod
    def _parse_sandbox_ref(ref: str) -> tuple[str, str]:
        prefix = "sandbox://"
        if not ref.startswith(prefix):
            raise ValueError(f"unsupported sandbox ref: {ref}")
        provider, separator, sandbox_id = ref[len(prefix):].partition("/")
        if not separator or not provider or not sandbox_id:
            raise ValueError(f"invalid sandbox ref: {ref}")
        return provider, sandbox_id

    def _ensure_binding(self, run: Run) -> SandboxBinding | None:
        if run.sandbox_binding is not None:
            return run.sandbox_binding
        if run.sandbox_ref is None:
            return None

        provider, sandbox_id = self._parse_sandbox_ref(run.sandbox_ref)
        run.sandbox_binding = SandboxBinding(
            provider=provider,
            sandbox_id=sandbox_id,
            sandbox_ref=run.sandbox_ref,
            snapshot_ref=run.sandbox_snapshot_ref,
        )
        return run.sandbox_binding

    def _sandbox_from_binding(
        self,
        binding: SandboxBinding,
        *,
        session_id: str,
        status: SandboxStatus = SandboxStatus.BOUND,
    ) -> Sandbox:
        return Sandbox(
            sandbox_id=binding.sandbox_id,
            provider=binding.provider,
            status=status,
            snapshot_ref=binding.snapshot_ref,
            session_id=session_id,
        )

    def _current_sandbox(self, run: Run) -> Sandbox | None:
        current = self._sandboxes.get(run.run_id)
        if current is not None:
            return current

        binding = self._ensure_binding(run)
        if binding is None:
            return None

        status = (
            SandboxStatus.PAUSED
            if run.status == RunStatus.PAUSED
            else SandboxStatus.BOUND
        )
        current = self._sandbox_from_binding(
            binding,
            session_id=run.session_id,
            status=status,
        )
        self._sandboxes[run.run_id] = current
        return current

    def _best_effort_terminate(
        self,
        sandbox: Sandbox,
    ) -> None:
        if self.sandbox_provider is None:
            return
        try:
            self.sandbox_provider.terminate(sandbox)
        except Exception:
            # Cleanup must never mask the primary allocation/persistence error.
            pass

    def _replace_sandbox(
        self,
        run: Run,
        sandbox: Sandbox,
        *,
        previous: SandboxBinding | None,
        rebind_key: str | None,
    ) -> Run:
        sandbox_ref = self._sandbox_ref(sandbox)
        previous_refs = list(previous.previous_refs) if previous else []
        pending_cleanup_refs = (
            list(previous.pending_cleanup_refs)
            if previous
            else []
        )

        if previous is not None and previous.sandbox_ref != sandbox_ref:
            if previous.sandbox_ref not in previous_refs:
                previous_refs.append(previous.sandbox_ref)
            if previous.sandbox_ref not in pending_cleanup_refs:
                pending_cleanup_refs.append(previous.sandbox_ref)

        binding = SandboxBinding(
            provider=sandbox.provider,
            sandbox_id=sandbox.sandbox_id,
            sandbox_ref=sandbox_ref,
            revision=(previous.revision + 1) if previous else 1,
            snapshot_ref=sandbox.snapshot_ref,
            previous_refs=previous_refs,
            pending_cleanup_refs=pending_cleanup_refs,
            last_rebind_key=rebind_key,
        )

        original_sandbox_ref = run.sandbox_ref
        original_snapshot_ref = run.sandbox_snapshot_ref
        original_binding = run.sandbox_binding

        run.sandbox_ref = sandbox_ref
        run.sandbox_snapshot_ref = sandbox.snapshot_ref
        run.sandbox_binding = binding

        try:
            self.store.save_run(run)
        except Exception:
            # A replacement that was allocated but never durably bound to the
            # canonical Run is an orphan. Roll back the in-memory Run as well,
            # because some RuntimeStore implementations return object refs.
            run.sandbox_ref = original_sandbox_ref
            run.sandbox_snapshot_ref = original_snapshot_ref
            run.sandbox_binding = original_binding
            self._best_effort_terminate(sandbox)
            self._sandboxes.pop(run.run_id, None)
            raise

        self._sandboxes[run.run_id] = sandbox
        return self.cleanup_retired_sandboxes(run.run_id)

    def cleanup_retired_sandboxes(self, run_id: str) -> Run:
        """Retry cleanup for sandboxes retired by a durable replacement."""
        run = self.store.get_run(run_id)
        binding = self._ensure_binding(run)
        if binding is None or not binding.pending_cleanup_refs:
            return run
        if self.sandbox_provider is None:
            return run

        remaining: list[str] = []
        changed = False

        for ref in binding.pending_cleanup_refs:
            try:
                provider, sandbox_id = self._parse_sandbox_ref(ref)
                if provider != self.sandbox_provider.name:
                    remaining.append(ref)
                    continue
                retired = Sandbox(
                    sandbox_id=sandbox_id,
                    provider=provider,
                    status=SandboxStatus.BOUND,
                    session_id=run.session_id,
                )
                self.sandbox_provider.terminate(retired)
                changed = True
            except Exception:
                remaining.append(ref)

        if changed or remaining != binding.pending_cleanup_refs:
            binding.pending_cleanup_refs = remaining
            run.sandbox_binding = binding
            self.store.save_run(run)

        return run

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
        owned_sandbox: Sandbox | None = None
        resolved_sandbox_ref = sandbox_ref
        sandbox_binding: SandboxBinding | None = None

        if sandbox_ref is None and self.sandbox_provider is not None:
            allocated = self.sandbox_provider.allocate()
            try:
                owned_sandbox = self.sandbox_provider.bind(
                    allocated,
                    session_id=session_id,
                )
            except Exception:
                self._best_effort_terminate(allocated)
                raise

            resolved_sandbox_ref = self._sandbox_ref(owned_sandbox)
            sandbox_binding = SandboxBinding(
                provider=owned_sandbox.provider,
                sandbox_id=owned_sandbox.sandbox_id,
                sandbox_ref=resolved_sandbox_ref,
            )

        workflow_ref = None
        try:
            if self.workflow_driver is not None:
                workflow_ref = self.workflow_driver.start_run(
                    runtime_run_id=run_id,
                    session_id=session_id,
                )
        except Exception:
            if owned_sandbox is not None:
                self._best_effort_terminate(owned_sandbox)
            raise

        run = Run(
            run_id=run_id,
            session_id=session_id,
            status=RunStatus.RUNNING,
            sandbox_ref=resolved_sandbox_ref,
            sandbox_binding=sandbox_binding,
            workflow_ref=workflow_ref,
            budget=budget,
        )

        if owned_sandbox is not None:
            self._sandboxes[run_id] = owned_sandbox

        try:
            self.store.save_run(run)
        except Exception:
            if owned_sandbox is not None:
                self._best_effort_terminate(owned_sandbox)
                self._sandboxes.pop(run_id, None)
            raise
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

        if self.sandbox_provider is not None:
            current = self._current_sandbox(run)
            if current is not None:
                sandbox = self.sandbox_provider.snapshot(current)
                self._sandboxes[run_id] = sandbox
                run.sandbox_snapshot_ref = sandbox.snapshot_ref
                binding = self._ensure_binding(run)
                if binding is not None:
                    binding.snapshot_ref = sandbox.snapshot_ref
                    run.sandbox_binding = binding

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
            previous = self._ensure_binding(run)
            sandbox = self.sandbox_provider.resume(
                run.sandbox_snapshot_ref,
                session_id=run.session_id,
            )
            previous_status = run.status
            run.status = RunStatus.RUNNING
            try:
                return self._replace_sandbox(
                    run,
                    sandbox,
                    previous=previous,
                    rebind_key=f"resume:{run.sandbox_snapshot_ref}",
                )
            except Exception:
                run.status = previous_status
                raise

        run.status = RunStatus.RUNNING
        self.store.save_run(run)
        return run

    def rebind_run_sandbox(
        self,
        run_id: str,
        *,
        snapshot_ref: str | None = None,
        rebind_key: str | None = None,
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

        previous = self._ensure_binding(run)
        restore_ref = snapshot_ref or run.sandbox_snapshot_ref
        effective_key = rebind_key
        if effective_key is None and restore_ref is not None:
            effective_key = f"snapshot:{restore_ref}"

        if (
            effective_key is not None
            and previous is not None
            and previous.last_rebind_key == effective_key
        ):
            return run

        if restore_ref is not None:
            sandbox = self.sandbox_provider.resume(
                restore_ref,
                session_id=run.session_id,
            )
        else:
            sandbox = self.sandbox_provider.allocate()
            sandbox = self.sandbox_provider.bind(
                sandbox,
                session_id=run.session_id,
            )

        return self._replace_sandbox(
            run,
            sandbox,
            previous=previous,
            rebind_key=effective_key,
        )

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
