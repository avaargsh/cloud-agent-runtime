from __future__ import annotations

import json
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
        self._cleanup_ack_unknown_once = False

    @staticmethod
    def _transition_run(
        run: Run,
        *,
        to_status: RunStatus,
        event_type: str,
        event_id: str | None = None,
        evidence_refs: list[str] | None = None,
    ) -> None:
        """Append a durable, Run-scoped audit record before changing status."""
        if run.status == to_status:
            return
        sequence = len(run.transition_events) + 1
        run.transition_events.append(
            {
                "transition_id": f"{run.run_id}:transition:{sequence}",
                "run_id": run.run_id,
                "session_id": run.session_id,
                "sequence": sequence,
                "event_type": event_type,
                "event_id": event_id,
                "from_status": run.status.value,
                "to_status": to_status.value,
                "evidence_refs": list(evidence_refs or []),
            }
        )
        run.status = to_status

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

    def _best_effort_terminate_workflow(
        self,
        workflow,
    ) -> bool:
        if self.workflow_driver is None:
            return False
        terminate = getattr(
            self.workflow_driver,
            "terminate_run",
            None,
        )
        if terminate is None:
            return False
        try:
            terminate(
                workflow,
                reason="runtime binding persistence failed",
            )
            return True
        except Exception:
            return False

    def _best_effort_save_failed_run(
        self,
        run: Run,
    ) -> None:
        try:
            self.store.save_run(run)
        except Exception:
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
            raise

        self._sandboxes[run.run_id] = sandbox
        return self.cleanup_retired_sandboxes(run.run_id)

    def cleanup_retired_sandboxes(self, run_id: str) -> Run:
        """Retry retired-sandbox cleanup until an independent provider query says Gone."""
        from .temporal_driver import deterministic_cleanup_operation_id

        run = self.store.get_run(run_id)
        binding = self._ensure_binding(run)
        if binding is None or not binding.pending_cleanup_refs:
            return run
        if self.sandbox_provider is None:
            return run

        original_refs = list(binding.pending_cleanup_refs)
        remaining: list[str] = []
        changed = False
        for ref in original_refs:
            operation_id = deterministic_cleanup_operation_id(run_id, ref)
            # Count logical mutation attempts, not individual audit
            # events. Each attempt can emit intent + unknown/observed/ack events,
            # all of which must keep the same attempt number.
            attempt = sum(
                1
                for event in run.recovery_evidence
                if event.get("operation_id") == operation_id
                and event.get("outcome") in {"intent_persisted", "blocked"}
            ) + 1
            try:
                provider, sandbox_id = self._parse_sandbox_ref(ref)
                if provider != self.sandbox_provider.name:
                    remaining.append(ref)
                    run.recovery_operation_id = operation_id
                    run.recovery_state = "unknown"
                    run.recovery_attempts = max(run.recovery_attempts, attempt)
                    run.recovery_evidence.append({
                        "operation_id": operation_id,
                        "outcome": "blocked",
                        "attempt": str(attempt),
                        "detail": (
                            f"cleanup provider mismatch: expected "
                            f"{self.sandbox_provider.name}, got {provider}; "
                            f"owner_run={run_id}; sandbox_ref={ref}"
                        ),
                    })
                    changed = True
                    continue

                retired = Sandbox(
                    sandbox_id=sandbox_id,
                    provider=provider,
                    status=SandboxStatus.BOUND,
                    session_id=run.session_id,
                )
                # Persist intent before every remote mutation. A timeout or
                # lost response leaves the same reference pending and retryable.
                run.recovery_operation_id = operation_id
                run.recovery_state = "cleanup_pending"
                run.recovery_attempts = max(run.recovery_attempts, attempt)
                run.recovery_evidence.append({
                    "operation_id": operation_id,
                    "outcome": "intent_persisted",
                    "attempt": str(attempt),
                    "detail": f"owner_run={run_id}; sandbox_ref={ref}",
                })
                self.store.save_run(run)

                try:
                    self.sandbox_provider.terminate(retired)
                    if self._cleanup_ack_unknown_once:
                        self._cleanup_ack_unknown_once = False
                        raise RuntimeError(
                            "injected lost cleanup acknowledgement after provider side effect"
                        )

                    # A successful delete response is only a request receipt.
                    # Require a separate Provider read proving the resource is Gone.
                    observe_gone = getattr(self.sandbox_provider, "is_gone", None)
                    if not callable(observe_gone):
                        raise RuntimeError(
                            "sandbox provider cannot independently confirm resource Gone"
                        )
                    if observe_gone(sandbox_id) is not True:
                        raise RuntimeError(
                            "provider has not independently confirmed resource Gone"
                        )
                    run.recovery_evidence.append({
                        "operation_id": operation_id,
                        "outcome": "observed_gone",
                        "attempt": str(attempt),
                        "detail": f"independent provider observation confirms Gone: {ref}",
                    })
                except Exception as exc:
                    remaining.append(ref)
                    run.recovery_state = "unknown"
                    run.recovery_evidence.append({
                        "operation_id": operation_id,
                        "outcome": "unknown",
                        "attempt": str(attempt),
                        "detail": (
                            f"cleanup unresolved after terminate/observation: "
                            f"{type(exc).__name__}: {exc}; ownership retained for retry"
                        ),
                    })
                    changed = True
                    continue

                changed = True
                run.recovery_state = "cleanup_acknowledged"
                if run.status == RunStatus.CANCELLED and ref == binding.sandbox_ref:
                    run.metadata = {
                        **dict(run.metadata),
                        "cancel_cleanup_completed_ref": ref,
                    }
                run.recovery_evidence.append({
                    "operation_id": operation_id,
                    "outcome": "acknowledged",
                    "attempt": str(attempt),
                    "detail": (
                        f"terminate acknowledged and independent Gone observed; "
                        f"owner_run={run_id}; sandbox_ref={ref}"
                    ),
                })

            except Exception as exc:
                # Malformed refs and provider lookup errors are not proof of
                # deletion. Preserve ownership and retry with evidence.
                remaining.append(ref)
                run.recovery_operation_id = operation_id
                run.recovery_state = "unknown"
                run.recovery_evidence.append({
                    "operation_id": operation_id,
                    "outcome": "blocked",
                    "attempt": str(attempt),
                    "detail": f"cleanup could not be verified: {type(exc).__name__}: {exc}",
                })
                changed = True

        if changed or remaining != original_refs:
            binding.pending_cleanup_refs = remaining
            run.sandbox_binding = binding
            # Run-level summary must not say cleanup completed while any
            # resource still lacks authoritative Gone evidence.
            if remaining:
                run.recovery_state = "unknown"
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
        run_id: str | None = None,
    ) -> Run:
        session = self.store.get_session(session_id)
        if session.status != SessionStatus.ACTIVE:
            raise ValueError("runs can only start on an active session")

        if run_id is None:
            run_id = str(uuid4())
        elif not run_id.strip():
            raise ValueError("run_id must not be empty")
        else:
            try:
                self.store.get_run(run_id)
            except KeyError:
                pass
            else:
                raise ValueError(f"run already exists: {run_id}")

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

        run = Run(
            run_id=run_id,
            session_id=session_id,
            status=RunStatus.CREATED,
            sandbox_ref=resolved_sandbox_ref,
            sandbox_binding=sandbox_binding,
            workflow_ref=None,
            budget=budget,
        )

        self._transition_run(
            run,
            to_status=RunStatus.RUNNING,
            event_type="run_started",
        )

        if owned_sandbox is not None:
            self._sandboxes[run_id] = owned_sandbox

        # Canonical Run identity must be durable before any external workflow
        # side effect is created.
        try:
            self.store.save_run(run)
        except Exception:
            if owned_sandbox is not None:
                self._best_effort_terminate(owned_sandbox)
                self._sandboxes.pop(run_id, None)
            raise

        if self.workflow_driver is None:
            return run

        try:
            workflow_ref = self.workflow_driver.start_run(
                runtime_run_id=run_id,
                session_id=session_id,
            )
        except Exception:
            self._transition_run(
                run,
                to_status=RunStatus.FAILED,
                event_type="workflow_start_failed",
            )
            self._best_effort_save_failed_run(run)
            if owned_sandbox is not None:
                self._best_effort_terminate(owned_sandbox)
                self._sandboxes.pop(run_id, None)
            raise

        run.workflow_ref = workflow_ref
        try:
            self.store.save_run(run)
        except Exception:
            terminated = self._best_effort_terminate_workflow(
                workflow_ref
            )
            self._transition_run(
                run,
                to_status=RunStatus.FAILED,
                event_type="workflow_binding_persistence_failed",
            )
            if terminated:
                run.workflow_ref = None
            self._best_effort_save_failed_run(run)
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
        self._transition_run(
            run,
            to_status=RunStatus.WAITING_APPROVAL,
            event_type="approval_requested",
            event_id=approval.approval_id,
            evidence_refs=list(approval.evidence_refs),
        )
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

        target_status = (
            ApprovalStatus.APPROVED
            if approved
            else ApprovalStatus.DENIED
        )

        if approval.status == ApprovalStatus.PENDING:
            approval.status = target_status
            approval.actor = actor
            approval.reason = reason
            self._transition_run(
                run,
                to_status=(
                    RunStatus.RUNNING
                    if approved
                    else RunStatus.FAILED
                ),
                event_type="approval_approved" if approved else "approval_denied",
                event_id=approval_id,
                evidence_refs=list(approval.evidence_refs),
            )
            # Approval resolution is durable before the external signal.
            self.store.save_run(run)
        else:
            # If the durable state committed but workflow signaling failed, an
            # identical retry must be able to resend the signal.
            if approval.status != target_status:
                raise ValueError(
                    "approval is already resolved with a different outcome"
                )
            if approval.actor != actor or approval.reason != reason:
                raise ValueError(
                    "approval retry metadata differs from durable resolution"
                )

        if self.workflow_driver is not None and run.workflow_ref is not None:
            self.workflow_driver.signal(
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

    def pause_run(self, run_id: str) -> Run:
        run = self.store.get_run(run_id)
        if run.status != RunStatus.RUNNING:
            raise ValueError("only a running run can be paused")

        cached_before: Sandbox | None = None
        cached_status: SandboxStatus | None = None
        cached_snapshot_ref: str | None = None
        original_snapshot_ref = run.sandbox_snapshot_ref
        original_binding_snapshot_ref = (
            run.sandbox_binding.snapshot_ref
            if run.sandbox_binding is not None
            else None
        )

        if self.sandbox_provider is not None:
            current = self._current_sandbox(run)
            if current is not None:
                cached_before = current
                cached_status = current.status
                cached_snapshot_ref = current.snapshot_ref
                sandbox = self.sandbox_provider.snapshot(current)
                self._sandboxes[run_id] = sandbox
                run.sandbox_snapshot_ref = sandbox.snapshot_ref
                binding = self._ensure_binding(run)
                if binding is not None:
                    binding.snapshot_ref = sandbox.snapshot_ref
                    run.sandbox_binding = binding

        original_transition_events = list(run.transition_events)
        self._transition_run(
            run,
            to_status=RunStatus.PAUSED,
            event_type="run_paused",
        )
        try:
            self.store.save_run(run)
        except Exception:
            run.status = RunStatus.RUNNING
            run.transition_events = original_transition_events
            run.sandbox_snapshot_ref = original_snapshot_ref
            if run.sandbox_binding is not None:
                run.sandbox_binding.snapshot_ref = (
                    original_binding_snapshot_ref
                )
            if cached_before is not None:
                cached_before.status = cached_status or SandboxStatus.BOUND
                cached_before.snapshot_ref = cached_snapshot_ref
                self._sandboxes[run_id] = cached_before
            raise
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
            original_transition_events = list(run.transition_events)
            self._transition_run(
                run,
                to_status=RunStatus.RUNNING,
                event_type="run_resumed",
            )
            try:
                return self._replace_sandbox(
                    run,
                    sandbox,
                    previous=previous,
                    rebind_key=f"resume:{run.sandbox_snapshot_ref}",
                )
            except Exception:
                run.status = previous_status
                run.transition_events = original_transition_events
                raise

        self._transition_run(
            run,
            to_status=RunStatus.RUNNING,
            event_type="run_resumed",
        )
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
            if previous.pending_cleanup_refs:
                return self.cleanup_retired_sandboxes(run.run_id)
            return run

        if restore_ref is not None:
            sandbox = self.sandbox_provider.resume(
                restore_ref,
                session_id=run.session_id,
            )
        else:
            allocated = self.sandbox_provider.allocate()
            try:
                sandbox = self.sandbox_provider.bind(
                    allocated,
                    session_id=run.session_id,
                )
            except Exception:
                self._best_effort_terminate(allocated)
                raise

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

    def _queue_cancel_sandbox_cleanup(self, run: Run) -> bool:
        """Persist cleanup intent for the sandbox bound to a cancelled Run."""
        try:
            binding = self._ensure_binding(run)
        except ValueError:
            # Explicit external sandbox refs are not managed by this provider
            # contract; they must not prevent the canonical Run from cancelling.
            return False
        if binding is None or not binding.sandbox_ref:
            return False
        if (
            run.metadata.get("cancel_cleanup_completed_ref") == binding.sandbox_ref
        ):
            return False
        if binding.sandbox_ref in binding.pending_cleanup_refs:
            return False
        binding.pending_cleanup_refs.append(binding.sandbox_ref)
        run.sandbox_binding = binding
        return True

    def _finish_cancel_sandbox_cleanup(self, run_id: str) -> Run:
        """Retry durable sandbox cleanup and drop terminal Run cache on success."""
        candidate = self.store.get_run(run_id)
        try:
            self._ensure_binding(candidate)
        except ValueError:
            # No supported provider binding exists for this external reference.
            self._sandboxes.pop(run_id, None)
            return candidate
        run = self.cleanup_retired_sandboxes(run_id)
        binding = run.sandbox_binding
        if run.status == RunStatus.CANCELLED and (
            binding is None or not binding.pending_cleanup_refs
        ):
            self._sandboxes.pop(run_id, None)
        return run

    def _signal_cancelled_workflow(self, run: Run) -> None:
        """Signal durable cancellation, attempting resource cleanup on failure."""
        if self.workflow_driver is None or run.workflow_ref is None:
            return
        payload = {
            "event_id": run.metadata.get("cancel_event_id") or None,
            "reason": run.metadata["terminal_reason"],
            "evidence_refs": json.loads(run.metadata["cancel_evidence_refs"]),
        }
        try:
            self.workflow_driver.signal(
                run.workflow_ref,
                name="cancel_run",
                payload=payload,
            )
        except Exception:
            # Cancellation is already durable. Try to stop resource consumption
            # even if the signal channel is unavailable; cleanup errors must not
            # replace the original workflow signal error.
            try:
                self._finish_cancel_sandbox_cleanup(run.run_id)
            except Exception:
                pass
            raise

    def cancel_run(
        self,
        run_id: str,
        *,
        reason: str = "cancelled by caller",
        event_id: str | None = None,
        evidence_refs: list[EvidenceRef | str] | None = None,
    ) -> Run:
        """Persist cancellation intent, signal the workflow, then retryable cleanup.

        The canonical Run and its cleanup intent are durable before the workflow
        signal. Retries must match the original request and reuse its identity.
        """
        run = self.store.get_run(run_id)
        refs = [str(ref) for ref in (evidence_refs or [])]
        cancellation_metadata = {
            "terminal_reason": reason,
            "cancel_event_id": event_id or "",
            "cancel_evidence_refs": json.dumps(
                refs, separators=(",", ":"), ensure_ascii=False
            ),
        }

        if run.status == RunStatus.CANCELLED:
            # The first durable cancellation request is authoritative. A retry
            # may redeliver it, but must not silently rewrite its intent.
            for key, value in cancellation_metadata.items():
                if run.metadata.get(key) != value:
                    raise ValueError(
                        "cancellation retry metadata differs from durable request"
                    )
            # Also backfill cleanup intent for cancelled Runs persisted before
            # cleanup was made part of the cancellation contract.
            if self._queue_cancel_sandbox_cleanup(run):
                self.store.save_run(run)
            self._signal_cancelled_workflow(run)
            return self._finish_cancel_sandbox_cleanup(run_id)

        if run.status in (RunStatus.SUCCEEDED, RunStatus.FAILED):
            raise ValueError(f"terminal run cannot be cancelled: {run.status.value}")

        self._transition_run(
            run,
            to_status=RunStatus.CANCELLED,
            event_type="run_cancelled",
            event_id=event_id,
            evidence_refs=refs,
        )
        run.evidence_refs.extend(
            ref for ref in (evidence_refs or []) if ref not in run.evidence_refs
        )
        run.metadata = {**dict(run.metadata), **cancellation_metadata}
        self._queue_cancel_sandbox_cleanup(run)
        self.store.save_run(run)

        self._signal_cancelled_workflow(run)
        return self._finish_cancel_sandbox_cleanup(run_id)

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

        self._transition_run(
            run,
            to_status=RunStatus.SUCCEEDED,
            event_type="run_succeeded",
            evidence_refs=[str(ref) for ref in (evidence_refs or [])],
        )
        run.artifact_refs.extend(artifact_refs or [])
        run.evidence_refs.extend(evidence_refs or [])
        self.store.save_run(run)
        return run
