import asyncio

import pytest

from cloud_agent_runtime import (
    ApprovalStatus,
    AsyncAgentRuntime,
    InMemoryAsyncWorkflowDriver,
    InMemorySandboxProvider,
    RunStatus,
)
from cloud_agent_runtime.store import InMemoryStore


def test_async_runtime_starts_workflow_and_signals_approval() -> None:
    async def scenario():
        driver = InMemoryAsyncWorkflowDriver()
        runtime = AsyncAgentRuntime(
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=driver,
        )

        session = runtime.create_session(
            agent_id="sre-agent",
            release_id="sre-agent-v1",
            tenant_id="tenant-a",
        )
        run = await runtime.start_run(
            session_id=session.session_id,
        )

        assert run.status == RunStatus.RUNNING
        assert run.workflow_ref is not None
        assert run.workflow_ref.provider == "in-memory-async"

        approval = runtime.request_approval(
            run.run_id,
            action="restart workload",
            evidence_refs=[
                "evidence://incident/e-1",
                "evidence://incident/e-2",
            ],
        )
        resolved = await runtime.resolve_approval(
            run.run_id,
            approval.approval_id,
            approved=True,
            actor="operator@example",
        )

        assert resolved.status == ApprovalStatus.APPROVED
        assert resolved.evidence_refs == (
            "evidence://incident/e-1",
            "evidence://incident/e-2",
        )
        assert driver.signals[0][1] == "approval_resolved"
        assert driver.signals[0][2]["evidence_refs"] == [
            "evidence://incident/e-1",
            "evidence://incident/e-2",
        ]

    asyncio.run(scenario())


def test_async_workflow_start_failure_marks_run_failed() -> None:
    class FailingDriver:
        name = "failing"

        async def start_run(self, **kwargs):
            raise RuntimeError("temporal unavailable")

        async def signal(self, *args, **kwargs):
            return None

        async def terminate_run(self, workflow, *, reason):
            return None

    async def scenario():
        runtime = AsyncAgentRuntime(
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=FailingDriver(),
        )
        session = runtime.create_session(
            agent_id="sre-agent",
            release_id="sre-agent-v1",
            tenant_id="tenant-a",
        )

        with pytest.raises(RuntimeError):
            await runtime.start_run(
                session_id=session.session_id,
            )

        runs = list(runtime.store.runs.values())
        assert len(runs) == 1
        assert runs[0].status == RunStatus.FAILED
        assert runtime._sandboxes == {}

    asyncio.run(scenario())



class FailOnRunSaveNumberStore(InMemoryStore):
    def __init__(self, fail_on: int):
        super().__init__()
        self.fail_on = fail_on
        self.save_count = 0

    def save_run(self, run):
        self.save_count += 1
        if self.save_count == self.fail_on:
            raise RuntimeError("simulated run persistence failure")
        return super().save_run(run)


class TerminateFailAsyncDriver(InMemoryAsyncWorkflowDriver):
    async def terminate_run(self, workflow, *, reason):
        raise RuntimeError("simulated async termination failure")


def test_async_binding_persistence_failure_terminates_workflow():
    async def scenario():
        store = FailOnRunSaveNumberStore(fail_on=2)
        driver = InMemoryAsyncWorkflowDriver()
        runtime = AsyncAgentRuntime(
            store=store,
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=driver,
        )
        session = runtime.create_session(
            agent_id="sre-agent",
            release_id="release-v1",
            tenant_id="tenant-a",
        )

        with pytest.raises(
            RuntimeError,
            match="simulated run persistence failure",
        ):
            await runtime.start_run(
                session_id=session.session_id,
            )

        runs = list(store.runs.values())
        assert len(runs) == 1
        assert runs[0].status == RunStatus.FAILED
        assert runs[0].workflow_ref is None
        assert len(driver.terminated) == 1
        assert driver.terminated[0][1] == "runtime binding persistence failed"
        assert runtime._sandboxes == {}

    asyncio.run(scenario())


def test_async_termination_failure_retains_workflow_ref():
    async def scenario():
        store = FailOnRunSaveNumberStore(fail_on=2)
        driver = TerminateFailAsyncDriver()
        runtime = AsyncAgentRuntime(
            store=store,
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=driver,
        )
        session = runtime.create_session(
            agent_id="sre-agent",
            release_id="release-v1",
            tenant_id="tenant-a",
        )

        with pytest.raises(
            RuntimeError,
            match="simulated run persistence failure",
        ):
            await runtime.start_run(
                session_id=session.session_id,
            )

        runs = list(store.runs.values())
        assert len(runs) == 1
        assert runs[0].status == RunStatus.FAILED
        assert runs[0].workflow_ref is not None
        assert runtime._sandboxes == {}

    asyncio.run(scenario())



class FailOnceAsyncApprovalSignalDriver(InMemoryAsyncWorkflowDriver):
    def __init__(self):
        super().__init__()
        self.signal_attempts = 0

    async def signal(self, workflow, *, name: str, payload: dict):
        self.signal_attempts += 1
        if self.signal_attempts == 1:
            raise RuntimeError("simulated async signal failure")
        await super().signal(
            workflow,
            name=name,
            payload=payload,
        )


def test_async_approval_resolution_retry_redelivers_same_signal() -> None:
    async def scenario():
        driver = FailOnceAsyncApprovalSignalDriver()
        runtime = AsyncAgentRuntime(
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=driver,
        )
        session = runtime.create_session(
            agent_id="sre-agent",
            release_id="sre-agent-v1",
            tenant_id="tenant-a",
        )
        run = await runtime.start_run(
            session_id=session.session_id,
        )
        approval = runtime.request_approval(
            run.run_id,
            action="restart workload",
            evidence_refs=["evidence://approval/input"],
        )

        with pytest.raises(
            RuntimeError,
            match="simulated async signal failure",
        ):
            await runtime.resolve_approval(
                run.run_id,
                approval.approval_id,
                approved=True,
                actor="operator@example",
                reason="approved after review",
            )

        persisted = runtime.store.get_run(run.run_id)
        assert persisted.status == RunStatus.RUNNING
        assert (
            persisted.approvals[0].status
            == ApprovalStatus.APPROVED
        )

        retried = await runtime.resolve_approval(
            run.run_id,
            approval.approval_id,
            approved=True,
            actor="operator@example",
            reason="approved after review",
        )

        assert retried.status == ApprovalStatus.APPROVED
        assert driver.signal_attempts == 2
        assert driver.signals[0][2] == {
            "approval_id": approval.approval_id,
            "approved": True,
            "actor": "operator@example",
            "reason": "approved after review",
            "evidence_refs": ["evidence://approval/input"],
        }

    asyncio.run(scenario())


def test_accepted_start_with_lost_ack_reconciles_original_run_without_duplicate() -> None:
    async def scenario():
        from cloud_agent_runtime.async_runtime import RunRecoveryConflict
        from cloud_agent_runtime.temporal_driver import deterministic_start_operation_id

        store = InMemoryStore()
        driver = InMemoryAsyncWorkflowDriver()
        driver.start_outcome_unknown_once = True
        runtime = AsyncAgentRuntime(
            store=store,
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=driver,
        )
        session = runtime.create_session(
            agent_id="sre-agent",
            release_id="release-v1",
            tenant_id="tenant-a",
        )

        with pytest.raises(RuntimeError, match="lost acknowledgement"):
            await runtime.start_run(session_id=session.session_id)

        persisted_runs = list(store.runs.values())
        assert len(persisted_runs) == 1
        uncertain = persisted_runs[0]
        original_run_id = uncertain.run_id
        operation_id = deterministic_start_operation_id(original_run_id)
        assert uncertain.workflow_ref is None
        assert uncertain.recovery_state == "unknown"
        assert uncertain.recovery_operation_id == operation_id
        assert any(event["outcome"] == "unknown" for event in uncertain.recovery_evidence)

        # Reconstruct a new runtime process around the same durable store and
        # provider. Reconciliation must attach, not issue another start.
        restarted = AsyncAgentRuntime(
            store=store,
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=driver,
        )
        recovered = await restarted.reconcile_run(original_run_id)

        assert recovered.run_id == original_run_id
        assert recovered.workflow_ref.workflow_id == f"agent-run-{original_run_id}"
        assert recovered.recovery_state == "attached"
        assert recovered.recovery_operation_id == operation_id
        assert len(driver.started) == 1
        assert len(driver.workflows) == 1
        outcomes = [event["outcome"] for event in recovered.recovery_evidence]
        assert "unknown" in outcomes
        assert "attached" in outcomes

    asyncio.run(scenario())


def test_unknown_start_without_reconciliation_capability_fails_closed() -> None:
    async def scenario():
        from cloud_agent_runtime.async_runtime import RunRecoveryConflict

        class NoReconcileDriver:
            name = "no-reconcile"

            async def start_run(self, *, runtime_run_id, session_id, timeout_seconds=None):
                raise RuntimeError("connection reset after request write")

            async def signal(self, workflow, *, name, payload):
                return None

            async def terminate_run(self, workflow, *, reason, operation_id=None):
                return None

        store = InMemoryStore()
        runtime = AsyncAgentRuntime(
            store=store,
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=NoReconcileDriver(),
        )
        session = runtime.create_session(
            agent_id="sre-agent",
            release_id="release-v1",
            tenant_id="tenant-a",
        )

        with pytest.raises(RuntimeError, match="connection reset"):
            await runtime.start_run(session_id=session.session_id)
        run_id = next(iter(store.runs))
        persisted = store.get_run(run_id)
        assert persisted.recovery_state == "unknown"

        restarted = AsyncAgentRuntime(
            store=store,
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=NoReconcileDriver(),
        )
        with pytest.raises(RunRecoveryConflict, match="no new workflow was created"):
            await restarted.reconcile_run(run_id)
        assert store.get_run(run_id).recovery_state == "unknown"

    asyncio.run(scenario())


def test_uncertain_run_reconciliation_survives_sqlite_store_reconstruction(tmp_path) -> None:
    async def scenario():
        from cloud_agent_runtime.async_runtime import RunRecoveryConflict
        from cloud_agent_runtime import SQLiteStore

        db_path = tmp_path / "runtime.sqlite"
        store_before_restart = SQLiteStore(db_path)
        driver = InMemoryAsyncWorkflowDriver()
        driver.start_outcome_unknown_once = True
        runtime = AsyncAgentRuntime(
            store=store_before_restart,
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=driver,
        )
        session = runtime.create_session(
            agent_id="sre-agent",
            release_id="release-v1",
            tenant_id="tenant-a",
        )

        with pytest.raises(RuntimeError, match="lost acknowledgement"):
            await runtime.start_run(session_id=session.session_id)
        original_run_id = next(iter(store_before_restart.runs))
        store_before_restart.close()

        store_after_restart = SQLiteStore(db_path)
        persisted = store_after_restart.get_run(original_run_id)
        assert persisted.run_id == original_run_id
        assert persisted.workflow_ref is None
        assert persisted.recovery_state == "unknown"
        assert persisted.recovery_operation_id == f"agent-run-start:{original_run_id}"

        restarted_runtime = AsyncAgentRuntime(
            store=store_after_restart,
            sandbox_provider=InMemorySandboxProvider(),
            workflow_driver=driver,
        )
        recovered = await restarted_runtime.reconcile_run(original_run_id)
        assert recovered.run_id == original_run_id
        assert recovered.workflow_ref.workflow_id == f"agent-run-{original_run_id}"
        assert recovered.recovery_state == "attached"
        assert len(driver.started) == 1

        store_after_restart.close()

    asyncio.run(scenario())
