import asyncio

import pytest

from cloud_agent_runtime import (
    ApprovalStatus,
    AsyncAgentRuntime,
    InMemoryAsyncWorkflowDriver,
    InMemorySandboxProvider,
    RunStatus,
)
from cloud_agent_runtime.store import InMemoryStore, SQLiteStore
from cloud_agent_runtime.temporal_driver import TemporalWorkflowDriver


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


def test_async_workflow_start_failure_preserves_recoverable_pending_run() -> None:
    from cloud_agent_runtime.workflow import WorkflowRef

    class FailingDriver:
        name = "failing"

        def reference_for_run(self, runtime_run_id: str) -> WorkflowRef:
            return WorkflowRef(
                provider=self.name,
                workflow_id=f"workflow-{runtime_run_id}",
            )

        async def start_run(self, **kwargs):
            raise RuntimeError("temporal outcome unavailable")

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

        with pytest.raises(RuntimeError, match="temporal outcome unavailable"):
            await runtime.start_run(
                session_id=session.session_id,
                run_id="run-pending-1",
            )

        pending = runtime.store.get_run("run-pending-1")
        assert pending.status == RunStatus.CREATED
        assert pending.workflow_ref is not None
        assert pending.workflow_ref.workflow_id == "workflow-run-pending-1"
        assert pending.sandbox_ref is not None
        # A start error is an ambiguous provider outcome, not proof the
        # Workflow was not accepted. Keep the sandbox and canonical Run.
        assert len(runtime._sandboxes) == 1

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
        assert runs[0].status == RunStatus.CREATED
        assert runs[0].workflow_ref is not None
        assert runs[0].workflow_ref.workflow_id == f"workflow-{runs[0].run_id}"
        assert driver.terminated == []
        assert len(runtime._sandboxes) == 1

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
        assert runs[0].status == RunStatus.CREATED
        assert runs[0].workflow_ref is not None
        assert runs[0].workflow_ref.workflow_id == f"workflow-{runs[0].run_id}"
        assert driver.terminated == []
        assert len(runtime._sandboxes) == 1

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


def test_run_start_recovers_after_lost_temporal_ack_using_original_ids(tmp_path) -> None:
    from temporalio.exceptions import WorkflowAlreadyStartedError

    class FakeTemporalHandle:
        first_execution_run_id = "temporal-run-1"

        async def describe(self):
            return {"status": "RUNNING"}

    class AcceptThenLoseAckClient:
        """External Temporal state survives while the caller/store is rebuilt."""

        def __init__(self):
            self.workflows = {}
            self.start_requests = []
            self.lose_first_ack = True

        async def start_workflow(
            self,
            workflow_type,
            *,
            args,
            id,
            task_queue,
            id_reuse_policy,
        ):
            self.start_requests.append({
                "workflow_type": workflow_type,
                "args": args,
                "id": id,
                "task_queue": task_queue,
                "id_reuse_policy": id_reuse_policy,
            })
            if id in self.workflows:
                raise WorkflowAlreadyStartedError(id, "temporal-run-1")
            self.workflows[id] = FakeTemporalHandle()
            if self.lose_first_ack:
                self.lose_first_ack = False
                raise TimeoutError("Temporal accepted start but client lost ACK")
            return self.workflows[id]

        def get_workflow_handle(self, workflow_id, **kwargs):
            return self.workflows[workflow_id]

    async def scenario():
        database = tmp_path / "runtime.sqlite"
        client = AcceptThenLoseAckClient()
        provider = InMemorySandboxProvider()
        driver = TemporalWorkflowDriver(client=client, task_queue="agent-runs")

        first_store = SQLiteStore(database)
        first_runtime = AsyncAgentRuntime(
            store=first_store,
            sandbox_provider=provider,
            workflow_driver=driver,
        )
        session = first_runtime.create_session(
            agent_id="sre-agent",
            release_id="sre-agent-v1",
            tenant_id="tenant-a",
        )

        with pytest.raises(TimeoutError, match="client lost ACK"):
            await first_runtime.start_run(
                session_id=session.session_id,
                run_id="run-stable-123",
            )

        pending = first_store.get_run("run-stable-123")
        assert pending.status == RunStatus.CREATED
        assert pending.workflow_ref is not None
        assert pending.workflow_ref.workflow_id == "agent-run-run-stable-123"
        assert pending.workflow_ref.run_id is None
        assert pending.sandbox_ref is not None
        original_sandbox_ref = pending.sandbox_ref
        first_store.close()

        # Reconstruct the durable store and runtime as if the caller process
        # died. The fake Temporal service remains alive with its accepted
        # workflow, so replay must attach rather than create a second one.
        second_store = SQLiteStore(database)
        second_runtime = AsyncAgentRuntime(
            store=second_store,
            sandbox_provider=provider,
            workflow_driver=driver,
        )
        recovered = await second_runtime.start_run(
            session_id=session.session_id,
            run_id="run-stable-123",
        )

        assert recovered.run_id == "run-stable-123"
        assert recovered.session_id == session.session_id
        assert recovered.status == RunStatus.RUNNING
        assert recovered.workflow_ref is not None
        assert recovered.workflow_ref.workflow_id == "agent-run-run-stable-123"
        assert recovered.workflow_ref.run_id == "temporal-run-1"
        assert recovered.sandbox_ref == original_sandbox_ref
        assert list(client.workflows) == ["agent-run-run-stable-123"]
        assert [request["id"] for request in client.start_requests] == [
            "agent-run-run-stable-123",
            "agent-run-run-stable-123",
        ]
        assert all(
            request["args"] == [{
                "runtime_run_id": "run-stable-123",
                "session_id": session.session_id,
            }]
            for request in client.start_requests
        )
        second_store.close()

    asyncio.run(scenario())
