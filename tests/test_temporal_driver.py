import asyncio

import pytest

from cloud_agent_runtime.temporal_driver import (
    TemporalWorkflowConflict,
    TemporalWorkflowDriver,
)


class FakeHandle:
    first_execution_run_id = "temporal-run-1"

    def __init__(self):
        self.signals = []
        self.terminations = []
        self.status = "RUNNING"

    async def signal(self, name, payload):
        self.signals.append((name, payload))

    async def terminate(self, *, reason):
        self.terminations.append(reason)

    async def describe(self):
        return {"status": self.status}


class FakeClient:
    def __init__(self):
        self.started = []
        self.handle = FakeHandle()

    async def start_workflow(
        self,
        workflow_type,
        *,
        args,
        id,
        task_queue,
        id_reuse_policy,
    ):
        self.started.append(
            {
                "workflow_type": workflow_type,
                "args": args,
                "id": id,
                "task_queue": task_queue,
                "id_reuse_policy": id_reuse_policy,
            }
        )
        return self.handle

    def get_workflow_handle(self, workflow_id, **kwargs):
        return self.handle


def test_temporal_driver_uses_canonical_runtime_run_id() -> None:
    async def scenario():
        from temporalio.common import WorkflowIDReusePolicy

        client = FakeClient()
        driver = TemporalWorkflowDriver(
            client=client,
            task_queue="agent-runs",
        )

        ref = await driver.start_run(
            runtime_run_id="run-123",
            session_id="session-456",
        )

        assert ref.provider == "temporal"
        assert ref.workflow_id == "agent-run-run-123"
        assert ref.run_id == "temporal-run-1"
        assert client.started[0]["task_queue"] == "agent-runs"
        assert client.started[0]["args"][0]["session_id"] == "session-456"
        assert client.started[0]["id_reuse_policy"] is WorkflowIDReusePolicy.REJECT_DUPLICATE

        await driver.signal(
            ref,
            name="approval_resolved",
            payload={"approved": True},
        )
        assert client.handle.signals == [
            (
                "approval_resolved",
                {"approved": True},
            )
        ]

        await driver.terminate_run(
            ref,
            reason="binding persistence failed",
        )
        assert client.handle.terminations == [
            "binding persistence failed"
        ]

    asyncio.run(scenario())


def test_temporal_driver_attaches_when_workflow_id_already_exists() -> None:
    async def scenario():
        from temporalio.exceptions import WorkflowAlreadyStartedError

        client = FakeClient()

        async def already_started(*args, **kwargs):
            raise WorkflowAlreadyStartedError("agent-run-run-123", "temporal-run-existing")

        client.start_workflow = already_started
        driver = TemporalWorkflowDriver(client=client, task_queue="agent-runs")
        ref = await driver.start_run(runtime_run_id="run-123", session_id="session-456")

        assert ref.workflow_id == "agent-run-run-123"
        assert ref.run_id == "temporal-run-1"

    asyncio.run(scenario())



def test_temporal_driver_rejects_terminal_existing_workflow() -> None:
    async def scenario():
        from temporalio.exceptions import WorkflowAlreadyStartedError

        client = FakeClient()
        client.handle.status = "COMPLETED"

        async def already_started(*args, **kwargs):
            raise WorkflowAlreadyStartedError(
                "agent-run-run-123",
                "temporal-run-existing",
            )

        client.start_workflow = already_started
        driver = TemporalWorkflowDriver(
            client=client,
            task_queue="agent-runs",
        )

        with pytest.raises(
            TemporalWorkflowConflict,
            match="existing Temporal workflow is terminal",
        ):
            await driver.start_run(
                runtime_run_id="run-123",
                session_id="session-456",
            )

    asyncio.run(scenario())


def test_temporal_driver_rejects_duplicate_when_status_cannot_be_verified() -> None:
    async def scenario():
        from temporalio.exceptions import WorkflowAlreadyStartedError

        client = FakeClient()
        client.handle.describe = None

        async def already_started(*args, **kwargs):
            raise WorkflowAlreadyStartedError(
                "agent-run-run-123",
                "temporal-run-existing",
            )

        client.start_workflow = already_started
        driver = TemporalWorkflowDriver(
            client=client,
            task_queue="agent-runs",
        )

        with pytest.raises(
            TemporalWorkflowConflict,
            match="cannot verify existing Temporal workflow status",
        ):
            await driver.start_run(
                runtime_run_id="run-123",
                session_id="session-456",
            )

    asyncio.run(scenario())
