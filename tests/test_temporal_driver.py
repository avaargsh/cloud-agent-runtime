import asyncio

from cloud_agent_runtime.temporal_driver import TemporalWorkflowDriver


class FakeHandle:
    first_execution_run_id = "temporal-run-1"

    def __init__(self):
        self.signals = []

    async def signal(self, name, payload):
        self.signals.append((name, payload))


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
    ):
        self.started.append(
            {
                "workflow_type": workflow_type,
                "args": args,
                "id": id,
                "task_queue": task_queue,
            }
        )
        return self.handle

    def get_workflow_handle(self, workflow_id, **kwargs):
        return self.handle


def test_temporal_driver_uses_canonical_runtime_run_id() -> None:
    async def scenario():
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

    asyncio.run(scenario())


def test_temporal_driver_attaches_when_workflow_already_started(monkeypatch) -> None:
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
