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

        async def cancel_run(self, workflow):
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


class CancelFailAsyncDriver(InMemoryAsyncWorkflowDriver):
    async def cancel_run(self, workflow):
        raise RuntimeError("simulated async cancel failure")


def test_async_binding_persistence_failure_cancels_workflow():
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
        assert len(driver.cancelled) == 1
        assert runtime._sandboxes == {}

    asyncio.run(scenario())


def test_async_cancel_failure_retains_workflow_ref():
    async def scenario():
        store = FailOnRunSaveNumberStore(fail_on=2)
        driver = CancelFailAsyncDriver()
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
