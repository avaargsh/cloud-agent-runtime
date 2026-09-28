import asyncio

import pytest

from cloud_agent_runtime import (
    ApprovalStatus,
    AsyncAgentRuntime,
    InMemoryAsyncWorkflowDriver,
    InMemorySandboxProvider,
    RunStatus,
)


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

    asyncio.run(scenario())
