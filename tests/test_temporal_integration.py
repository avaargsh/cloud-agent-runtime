import asyncio
from uuid import uuid4

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cloud_agent_runtime.temporal_workflow import (
    AgentRunWorkflow,
)


def test_temporal_worker_signal_round_trip() -> None:
    async def scenario() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-agent-runs-{uuid4()}"

            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                handle = await env.client.start_workflow(
                    AgentRunWorkflow.run,
                    {
                        "runtime_run_id": "run-123",
                        "session_id": "session-456",
                    },
                    id=f"agent-run-{uuid4()}",
                    task_queue=task_queue,
                )

                await handle.signal(
                    AgentRunWorkflow.approval_resolved,
                    {
                        "approval_id": "approval-1",
                        "approved": True,
                        "actor": "operator@example",
                        "evidence_refs": [
                            "evidence://aiops/e-1",
                        ],
                    },
                )

                state = await handle.query(
                    AgentRunWorkflow.run_state
                )

                assert state["phase"] == "running"
                assert state["evidence_refs"] == [
                    "evidence://aiops/e-1",
                ]

                await handle.signal(
                    AgentRunWorkflow.complete_run,
                    {
                        "result": {
                            "summary": "verified",
                        },
                        "evidence_refs": [
                            "evidence://aiops/e-post",
                        ],
                    },
                )

                result = await handle.result()

                assert result["phase"] == "succeeded"
                assert result["runtime_run_id"] == "run-123"
                assert result["evidence_refs"] == [
                    "evidence://aiops/e-1",
                    "evidence://aiops/e-post",
                ]

    asyncio.run(scenario())
