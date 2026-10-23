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


def test_temporal_run_timeout_is_durable_and_exposes_transition_evidence() -> None:
    async def scenario() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-run-timeout-{uuid4()}"
            runtime_run_id = f"run-{uuid4()}"
            session_id = f"session-{uuid4()}"
            async with Worker(env.client, task_queue=task_queue, workflows=[AgentRunWorkflow]):
                handle = await env.client.start_workflow(
                    AgentRunWorkflow.run,
                    {
                        "runtime_run_id": runtime_run_id,
                        "session_id": session_id,
                        "timeout_seconds": 5,
                    },
                    id=f"agent-run-{runtime_run_id}",
                    task_queue=task_queue,
                )
                result = await handle.result()

            assert result["runtime_run_id"] == runtime_run_id
            assert result["session_id"] == session_id
            assert result["phase"] == "failed"
            assert result["terminal"] is True
            assert result["error"] == "run wall-clock budget exceeded"
            assert result["transition_events"] == [
                {
                    "event_id": f"run-started:{runtime_run_id}",
                    "event_type": "run_started",
                    "phase": "running",
                    "evidence_refs": [],
                },
                {
                    "event_id": f"run-timeout:{runtime_run_id}",
                    "event_type": "run_timed_out",
                    "phase": "failed",
                    "evidence_refs": [],
                },
            ]

    asyncio.run(scenario())
