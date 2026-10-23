import asyncio
from uuid import uuid4

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from cloud_agent_runtime.temporal_workflow import AgentRunWorkflow


def test_temporal_history_replays_canonical_run_and_deduplicates_approval() -> None:
    async def scenario() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-agent-replay-{uuid4()}"
            runtime_run_id = f"run-{uuid4()}"
            session_id = f"session-{uuid4()}"
            workflow_id = f"agent-run-{runtime_run_id}"
            approval_payload = {
                "approval_id": "approval-replay-1",
                "approved": True,
                "actor": "operator@example",
                "evidence_refs": ["evidence://recovery/before-replay"],
            }

            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                handle = await env.client.start_workflow(
                    AgentRunWorkflow.run,
                    {
                        "runtime_run_id": runtime_run_id,
                        "session_id": session_id,
                    },
                    id=workflow_id,
                    task_queue=task_queue,
                )
                initial = await handle.query(AgentRunWorkflow.run_state)
                assert initial["runtime_run_id"] == runtime_run_id
                assert initial["session_id"] == session_id

                await handle.signal(
                    AgentRunWorkflow.approval_resolved,
                    approval_payload,
                )
                await handle.signal(
                    AgentRunWorkflow.approval_resolved,
                    approval_payload,
                )
                await handle.signal(
                    AgentRunWorkflow.complete_run,
                    {
                        "result": {"summary": "replayed"},
                        "evidence_refs": ["evidence://recovery/after-replay"],
                    },
                )
                result = await handle.result()
                history = await handle.fetch_history()

            assert result["runtime_run_id"] == runtime_run_id
            assert result["session_id"] == session_id
            assert result["phase"] == "succeeded"
            assert result["evidence_refs"] == [
                "evidence://recovery/before-replay",
                "evidence://recovery/after-replay",
            ]
            assert history.events

            # Re-run the recorded event history in an isolated replay worker.
            # This detects nondeterminism without claiming a process-kill test.
            replayer = Replayer(workflows=[AgentRunWorkflow])
            await replayer.replay_workflow(history)

    asyncio.run(scenario())
