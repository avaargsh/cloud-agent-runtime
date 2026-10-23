import asyncio
from uuid import uuid4

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cloud_agent_runtime.temporal_workflow import AgentRunWorkflow


def test_worker_replacement_replays_open_run_and_deduplicates_signal() -> None:
    async def scenario() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-agent-recovery-{uuid4()}"
            runtime_run_id = f"run-{uuid4()}"
            session_id = f"session-{uuid4()}"
            workflow_id = f"agent-run-{runtime_run_id}"
            approval_payload = {
                "approval_id": "approval-replay-1",
                "approved": True,
                "actor": "operator@example",
                "evidence_refs": ["evidence://recovery/before-worker-restart"],
            }

            # Worker 1 processes the initial history and a nonterminal signal.
            # Exiting this context replaces the Worker; it does not terminate
            # the Temporal workflow or simulate an OS-level SIGKILL.
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
                assert initial["phase"] == "running"

                await handle.signal(
                    AgentRunWorkflow.approval_resolved,
                    approval_payload,
                )
                approved = await handle.query(
                    AgentRunWorkflow.run_state
                )
                assert approved["phase"] == "running"
                assert approved["approval_events"] == [approval_payload]
                assert approved["evidence_refs"] == [
                    "evidence://recovery/before-worker-restart"
                ]

            # Worker 2 processes new events on the same workflow ID. Sending a
            # signal first forces workflow-task processing/replay without
            # relying on a query to a workflow's former sticky task queue.
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                recovered = env.client.get_workflow_handle(workflow_id)
                await recovered.signal(
                    AgentRunWorkflow.approval_resolved,
                    approval_payload,
                )
                await recovered.signal(
                    AgentRunWorkflow.complete_run,
                    {
                        "result": {"summary": "recovered"},
                        "evidence_refs": ["evidence://recovery/after-restart"],
                    },
                )
                result = await recovered.result()

                assert result["runtime_run_id"] == runtime_run_id
                assert result["session_id"] == session_id
                assert result["phase"] == "succeeded"
                # The duplicate approval's evidence must not be appended twice.
                assert result["evidence_refs"] == [
                    "evidence://recovery/before-worker-restart",
                    "evidence://recovery/after-restart",
                ]

    asyncio.run(scenario())
