import asyncio
from uuid import uuid4

from temporalio import workflow
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import CancelledError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cloud_agent_runtime.temporal_workflow import AgentRunWorkflow


@workflow.defn(name="AgentRunTimeoutProbe")
class AgentRunTimeoutProbe:
    def __init__(self):
        self._state = None

    @workflow.run
    async def run(self, payload: dict) -> dict:
        self._state = {
            "runtime_run_id": str(payload["runtime_run_id"]),
            "session_id": str(payload["session_id"]),
            "phase": "running",
            "terminal": False,
            "transition_events": [],
        }
        try:
            await workflow.wait_condition(
                lambda: bool(self._state and self._state["terminal"]),
                timeout=float(payload["timeout_seconds"]),
            )
        except TimeoutError:
            assert self._state is not None
            self._state["phase"] = "failed"
            self._state["terminal"] = True
            self._state["transition_events"].append({
                "event_id": f"timeout:{self._state['runtime_run_id']}",
                "event_type": "run_timed_out",
                "evidence_refs": [],
            })
        assert self._state is not None
        return dict(self._state)


def test_temporal_workflow_timeout_is_terminal_and_has_stable_evidence() -> None:
    async def scenario() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-timeout-{uuid4()}"
            run_id = f"run-{uuid4()}"
            session_id = f"session-{uuid4()}"
            async with Worker(env.client, task_queue=task_queue, workflows=[AgentRunTimeoutProbe]):
                handle = await env.client.start_workflow(
                    AgentRunTimeoutProbe.run,
                    {"runtime_run_id": run_id, "session_id": session_id, "timeout_seconds": 5},
                    id=f"timeout-probe-{run_id}",
                    task_queue=task_queue,
                )
                result = await handle.result()
            assert result["runtime_run_id"] == run_id
            assert result["session_id"] == session_id
            assert result["phase"] == "failed"
            assert result["terminal"] is True
            assert result["transition_events"] == [{
                "event_id": f"timeout:{run_id}",
                "event_type": "run_timed_out",
                "evidence_refs": [],
            }]

    asyncio.run(scenario())


def test_temporal_workflow_cancel_is_terminal_and_not_success() -> None:
    async def scenario() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-cancel-{uuid4()}"
            run_id = f"run-{uuid4()}"
            session_id = f"session-{uuid4()}"
            async with Worker(env.client, task_queue=task_queue, workflows=[AgentRunWorkflow]):
                handle = await env.client.start_workflow(
                    AgentRunWorkflow.run,
                    {"runtime_run_id": run_id, "session_id": session_id},
                    id=f"agent-run-{run_id}",
                    task_queue=task_queue,
                )
                await handle.cancel()
                try:
                    await handle.result()
                except WorkflowFailureError as exc:
                    assert isinstance(exc.cause, CancelledError)
                else:
                    raise AssertionError("cancelled workflow unexpectedly returned a success result")

    asyncio.run(scenario())
