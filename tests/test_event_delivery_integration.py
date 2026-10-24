import asyncio
from uuid import uuid4

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cloud_agent_runtime import Run, RunStatus, Session, SQLiteStore
from cloud_agent_runtime.event_delivery import TemporalEventDispatcher
from cloud_agent_runtime.event_inbox import EventEnvelope, SQLiteEventInbox
from cloud_agent_runtime.temporal_driver import TemporalWorkflowDriver
from cloud_agent_runtime.temporal_workflow import AgentRunWorkflow
from cloud_agent_runtime.workflow import WorkflowRef


def test_external_event_signal_ack_survives_temporal_worker_replacement(tmp_path):
    async def scenario():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            queue = f"external-event-{uuid4()}"
            workflow_id = f"agent-run-{uuid4()}"
            database = tmp_path / "signal-ack.sqlite"
            store = SQLiteStore(database)
            store.save_session(Session("session-a", "agent-a", "release-a", "tenant-a"))

            async with Worker(
                env.client,
                task_queue=queue,
                workflows=[AgentRunWorkflow],
                max_cached_workflows=0,
            ):
                handle = await env.client.start_workflow(
                    AgentRunWorkflow.run,
                    {"runtime_run_id": "run-a", "session_id": "session-a"},
                    id=workflow_id,
                    task_queue=queue,
                )
                run_ref = WorkflowRef(
                    provider="temporal",
                    workflow_id=workflow_id,
                    run_id=handle.first_execution_run_id,
                )
                store.save_run(
                    Run("run-a", "session-a", status=RunStatus.RUNNING, workflow_ref=run_ref)
                )
                inbox = SQLiteEventInbox(database)
                event = EventEnvelope(
                    event_id="event-1",
                    tenant_id="tenant-a",
                    run_id="run-a",
                    event_type="human.reviewed",
                    correlation_id="task-a",
                    evidence_refs=("evidence://signed/decision",),
                )
                inbox.accept(event)
                driver = TemporalWorkflowDriver(client=env.client, task_queue=queue)
                dispatcher = TemporalEventDispatcher(inbox, driver)
                assert await dispatcher.deliver_pending(
                    run_id="run-a", tenant_id="tenant-a"
                ) == 1
                state = await handle.query(AgentRunWorkflow.run_state)
                assert len(state["external_events"]) == 1
                assert state["external_events"][0]["event_id"] == event.event_id
                assert state["approval_events"] == []
                inbox.close()

            # The canonical receipt is durable; a replacement Worker replays the
            # same execution instead of allocating another Run or resending.
            reopened = SQLiteEventInbox(database)
            async with Worker(
                env.client,
                task_queue=queue,
                workflows=[AgentRunWorkflow],
                max_cached_workflows=0,
            ):
                dispatcher = TemporalEventDispatcher(reopened, driver)
                assert await dispatcher.deliver_pending(
                    run_id="run-a", tenant_id="tenant-a"
                ) == 0
                state = await handle.query(AgentRunWorkflow.run_state)
                assert len(state["external_events"]) == 1
                await handle.signal(AgentRunWorkflow.complete_run, {"result": {"ok": True}})
                result = await handle.result()
                assert result["external_events"][0]["event_id"] == event.event_id
            reopened.close()
            store.close()

    asyncio.run(scenario())
