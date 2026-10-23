import asyncio
from uuid import uuid4

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cloud_agent_runtime import (
    AsyncAgentRuntime,
    Budget,
    SQLiteStore,
    TemporalWorkflowDriver,
)
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
                transitions = result["transition_events"]
                assert [
                    (event["from_phase"], event["to_phase"])
                    for event in transitions
                ] == [
                    ("pending", "running"),
                    ("running", "succeeded"),
                ]
                assert all(
                    event["runtime_run_id"] == "run-123"
                    and event["session_id"] == "session-456"
                    for event in transitions
                )

    asyncio.run(scenario())



def test_temporal_worker_restart_replays_history_and_deduplicates_signal() -> None:
    async def scenario() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-agent-restart-{uuid4()}"
            workflow_id = f"agent-run-restart-{uuid4()}"
            payload = {
                "runtime_run_id": "canonical-run-restart",
                "session_id": "canonical-session-restart",
            }
            handle = None

            # Worker A records the initial run and a durable waiting transition.
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                handle = await env.client.start_workflow(
                    AgentRunWorkflow.run,
                    payload,
                    id=workflow_id,
                    task_queue=task_queue,
                )
                await handle.signal(
                    AgentRunWorkflow.wait_for_input,
                    {"event_id": "wait-before-restart"},
                )
                waiting = await handle.query(AgentRunWorkflow.run_state)
                assert waiting["phase"] == "waiting"
                assert waiting["runtime_run_id"] == payload["runtime_run_id"]
                assert waiting["session_id"] == payload["session_id"]

            # Simulate Worker process loss: signals can still be persisted in
            # Temporal history while no worker is polling the task queue.
            assert handle is not None
            await handle.signal(
                AgentRunWorkflow.approval_resolved,
                {
                    "approval_id": "approval-durable-1",
                    "approved": True,
                    "evidence_refs": ["evidence://restart/approval"],
                },
            )
            await handle.signal(
                AgentRunWorkflow.approval_resolved,
                {
                    "approval_id": "approval-durable-1",
                    "approved": True,
                    "evidence_refs": ["evidence://restart/approval"],
                },
            )

            # Worker B replays the same workflow history, including both
            # approval signals, without replacing the canonical workflow/run.
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                replayed = await handle.query(AgentRunWorkflow.run_state)
                assert handle.id == workflow_id
                assert replayed["runtime_run_id"] == payload["runtime_run_id"]
                assert replayed["session_id"] == payload["session_id"]
                assert replayed["phase"] == "running"
                assert len(replayed["approval_events"]) == 1
                assert replayed["evidence_refs"] == [
                    "evidence://restart/approval"
                ]
                assert [
                    (event["from_phase"], event["to_phase"])
                    for event in replayed["transition_events"]
                ] == [
                    ("pending", "running"),
                    ("running", "waiting"),
                    ("waiting", "running"),
                ]

                await handle.signal(
                    AgentRunWorkflow.complete_run,
                    {
                        "event_id": "complete-after-restart",
                        "result": {"replayed": True},
                        "evidence_refs": ["evidence://restart/complete"],
                    },
                )
                result = await handle.result()
                assert result["phase"] == "succeeded"
                assert result["runtime_run_id"] == payload["runtime_run_id"]
                assert result["session_id"] == payload["session_id"]
                assert result["result"] == {"replayed": True}
                assert [
                    (event["from_phase"], event["to_phase"])
                    for event in result["transition_events"]
                ] == [
                    ("pending", "running"),
                    ("running", "waiting"),
                    ("waiting", "running"),
                    ("running", "succeeded"),
                ]

    asyncio.run(scenario())


def test_temporal_run_timeout_is_durable_and_terminal() -> None:
    async def scenario() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-agent-timeout-{uuid4()}"
            payload = {
                "runtime_run_id": "canonical-run-timeout",
                "session_id": "canonical-session-timeout",
                "timeout_seconds": 2,
                "timeout_event_id": "deadline-timeout-1",
                "evidence_refs": ["evidence://timeout/start"],
            }

            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                handle = await env.client.start_workflow(
                    AgentRunWorkflow.run,
                    payload,
                    id=f"agent-run-timeout-{uuid4()}",
                    task_queue=task_queue,
                )
                result = await handle.result()

                assert result["phase"] == "failed"
                assert result["runtime_run_id"] == payload["runtime_run_id"]
                assert result["session_id"] == payload["session_id"]
                assert result["error"] == "run timed out after 2 seconds"
                assert result["evidence_refs"] == ["evidence://timeout/start"]
                assert [
                    (event["from_phase"], event["to_phase"])
                    for event in result["transition_events"]
                ] == [
                    ("pending", "running"),
                    ("running", "failed"),
                ]
                timeout_event = result["transition_events"][-1]
                assert timeout_event["event_type"] == "run_timed_out"
                assert timeout_event["event_id"] == "deadline-timeout-1"
                assert timeout_event["runtime_run_id"] == payload["runtime_run_id"]
                assert timeout_event["session_id"] == payload["session_id"]

    asyncio.run(scenario())



def test_run_budget_timeout_is_reconciled_after_temporal_execution_closes(tmp_path) -> None:
    async def scenario() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-agent-budget-timeout-{uuid4()}"
            database = tmp_path / "budget-timeout.db"
            store = SQLiteStore(database)
            runtime = AsyncAgentRuntime(
                store=store,
                workflow_driver=TemporalWorkflowDriver(
                    client=env.client,
                    task_queue=task_queue,
                ),
            )
            session = runtime.create_session(
                agent_id="sre-agent",
                release_id="release-v1",
                tenant_id="tenant-timeout",
            )

            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                run = await runtime.start_run(
                    session_id=session.session_id,
                    budget=Budget(max_wall_seconds=2),
                )
                assert run.status.value == "running"
                assert run.workflow_ref is not None

                handle = env.client.get_workflow_handle(
                    run.workflow_ref.workflow_id,
                    run_id=run.workflow_ref.run_id,
                )
                result = await handle.result()
                assert result["phase"] == "failed"
                assert result["runtime_run_id"] == run.run_id
                assert result["session_id"] == session.session_id
                assert result["transition_events"][-1]["event_type"] == "run_timed_out"
                assert result["transition_events"][-1]["event_id"] == (
                    f"timeout:{run.run_id}"
                )

                reconciled = await runtime.reconcile_run(run.run_id)
                assert reconciled.status.value == "failed"
                assert reconciled.run_id == run.run_id
                assert reconciled.session_id == session.session_id
                assert reconciled.workflow_ref == run.workflow_ref
                assert [
                    (event["from_status"], event["to_status"])
                    for event in reconciled.transition_events
                ] == [
                    ("created", "running"),
                    ("running", "failed"),
                ]
                assert reconciled.transition_events[-1]["event_type"] == (
                    "workflow_run_timed_out"
                )
                assert reconciled.transition_events[-1]["event_id"] == (
                    f"timeout:{run.run_id}"
                )
                assert reconciled.metadata["terminal_reason"] == (
                    "run timed out after 2 seconds"
                )

                recovered = store.get_run(run.run_id)
                assert recovered.status.value == "failed"
                assert recovered.transition_events == reconciled.transition_events
                assert recovered.workflow_ref == run.workflow_ref

            store.close()

    asyncio.run(scenario())



def test_temporal_deadline_survives_worker_restart_and_replay() -> None:
    async def scenario() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"test-agent-timeout-restart-{uuid4()}"
            workflow_id = f"agent-run-timeout-restart-{uuid4()}"
            payload = {
                "runtime_run_id": "canonical-run-timeout-restart",
                "session_id": "canonical-session-timeout-restart",
                "timeout_seconds": 3,
                "timeout_event_id": "timeout-durable-before-restart",
            }
            handle = None

            # Worker A must persist the timer command before shutdown.
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                handle = await env.client.start_workflow(
                    AgentRunWorkflow.run,
                    payload,
                    id=workflow_id,
                    task_queue=task_queue,
                )
                state = await handle.query(AgentRunWorkflow.run_state)
                assert state["phase"] == "running"
                assert state["terminal"] is False
                assert handle.id == workflow_id

            # Worker B replays the workflow and its durable timer. No signal or
            # local process timer is needed to make the timeout terminal.
            assert handle is not None
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                result = await handle.result()
                assert result["phase"] == "failed"
                assert result["runtime_run_id"] == payload["runtime_run_id"]
                assert result["session_id"] == payload["session_id"]
                assert result["error"] == "run timed out after 3 seconds"
                assert [
                    (event["from_phase"], event["to_phase"])
                    for event in result["transition_events"]
                ] == [("pending", "running"), ("running", "failed")]
                assert result["transition_events"][-1]["event_type"] == "run_timed_out"
                assert result["transition_events"][-1]["event_id"] == (
                    "timeout-durable-before-restart"
                )

    asyncio.run(scenario())
