import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path
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
                max_cached_workflows=0,
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
                max_cached_workflows=0,
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
                max_cached_workflows=0,
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
                max_cached_workflows=0,
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



def test_temporal_worker_process_sigkill_restarts_same_workflow_identity(tmp_path) -> None:
    """Kill the OS Worker process, append history while it is absent, and replay."""
    if os.name != "posix":
        import pytest
        pytest.skip("the process-level SIGKILL experiment requires POSIX")

    def log_tail(path: Path) -> str:
        try:
            return path.read_text(encoding="utf-8", errors="replace")[-5000:]
        except OSError:
            return "<worker log not available>"

    async def wait_for_state(
        handle,
        predicate,
        *,
        timeout_seconds=30.0,
        worker=None,
        log_path: Path | None = None,
    ):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        last_error = None
        last_state = None
        while loop.time() < deadline:
            if worker is not None and worker.poll() is not None:
                raise AssertionError(
                    f"worker process exited unexpectedly with code {worker.returncode}; "
                    f"log={log_tail(log_path) if log_path else '<no log>'}"
                )
            try:
                state = await asyncio.wait_for(
                    handle.query(AgentRunWorkflow.run_state),
                    timeout=2.0,
                )
                last_state = state
                if predicate(state):
                    return state
            except Exception as exc:
                last_error = exc
            await asyncio.sleep(0.1)
        raise AssertionError(
            f"workflow did not reach expected state before timeout; "
            f"last state={last_state!r}; last query error={last_error!r}; "
            f"worker poll={worker.poll() if worker is not None else None}; "
            f"log={log_tail(log_path) if log_path else '<no log>'}"
        )

    def spawn_worker(
        target: str,
        task_queue: str,
        repository_root: Path,
        log_path: Path,
        log_handles: list,
        *,
        max_cached_workflows: int | None = None,
    ):
        worker_env = os.environ.copy()
        source_root = str(repository_root / "src")
        existing_pythonpath = worker_env.get("PYTHONPATH")
        worker_env["PYTHONPATH"] = (
            source_root
            if not existing_pythonpath
            else source_root + os.pathsep + existing_pythonpath
        )
        worker_env["PYTHONUNBUFFERED"] = "1"
        log_handle = log_path.open("wb")
        log_handles.append(log_handle)
        worker_command = [
            sys.executable,
            "-m",
            "cloud_agent_runtime.temporal_worker",
            "--target",
            target,
            "--namespace",
            "default",
            "--task-queue",
            task_queue,
        ]
        if max_cached_workflows is not None:
            worker_command.extend(
                ["--max-cached-workflows", str(max_cached_workflows)]
            )
        return subprocess.Popen(
            worker_command,
            cwd=repository_root,
            env=worker_env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )

    async def scenario() -> None:
        async with await WorkflowEnvironment.start_local() as env:
            assert not env.supports_time_skipping, "SIGKILL replay must run against the full local Temporal server"
            task_queue = f"test-agent-sigkill-{uuid4()}"
            workflow_id = f"agent-run-sigkill-{uuid4()}"
            runtime_run_id = f"canonical-run-{uuid4()}"
            session_id = f"canonical-session-{uuid4()}"
            payload = {
                "runtime_run_id": runtime_run_id,
                "session_id": session_id,
            }
            # The Temporal SDK's Client.config is a method; its service
            # client exposes the concrete target needed by the subprocess.
            target = env.client.service_client.config.target_host
            repository_root = Path(__file__).resolve().parents[1]
            workers: list[subprocess.Popen] = []
            worker_log_handles = []
            worker_a_log = tmp_path / "temporal-worker-a.log"
            worker_b_log = tmp_path / "temporal-worker-b.log"
            handle = None
            first_execution_run_id = None
            try:
                # Disable sticky workflow caching on Worker A as well as
                # Worker B. Otherwise the Temporal test server may keep
                # routing recovery to the dead process's sticky queue.
                worker_a = spawn_worker(
                    target,
                    task_queue,
                    repository_root,
                    worker_a_log,
                    worker_log_handles,
                    max_cached_workflows=0,
                )
                workers.append(worker_a)
                handle = await env.client.start_workflow(
                    AgentRunWorkflow.run,
                    payload,
                    id=workflow_id,
                    task_queue=task_queue,
                )
                # Do not confuse a pre-initialization signal with a
                # Worker-restart recovery test. First prove the workflow's
                # start transition is durable, then move it to Waiting.
                started = await wait_for_state(
                    handle,
                    lambda state: state.get("phase") == "running",
                    worker=worker_a,
                    log_path=worker_a_log,
                )
                assert started["runtime_run_id"] == runtime_run_id
                assert started["session_id"] == session_id
                await handle.signal(
                    AgentRunWorkflow.wait_for_input,
                    {"event_id": "durable-wait-before-sigkill"},
                )
                waiting = await wait_for_state(
                    handle,
                    lambda state: state.get("phase") == "waiting",
                    worker=worker_a,
                    log_path=worker_a_log,
                )
                assert worker_a.poll() is None
                assert waiting["runtime_run_id"] == runtime_run_id
                assert waiting["session_id"] == session_id
                assert handle.id == workflow_id
                first_execution_run_id = handle.first_execution_run_id

                # This is an OS-level uncatchable kill, not Worker context exit.
                worker_a.send_signal(signal.SIGKILL)
                return_code = await asyncio.to_thread(worker_a.wait, timeout=10)
                assert return_code == -signal.SIGKILL

                # Temporal must accept and persist events while no Worker polls.
                for _ in range(2):
                    await handle.signal(
                        AgentRunWorkflow.approval_resolved,
                        {
                            "approval_id": "approval-during-outage",
                            "approved": True,
                            "evidence_refs": ["evidence://sigkill/approval"],
                        },
                    )

                worker_b = spawn_worker(
                    target,
                    task_queue,
                    repository_root,
                    worker_b_log,
                    worker_log_handles,
                    max_cached_workflows=0,
                )
                workers.append(worker_b)
                replayed = await wait_for_state(
                    handle,
                    lambda state: (
                        state.get("phase") == "running"
                        and len(state.get("approval_events", [])) == 1
                    ),
                    worker=worker_b,
                    log_path=worker_b_log,
                )
                assert worker_b.poll() is None
                assert handle.id == workflow_id
                assert handle.first_execution_run_id == first_execution_run_id
                assert replayed["runtime_run_id"] == runtime_run_id
                assert replayed["session_id"] == session_id
                assert len(replayed["approval_events"]) == 1
                assert replayed["evidence_refs"] == ["evidence://sigkill/approval"]
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
                        "event_id": "complete-after-sigkill",
                        "result": {"worker_restarted": True},
                        "evidence_refs": ["evidence://sigkill/complete"],
                    },
                )
                result = await asyncio.wait_for(handle.result(), timeout=30.0)
                assert result["phase"] == "succeeded"
                assert result["runtime_run_id"] == runtime_run_id
                assert result["session_id"] == session_id
                assert result["result"] == {"worker_restarted": True}
                assert [
                    (event["from_phase"], event["to_phase"])
                    for event in result["transition_events"]
                ] == [
                    ("pending", "running"),
                    ("running", "waiting"),
                    ("waiting", "running"),
                    ("running", "succeeded"),
                ]
            finally:
                for worker in workers:
                    if worker.poll() is None:
                        worker.kill()
                        await asyncio.to_thread(worker.wait, timeout=10)
                for log_handle in worker_log_handles:
                    log_handle.close()

    asyncio.run(scenario())
