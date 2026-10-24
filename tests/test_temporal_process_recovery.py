from __future__ import annotations

import asyncio
import json
import signal
import sqlite3
import sys
from uuid import uuid4

from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cloud_agent_runtime.temporal_workflow import AgentRunWorkflow


def test_real_subprocess_sigkill_after_temporal_acceptance_reconciles_sqlite(tmp_path) -> None:
    async def scenario() -> None:
        db_path = tmp_path / "process-restart.sqlite"
        task_queue = f"p0-process-recovery-{uuid4()}"
        async with await WorkflowEnvironment.start_time_skipping() as env:
            target = env.client.service_client.config.target_host
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                crash = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "cloud_agent_runtime.recovery_probe",
                    "crash-after-accepted-start",
                    "--sqlite-path",
                    str(db_path),
                    "--temporal-address",
                    target,
                    "--namespace",
                    "default",
                    "--task-queue",
                    task_queue,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(crash.communicate(), timeout=45)
                assert crash.returncode == -signal.SIGKILL, (
                    f"caller was expected to die by SIGKILL; returncode={crash.returncode}; "
                    f"stdout={stdout.decode(errors='replace')}; stderr={stderr.decode(errors='replace')}"
                )

                with sqlite3.connect(db_path) as connection:
                    rows = connection.execute(
                        "SELECT run_id, payload FROM runs"
                    ).fetchall()
                assert len(rows) == 1, "expected exactly one durable canonical Run"
                run_id, payload_text = rows[0]
                persisted = json.loads(payload_text)
                workflow_id = f"agent-run-{run_id}"
                operation_id = f"agent-run-start:{run_id}"
                assert persisted["run_id"] == run_id
                assert persisted["recovery_operation_id"] == operation_id
                assert persisted["recovery_state"] == "start_pending"
                assert persisted["workflow_ref"] is None

                reconcile = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-X",
                    "faulthandler",
                    "-m",
                    "cloud_agent_runtime.recovery_probe",
                    "reconcile-sqlite",
                    "--sqlite-path",
                    str(db_path),
                    "--run-id",
                    run_id,
                    "--temporal-address",
                    target,
                    "--namespace",
                    "default",
                    "--task-queue",
                    task_queue,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                recovered_stdout, recovered_stderr = await asyncio.wait_for(
                    reconcile.communicate(), timeout=45
                )
                assert reconcile.returncode == 0, (
                    f"reconcile process failed with {reconcile.returncode}; "
                    f"stdout={recovered_stdout.decode(errors='replace')}; "
                    f"stderr={recovered_stderr.decode(errors='replace')}"
                )
                recovered = json.loads(recovered_stdout.decode())
                assert recovered["run_id"] == run_id
                assert recovered["recovery_state"] == "attached"
                assert recovered["recovery_operation_id"] == operation_id
                assert recovered["workflow_ref"]["workflow_id"] == workflow_id

                # Temporal's embedded time-skipping test server does not
                # implement ListWorkflowExecutions. Prove non-duplication with
                # the deterministic Workflow ID's existing handle and the
                # server's duplicate-start rejection instead.
                handle = env.client.get_workflow_handle(workflow_id)
                description = await handle.describe()
                assert description.status.name == "RUNNING"
                try:
                    await env.client.start_workflow(
                        AgentRunWorkflow.run,
                        {
                            "runtime_run_id": run_id,
                            "session_id": persisted["session_id"],
                        },
                        id=workflow_id,
                        task_queue=task_queue,
                    )
                except WorkflowAlreadyStartedError:
                    pass
                else:
                    raise AssertionError("Temporal accepted a duplicate deterministic Workflow ID")

                with sqlite3.connect(db_path) as connection:
                    final_payload = connection.execute(
                        "SELECT payload FROM runs WHERE run_id = ?", (run_id,)
                    ).fetchone()[0]
                final_run = json.loads(final_payload)
                assert final_run["workflow_ref"]["workflow_id"] == workflow_id
                assert final_run["recovery_state"] == "attached"
                outcomes = [
                    event["outcome"]
                    for event in final_run["recovery_evidence"]
                ]
                # The caller was SIGKILLed after Temporal accepted StartWorkflow
                # but before it could append an "unknown" outcome. The durable
                # journal must therefore prove pre-call intent and post-restart
                # attachment; a third in-process event is not valid after an
                # abrupt process death.
                assert outcomes.count("intent_persisted") == 1
                assert outcomes.count("attached") == 1

    asyncio.run(scenario())
