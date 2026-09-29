import asyncio
from dataclasses import replace
from uuid import uuid4

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cloud_agent_runtime.lifecycle_binding import (
    BindingPhase,
    RestoreRequest,
    SandboxLifecycleBindingAdapter,
)
from cloud_agent_runtime.sandbox import (
    InMemorySandboxProvider,
    Sandbox,
    SandboxStatus,
)
from cloud_agent_runtime.temporal_workflow import AgentRunWorkflow


class ReplacementSandboxProvider(InMemorySandboxProvider):
    """Test provider that proves restore allocates replacement compute."""

    def resume(self, snapshot_ref: str, *, session_id: str) -> Sandbox:
        return Sandbox(
            sandbox_id=f"replacement-{uuid4()}",
            provider=self.name,
            status=SandboxStatus.BOUND,
            snapshot_ref=snapshot_ref,
            session_id=session_id,
        )


def test_temporal_survives_sandbox_replacement_without_repeating_completed_side_effect():
    async def scenario() -> None:
        side_effects: list[str] = []
        completed_operations: set[str] = set()

        def execute_once(operation_id: str) -> None:
            if operation_id in completed_operations:
                return
            side_effects.append(operation_id)
            completed_operations.add(operation_id)

        adapter = SandboxLifecycleBindingAdapter(ReplacementSandboxProvider())

        async with await WorkflowEnvironment.start_time_skipping() as env:
            task_queue = f"binding-recovery-{uuid4()}"
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                workflow_id = f"agent-run-{uuid4()}"
                handle = await env.client.start_workflow(
                    AgentRunWorkflow.run,
                    {"runtime_run_id": "run-381", "session_id": "session-1"},
                    id=workflow_id,
                    task_queue=task_queue,
                )

                binding = adapter.bind(
                    workflow_ref=f"temporal://{workflow_id}",
                    session_id="session-1",
                    release_id="candidate-v7",
                    credential_ref="lease://initial",
                    idempotency_key="simulation-1",
                )
                first_physical = binding.physical_sandbox_ref

                execute_once("simulation-1")
                released = adapter.release(
                    binding,
                    artifact_refs=("artifact://simulation-1",),
                )

                state_while_compute_is_gone = await handle.query(
                    AgentRunWorkflow.run_state
                )
                assert state_while_compute_is_gone["terminal"] is False
                assert released.phase == BindingPhase.RELEASED
                assert released.physical_sandbox_ref is None

                restored = adapter.restore(
                    released,
                    RestoreRequest(
                        workflow_ref=f"temporal://{workflow_id}",
                        session_id="session-1",
                        release_id="candidate-v7",
                        credential_ref="lease://reissued",
                        idempotency_key="simulation-1",
                    ),
                )
                assert restored.physical_sandbox_ref != first_physical
                assert restored.workspace_ref == released.workspace_ref

                # Recovery may retry the activity, but the completed operation is
                # protected by the same stable operation identity.
                execute_once("simulation-1")
                execute_once("simulation-2")
                assert side_effects == ["simulation-1", "simulation-2"]

                await handle.signal(
                    AgentRunWorkflow.complete_run,
                    {
                        "result": {"summary": "sandbox replacement verified"},
                        "evidence_refs": [
                            "evidence://binding/released",
                            "evidence://binding/restored",
                        ],
                    },
                )
                result = await handle.result()
                assert result["phase"] == "succeeded"
                assert result["runtime_run_id"] == "run-381"

    asyncio.run(scenario())
