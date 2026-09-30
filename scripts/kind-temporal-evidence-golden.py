"""AgentOS v3.2 live Golden Slice.

Uses a real Temporal test server and real Kubernetes Pods in a disposable kind
cluster. Evidence is archived through the same object-store contract used by
S3ObjectStore; the storage-provider adapter is tested separately.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from cloud_agent_runtime import (
    AppendOnlyEvidenceCollector,
    AsyncAgentRuntime,
    EvidenceArchive,
    EvidenceObjectStore,
    InMemoryObjectStore,
    KubernetesSandboxProvider,
    OTelEvidenceAdapter,
    OTelSpanSnapshot,
    Provenance,
    TemporalWorkflowDriver,
)
from cloud_agent_runtime.temporal_workflow import AgentRunWorkflow


async def scenario() -> None:
    namespace = "agentos-v32-golden"
    provider = KubernetesSandboxProvider(namespace=namespace)
    object_backend = InMemoryObjectStore()
    archive = EvidenceArchive(
        collector=AppendOnlyEvidenceCollector(
            collector_id="external-evidence",
        ),
        object_store=EvidenceObjectStore(object_backend),
    )

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"agentos-v32-{uuid4()}"

        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[AgentRunWorkflow],
        ):
            driver = TemporalWorkflowDriver(
                client=env.client,
                task_queue=task_queue,
            )
            runtime = AsyncAgentRuntime(
                sandbox_provider=provider,
                workflow_driver=driver,
            )
            session = runtime.create_session(
                agent_id="sre-agent",
                release_id="agentos-v32-golden",
                tenant_id="golden-demo",
            )
            run = await runtime.start_run(
                session_id=session.session_id,
            )

            assert run.workflow_ref is not None
            canonical_run_id = run.run_id
            canonical_session_id = run.session_id
            canonical_workflow_id = run.workflow_ref.workflow_id
            canonical_temporal_run_id = run.workflow_ref.run_id

            workflow_before = await driver.query(
                run.workflow_ref,
                name="run_state",
            )
            assert workflow_before["runtime_run_id"] == canonical_run_id
            assert workflow_before["session_id"] == canonical_session_id

            sandbox_a = runtime._sandboxes[canonical_run_id]
            sandbox_a_ref = run.sandbox_ref
            sandbox_a_id = sandbox_a.sandbox_id

            checkpoint = provider.snapshot(sandbox_a).snapshot_ref
            assert checkpoint is not None

            # Inject real Pod loss. The runtime must not resurrect Sandbox A.
            provider.terminate(sandbox_a)
            assert sandbox_a.status.value == "terminated"

            rebound = runtime.rebind_run_sandbox(
                canonical_run_id,
                snapshot_ref=checkpoint,
            )
            sandbox_b = runtime._sandboxes[canonical_run_id]
            sandbox_b_id = sandbox_b.sandbox_id

            assert rebound.run_id == canonical_run_id
            assert rebound.session_id == canonical_session_id
            assert rebound.workflow_ref is not None
            assert rebound.workflow_ref.workflow_id == canonical_workflow_id
            assert rebound.workflow_ref.run_id == canonical_temporal_run_id
            assert rebound.sandbox_ref != sandbox_a_ref
            assert sandbox_b_id != sandbox_a_id
            assert sandbox_a.status.value == "terminated"

            workflow_after = await driver.query(
                rebound.workflow_ref,
                name="run_state",
            )
            assert workflow_after["runtime_run_id"] == canonical_run_id
            assert workflow_after["session_id"] == canonical_session_id

            trace_id = uuid4().hex
            span_id = uuid4().hex[:16]
            event = OTelEvidenceAdapter().to_event(
                OTelSpanSnapshot(
                    trace_id=trace_id,
                    span_id=span_id,
                    name="sandbox.rebind",
                    status="OK",
                    attributes={
                        "agent.run_id": canonical_run_id,
                        "agent.action_id": "sandbox-rebind-001",
                        "agent.evidence.event_type": "sandbox.rebind",
                        "agent.artifact.digest": (
                            "sha256:" + ("a" * 64)
                        ),
                        "agent.policy.digest": (
                            "sha256:" + ("b" * 64)
                        ),
                        "sandbox.previous_id": sandbox_a_id,
                        "sandbox.current_id": sandbox_b_id,
                    },
                ),
                provenance=Provenance(
                    generated_by="runtime",
                    observed_by="otel-collector",
                    authorized_by="agentos-policy",
                    executed_by="kubernetes-sandbox-provider",
                    committed_by="evidence-object-store",
                ),
            )
            archived = archive.record(event)
            replayed = archive.replay(
                archived.manifest.manifest_key,
            )

            assert replayed["event"]["run_id"] == canonical_run_id
            assert (
                replayed["event"]["payload"]["trace_id"]
                == trace_id
            )
            assert (
                replayed["event"]["payload"]["attributes"][
                    "sandbox.previous_id"
                ]
                == sandbox_a_id
            )
            assert (
                replayed["event"]["payload"]["attributes"][
                    "sandbox.current_id"
                ]
                == sandbox_b_id
            )

            evidence_ref = (
                "object://"
                + archived.manifest.manifest_key
            )
            await driver.signal(
                rebound.workflow_ref,
                name="complete_run",
                payload={
                    "result": {
                        "sandbox_rebound": True,
                        "evidence_replayed": True,
                    },
                    "evidence_refs": [evidence_ref],
                },
            )

            handle = env.client.get_workflow_handle(
                canonical_workflow_id,
                run_id=canonical_temporal_run_id,
            )
            result = await handle.result()
            assert result["phase"] == "succeeded"
            assert result["runtime_run_id"] == canonical_run_id
            assert result["session_id"] == canonical_session_id
            assert result["evidence_refs"] == [evidence_ref]

            print("PASS")
            print(f"run_id={canonical_run_id}")
            print(f"session_id={canonical_session_id}")
            print(f"workflow_id={canonical_workflow_id}")
            print(
                f"temporal_run_id={canonical_temporal_run_id}"
            )
            print(f"sandbox_a_ref={sandbox_a_ref}")
            print(f"sandbox_b_ref={rebound.sandbox_ref}")
            print(f"sandbox_a_id={sandbox_a_id}")
            print(f"sandbox_b_id={sandbox_b_id}")
            print(
                "evidence_manifest="
                f"{archived.manifest.manifest_key}"
            )
            print(f"evidence_digest={archived.manifest.digest}")
            print(f"trace_id={trace_id}")


if __name__ == "__main__":
    asyncio.run(scenario())
