from cloud_agent_runtime import (
    AgentRuntime,
    InMemorySandboxProvider,
    InMemoryWorkflowDriver,
)


def test_run_survives_sandbox_replacement_with_durable_refs():
    runtime = AgentRuntime(
        sandbox_provider=InMemorySandboxProvider(),
        workflow_driver=InMemoryWorkflowDriver(),
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="checkout-sre-golden-v1",
        tenant_id="golden-demo",
    )
    run = runtime.start_run(session_id=session.session_id)

    canonical_run_id = run.run_id
    workflow_ref = run.workflow_ref
    sandbox_a = run.sandbox_ref

    # Durable facts belong to the Run, not to the sandbox instance.
    run.artifact_refs.append("artifact://golden/run-001/diagnostic.json")
    run.evidence_refs.append("evidence://sha256/frozen-before-rebind")
    runtime.store.save_run(run)

    active = runtime._sandboxes[run.run_id]
    snapshotted = runtime.sandbox_provider.snapshot(active)
    assert snapshotted.snapshot_ref is not None

    # Simulate loss of Sandbox A after a durable snapshot was produced.
    runtime.sandbox_provider.terminate(active)
    assert active.status.value == "terminated"

    rebound = runtime.rebind_run_sandbox(
        run.run_id,
        snapshot_ref=snapshotted.snapshot_ref,
    )

    assert rebound.run_id == canonical_run_id
    assert rebound.workflow_ref == workflow_ref
    assert rebound.sandbox_ref != sandbox_a
    assert active.status.value == "terminated"
    assert rebound.artifact_refs == [
        "artifact://golden/run-001/diagnostic.json"
    ]
    assert rebound.evidence_refs == [
        "evidence://sha256/frozen-before-rebind"
    ]

    completed = runtime.complete_run(
        canonical_run_id,
        evidence_refs=["evidence://sha256/after-rebind"],
    )
    assert completed.run_id == canonical_run_id
    assert completed.workflow_ref == workflow_ref
    assert completed.sandbox_ref == rebound.sandbox_ref
    assert completed.evidence_refs == [
        "evidence://sha256/frozen-before-rebind",
        "evidence://sha256/after-rebind",
    ]
