from pathlib import Path

from cloud_agent_runtime import (
    AgentRuntime,
    ApprovalStatus,
    Budget,
    CapabilityBinding,
    InMemoryWorkflowDriver,
    SQLiteStore,
)


def test_sqlite_round_trip_preserves_runtime_state(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "runtime.db")
    runtime = AgentRuntime(
        store=store,
        workflow_driver=InMemoryWorkflowDriver(),
    )

    session = runtime.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
        capabilities=[
            CapabilityBinding(
                name="repo",
                kind="mcp",
                provider="github",
                endpoint_ref="mcp://github",
                mode="read-write",
            )
        ],
    )
    run = runtime.start_run(
        session_id=session.session_id,
        sandbox_ref="sandbox://external/demo",
        budget=Budget(
            max_tool_calls=10,
            max_model_tokens=20_000,
            max_cost_usd=2.5,
        ),
    )
    approval = runtime.request_approval(
        run.run_id,
        action="merge pull request",
    )

    # Re-open through a second store instance to prove persistence is not
    # relying on in-memory object identity.
    store.close()
    reopened = SQLiteStore(tmp_path / "runtime.db")

    loaded_session = reopened.get_session(session.session_id)
    loaded_run = reopened.get_run(run.run_id)

    assert loaded_session.capability_bindings["repo"].provider == "github"
    assert loaded_run.budget is not None
    assert loaded_run.budget.max_tool_calls == 10
    assert loaded_run.workflow_ref is not None
    assert loaded_run.approvals[0].approval_id == approval.approval_id
    assert loaded_run.approvals[0].status == ApprovalStatus.PENDING

    reopened.close()
