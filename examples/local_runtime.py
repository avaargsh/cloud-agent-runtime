from cloud_agent_runtime import AgentRuntime


runtime = AgentRuntime()

session = runtime.create_session(
    agent_id="codex-agent",
    release_id="codex-agent-v1",
    tenant_id="demo",
)

run = runtime.start_run(
    session_id=session.session_id,
    sandbox_ref="sandbox://local-demo",
)

runtime.complete_run(
    run.run_id,
    artifact_refs=["artifact://example.patch"],
    evidence_refs=["trace://example-run"],
)

print(session)
print(runtime.store.get_run(run.run_id))
