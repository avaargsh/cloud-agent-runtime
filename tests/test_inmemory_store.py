from cloud_agent_runtime import InMemoryStore, Run, Session


def test_inmemory_store_save_is_snapshot_not_live_reference():
    store = InMemoryStore()
    session = Session(
        session_id="session-1",
        agent_id="agent-1",
        release_id="release-1",
        tenant_id="tenant-1",
    )
    run = Run(
        run_id="run-1",
        session_id=session.session_id,
    )

    store.save_session(session)
    store.save_run(run)

    session.provider_refs["sandbox"] = "sandbox://mutated"
    run.evidence_refs.append("evidence://mutated")

    persisted_session = store.get_session(session.session_id)
    persisted_run = store.get_run(run.run_id)

    assert persisted_session.provider_refs == {}
    assert persisted_run.evidence_refs == []


def test_inmemory_store_get_returns_detached_copy():
    store = InMemoryStore()
    session = Session(
        session_id="session-1",
        agent_id="agent-1",
        release_id="release-1",
        tenant_id="tenant-1",
    )
    run = Run(
        run_id="run-1",
        session_id=session.session_id,
    )
    store.save_session(session)
    store.save_run(run)

    loaded_session = store.get_session(session.session_id)
    loaded_run = store.get_run(run.run_id)
    loaded_session.state_refs["checkpoint"] = "state://changed"
    loaded_run.artifact_refs.append("artifact://changed")

    assert store.get_session(session.session_id).state_refs == {}
    assert store.get_run(run.run_id).artifact_refs == []
