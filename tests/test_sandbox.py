from cloud_agent_runtime.sandbox import (
    InMemorySandboxProvider,
    SandboxStatus,
)


def test_snapshot_and_resume() -> None:
    provider = InMemorySandboxProvider()
    sandbox = provider.allocate()
    provider.bind(sandbox, session_id="session-1")

    paused = provider.snapshot(sandbox)

    assert paused.status == SandboxStatus.PAUSED
    assert paused.snapshot_ref is not None

    resumed = provider.resume(
        paused.snapshot_ref,
        session_id="session-1",
    )

    assert resumed.status == SandboxStatus.BOUND
    assert resumed.session_id == "session-1"
