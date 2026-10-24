from dataclasses import replace

import pytest

from cloud_agent_runtime import Run, RunStatus, Session, SQLiteStore
from cloud_agent_runtime.event_inbox import (
    EventConflictError,
    EventEnvelope,
    SQLiteEventInbox,
)


def seeded_database(tmp_path):
    path = tmp_path / "event-inbox.sqlite"
    store = SQLiteStore(path)
    store.save_session(Session("session-a", "agent", "release", "tenant-a"))
    store.save_session(Session("session-b", "agent", "release", "tenant-b"))
    store.save_run(Run("run-a", "session-a", status=RunStatus.RUNNING))
    store.save_run(Run("run-b", "session-b", status=RunStatus.RUNNING))
    return store, path


def envelope(event_id="e-1", *, run_id="run-a", tenant_id="tenant-a"):
    return EventEnvelope(
        event_id=event_id,
        run_id=run_id,
        tenant_id=tenant_id,
        event_type="human.reviewed",
        correlation_id="task-123",
        causation_id="request-456",
        payload_ref="evidence://sha256/payload",
        evidence_refs=("evidence://sha256/decision",),
    )


def test_event_is_durable_ordered_and_exact_retries_are_idempotent(tmp_path):
    store, path = seeded_database(tmp_path)
    first = envelope()
    inbox = SQLiteEventInbox(path)
    assert inbox.accept(first) is True
    assert inbox.accept(first) is False
    assert inbox.accept(envelope("e-2")) is True
    inbox.close()

    reopened = SQLiteEventInbox(path)
    assert reopened.accept(first) is False
    assert reopened.list_events(run_id="run-a", tenant_id="tenant-a") == [
        first,
        envelope("e-2"),
    ]
    assert reopened.list_events(run_id="run-a", tenant_id="tenant-a", limit=1) == [first]
    reopened.close()
    store.close()


def test_event_id_collision_with_different_content_is_rejected(tmp_path):
    store, path = seeded_database(tmp_path)
    inbox = SQLiteEventInbox(path)
    first = envelope()
    assert inbox.accept(first)
    for modified in (
        replace(first, event_type="human.rejected"),
        replace(first, payload_ref="evidence://sha256/different"),
        replace(first, correlation_id="different-task"),
        envelope("e-1", run_id="run-b", tenant_id="tenant-b"),
    ):
        with pytest.raises((EventConflictError, PermissionError)):
            inbox.accept(modified)
    assert inbox.list_events(run_id="run-a", tenant_id="tenant-a") == [first]
    inbox.close()
    store.close()


def test_cross_tenant_run_and_unknown_run_fail_closed(tmp_path):
    store, path = seeded_database(tmp_path)
    inbox = SQLiteEventInbox(path)
    for target in (
        envelope("untrusted", run_id="run-a", tenant_id="tenant-b"),
        envelope("missing", run_id="nonexistent", tenant_id="tenant-a"),
    ):
        with pytest.raises(PermissionError, match="target is unavailable"):
            inbox.accept(target)
    with pytest.raises(PermissionError):
        inbox.list_events(run_id="run-b", tenant_id="tenant-a")
    assert inbox.list_events(run_id="run-a", tenant_id="tenant-a") == []
    inbox.close()
    store.close()


def test_terminal_run_rejects_new_events_but_preserves_old_retry(tmp_path):
    store, path = seeded_database(tmp_path)
    inbox = SQLiteEventInbox(path)
    first = envelope()
    assert inbox.accept(first)
    run = store.get_run("run-a")
    run.status = RunStatus.SUCCEEDED
    store.save_run(run)
    assert inbox.accept(first) is False
    with pytest.raises(ValueError, match="terminal Run"):
        inbox.accept(envelope("new-event"))
    assert inbox.list_events(run_id="run-a", tenant_id="tenant-a") == [first]
    inbox.close()
    store.close()


def test_independent_connections_do_not_duplicate_events(tmp_path):
    store, path = seeded_database(tmp_path)
    left, right = SQLiteEventInbox(path), SQLiteEventInbox(path)
    assert left.accept(envelope()) is True
    assert right.accept(envelope()) is False
    assert len(right.list_events(run_id="run-a", tenant_id="tenant-a")) == 1
    left.close()
    right.close()
    store.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": ""},
        {"tenant_id": ""},
        {"event_type": ""},
        {"correlation_id": " "},
        {"schema_version": 2},
        {"schema_version": True},
        {"causation_id": ""},
        {"payload_ref": ""},
        {"evidence_refs": ("",)},
    ],
)
def test_rejects_invalid_event_envelopes(changes):
    with pytest.raises(ValueError):
        replace(envelope(), **changes)


@pytest.mark.parametrize("limit", [0, -1, 1001, True, 1.5])
def test_rejects_invalid_query_limits(tmp_path, limit):
    store, path = seeded_database(tmp_path)
    inbox = SQLiteEventInbox(path)
    with pytest.raises(ValueError, match="limit"):
        inbox.list_events(run_id="run-a", tenant_id="tenant-a", limit=limit)
    inbox.close()
    store.close()
