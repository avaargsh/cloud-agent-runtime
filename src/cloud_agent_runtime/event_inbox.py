from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import sqlite3


EVENT_SCHEMA_VERSION = 1
_TERMINAL_RUN_STATUSES = frozenset({"succeeded", "failed", "cancelled"})


class EventConflictError(ValueError):
    """An event ID has already been bound to different immutable content."""


@dataclass(frozen=True)
class EventEnvelope:
    """Versioned, reference-only external event bound to a canonical Run."""

    event_id: str
    tenant_id: str
    run_id: str
    event_type: str
    correlation_id: str
    causation_id: str | None = None
    payload_ref: str | None = None
    evidence_refs: tuple[str, ...] = ()
    schema_version: int = EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported external event schema_version")
        for field_name in ("event_id", "tenant_id", "run_id", "event_type", "correlation_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        for field_name in ("causation_id", "payload_ref"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{field_name} must be a non-empty string when set")
        if not isinstance(self.evidence_refs, tuple) or any(
            not isinstance(ref, str) or not ref.strip() for ref in self.evidence_refs
        ):
            raise ValueError("evidence_refs must be a tuple of non-empty strings")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> EventEnvelope:
        return cls(
            event_id=payload["event_id"],
            tenant_id=payload["tenant_id"],
            run_id=payload["run_id"],
            event_type=payload["event_type"],
            correlation_id=payload["correlation_id"],
            causation_id=payload.get("causation_id"),
            payload_ref=payload.get("payload_ref"),
            evidence_refs=tuple(payload.get("evidence_refs", ())),
            schema_version=payload["schema_version"],
        )


class SQLiteEventInbox:
    """Durable event ingestion, not an automatic Temporal signal dispatcher.

    The database must already contain the RuntimeStore's canonical runs and
    sessions tables. No event is forwarded to a Worker by this class.
    """

    def __init__(self, path: str | Path) -> None:
        self._connection = sqlite3.connect(str(path), timeout=30)
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS external_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                run_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            "CREATE INDEX IF NOT EXISTS external_events_run_sequence "
            "ON external_events(run_id, sequence)"
        )
        self._connection.commit()

    def _verify_owner(self, run_id: str, tenant_id: str) -> dict:
        row = self._connection.execute(
            "SELECT payload FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise PermissionError("external event target is unavailable")
        run = json.loads(row[0])
        session_id = run["session_id"]
        session_row = self._connection.execute(
            "SELECT payload FROM sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        if session_row is None or json.loads(session_row[0]).get("tenant_id") != tenant_id:
            raise PermissionError("external event target is unavailable")
        return run

    def accept(self, event: EventEnvelope) -> bool:
        """Persist once; exact replays return False and changed replays fail.

        BEGIN IMMEDIATE serializes ownership checks and event insertion across
        independent processes using the same SQLite database.
        """
        payload = json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":"))
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            run = self._verify_owner(event.run_id, event.tenant_id)
            existing = self._connection.execute(
                "SELECT payload FROM external_events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            if existing is not None:
                if existing[0] != payload:
                    raise EventConflictError("event_id conflicts with an existing event")
                return False
            if run["status"] in _TERMINAL_RUN_STATUSES:
                raise ValueError("cannot ingest a new event for a terminal Run")
            self._connection.execute(
                "INSERT INTO external_events(event_id, run_id, tenant_id, payload) "
                "VALUES (?, ?, ?, ?)",
                (event.event_id, event.run_id, event.tenant_id, payload),
            )
            return True

    def list_events(
        self, *, run_id: str, tenant_id: str, limit: int = 100
    ) -> list[EventEnvelope]:
        if type(limit) is not int or not (1 <= limit <= 1000):
            raise ValueError("limit must be an integer between 1 and 1000")
        self._verify_owner(run_id, tenant_id)
        rows = self._connection.execute(
            "SELECT payload FROM external_events "
            "WHERE run_id = ? AND tenant_id = ? ORDER BY sequence LIMIT ?",
            (run_id, tenant_id, limit),
        ).fetchall()
        return [EventEnvelope.from_dict(json.loads(row[0])) for row in rows]

    def close(self) -> None:
        self._connection.close()
