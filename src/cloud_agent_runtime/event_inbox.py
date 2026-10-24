from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
import sqlite3

from .workflow import WorkflowRef


EVENT_SCHEMA_VERSION = 1
_TERMINAL_RUN_STATUSES = frozenset({"succeeded", "failed", "cancelled"})


class EventConflictError(ValueError):
    """An event ID has already been bound to different immutable content."""


@dataclass(frozen=True)
class EventRetryState:
    attempts: int
    next_attempt_at: float
    last_error_class: str
    blocked: bool


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
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS external_event_receipts (
                event_id TEXT PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                workflow_run_id TEXT
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS external_event_delivery_claims (
                event_id TEXT PRIMARY KEY,
                owner_token TEXT NOT NULL,
                expires_at INTEGER NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS external_event_retries (
                event_id TEXT PRIMARY KEY,
                attempts INTEGER NOT NULL CHECK (attempts >= 1),
                next_attempt_at REAL NOT NULL,
                last_error_class TEXT NOT NULL,
                blocked INTEGER NOT NULL DEFAULT 0
            )
            """
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

    def bound_workflow(self, *, run_id: str, tenant_id: str) -> tuple[WorkflowRef, str]:
        """Read canonical workflow reference; never derive it from untrusted input."""
        run = self._verify_owner(run_id, tenant_id)
        binding = run.get("workflow_ref")
        if (
            not isinstance(binding, dict)
            or binding.get("provider") != "temporal"
            or not isinstance(binding.get("workflow_id"), str)
            or not binding["workflow_id"].strip()
            or not isinstance(binding.get("run_id"), str)
            or not binding["run_id"].strip()
        ):
            raise PermissionError("canonical Temporal workflow binding is unavailable")
        return WorkflowRef(**binding), run["session_id"]

    def list_pending(
        self, *, run_id: str, tenant_id: str, limit: int = 100
    ) -> list[EventEnvelope]:
        if type(limit) is not int or not (1 <= limit <= 1000):
            raise ValueError("limit must be an integer between 1 and 1000")
        self._verify_owner(run_id, tenant_id)
        rows = self._connection.execute(
            "SELECT e.payload FROM external_events e "
            "LEFT JOIN external_event_receipts r ON r.event_id = e.event_id "
            "WHERE e.run_id = ? AND e.tenant_id = ? AND r.event_id IS NULL "
            "ORDER BY e.sequence LIMIT ?",
            (run_id, tenant_id, limit),
        ).fetchall()
        return [EventEnvelope.from_dict(json.loads(row[0])) for row in rows]

    def _assert_stored_event(self, event: EventEnvelope) -> None:
        self._verify_owner(event.run_id, event.tenant_id)
        row = self._connection.execute(
            "SELECT payload FROM external_events "
            "WHERE event_id = ? AND run_id = ? AND tenant_id = ?",
            (event.event_id, event.run_id, event.tenant_id),
        ).fetchone()
        expected = json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":"))
        if row is None or row[0] != expected:
            raise EventConflictError("retry state is not bound to stored event")

    def get_retry_state(self, event: EventEnvelope) -> EventRetryState | None:
        """Read durable retry eligibility for the exact immutable event."""
        self._assert_stored_event(event)
        row = self._connection.execute(
            "SELECT attempts, next_attempt_at, last_error_class, blocked "
            "FROM external_event_retries WHERE event_id = ?",
            (event.event_id,),
        ).fetchone()
        if row is None:
            return None
        return EventRetryState(row[0], row[1], row[2], bool(row[3]))

    def record_retry_failure(
        self,
        event: EventEnvelope,
        *,
        next_attempt_at: float,
        last_error_class: str,
        blocked: bool = False,
    ) -> EventRetryState:
        """Retain an unconfirmed event's next retry eligibility atomically."""
        if (
            isinstance(next_attempt_at, bool)
            or not isinstance(next_attempt_at, (int, float))
            or not math.isfinite(next_attempt_at)
            or next_attempt_at < 0
        ):
            raise ValueError("next_attempt_at must be a nonnegative finite timestamp")
        if not isinstance(last_error_class, str) or not last_error_class.strip():
            raise ValueError("last_error_class must be nonempty")
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            self._assert_stored_event(event)
            receipt = self._connection.execute(
                "SELECT 1 FROM external_event_receipts WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            if receipt is not None:
                raise EventConflictError("already-delivered event cannot be scheduled")
            self._connection.execute(
                "INSERT INTO external_event_retries "
                "(event_id, attempts, next_attempt_at, last_error_class, blocked) "
                "VALUES (?, 1, ?, ?, ?) "
                "ON CONFLICT(event_id) DO UPDATE SET "
                "attempts = attempts + 1, next_attempt_at = excluded.next_attempt_at, "
                "last_error_class = excluded.last_error_class, blocked = excluded.blocked",
                (event.event_id, float(next_attempt_at), last_error_class, int(blocked)),
            )
            row = self._connection.execute(
                "SELECT attempts, next_attempt_at, last_error_class, blocked "
                "FROM external_event_retries WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
        assert row is not None
        return EventRetryState(row[0], row[1], row[2], bool(row[3]))

    def reset_retry_state(self, event: EventEnvelope) -> None:
        """Trusted operator action; no delivery receipt is removed."""
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            self._assert_stored_event(event)
            self._connection.execute(
                "DELETE FROM external_event_retries WHERE event_id = ?",
                (event.event_id,),
            )

    def try_claim(
        self, event: EventEnvelope, *, owner_token: str, lease_seconds: int = 60
    ) -> bool:
        """Atomically claim an unacknowledged event across SQLite connections.

        An expired worker may overlap a successor; Workflow event-id deduplication
        supplies logical idempotency, not exactly-once Signal transport.
        """
        if not isinstance(owner_token, str) or not owner_token.strip():
            raise ValueError("owner_token must be non-empty")
        if type(lease_seconds) is not int or not (1 <= lease_seconds <= 3600):
            raise ValueError("lease_seconds must be an integer between 1 and 3600")
        payload = json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":"))
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            self._verify_owner(event.run_id, event.tenant_id)
            row = self._connection.execute(
                "SELECT payload FROM external_events WHERE event_id = ? "
                "AND run_id = ? AND tenant_id = ?",
                (event.event_id, event.run_id, event.tenant_id),
            ).fetchone()
            if row is None or row[0] != payload:
                raise EventConflictError("delivery claim is not bound to stored event")
            if self._connection.execute(
                "SELECT 1 FROM external_event_receipts WHERE event_id = ?",
                (event.event_id,),
            ).fetchone():
                return False
            cursor = self._connection.execute(
                "INSERT INTO external_event_delivery_claims "
                "(event_id, owner_token, expires_at) "
                "VALUES (?, ?, CAST(strftime('%s', 'now') AS INTEGER) + ?) "
                "ON CONFLICT(event_id) DO UPDATE SET "
                "owner_token = excluded.owner_token, "
                "expires_at = excluded.expires_at "
                "WHERE external_event_delivery_claims.expires_at <= "
                "CAST(strftime('%s', 'now') AS INTEGER)",
                (event.event_id, owner_token, lease_seconds),
            )
            return cursor.rowcount == 1

    def release_claim(self, event: EventEnvelope, *, owner_token: str) -> bool:
        """Release only the caller's claim, never a replacement worker's claim."""
        if not isinstance(owner_token, str) or not owner_token.strip():
            raise ValueError("owner_token must be non-empty")
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            self._verify_owner(event.run_id, event.tenant_id)
            cursor = self._connection.execute(
                "DELETE FROM external_event_delivery_claims "
                "WHERE event_id = ? AND owner_token = ?",
                (event.event_id, owner_token),
            )
            return cursor.rowcount == 1

    def mark_delivered(
        self, event: EventEnvelope, workflow: WorkflowRef, *, owner_token: str
    ) -> None:
        """Persist a receipt only after the owning workflow has confirmed it.

        The caller must verify the server-observed event first; an RPC Signal ACK
        alone is insufficient. This method checks immutable local ownership.
        """
        payload = json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":"))
        if not isinstance(owner_token, str) or not owner_token.strip():
            raise ValueError("owner_token must be non-empty")
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            run = self._verify_owner(event.run_id, event.tenant_id)
            binding = run.get("workflow_ref")
            if (
                not isinstance(binding, dict)
                or binding.get("provider") != workflow.provider
                or binding.get("workflow_id") != workflow.workflow_id
                or binding.get("run_id") != workflow.run_id
            ):
                raise PermissionError("canonical Temporal workflow binding changed")
            claim = self._connection.execute(
                "SELECT owner_token, expires_at FROM external_event_delivery_claims "
                "WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            now = self._connection.execute(
                "SELECT CAST(strftime('%s', 'now') AS INTEGER)"
            ).fetchone()[0]
            if claim is None or claim[0] != owner_token or claim[1] <= now:
                raise PermissionError("delivery claim is missing, expired or owned elsewhere")
            row = self._connection.execute(
                "SELECT payload FROM external_events WHERE event_id = ? "
                "AND run_id = ? AND tenant_id = ?",
                (event.event_id, event.run_id, event.tenant_id),
            ).fetchone()
            if row is None or row[0] != payload:
                raise EventConflictError("delivery receipt is not bound to stored event")
            existing = self._connection.execute(
                "SELECT workflow_id, workflow_run_id FROM external_event_receipts "
                "WHERE event_id = ?", (event.event_id,)
            ).fetchone()
            receipt = (workflow.workflow_id, workflow.run_id)
            if existing is not None:
                if tuple(existing) != receipt:
                    raise EventConflictError("delivery receipt has conflicting workflow")
                self._connection.execute(
                    "DELETE FROM external_event_retries WHERE event_id = ?",
                    (event.event_id,),
                )
                return
            self._connection.execute(
                "INSERT INTO external_event_receipts "
                "(event_id, workflow_id, workflow_run_id) VALUES (?, ?, ?)",
                (event.event_id, *receipt),
            )
            self._connection.execute(
                "DELETE FROM external_event_delivery_claims WHERE event_id = ? "
                "AND owner_token = ?",
                (event.event_id, owner_token),
            )
            self._connection.execute(
                "DELETE FROM external_event_retries WHERE event_id = ?",
                (event.event_id,),
            )

    def close(self) -> None:
        self._connection.close()
