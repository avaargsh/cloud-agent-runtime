from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import json
import sqlite3
from pathlib import Path
import time
from typing import Protocol

from .models import Run, Session
from .serialization import (
    run_from_dict,
    run_to_dict,
    session_from_dict,
    session_to_dict,
)


@dataclass(frozen=True)
class SandboxAllocationIntent:
    """Durable ownership record for a Sandbox allocated before Run binding."""

    run_id: str
    session_id: str
    provider: str
    sandbox_id: str
    state: str = "allocating"
    updated_at: float = 0.0

    def __post_init__(self) -> None:
        if self.state not in {"allocating", "cleanup_pending"}:
            raise ValueError(f"unsupported allocation intent state: {self.state}")
        if not self.updated_at:
            object.__setattr__(self, "updated_at", time.time())


class RuntimeStore(Protocol):
    def save_session(self, session: Session) -> None:
        ...

    def get_session(self, session_id: str) -> Session:
        ...

    def save_run(self, run: Run) -> None:
        ...

    def get_run(self, run_id: str) -> Run:
        ...


class SandboxAllocationIntentStore(Protocol):
    """Optional durable-storage capability required by recoverable-ID providers.

    Keeping this separate lets existing RuntimeStore implementations continue
    to serve Providers that do not support caller-assigned recoverable IDs.
    Runtime checks the capability before the Provider can create resources.
    """

    def save_allocation_intent(self, intent: SandboxAllocationIntent) -> None:
        ...

    def list_allocation_intents(self) -> list[SandboxAllocationIntent]:
        ...

    def delete_allocation_intent(self, provider: str, sandbox_id: str) -> None:
        ...


class InMemoryStore:
    def __init__(self) -> None:
        self.sessions: dict[str, Session] = {}
        self.runs: dict[str, Run] = {}
        self.allocation_intents: dict[tuple[str, str], SandboxAllocationIntent] = {}

    def save_session(self, session: Session) -> None:
        existing = self.sessions.get(session.session_id)
        if existing is not None and (
            existing.agent_id != session.agent_id
            or existing.release_id != session.release_id
            or existing.tenant_id != session.tenant_id
        ):
            raise ValueError(
                "session identity is immutable: agent, release, and tenant "
                "must match the existing session"
            )
        self.sessions[session.session_id] = deepcopy(session)

    def get_session(self, session_id: str) -> Session:
        try:
            return deepcopy(self.sessions[session_id])
        except KeyError as exc:
            raise KeyError(f"unknown session: {session_id}") from exc

    def save_run(self, run: Run) -> None:
        existing = self.runs.get(run.run_id)
        if existing is not None and existing.session_id != run.session_id:
            raise ValueError(
                "run identity is immutable: session_id must match the existing run"
            )
        self.runs[run.run_id] = deepcopy(run)

    def get_run(self, run_id: str) -> Run:
        try:
            return deepcopy(self.runs[run_id])
        except KeyError as exc:
            raise KeyError(f"unknown run: {run_id}") from exc

    def save_allocation_intent(self, intent: SandboxAllocationIntent) -> None:
        key = (intent.provider, intent.sandbox_id)
        existing = self.allocation_intents.get(key)
        if existing is not None and (
            existing.run_id != intent.run_id
            or existing.session_id != intent.session_id
        ):
            raise ValueError("sandbox allocation identity cannot be rebound")
        self.allocation_intents[key] = deepcopy(intent)

    def list_allocation_intents(self) -> list[SandboxAllocationIntent]:
        return [deepcopy(item) for item in self.allocation_intents.values()]

    def delete_allocation_intent(self, provider: str, sandbox_id: str) -> None:
        self.allocation_intents.pop((provider, sandbox_id), None)


class SQLiteStore:
    """Small durable reference store for local runtime experiments."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._connection = sqlite3.connect(self.path)
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sandbox_allocation_intents (
                provider TEXT NOT NULL,
                sandbox_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('allocating', 'cleanup_pending')),
                updated_at REAL NOT NULL,
                PRIMARY KEY (provider, sandbox_id)
            )
            """
        )
        self._connection.commit()

    def save_session(self, session: Session) -> None:
        payload = json.dumps(
            session_to_dict(session),
            separators=(",", ":"),
            sort_keys=True,
        )
        # Serialize the read/validate/write sequence so concurrent callers
        # cannot rebind an existing canonical session ID to another identity.
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            row = self._connection.execute(
                "SELECT payload FROM sessions WHERE session_id = ?",
                (session.session_id,),
            ).fetchone()
            if row is not None:
                existing = session_from_dict(json.loads(row[0]))
                if (
                    existing.agent_id != session.agent_id
                    or existing.release_id != session.release_id
                    or existing.tenant_id != session.tenant_id
                ):
                    raise ValueError(
                        "session identity is immutable: agent, release, and "
                        "tenant must match the existing session"
                    )
            self._connection.execute(
                """
                INSERT INTO sessions(session_id, payload)
                VALUES (?, ?)
                ON CONFLICT(session_id)
                DO UPDATE SET payload = excluded.payload
                """,
                (session.session_id, payload),
            )

    def get_session(self, session_id: str) -> Session:
        row = self._connection.execute(
            "SELECT payload FROM sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()

        if row is None:
            raise KeyError(f"unknown session: {session_id}")

        return session_from_dict(json.loads(row[0]))

    def save_run(self, run: Run) -> None:
        payload = json.dumps(
            run_to_dict(run),
            separators=(",", ":"),
            sort_keys=True,
        )
        # A canonical Run may advance state and bindings, but it cannot be
        # reassigned to another Session. BEGIN IMMEDIATE makes validation and
        # upsert one serialized transaction across SQLite connections.
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            row = self._connection.execute(
                "SELECT payload FROM runs WHERE run_id = ?",
                (run.run_id,),
            ).fetchone()
            if row is not None:
                existing = run_from_dict(json.loads(row[0]))
                if existing.session_id != run.session_id:
                    raise ValueError(
                        "run identity is immutable: session_id must match "
                        "the existing run"
                    )
            self._connection.execute(
                """
                INSERT INTO runs(run_id, payload)
                VALUES (?, ?)
                ON CONFLICT(run_id)
                DO UPDATE SET payload = excluded.payload
                """,
                (run.run_id, payload),
            )

    def get_run(self, run_id: str) -> Run:
        row = self._connection.execute(
            "SELECT payload FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()

        if row is None:
            raise KeyError(f"unknown run: {run_id}")

        return run_from_dict(json.loads(row[0]))

    def save_allocation_intent(self, intent: SandboxAllocationIntent) -> None:
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            row = self._connection.execute(
                """
                SELECT run_id, session_id
                FROM sandbox_allocation_intents
                WHERE provider = ? AND sandbox_id = ?
                """,
                (intent.provider, intent.sandbox_id),
            ).fetchone()
            if row is not None and row != (intent.run_id, intent.session_id):
                raise ValueError("sandbox allocation identity cannot be rebound")
            self._connection.execute(
                """
                INSERT INTO sandbox_allocation_intents
                    (provider, sandbox_id, run_id, session_id, state, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, sandbox_id) DO UPDATE SET
                    state = excluded.state,
                    updated_at = excluded.updated_at
                """,
                (
                    intent.provider,
                    intent.sandbox_id,
                    intent.run_id,
                    intent.session_id,
                    intent.state,
                    intent.updated_at,
                ),
            )

    def list_allocation_intents(self) -> list[SandboxAllocationIntent]:
        rows = self._connection.execute(
            """
            SELECT run_id, session_id, provider, sandbox_id, state, updated_at
            FROM sandbox_allocation_intents
            ORDER BY updated_at, provider, sandbox_id
            """
        ).fetchall()
        return [
            SandboxAllocationIntent(
                run_id=row[0],
                session_id=row[1],
                provider=row[2],
                sandbox_id=row[3],
                state=row[4],
                updated_at=row[5],
            )
            for row in rows
        ]

    def delete_allocation_intent(self, provider: str, sandbox_id: str) -> None:
        with self._connection:
            self._connection.execute(
                """
                DELETE FROM sandbox_allocation_intents
                WHERE provider = ? AND sandbox_id = ?
                """,
                (provider, sandbox_id),
            )

    def close(self) -> None:
        self._connection.close()
