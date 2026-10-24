from __future__ import annotations

from copy import deepcopy
import json
import sqlite3
from pathlib import Path
from threading import RLock
from typing import Any, Protocol

from .models import Run, Session
from .serialization import (
    run_from_dict,
    run_to_dict,
    session_from_dict,
    session_to_dict,
)


class RuntimeStore(Protocol):
    def save_session(self, session: Session) -> None:
        ...

    def get_session(self, session_id: str) -> Session:
        ...

    def save_run(self, run: Run) -> None:
        ...

    def get_run(self, run_id: str) -> Run:
        ...
    def persist_compute_request(
        self,
        run_id: str,
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        """Atomically persist one immutable request intent before side effects."""
        ...


def _same_compute_intent(left: dict[str, Any], right: dict[str, Any]) -> bool:
    # requestId and intentDigest are generated from the first committed intent.
    # Racing retries may build different candidates; compare the logical intent
    # and always return the first stored request/idempotency key.
    left_intent = {
        key: value for key, value in left.items()
        if key not in {"requestId", "intentDigest"}
    }
    right_intent = {
        key: value for key, value in right.items()
        if key not in {"requestId", "intentDigest"}
    }
    return left_intent == right_intent




class InMemoryStore:
    def __init__(self) -> None:
        self.sessions: dict[str, Session] = {}
        self.runs: dict[str, Run] = {}
        self._lock = RLock()

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
        with self._lock:
            existing = self.runs.get(run.run_id)
            if existing is not None and existing.session_id != run.session_id:
                raise ValueError(
                    "run identity is immutable: session_id must match the existing run"
                )
            if existing is not None and existing.compute_request != run.compute_request:
                raise ValueError(
                    "compute request intent can only be initialized through atomic persistence"
                )
            self.runs[run.run_id] = deepcopy(run)

    def get_run(self, run_id: str) -> Run:
        with self._lock:
            try:
                return deepcopy(self.runs[run_id])
            except KeyError as exc:
                raise KeyError(f"unknown run: {run_id}") from exc

    def persist_compute_request(
        self,
        run_id: str,
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        with self._lock:
            try:
                run = deepcopy(self.runs[run_id])
            except KeyError as exc:
                raise KeyError(f"unknown run: {run_id}") from exc
            if run.compute_request is not None:
                if not _same_compute_intent(run.compute_request, candidate):
                    raise ValueError("compute request intent is immutable for a Run")
                return deepcopy(run.compute_request)
            run.compute_request = deepcopy(candidate)
            self.runs[run_id] = run
            return deepcopy(run.compute_request)


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
                if existing.compute_request != run.compute_request:
                    raise ValueError(
                        "compute request intent can only be initialized through atomic persistence"
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

    def persist_compute_request(
        self,
        run_id: str,
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        # Read/compare/write within one serialized DB transaction. If multiple
        # workers race to prepare a first request, the winner's 128-bit key is
        # returned to every retry before anyone can call the Control Plane.
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            row = self._connection.execute(
                "SELECT payload FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown run: {run_id}")
            run = run_from_dict(json.loads(row[0]))
            if run.compute_request is not None:
                if not _same_compute_intent(run.compute_request, candidate):
                    raise ValueError("compute request intent is immutable for a Run")
                return deepcopy(run.compute_request)
            run.compute_request = deepcopy(candidate)
            payload = json.dumps(
                run_to_dict(run),
                separators=(",", ":"),
                sort_keys=True,
            )
            self._connection.execute(
                "UPDATE runs SET payload = ? WHERE run_id = ?",
                (payload, run_id),
            )
            return deepcopy(run.compute_request)

    def close(self) -> None:
        self._connection.close()
