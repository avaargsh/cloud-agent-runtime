from __future__ import annotations

from copy import deepcopy
import json
import sqlite3
from pathlib import Path
from typing import Protocol

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


class InMemoryStore:
    def __init__(self) -> None:
        self.sessions: dict[str, Session] = {}
        self.runs: dict[str, Run] = {}

    def save_session(self, session: Session) -> None:
        self.sessions[session.session_id] = deepcopy(session)

    def get_session(self, session_id: str) -> Session:
        try:
            return deepcopy(self.sessions[session_id])
        except KeyError as exc:
            raise KeyError(f"unknown session: {session_id}") from exc

    def save_run(self, run: Run) -> None:
        self.runs[run.run_id] = deepcopy(run)

    def get_run(self, run_id: str) -> Run:
        try:
            return deepcopy(self.runs[run_id])
        except KeyError as exc:
            raise KeyError(f"unknown run: {run_id}") from exc


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
        self._connection.execute(
            """
            INSERT INTO sessions(session_id, payload)
            VALUES (?, ?)
            ON CONFLICT(session_id)
            DO UPDATE SET payload = excluded.payload
            """,
            (session.session_id, payload),
        )
        self._connection.commit()

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
        self._connection.execute(
            """
            INSERT INTO runs(run_id, payload)
            VALUES (?, ?)
            ON CONFLICT(run_id)
            DO UPDATE SET payload = excluded.payload
            """,
            (run.run_id, payload),
        )
        self._connection.commit()

    def get_run(self, run_id: str) -> Run:
        row = self._connection.execute(
            "SELECT payload FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()

        if row is None:
            raise KeyError(f"unknown run: {run_id}")

        return run_from_dict(json.loads(row[0]))

    def close(self) -> None:
        self._connection.close()
