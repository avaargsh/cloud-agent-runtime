from __future__ import annotations

from .models import Run, Session


class InMemoryStore:
    def __init__(self) -> None:
        self.sessions: dict[str, Session] = {}
        self.runs: dict[str, Run] = {}

    def save_session(self, session: Session) -> None:
        self.sessions[session.session_id] = session

    def get_session(self, session_id: str) -> Session:
        try:
            return self.sessions[session_id]
        except KeyError as exc:
            raise KeyError(f"unknown session: {session_id}") from exc

    def save_run(self, run: Run) -> None:
        self.runs[run.run_id] = run

    def get_run(self, run_id: str) -> Run:
        try:
            return self.runs[run_id]
        except KeyError as exc:
            raise KeyError(f"unknown run: {run_id}") from exc
