from __future__ import annotations

from uuid import uuid4

from .models import Run, RunStatus, Session, SessionStatus
from .store import InMemoryStore


class AgentRuntime:
    """Minimal reference state machine for Session and Run lifecycle."""

    def __init__(self, store: InMemoryStore | None = None) -> None:
        self.store = store or InMemoryStore()

    def create_session(
        self,
        *,
        agent_id: str,
        release_id: str,
        tenant_id: str,
    ) -> Session:
        session = Session(
            session_id=str(uuid4()),
            agent_id=agent_id,
            release_id=release_id,
            tenant_id=tenant_id,
        )
        self.store.save_session(session)
        return session

    def start_run(
        self,
        *,
        session_id: str,
        sandbox_ref: str | None = None,
    ) -> Run:
        session = self.store.get_session(session_id)
        if session.status != SessionStatus.ACTIVE:
            raise ValueError("runs can only start on an active session")

        run = Run(
            run_id=str(uuid4()),
            session_id=session_id,
            status=RunStatus.RUNNING,
            sandbox_ref=sandbox_ref,
        )
        self.store.save_run(run)
        return run

    def pause_session(self, session_id: str) -> Session:
        session = self.store.get_session(session_id)
        session.status = SessionStatus.PAUSED
        self.store.save_session(session)
        return session

    def resume_session(self, session_id: str) -> Session:
        session = self.store.get_session(session_id)
        if session.status == SessionStatus.CLOSED:
            raise ValueError("closed session cannot be resumed")
        session.status = SessionStatus.ACTIVE
        self.store.save_session(session)
        return session

    def complete_run(
        self,
        run_id: str,
        *,
        artifact_refs: list[str] | None = None,
        evidence_refs: list[str] | None = None,
    ) -> Run:
        run = self.store.get_run(run_id)
        if run.status != RunStatus.RUNNING:
            raise ValueError("only running runs can complete")

        run.status = RunStatus.SUCCEEDED
        run.artifact_refs.extend(artifact_refs or [])
        run.evidence_refs.extend(evidence_refs or [])
        self.store.save_run(run)
        return run
