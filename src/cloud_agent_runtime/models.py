from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping


class SessionStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    CLOSED = "closed"


class RunStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class Session:
    session_id: str
    agent_id: str
    release_id: str
    tenant_id: str
    status: SessionStatus = SessionStatus.ACTIVE
    provider_refs: dict[str, str] = field(default_factory=dict)
    state_refs: dict[str, str] = field(default_factory=dict)


@dataclass
class Run:
    run_id: str
    session_id: str
    status: RunStatus = RunStatus.CREATED
    sandbox_ref: str | None = None
    artifact_refs: list[str] = field(default_factory=list)
    evidence_refs: list[str] = field(default_factory=list)
    metadata: Mapping[str, str] = field(default_factory=dict)
