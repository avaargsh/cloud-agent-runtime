from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping

from .contracts import (
    Approval,
    ArtifactRef,
    Budget,
    CapabilityBinding,
    EvidenceRef,
)
from .workflow import WorkflowRef


class SessionStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    CLOSED = "closed"


class RunStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
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
    capability_bindings: dict[str, CapabilityBinding] = field(default_factory=dict)


@dataclass
class SandboxBinding:
    """Durable binding between a canonical Run and disposable sandbox execution."""

    provider: str
    sandbox_id: str
    sandbox_ref: str
    revision: int = 1
    snapshot_ref: str | None = None
    previous_refs: list[str] = field(default_factory=list)
    pending_cleanup_refs: list[str] = field(default_factory=list)
    last_rebind_key: str | None = None


@dataclass
class Run:
    run_id: str
    session_id: str
    status: RunStatus = RunStatus.CREATED
    sandbox_ref: str | None = None
    sandbox_snapshot_ref: str | None = None
    sandbox_binding: SandboxBinding | None = None
    workflow_ref: WorkflowRef | None = None
    artifact_refs: list[ArtifactRef | str] = field(default_factory=list)
    evidence_refs: list[EvidenceRef | str] = field(default_factory=list)
    approvals: list[Approval] = field(default_factory=list)
    budget: Budget | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)
