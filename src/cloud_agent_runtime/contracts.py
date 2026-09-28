from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping


@dataclass(frozen=True)
class ArtifactRef:
    uri: str
    media_type: str
    checksum: str | None = None
    version: str | None = None


@dataclass(frozen=True)
class EvidenceRef:
    uri: str
    media_type: str
    source: str
    checksum: str | None = None
    immutable: bool = True


class ApprovalStatus(str, Enum):
    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"


@dataclass
class Approval:
    approval_id: str
    action: str
    evidence_refs: tuple[str, ...] = ()
    status: ApprovalStatus = ApprovalStatus.PENDING
    actor: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class Budget:
    max_tool_calls: int | None = None
    max_model_tokens: int | None = None
    max_cost_usd: float | None = None
    max_wall_seconds: int | None = None


@dataclass(frozen=True)
class CapabilityBinding:
    name: str
    kind: str
    provider: str
    endpoint_ref: str | None = None
    mode: str = "read-only"
    config: Mapping[str, str] = field(default_factory=dict)
