from __future__ import annotations

from typing import Any

from .contracts import (
    Approval,
    ApprovalStatus,
    ArtifactRef,
    Budget,
    CapabilityBinding,
    EvidenceRef,
)
from .models import (
    Run,
    RunStatus,
    SandboxBinding,
    Session,
    SessionStatus,
)
from .workflow import WorkflowRef


def _encode_ref(value: ArtifactRef | EvidenceRef | str) -> dict[str, Any]:
    if isinstance(value, str):
        return {"kind": "string", "value": value}
    if isinstance(value, ArtifactRef):
        return {
            "kind": "artifact",
            "uri": value.uri,
            "media_type": value.media_type,
            "checksum": value.checksum,
            "version": value.version,
        }
    if isinstance(value, EvidenceRef):
        return {
            "kind": "evidence",
            "uri": value.uri,
            "media_type": value.media_type,
            "source": value.source,
            "checksum": value.checksum,
            "immutable": value.immutable,
        }
    raise TypeError(f"unsupported ref: {type(value)!r}")


def _decode_ref(value: dict[str, Any]) -> ArtifactRef | EvidenceRef | str:
    kind = value["kind"]
    if kind == "string":
        return str(value["value"])
    if kind == "artifact":
        return ArtifactRef(
            uri=value["uri"],
            media_type=value["media_type"],
            checksum=value.get("checksum"),
            version=value.get("version"),
        )
    if kind == "evidence":
        return EvidenceRef(
            uri=value["uri"],
            media_type=value["media_type"],
            source=value["source"],
            checksum=value.get("checksum"),
            immutable=bool(value.get("immutable", True)),
        )
    raise ValueError(f"unsupported serialized ref kind: {kind}")


def session_to_dict(session: Session) -> dict[str, Any]:
    return {
        "session_id": session.session_id,
        "agent_id": session.agent_id,
        "release_id": session.release_id,
        "tenant_id": session.tenant_id,
        "status": session.status.value,
        "provider_refs": dict(session.provider_refs),
        "state_refs": dict(session.state_refs),
        "capability_bindings": {
            name: {
                "name": binding.name,
                "kind": binding.kind,
                "provider": binding.provider,
                "endpoint_ref": binding.endpoint_ref,
                "mode": binding.mode,
                "config": dict(binding.config),
            }
            for name, binding in session.capability_bindings.items()
        },
    }


def session_from_dict(data: dict[str, Any]) -> Session:
    return Session(
        session_id=data["session_id"],
        agent_id=data["agent_id"],
        release_id=data["release_id"],
        tenant_id=data["tenant_id"],
        status=SessionStatus(data["status"]),
        provider_refs=dict(data.get("provider_refs", {})),
        state_refs=dict(data.get("state_refs", {})),
        capability_bindings={
            name: CapabilityBinding(**value)
            for name, value in data.get("capability_bindings", {}).items()
        },
    )


def _sandbox_binding_to_dict(
    binding: SandboxBinding | None,
) -> dict[str, Any] | None:
    if binding is None:
        return None
    return {
        "provider": binding.provider,
        "sandbox_id": binding.sandbox_id,
        "sandbox_ref": binding.sandbox_ref,
        "revision": binding.revision,
        "snapshot_ref": binding.snapshot_ref,
        "previous_refs": list(binding.previous_refs),
        "pending_cleanup_refs": list(binding.pending_cleanup_refs),
        "last_rebind_key": binding.last_rebind_key,
    }


def _sandbox_binding_from_dict(
    value: dict[str, Any] | None,
) -> SandboxBinding | None:
    if value is None:
        return None
    return SandboxBinding(
        provider=value["provider"],
        sandbox_id=value["sandbox_id"],
        sandbox_ref=value["sandbox_ref"],
        revision=int(value.get("revision", 1)),
        snapshot_ref=value.get("snapshot_ref"),
        previous_refs=list(value.get("previous_refs", [])),
        pending_cleanup_refs=list(value.get("pending_cleanup_refs", [])),
        last_rebind_key=value.get("last_rebind_key"),
    )


def run_to_dict(run: Run) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "session_id": run.session_id,
        "status": run.status.value,
        "sandbox_ref": run.sandbox_ref,
        "sandbox_snapshot_ref": run.sandbox_snapshot_ref,
        "sandbox_binding": _sandbox_binding_to_dict(run.sandbox_binding),
        "workflow_ref": (
            {
                "provider": run.workflow_ref.provider,
                "workflow_id": run.workflow_ref.workflow_id,
                "run_id": run.workflow_ref.run_id,
            }
            if run.workflow_ref is not None
            else None
        ),
        "artifact_refs": [_encode_ref(value) for value in run.artifact_refs],
        "evidence_refs": [_encode_ref(value) for value in run.evidence_refs],
        "approvals": [
            {
                "approval_id": approval.approval_id,
                "action": approval.action,
                "evidence_refs": list(approval.evidence_refs),
                "status": approval.status.value,
                "actor": approval.actor,
                "reason": approval.reason,
            }
            for approval in run.approvals
        ],
        "budget": (
            {
                "max_tool_calls": run.budget.max_tool_calls,
                "max_model_tokens": run.budget.max_model_tokens,
                "max_cost_usd": run.budget.max_cost_usd,
                "max_wall_seconds": run.budget.max_wall_seconds,
            }
            if run.budget is not None
            else None
        ),
        "metadata": dict(run.metadata),
    }


def run_from_dict(data: dict[str, Any]) -> Run:
    workflow_data = data.get("workflow_ref")
    budget_data = data.get("budget")

    return Run(
        run_id=data["run_id"],
        session_id=data["session_id"],
        status=RunStatus(data["status"]),
        sandbox_ref=data.get("sandbox_ref"),
        sandbox_snapshot_ref=data.get("sandbox_snapshot_ref"),
        sandbox_binding=_sandbox_binding_from_dict(
            data.get("sandbox_binding")
        ),
        workflow_ref=(
            WorkflowRef(**workflow_data)
            if workflow_data is not None
            else None
        ),
        artifact_refs=[
            _decode_ref(value)
            for value in data.get("artifact_refs", [])
        ],
        evidence_refs=[
            _decode_ref(value)
            for value in data.get("evidence_refs", [])
        ],
        approvals=[
            Approval(
                approval_id=value["approval_id"],
                action=value["action"],
                evidence_refs=tuple(value.get("evidence_refs", ())),
                status=ApprovalStatus(value["status"]),
                actor=value.get("actor"),
                reason=value.get("reason"),
            )
            for value in data.get("approvals", [])
        ],
        budget=Budget(**budget_data) if budget_data is not None else None,
        metadata=dict(data.get("metadata", {})),
    )
