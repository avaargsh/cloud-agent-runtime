import pytest

from cloud_agent_runtime.evidence_collector import Provenance
from cloud_agent_runtime.otel_evidence import (
    OTelEvidenceAdapter,
    OTelEvidenceError,
    OTelSpanSnapshot,
)


PROVENANCE = Provenance(
    generated_by="tool-proxy",
    observed_by="otel-collector",
    authorized_by="opa",
    executed_by="mcp-gateway",
    committed_by="deploy-service",
)


def test_otel_span_projects_trace_and_execution_provenance() -> None:
    span = OTelSpanSnapshot(
        trace_id="a" * 32,
        span_id="b" * 16,
        name="mcp.deploy",
        status="OK",
        attributes={
            "agent.run_id": "run-001",
            "agent.action_id": "deploy-001",
            "agent.evidence.event_type": "tool.commit",
            "agent.policy.digest": "sha256:" + ("c" * 64),
            "agent.artifact.digest": "sha256:" + ("d" * 64),
        },
    )

    event = OTelEvidenceAdapter().to_event(
        span,
        provenance=PROVENANCE,
    )

    assert event.run_id == "run-001"
    assert event.event_type == "tool.commit"
    assert event.payload["trace_id"] == "a" * 32
    assert event.payload["span_id"] == "b" * 16
    assert event.payload["action_id"] == "deploy-001"
    assert event.payload["policy_digest"].startswith("sha256:")
    assert event.payload["artifact_digest"].startswith("sha256:")


def test_otel_projection_requires_canonical_run_identity() -> None:
    span = OTelSpanSnapshot(
        trace_id="a" * 32,
        span_id="b" * 16,
        name="tool.commit",
        attributes={},
    )

    with pytest.raises(OTelEvidenceError, match="agent.run_id"):
        OTelEvidenceAdapter().to_event(
            span,
            provenance=PROVENANCE,
        )
