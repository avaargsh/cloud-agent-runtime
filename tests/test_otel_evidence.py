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
    assert event.payload["policy_digest"] == "sha256:" + ("c" * 64)
    assert event.payload["artifact_digest"] == "sha256:" + ("d" * 64)


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


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("agent.policy.digest", "sha256:not-a-digest"),
        ("agent.policy.digest", "sha256:" + ("A" * 64)),
        ("agent.artifact.digest", "md5:" + ("a" * 32)),
        ("agent.artifact.digest", 123),
    ],
)
def test_otel_projection_rejects_noncanonical_digests(
    attribute,
    value,
) -> None:
    span = OTelSpanSnapshot(
        trace_id="a" * 32,
        span_id="b" * 16,
        name="tool.commit",
        attributes={
            "agent.run_id": "run-001",
            attribute: value,
        },
    )

    with pytest.raises(
        OTelEvidenceError,
        match=attribute.replace(".", r"\."),
    ):
        OTelEvidenceAdapter().to_event(
            span,
            provenance=PROVENANCE,
        )

def test_otel_projection_binds_recovery_operation_and_retry_provenance() -> None:
    span = OTelSpanSnapshot(
        trace_id="a" * 32,
        span_id="b" * 16,
        name="sandbox.rebind",
        status="OK",
        attributes={
            "agent.run_id": "run-001",
            "agent.evidence.event_type": "runtime.recovery",
            "agent.operation.id": "op-rebind-002",
            "agent.retry.key": "recovery-attempt-42",
            "agent.recovery.original_operation_id": "op-rebind-001",
            "agent.recovery.reason": "cleanup-ack-lost",
        },
    )

    event = OTelEvidenceAdapter().to_event(
        span,
        provenance=PROVENANCE,
    )

    assert event.event_type == "runtime.recovery"
    assert event.payload["operation_id"] == "op-rebind-002"
    assert event.payload["retry_key"] == "recovery-attempt-42"
    assert event.payload["original_operation_id"] == "op-rebind-001"
    assert event.payload["recovery_reason"] == "cleanup-ack-lost"


@pytest.mark.parametrize(
    "attribute",
    [
        "agent.operation.id",
        "agent.retry.key",
        "agent.recovery.original_operation_id",
        "agent.recovery.reason",
    ],
)
def test_otel_projection_rejects_empty_recovery_identity(attribute) -> None:
    span = OTelSpanSnapshot(
        trace_id="a" * 32,
        span_id="b" * 16,
        name="runtime.recovery",
        attributes={
            "agent.run_id": "run-001",
            attribute: "",
        },
    )

    with pytest.raises(
        OTelEvidenceError,
        match=attribute.replace(".", r"\."),
    ):
        OTelEvidenceAdapter().to_event(
            span,
            provenance=PROVENANCE,
        )

