import pytest

from cloud_agent_runtime.evidence_collector import (
    AppendOnlyEvidenceCollector,
    EvidenceEvent,
    EvidenceIntegrityError,
    Provenance,
)


def event(event_id: str, event_type: str) -> EvidenceEvent:
    return EvidenceEvent(
        event_id=event_id,
        run_id="run-001",
        event_type=event_type,
        provenance=Provenance(
            generated_by="tool-proxy",
            observed_by="sandbox-supervisor",
            authorized_by="opa",
            executed_by="mcp-gateway",
            committed_by="target-service",
        ),
        payload={"status": "ok"},
    )


def test_external_evidence_survives_harness_log_deletion() -> None:
    collector = AppendOnlyEvidenceCollector(
        collector_id="execution-evidence"
    )
    collector.append(event("evt-1", "tool.request"))
    collector.append(event("evt-2", "tool.commit"))
    trusted_head = collector.head_digest

    harness_trace = ["tool.request", "tool.commit"]
    harness_trace.clear()

    assert harness_trace == []
    assert len(collector.records) == 2
    collector.verify(expected_head_digest=trusted_head)


def test_auditor_rejects_a_rewritten_head() -> None:
    collector = AppendOnlyEvidenceCollector(
        collector_id="execution-evidence"
    )
    collector.append(event("evt-1", "tool.commit"))

    with pytest.raises(EvidenceIntegrityError):
        collector.verify(
            expected_head_digest="sha256:" + ("0" * 64)
        )
