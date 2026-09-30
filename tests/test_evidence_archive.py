from cloud_agent_runtime.evidence_archive import EvidenceArchive
from cloud_agent_runtime.evidence_collector import (
    AppendOnlyEvidenceCollector,
    Provenance,
)
from cloud_agent_runtime.evidence_object_store import (
    EvidenceObjectStore,
    InMemoryObjectStore,
)
from cloud_agent_runtime.otel_evidence import (
    OTelEvidenceAdapter,
    OTelSpanSnapshot,
)


def test_otel_event_archives_and_replays_without_harness_log() -> None:
    backend = InMemoryObjectStore()
    archive = EvidenceArchive(
        collector=AppendOnlyEvidenceCollector(
            collector_id="external-evidence",
        ),
        object_store=EvidenceObjectStore(backend),
    )

    event = OTelEvidenceAdapter().to_event(
        OTelSpanSnapshot(
            trace_id="1" * 32,
            span_id="2" * 16,
            name="mcp.deploy",
            status="OK",
            attributes={
                "agent.run_id": "run-001",
                "agent.action_id": "deploy-001",
                "agent.evidence.event_type": "tool.commit",
            },
        ),
        provenance=Provenance(
            generated_by="tool-proxy",
            observed_by="otel-collector",
            authorized_by="opa",
            executed_by="mcp-gateway",
            committed_by="deploy-service",
        ),
    )
    archived = archive.record(event)

    harness_log = ["deploy requested", "deploy committed"]
    harness_log.clear()

    replayed = archive.replay(archived.manifest.manifest_key)

    assert harness_log == []
    assert replayed["event"]["run_id"] == "run-001"
    assert replayed["event"]["payload"]["trace_id"] == "1" * 32
    assert replayed["record_digest"] == archived.record_digest
