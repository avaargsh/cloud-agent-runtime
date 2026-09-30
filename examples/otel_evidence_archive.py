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


backend = InMemoryObjectStore()
archive = EvidenceArchive(
    collector=AppendOnlyEvidenceCollector(
        collector_id="external-evidence",
    ),
    object_store=EvidenceObjectStore(backend),
)

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
    provenance=Provenance(
        generated_by="tool-proxy",
        observed_by="otel-collector",
        authorized_by="opa",
        executed_by="mcp-gateway",
        committed_by="deploy-service",
    ),
)
archived = archive.record(event)
replayed = archive.replay(archived.manifest.manifest_key)

print("manifest_key", archived.manifest.manifest_key)
print("evidence_digest", archived.manifest.digest)
print("record_digest", archived.record_digest)
print("trace_id", replayed["event"]["payload"]["trace_id"])
