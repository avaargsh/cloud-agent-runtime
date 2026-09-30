from cloud_agent_runtime.evidence_object_store import (
    EvidenceObjectStore,
    InMemoryObjectStore,
)


def test_evidence_is_persisted_as_object_manifest() -> None:
    backend = InMemoryObjectStore()
    store = EvidenceObjectStore(backend)

    manifest = store.persist(
        run_id="run-001",
        evidence={"event": "tool.commit", "status": "ok"},
    )

    assert manifest.object_key.startswith("evidence/run-001/")
    assert manifest.digest.startswith("sha256:")
    assert len(backend.objects) == 1
