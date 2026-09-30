import pytest

from cloud_agent_runtime.evidence_object_store import (
    EvidenceObjectIntegrityError,
    EvidenceObjectStore,
    InMemoryObjectStore,
)


def test_evidence_and_manifest_are_persisted_as_content_addressed_objects() -> None:
    backend = InMemoryObjectStore()
    store = EvidenceObjectStore(backend)

    manifest = store.persist(
        run_id="run-001",
        evidence={"event": "tool.commit", "status": "ok"},
    )

    assert manifest.object_key.startswith("evidence/run-001/")
    assert manifest.manifest_key.startswith("evidence-manifests/run-001/")
    assert manifest.digest.startswith("sha256:")
    assert manifest.manifest_digest.startswith("sha256:")
    assert len(backend.objects) == 2

    replayed = store.replay(manifest.manifest_key)
    assert replayed == {"event": "tool.commit", "status": "ok"}


def test_replay_detects_evidence_object_tampering() -> None:
    backend = InMemoryObjectStore()
    store = EvidenceObjectStore(backend)
    manifest = store.persist(
        run_id="run-001",
        evidence={"event": "tool.commit", "status": "ok"},
    )

    backend.objects[manifest.object_key] = b'{"event":"tool.commit","status":"rewritten"}'

    with pytest.raises(
        EvidenceObjectIntegrityError,
        match="digest does not match manifest",
    ):
        store.replay(manifest.manifest_key)


def test_replay_detects_manifest_tampering() -> None:
    backend = InMemoryObjectStore()
    store = EvidenceObjectStore(backend)
    manifest = store.persist(
        run_id="run-001",
        evidence={"event": "tool.commit"},
    )

    backend.objects[manifest.manifest_key] = b'{"run_id":"other"}'

    with pytest.raises(
        EvidenceObjectIntegrityError,
        match="manifest object digest",
    ):
        store.replay(manifest.manifest_key)
