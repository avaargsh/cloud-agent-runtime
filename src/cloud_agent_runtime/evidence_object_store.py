from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import PurePosixPath
from typing import Any, Mapping, Protocol


class EvidenceObjectIntegrityError(RuntimeError):
    pass


class ObjectStore(Protocol):
    def put(self, key: str, body: bytes) -> str:
        ...

    def get(self, key: str) -> bytes:
        ...


@dataclass(frozen=True)
class EvidenceManifest:
    run_id: str
    object_key: str
    digest: str
    manifest_key: str
    manifest_digest: str
    content_type: str = "application/json"


class InMemoryObjectStore:
    """Reference S3-compatible object store boundary.

    Production deployments can map this contract to S3, MinIO, or Ceph RGW
    with versioning and object-lock retention enabled.
    """

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, body: bytes) -> str:
        previous = self.objects.get(key)
        if previous is not None and previous != body:
            raise EvidenceObjectIntegrityError(
                f"immutable object already exists with different bytes: {key}"
            )
        self.objects[key] = body
        return _sha256(body)

    def get(self, key: str) -> bytes:
        try:
            return self.objects[key]
        except KeyError as exc:
            raise KeyError(f"unknown evidence object: {key}") from exc


def _sha256(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


class EvidenceObjectStore:
    """Persist evidence and its immutable manifest as content-addressed objects."""

    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def persist(
        self,
        *,
        run_id: str,
        evidence: Mapping[str, Any],
    ) -> EvidenceManifest:
        body = _canonical_json(evidence)
        digest = _sha256(body)
        digest_hex = digest.removeprefix("sha256:")
        object_key = f"evidence/{run_id}/{digest_hex}.json"
        written_digest = self.store.put(object_key, body)
        if written_digest != digest:
            raise EvidenceObjectIntegrityError(
                "object store returned a digest different from local content"
            )

        manifest_payload = {
            "run_id": run_id,
            "object_key": object_key,
            "digest": digest,
            "content_type": "application/json",
        }
        manifest_body = _canonical_json(manifest_payload)
        manifest_digest = _sha256(manifest_body)
        manifest_hex = manifest_digest.removeprefix("sha256:")
        manifest_key = f"evidence-manifests/{run_id}/{manifest_hex}.json"
        written_manifest_digest = self.store.put(manifest_key, manifest_body)
        if written_manifest_digest != manifest_digest:
            raise EvidenceObjectIntegrityError(
                "object store returned a manifest digest different from local content"
            )

        return EvidenceManifest(
            run_id=run_id,
            object_key=object_key,
            digest=digest,
            manifest_key=manifest_key,
            manifest_digest=manifest_digest,
        )

    def load(self, manifest: EvidenceManifest) -> dict[str, Any]:
        body = self.store.get(manifest.object_key)
        if _sha256(body) != manifest.digest:
            raise EvidenceObjectIntegrityError(
                "evidence object digest does not match manifest"
            )
        return json.loads(body.decode("utf-8"))

    def load_manifest(self, manifest_key: str) -> EvidenceManifest:
        body = self.store.get(manifest_key)
        actual_digest = _sha256(body)
        expected_hex = PurePosixPath(manifest_key).stem
        expected_digest = "sha256:" + expected_hex
        if actual_digest != expected_digest:
            raise EvidenceObjectIntegrityError(
                "manifest object digest does not match content-addressed key"
            )

        payload = json.loads(body.decode("utf-8"))
        return EvidenceManifest(
            run_id=payload["run_id"],
            object_key=payload["object_key"],
            digest=payload["digest"],
            manifest_key=manifest_key,
            manifest_digest=actual_digest,
            content_type=payload.get("content_type", "application/json"),
        )

    def replay(self, manifest_key: str) -> dict[str, Any]:
        return self.load(self.load_manifest(manifest_key))
