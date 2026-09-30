from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Protocol


class ObjectStore(Protocol):
    def put(self, key: str, body: bytes) -> str:
        ...


@dataclass(frozen=True)
class EvidenceManifest:
    run_id: str
    object_key: str
    digest: str
    content_type: str = "application/json"


class InMemoryObjectStore:
    """Reference S3-compatible object store boundary.

    Production deployments can map this contract to S3/MinIO with object
    lock/retention policies. Runtime keeps only immutable manifest references.
    """

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, body: bytes) -> str:
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        self.objects[key] = body
        return digest


class EvidenceObjectStore:
    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def persist(self, *, run_id: str, evidence: dict) -> EvidenceManifest:
        body = json.dumps(
            evidence,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        key = f"evidence/{run_id}/{hashlib.sha256(body).hexdigest()}.json"
        digest = self.store.put(key, body)

        return EvidenceManifest(
            run_id=run_id,
            object_key=key,
            digest=digest,
        )
