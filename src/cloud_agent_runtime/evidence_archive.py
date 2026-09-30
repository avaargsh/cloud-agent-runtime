from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .evidence_collector import (
    AppendOnlyEvidenceCollector,
    EvidenceEvent,
    EvidenceRecord,
)
from .evidence_object_store import EvidenceManifest, EvidenceObjectStore


@dataclass(frozen=True)
class ArchivedEvidence:
    record_digest: str
    manifest: EvidenceManifest


class EvidenceArchive:
    """Bridge execution-external collection to immutable object storage."""

    def __init__(
        self,
        *,
        collector: AppendOnlyEvidenceCollector,
        object_store: EvidenceObjectStore,
    ) -> None:
        self.collector = collector
        self.object_store = object_store

    def record(self, event: EvidenceEvent) -> ArchivedEvidence:
        record = self.collector.append(event)
        manifest = self.object_store.persist(
            run_id=event.run_id,
            evidence=self._record_payload(record),
        )
        return ArchivedEvidence(
            record_digest=record.digest,
            manifest=manifest,
        )

    def replay(self, manifest_key: str) -> dict[str, Any]:
        payload = self.object_store.replay(manifest_key)
        required = {"event", "previous_digest", "record_digest"}
        missing = required - payload.keys()
        if missing:
            raise ValueError(
                "archived evidence record is missing: "
                + ", ".join(sorted(missing))
            )
        return payload

    @staticmethod
    def _record_payload(record: EvidenceRecord) -> dict[str, Any]:
        return {
            "event": {
                "event_id": record.event.event_id,
                "run_id": record.event.run_id,
                "event_type": record.event.event_type,
                "provenance": asdict(record.event.provenance),
                "payload": dict(record.event.payload),
            },
            "previous_digest": record.previous_digest,
            "record_digest": record.digest,
        }
