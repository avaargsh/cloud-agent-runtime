from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from typing import Any, Mapping


class EvidenceIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True)
class Provenance:
    generated_by: str | None
    observed_by: str | None
    authorized_by: str | None
    executed_by: str | None
    committed_by: str | None


@dataclass(frozen=True)
class EvidenceEvent:
    event_id: str
    run_id: str
    event_type: str
    provenance: Provenance
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class EvidenceRecord:
    event: EvidenceEvent
    previous_digest: str | None
    digest: str


def _event_payload(event: EvidenceEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "run_id": event.run_id,
        "event_type": event.event_type,
        "provenance": asdict(event.provenance),
        "payload": dict(event.payload),
    }


def _digest(event: EvidenceEvent, previous_digest: str | None) -> str:
    canonical = json.dumps(
        {
            "event": _event_payload(event),
            "previous_digest": previous_digest,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return "sha256:" + sha256(canonical).hexdigest()


class AppendOnlyEvidenceCollector:
    """Execution-external evidence chain.

    The collector deliberately has no update/delete API. An auditor can verify the
    complete chain against an independently retained head digest. The head digest
    is the trust anchor; a hash chain alone is not a substitute for trusted
    storage or signing.
    """

    def __init__(self, *, collector_id: str) -> None:
        self.collector_id = collector_id
        self._records: list[EvidenceRecord] = []
        self._event_ids: set[str] = set()

    @property
    def records(self) -> tuple[EvidenceRecord, ...]:
        return tuple(self._records)

    @property
    def head_digest(self) -> str | None:
        return self._records[-1].digest if self._records else None

    def append(self, event: EvidenceEvent) -> EvidenceRecord:
        if not event.event_id:
            raise EvidenceIntegrityError("event_id is required")
        if event.event_id in self._event_ids:
            raise EvidenceIntegrityError(
                f"duplicate evidence event: {event.event_id}"
            )

        previous = self.head_digest
        record = EvidenceRecord(
            event=event,
            previous_digest=previous,
            digest=_digest(event, previous),
        )
        self._records.append(record)
        self._event_ids.add(event.event_id)
        return record

    def verify(self, *, expected_head_digest: str | None = None) -> None:
        previous: str | None = None
        for record in self._records:
            if record.previous_digest != previous:
                raise EvidenceIntegrityError("evidence chain order changed")
            if _digest(record.event, previous) != record.digest:
                raise EvidenceIntegrityError("evidence record changed")
            previous = record.digest

        if (
            expected_head_digest is not None
            and previous != expected_head_digest
        ):
            raise EvidenceIntegrityError(
                "evidence head does not match trusted checkpoint"
            )
