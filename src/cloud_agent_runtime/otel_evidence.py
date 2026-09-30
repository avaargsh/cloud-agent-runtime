from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

from .evidence_collector import EvidenceEvent, Provenance


_TRACE_ID = re.compile(r"^[0-9a-f]{32}$")
_SPAN_ID = re.compile(r"^[0-9a-f]{16}$")
_SHA256_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


class OTelEvidenceError(ValueError):
    pass


@dataclass(frozen=True)
class OTelSpanSnapshot:
    trace_id: str
    span_id: str
    name: str
    attributes: Mapping[str, Any]
    status: str | None = None


def _validate_digest(value: Any, *, attribute: str) -> str:
    if not isinstance(value, str) or not _SHA256_DIGEST.fullmatch(value):
        raise OTelEvidenceError(
            f"{attribute} must be sha256:<64 lowercase hex>"
        )
    return value


class OTelEvidenceAdapter:
    """Project an exported OTel span into an execution EvidenceEvent.

    The adapter accepts an already-exported span snapshot rather than depending
    on the OpenTelemetry SDK. Instrumentation/export remains owned by OTel.
    """

    def to_event(
        self,
        span: OTelSpanSnapshot,
        *,
        provenance: Provenance,
        event_id: str | None = None,
    ) -> EvidenceEvent:
        trace_id = span.trace_id.lower()
        span_id = span.span_id.lower()
        if not _TRACE_ID.fullmatch(trace_id):
            raise OTelEvidenceError(
                "trace_id must be 32 lowercase hex characters"
            )
        if not _SPAN_ID.fullmatch(span_id):
            raise OTelEvidenceError(
                "span_id must be 16 lowercase hex characters"
            )

        attributes = dict(span.attributes)
        run_id = attributes.get("agent.run_id")
        if not isinstance(run_id, str) or not run_id:
            raise OTelEvidenceError("span requires agent.run_id")

        event_type = attributes.get(
            "agent.evidence.event_type",
            span.name,
        )
        if not isinstance(event_type, str) or not event_type:
            raise OTelEvidenceError(
                "evidence event type must be a non-empty string"
            )

        action_id = attributes.get("agent.action_id")
        policy_digest = attributes.get("agent.policy.digest")
        artifact_digest = attributes.get("agent.artifact.digest")

        payload: dict[str, Any] = {
            "trace_id": trace_id,
            "span_id": span_id,
            "span_name": span.name,
            "status": span.status,
            "attributes": attributes,
        }
        if action_id is not None:
            payload["action_id"] = action_id
        if policy_digest is not None:
            payload["policy_digest"] = _validate_digest(
                policy_digest,
                attribute="agent.policy.digest",
            )
        if artifact_digest is not None:
            payload["artifact_digest"] = _validate_digest(
                artifact_digest,
                attribute="agent.artifact.digest",
            )

        return EvidenceEvent(
            event_id=event_id or f"{trace_id}:{span_id}",
            run_id=run_id,
            event_type=event_type,
            provenance=provenance,
            payload=payload,
        )
