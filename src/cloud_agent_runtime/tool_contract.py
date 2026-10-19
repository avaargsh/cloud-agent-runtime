from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


class RetryableToolError(RuntimeError):
    """The invocation may be retried according to the execution contract."""


class ToolContractViolation(RuntimeError):
    """The requested retry semantics cannot safely execute a side effect."""


class UnverifiedToolOutcome(ToolContractViolation):
    """An independently observed outcome is missing, unknown, or mismatched.

    The caller must reconcile the existing operation, not blindly reinvoke it.
    """


@dataclass(frozen=True)
class ToolContract:
    effect: str
    idempotency_mode: str = "required"
    key_template: str = "{run_id}:{action_id}"
    max_attempts: int = 3
    verification_mode: str = "read-after-write"

    def validate(self) -> None:
        if self.max_attempts < 1:
            raise ToolContractViolation("max_attempts must be at least 1")
        if self.verification_mode not in {
            "none",
            "read-after-write",
            "verified-outcome",
        }:
            raise ToolContractViolation("unsupported verification mode")
        if (
            self.effect != "read-only"
            and self.max_attempts > 1
            and self.idempotency_mode == "none"
        ):
            raise ToolContractViolation(
                "side-effecting retries require idempotency support"
            )


@dataclass(frozen=True)
class VerifiedToolOutcome:
    """Result of an external *read-only* independent verifier.

    This is only a typed return value. Trust in the verifier, its evidence
    producer, and its observation freshness must be enforced by the caller.
    """

    idempotency_key: str
    target_ref: str
    status: str  # succeeded | failed | unknown
    evidence_ref: str | None = None


@dataclass(frozen=True)
class ToolExecutionReceipt:
    idempotency_key: str
    attempts: int
    recovered_from_limbo: bool
    result: Any
    verification_ref: str | None = None


def _verified_evidence_ref(
    *,
    key: str,
    expected_target_ref: str,
    observed: Any,
) -> str:
    if not isinstance(observed, VerifiedToolOutcome):
        raise UnverifiedToolOutcome("verifier returned no typed outcome")
    if observed.idempotency_key != key or observed.target_ref != expected_target_ref:
        raise UnverifiedToolOutcome("independent observation identity mismatch")
    if observed.status != "succeeded":
        raise UnverifiedToolOutcome("independent outcome is not verified successful")
    if not isinstance(observed.evidence_ref, str) or not observed.evidence_ref.strip():
        raise UnverifiedToolOutcome("verified outcome is missing an evidence reference")
    return observed.evidence_ref


def execute_tool(
    *,
    run_id: str,
    action_id: str,
    contract: ToolContract,
    invoke: Callable[[str], Any],
    verify: Callable[[str], Any | None] | None = None,
    expected_target_ref: str | None = None,
) -> ToolExecutionReceipt:
    """Run one bounded tool action; never use an ACK as independent outcome proof.

    Legacy read-after-write resolves ambiguous ACKs before an idempotent retry.
    The opt-in verified-outcome mode also checks a successful ACK, binds the
    observation to the exact operation and resource, and fails closed on UNKNOWN.
    It never silently reinvokes a mutation when the observer cannot prove it.
    """
    contract.validate()
    key = contract.key_template.format(run_id=run_id, action_id=action_id)
    strict = contract.verification_mode == "verified-outcome"

    if contract.verification_mode != "none" and verify is None:
        raise ToolContractViolation(
            "verification callback is required by the tool contract"
        )
    if strict and (not isinstance(expected_target_ref, str) or not expected_target_ref.strip()):
        raise ToolContractViolation("verified-outcome requires an exact target reference")

    for attempt in range(1, contract.max_attempts + 1):
        try:
            result = invoke(key)
        except RetryableToolError:
            if strict:
                # A lost ACK is not an invitation to repeat the side effect.
                # UNKNOWN (including observer outage) remains unresolved.
                assert verify is not None
                observed = verify(key)
                evidence_ref = _verified_evidence_ref(
                    key=key,
                    expected_target_ref=expected_target_ref,
                    observed=observed,
                )
                return ToolExecutionReceipt(
                    idempotency_key=key,
                    attempts=attempt,
                    recovered_from_limbo=True,
                    result=observed,
                    verification_ref=evidence_ref,
                )
            if contract.verification_mode != "none":
                assert verify is not None
                observed = verify(key)
                if observed is not None:
                    return ToolExecutionReceipt(
                        idempotency_key=key,
                        attempts=attempt,
                        recovered_from_limbo=True,
                        result=observed,
                    )
            if attempt == contract.max_attempts:
                raise
        else:
            evidence_ref = None
            if strict:
                assert verify is not None
                evidence_ref = _verified_evidence_ref(
                    key=key,
                    expected_target_ref=expected_target_ref,
                    observed=verify(key),
                )
            return ToolExecutionReceipt(
                idempotency_key=key,
                attempts=attempt,
                recovered_from_limbo=False,
                result=result,
                verification_ref=evidence_ref,
            )

    raise AssertionError("unreachable")
