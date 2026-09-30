from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


class RetryableToolError(RuntimeError):
    """The invocation may be retried according to the execution contract."""


class ToolContractViolation(RuntimeError):
    """The requested retry semantics cannot safely execute a side effect."""


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
        if (
            self.effect != "read-only"
            and self.max_attempts > 1
            and self.idempotency_mode == "none"
        ):
            raise ToolContractViolation(
                "side-effecting retries require idempotency support"
            )


@dataclass(frozen=True)
class ToolExecutionReceipt:
    idempotency_key: str
    attempts: int
    recovered_from_limbo: bool
    result: Any


def execute_tool(
    *,
    run_id: str,
    action_id: str,
    contract: ToolContract,
    invoke: Callable[[str], Any],
    verify: Callable[[str], Any | None] | None = None,
) -> ToolExecutionReceipt:
    """Execute a tool operation without delegating side-effect safety to the model.

    RetryableToolError represents an ambiguous delivery outcome such as a lost ACK.
    When verification is configured, the runtime resolves that ambiguity before a
    retry. This prevents a committed write from being repeated merely because the
    caller did not receive the original acknowledgement.
    """
    contract.validate()
    key = contract.key_template.format(run_id=run_id, action_id=action_id)

    if contract.verification_mode != "none" and verify is None:
        raise ToolContractViolation(
            "verification callback is required by the tool contract"
        )

    for attempt in range(1, contract.max_attempts + 1):
        try:
            result = invoke(key)
            return ToolExecutionReceipt(
                idempotency_key=key,
                attempts=attempt,
                recovered_from_limbo=False,
                result=result,
            )
        except RetryableToolError:
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

    raise AssertionError("unreachable")
