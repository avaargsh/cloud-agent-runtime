import pytest

from cloud_agent_runtime.tool_contract import (
    RetryableToolError,
    ToolContract,
    ToolContractViolation,
    UnverifiedToolOutcome,
    VerifiedToolOutcome,
    execute_tool,
)


def strict_contract() -> ToolContract:
    return ToolContract(
        effect="external-side-effect",
        max_attempts=3,
        verification_mode="verified-outcome",
    )


def verified(key: str) -> VerifiedToolOutcome:
    return VerifiedToolOutcome(
        idempotency_key=key,
        target_ref="cluster/tenant-a/workload/uid-1",
        status="succeeded",
        evidence_ref="sha256:independently-verified-proof",
    )


def test_success_ack_still_requires_independent_observation() -> None:
    calls = []

    def invoke(key: str) -> dict:
        calls.append(("invoke", key))
        return {"accepted": True}

    def observe(key: str) -> VerifiedToolOutcome:
        calls.append(("verify", key))
        return verified(key)

    receipt = execute_tool(
        run_id="run-1",
        action_id="action-1",
        contract=strict_contract(),
        expected_target_ref="cluster/tenant-a/workload/uid-1",
        invoke=invoke,
        verify=observe,
    )

    assert calls == [
        ("invoke", "run-1:action-1"),
        ("verify", "run-1:action-1"),
    ]
    assert receipt.recovered_from_limbo is False
    assert receipt.result == {"accepted": True}
    assert receipt.verification_ref == "sha256:independently-verified-proof"


def test_lost_ack_is_reconciled_without_reissuing_mutation() -> None:
    calls = []

    def invoke(key: str) -> None:
        calls.append(key)
        raise RetryableToolError("ACK lost after potential commit")

    receipt = execute_tool(
        run_id="run-1",
        action_id="action-1",
        contract=strict_contract(),
        expected_target_ref="cluster/tenant-a/workload/uid-1",
        invoke=invoke,
        verify=verified,
    )
    assert calls == ["run-1:action-1"]
    assert receipt.attempts == 1
    assert receipt.recovered_from_limbo
    assert receipt.verification_ref == "sha256:independently-verified-proof"


@pytest.mark.parametrize(
    "observation",
    [
        None,
        {"status": "succeeded"},
        VerifiedToolOutcome("different-operation", "cluster/tenant-a/workload/uid-1", "succeeded", "proof"),
        VerifiedToolOutcome("run-1:action-1", "cluster/tenant-b/workload/uid-2", "succeeded", "proof"),
        VerifiedToolOutcome("run-1:action-1", "cluster/tenant-a/workload/uid-1", "unknown", "proof"),
        VerifiedToolOutcome("run-1:action-1", "cluster/tenant-a/workload/uid-1", "failed", "proof"),
        VerifiedToolOutcome("run-1:action-1", "cluster/tenant-a/workload/uid-1", "succeeded", ""),
    ],
)
@pytest.mark.parametrize("lost_ack", [False, True])
def test_unverifiable_operation_fails_closed_without_replay(
    observation, lost_ack: bool
) -> None:
    attempts = []

    def invoke(key: str) -> dict:
        attempts.append(key)
        if lost_ack:
            raise RetryableToolError("ambiguous provider commit")
        return {"accepted": True}

    with pytest.raises(UnverifiedToolOutcome):
        execute_tool(
            run_id="run-1",
            action_id="action-1",
            contract=strict_contract(),
            expected_target_ref="cluster/tenant-a/workload/uid-1",
            invoke=invoke,
            verify=lambda _: observation,
        )
    assert attempts == ["run-1:action-1"]


def test_missing_target_or_verifier_rejected_before_side_effect() -> None:
    attempts = []
    for kwargs in (
        {"verify": verified},
        {"expected_target_ref": "cluster/tenant-a/workload/uid-1"},
    ):
        with pytest.raises(ToolContractViolation):
            execute_tool(
                run_id="run-1",
                action_id="action-1",
                contract=strict_contract(),
                invoke=lambda key: attempts.append(key),
                **kwargs,
            )
    assert attempts == []


def test_observer_error_does_not_trigger_second_mutation() -> None:
    attempts = []

    def invoke(key: str) -> None:
        attempts.append(key)
        raise RetryableToolError("ambiguous commit")

    def unavailable(_: str) -> VerifiedToolOutcome:
        raise OSError("independent observer unavailable")

    with pytest.raises(OSError, match="observer unavailable"):
        execute_tool(
            run_id="run-1",
            action_id="action-1",
            contract=strict_contract(),
            expected_target_ref="cluster/tenant-a/workload/uid-1",
            invoke=invoke,
            verify=unavailable,
        )
    assert attempts == ["run-1:action-1"]


def test_unknown_verification_mode_rejected_before_side_effect() -> None:
    attempts = []
    with pytest.raises(ToolContractViolation, match="unsupported verification"):
        execute_tool(
            run_id="run-1",
            action_id="action-1",
            contract=ToolContract(effect="external-side-effect", verification_mode="typo"),
            invoke=lambda key: attempts.append(key),
        )
    assert attempts == []
