import pytest

from cloud_agent_runtime.tool_contract import (
    RetryableToolError,
    ToolContract,
    ToolContractViolation,
    execute_tool,
)


def test_lost_ack_is_verified_before_retrying_side_effect() -> None:
    committed = {}
    calls = []

    def invoke(key):
        calls.append(key)
        if key not in committed:
            committed[key] = {"deployment": "v2"}
            raise RetryableToolError("lost ACK after commit")
        return committed[key]

    receipt = execute_tool(
        run_id="run-001",
        action_id="deploy-001",
        contract=ToolContract(
            effect="external-side-effect",
            idempotency_mode="required",
            max_attempts=3,
            verification_mode="read-after-write",
        ),
        invoke=invoke,
        verify=committed.get,
    )

    assert receipt.idempotency_key == "run-001:deploy-001"
    assert receipt.attempts == 1
    assert receipt.recovered_from_limbo is True
    assert calls == ["run-001:deploy-001"]
    assert len(committed) == 1


def test_side_effecting_retry_without_idempotency_fails_closed() -> None:
    with pytest.raises(ToolContractViolation):
        execute_tool(
            run_id="run-001",
            action_id="deploy-001",
            contract=ToolContract(
                effect="external-side-effect",
                idempotency_mode="none",
                max_attempts=2,
                verification_mode="none",
            ),
            invoke=lambda key: None,
        )


def test_contract_requires_verifier_when_verification_is_enabled() -> None:
    with pytest.raises(ToolContractViolation):
        execute_tool(
            run_id="run-001",
            action_id="deploy-001",
            contract=ToolContract(
                effect="external-side-effect",
                verification_mode="read-after-write",
            ),
            invoke=lambda key: None,
        )
