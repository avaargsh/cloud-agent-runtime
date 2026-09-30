from cloud_agent_runtime import (
    AppendOnlyEvidenceCollector,
    EvidenceEvent,
    Provenance,
    RetryableToolError,
    ToolContract,
    execute_tool,
)


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

collector = AppendOnlyEvidenceCollector(
    collector_id="execution-evidence"
)
collector.append(
    EvidenceEvent(
        event_id="evt-commit",
        run_id="run-001",
        event_type="tool.commit",
        provenance=Provenance(
            generated_by="tool-proxy",
            observed_by="sandbox-supervisor",
            authorized_by="opa",
            executed_by="mcp-gateway",
            committed_by="deploy-service",
        ),
        payload={"idempotency_key": receipt.idempotency_key},
    )
)
trusted_head = collector.head_digest
collector.verify(expected_head_digest=trusted_head)

print("duplicate_side_effect_count", max(0, len(calls) - len(committed)))
print("side_effect_count", len(committed))
print("recovered_from_limbo", receipt.recovered_from_limbo)
print("evidence_head", trusted_head)
