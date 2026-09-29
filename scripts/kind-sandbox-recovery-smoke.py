"""Live kind smoke for the sandbox replacement invariant.

Prerequisites: kubectl context points at a disposable kind cluster.
"""

from cloud_agent_runtime import (
    AgentRuntime,
    InMemoryWorkflowDriver,
    KubernetesSandboxProvider,
)

provider = KubernetesSandboxProvider(namespace="agent-sandbox-smoke")
runtime = AgentRuntime(
    sandbox_provider=provider,
    workflow_driver=InMemoryWorkflowDriver(),
)
session = runtime.create_session(
    agent_id="sre-agent",
    release_id="checkout-sre-golden-v1",
    tenant_id="golden-demo",
)
run = runtime.start_run(session_id=session.session_id)
run_id = run.run_id
workflow_ref = run.workflow_ref
sandbox_a_ref = run.sandbox_ref
sandbox_a_id = runtime._sandboxes[run_id].sandbox_id

active = runtime._sandboxes[run_id]
checkpoint = provider.snapshot(active).snapshot_ref
assert checkpoint

# Inject actual Pod loss.
provider.terminate(active)
active.status = active.status.BOUND

rebound = runtime.rebind_run_sandbox(run_id, snapshot_ref=checkpoint)
sandbox_b_id = runtime._sandboxes[run_id].sandbox_id
assert rebound.run_id == run_id
assert rebound.workflow_ref == workflow_ref
assert rebound.sandbox_ref != sandbox_a_ref
assert sandbox_b_id != sandbox_a_id

print("PASS")
print(f"run_id={run_id}")
print(f"workflow_ref={workflow_ref}")
print(f"sandbox_a_ref={sandbox_a_ref}")
print(f"sandbox_b_ref={rebound.sandbox_ref}")
print(f"sandbox_a_id={sandbox_a_id}")
print(f"sandbox_b_id={sandbox_b_id}")
