from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from pathlib import Path
from typing import Any

from .temporal_driver import TemporalWorkflowDriver, deterministic_start_operation_id

from .async_runtime import AsyncAgentRuntime
from .sandbox import InMemorySandboxProvider
from .store import SQLiteStore


class CrashAfterAcceptedStartDriver:
    """Kill the caller after Temporal accepts start but before Run binds its WorkflowRef."""

    name = "temporal-crash-after-accepted-start"

    def __init__(self, driver: TemporalWorkflowDriver) -> None:
        self.driver = driver

    async def start_run(
        self,
        *,
        runtime_run_id: str,
        session_id: str,
        timeout_seconds: float | None = None,
    ):
        ref = await self.driver.start_run(
            runtime_run_id=runtime_run_id,
            session_id=session_id,
            timeout_seconds=timeout_seconds,
        )
        # The RPC was accepted, but AsyncAgentRuntime has not persisted the
        # returned WorkflowRef. This actual process death leaves the durable
        # Run at start_pending so a different process must reconcile it.
        os.kill(os.getpid(), signal.SIGKILL)
        raise AssertionError(f"SIGKILL should have ended process after {ref.workflow_id}")

    async def reconcile_start(self, **kwargs):
        return await self.driver.reconcile_start(**kwargs)

    async def signal(self, workflow, *, name: str, payload: dict) -> None:
        await self.driver.signal(workflow, name=name, payload=payload)

    async def terminate_run(self, workflow, *, reason: str, operation_id: str | None = None) -> None:
        await self.driver.terminate_run(workflow, reason=reason)


async def _crash_after_accepted_start(args) -> None:
    store = SQLiteStore(args.sqlite_path)
    driver = await TemporalWorkflowDriver.connect(
        args.temporal_address,
        task_queue=args.task_queue,
        namespace=args.namespace,
    )
    runtime = AsyncAgentRuntime(
        store=store,
        sandbox_provider=InMemorySandboxProvider(),
        workflow_driver=CrashAfterAcceptedStartDriver(driver),
    )
    session = runtime.create_session(
        agent_id="p0-recovery-probe",
        release_id="p0-recovery-probe-v1",
        tenant_id="p0-recovery-test",
    )
    # The process is expected to be SIGKILLed in the driver's start_run,
    # after the provider creates the workflow but before its ref can be saved.
    await runtime.start_run(session_id=session.session_id)
    raise AssertionError("expected crash-after-accepted-start driver to SIGKILL")


async def _start_awaiting_ack(args) -> None:
    """Persist a canonical Run, then block on a proxied StartWorkflow ACK.

    The parent process owns the TCP response-drop proxy and SIGKILLs this
    process after independently observing the accepted Temporal Workflow.
    """
    store = SQLiteStore(args.sqlite_path)
    driver = await TemporalWorkflowDriver.connect(
        args.temporal_address,
        task_queue=args.task_queue,
        namespace=args.namespace,
    )
    runtime = AsyncAgentRuntime(
        store=store,
        sandbox_provider=None,
        workflow_driver=driver,
    )
    session = runtime.create_session(
        agent_id="p0-real-ack-drop-probe",
        release_id="p0-real-ack-drop-probe-v1",
        tenant_id="p0-real-ack-drop-test",
    )
    # Explicit caller identity is known before the first request. The
    # external sandbox reference prevents this fault test from accidentally
    # testing the process-local in-memory sandbox provider.
    await runtime.start_run(
        session_id=session.session_id,
        sandbox_ref=f"sandbox://ack-drop/{args.run_id}",
        run_id=args.run_id,
    )
    raise RuntimeError("unexpectedly received the StartWorkflow acknowledgement")


async def _reconcile_sqlite(args) -> None:
    store = SQLiteStore(args.sqlite_path)
    driver = await TemporalWorkflowDriver.connect(
        args.temporal_address,
        task_queue=args.task_queue,
        namespace=args.namespace,
    )
    runtime = AsyncAgentRuntime(
        store=store,
        sandbox_provider=InMemorySandboxProvider(),
        workflow_driver=driver,
    )
    print("[DEBUG-temporal-reconcile-close] before-reconcile", file=sys.stderr, flush=True)
    run = await runtime.reconcile_run(args.run_id)
    print("[DEBUG-temporal-reconcile-close] after-reconcile", file=sys.stderr, flush=True)
    print(json.dumps({
        "run_id": run.run_id,
        "status": run.status.value,
        "recovery_state": run.recovery_state,
        "recovery_operation_id": run.recovery_operation_id,
        "workflow_ref": {
            "provider": run.workflow_ref.provider,
            "workflow_id": run.workflow_ref.workflow_id,
            "run_id": run.workflow_ref.run_id,
        } if run.workflow_ref else None,
        "recovery_evidence": run.recovery_evidence,
    }, sort_keys=True), flush=True)
    print("[DEBUG-temporal-reconcile-close] before-store-close", file=sys.stderr, flush=True)
    store.close()
    print("[DEBUG-temporal-reconcile-close] after-store-close", file=sys.stderr, flush=True)
    close = getattr(driver.client, "close", None)
    if close:
        print("[DEBUG-temporal-reconcile-close] before-client-close", file=sys.stderr, flush=True)
        maybe = close()
        if asyncio.iscoroutine(maybe):
            await maybe
        print("[DEBUG-temporal-reconcile-close] after-client-close", file=sys.stderr, flush=True)
    print("[DEBUG-temporal-reconcile-close] before-loop-exit", file=sys.stderr, flush=True)



def _write_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(record, sort_keys=True))
    os.replace(temp, path)


def _read_record(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


async def _start_intent(args) -> None:
    """Persist a stable intent before making the request that the proxy may ACK-drop."""
    run_id = args.run_id
    operation_id = deterministic_start_operation_id(run_id)
    workflow_id = f"agent-run-{run_id}"
    path = Path(args.state_file)
    record = {
        "run_id": run_id,
        "session_id": args.session_id,
        "status": "running",
        "workflow_id": workflow_id,
        "start_operation_id": operation_id,
        "recovery_state": "start_pending",
        "events": [{
            "operation_id": operation_id,
            "outcome": "intent_persisted",
            "attempt": 0,
            "detail": "fsynced intent written before Temporal start call",
        }],
    }
    _write_record(path, record)

    # fsync parent directory and state file so a SIGKILL/reboot test does not
    # accidentally test page-cache luck rather than persisted intent.
    with path.open("rb") as persisted:
        os.fsync(persisted.fileno())
    try:
        directory_fd = os.open(str(path.parent), os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except (AttributeError, OSError):
        pass

    driver = await TemporalWorkflowDriver.connect(
        args.temporal_address,
        task_queue=args.task_queue,
        namespace=args.namespace,
    )
    try:
        # Configure TCP proxy fault injection to forward the StartWorkflow
        # request and drop its response. The helper treats all transport errors
        # as ambiguous; it must not overwrite the intent as FAILED.
        try:
            ref = await driver.start_run(runtime_run_id=run_id, session_id=args.session_id)
        except Exception as exc:
            record["recovery_state"] = "unknown"
            record["events"].append({
                "operation_id": operation_id,
                "outcome": "unknown",
                "attempt": 1,
                "detail": f"caller received no definitive start ACK: {type(exc).__name__}: {exc}",
            })
            _write_record(path, record)
            print(json.dumps({"phase": "unknown", **record}, sort_keys=True))
            raise

        record["workflow_ref"] = {
            "provider": ref.provider,
            "workflow_id": ref.workflow_id,
            "run_id": ref.run_id,
        }
        record["recovery_state"] = "attached"
        record["events"].append({
            "operation_id": operation_id,
            "outcome": "provider_acknowledged",
            "attempt": 1,
            "detail": f"workflow_id={ref.workflow_id}; temporal_run_id={ref.run_id}",
        })
        _write_record(path, record)
        print(json.dumps({"phase": "acknowledged", **record}, sort_keys=True))
    finally:
        close = getattr(driver.client, "close", None)
        if close:
            maybe = close()
            if asyncio.iscoroutine(maybe):
                await maybe


async def _reconcile(args) -> None:
    path = Path(args.state_file)
    record = _read_record(path)
    run_id = str(record["run_id"])
    operation_id = deterministic_start_operation_id(run_id)
    if record.get("start_operation_id") != operation_id:
        raise RuntimeError("persisted start operation identity mismatch")

    driver = await TemporalWorkflowDriver.connect(
        args.temporal_address,
        task_queue=args.task_queue,
        namespace=args.namespace,
    )
    try:
        try:
            ref = await driver.reconcile_start(
                runtime_run_id=run_id,
                session_id=str(record["session_id"]),
                operation_id=operation_id,
            )
        except Exception as exc:
            record["recovery_state"] = "unknown"
            record.setdefault("events", []).append({
                "operation_id": operation_id,
                "outcome": "unknown",
                "attempt": len(record.get("events", [])),
                "detail": f"provider state could not be verified: {type(exc).__name__}: {exc}; no start retry issued",
            })
            _write_record(path, record)
            print(json.dumps({"phase": "unknown", **record}, sort_keys=True))
            raise

        if ref is None:
            record["recovery_state"] = "unknown"
            record.setdefault("events", []).append({
                "operation_id": operation_id,
                "outcome": "unknown",
                "attempt": len(record.get("events", [])),
                "detail": "provider did not confirm the deterministic workflow; no replacement workflow started",
            })
            _write_record(path, record)
            raise RuntimeError("workflow outcome remains unknown; refusing a second start")

        record["workflow_ref"] = {
            "provider": ref.provider,
            "workflow_id": ref.workflow_id,
            "run_id": ref.run_id,
        }
        record["recovery_state"] = "attached"
        record.setdefault("events", []).append({
            "operation_id": operation_id,
            "outcome": "attached",
            "attempt": len(record.get("events", [])),
            "detail": f"attached existing workflow_id={ref.workflow_id}; provider={ref.provider}; temporal_run_id={ref.run_id}",
        })
        _write_record(path, record)
        print(json.dumps(record, sort_keys=True))
    finally:
        close = getattr(driver.client, "close", None)
        if close:
            maybe = close()
            if asyncio.iscoroutine(maybe):
                await maybe


def main() -> None:
    parser = argparse.ArgumentParser(prog="cloud-agent-runtime-recovery-probe")
    parser.add_argument(
        "command",
        choices=[
            "start-intent",
            "reconcile",
            "crash-after-accepted-start",
            "start-await-ack",
            "reconcile-sqlite",
        ],
    )
    parser.add_argument("--state-file")
    parser.add_argument("--sqlite-path")
    parser.add_argument("--run-id", default=os.environ.get("RECOVERY_RUN_ID", ""))
    parser.add_argument("--session-id", default=os.environ.get("RECOVERY_SESSION_ID", ""))
    parser.add_argument("--temporal-address", default=os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    parser.add_argument("--namespace", default=os.environ.get("TEMPORAL_NAMESPACE", "default"))
    parser.add_argument("--task-queue", default=os.environ.get("TEMPORAL_TASK_QUEUE", "agent-runs"))
    args = parser.parse_args()

    if args.command in {"start-intent", "reconcile"}:
        if not args.state_file:
            parser.error("--state-file is required for start-intent/reconcile")
        if args.command == "start-intent":
            if not args.run_id.strip() or not args.session_id.strip():
                parser.error("start-intent requires --run-id and --session-id")
            asyncio.run(_start_intent(args))
        else:
            asyncio.run(_reconcile(args))
        return

    if not args.sqlite_path:
        parser.error("--sqlite-path is required for process-recovery commands")
    if args.command == "crash-after-accepted-start":
        asyncio.run(_crash_after_accepted_start(args))
    elif args.command == "start-await-ack":
        if not args.run_id.strip():
            parser.error("start-await-ack requires --run-id")
        asyncio.run(_start_awaiting_ack(args))
    else:
        if not args.run_id.strip():
            parser.error("reconcile-sqlite requires --run-id")
        asyncio.run(_reconcile_sqlite(args))


if __name__ == "__main__":
    main()
