from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

from .temporal_driver import TemporalWorkflowDriver, deterministic_start_operation_id


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
    parser.add_argument("command", choices=["start-intent", "reconcile"])
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--run-id", default=os.environ.get("RECOVERY_RUN_ID", ""))
    parser.add_argument("--session-id", default=os.environ.get("RECOVERY_SESSION_ID", ""))
    parser.add_argument("--temporal-address", default=os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    parser.add_argument("--namespace", default=os.environ.get("TEMPORAL_NAMESPACE", "default"))
    parser.add_argument("--task-queue", default=os.environ.get("TEMPORAL_TASK_QUEUE", "agent-runs"))
    args = parser.parse_args()

    if args.command == "start-intent":
        if not args.run_id.strip() or not args.session_id.strip():
            parser.error("start-intent requires --run-id and --session-id")
        asyncio.run(_start_intent(args))
    else:
        asyncio.run(_reconcile(args))


if __name__ == "__main__":
    main()
