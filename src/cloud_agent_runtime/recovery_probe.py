from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from .models import Run, RunStatus
from .serialization import run_from_dict, run_to_dict, session_from_dict, session_to_dict
from .store import SQLiteStore
from .temporal_driver import TemporalWorkflowDriver, deterministic_start_operation_id
from .async_runtime import AsyncAgentRuntime
from .sandbox import InMemorySandboxProvider


class DurableSQLiteStore(SQLiteStore):
    """Test-only SQLite-backed Run/Session store exposing small CLI persistence ops."""

    def save_session(self, session):
        super().save_session(session)

    def get_session(self, session_id):
        return super().get_session(session_id)


def _write_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(record, sort_keys=True))
    os.replace(temp, path)


def _read_record(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


async def _connect_driver(args):
    return await TemporalWorkflowDriver.connect(
        args.temporal_address,
        task_queue=args.task_queue,
        namespace=args.namespace,
    )


async def _start_intent(args) -> None:
    """Persist canonical Run intent, then request Temporal start, then exit immediately."""
    run_id = args.run_id
    operation_id = deterministic_start_operation_id(run_id)
    workflow_id = f"agent-run-{run_id}"
    record = {
        "run_id": run_id,
        "session_id": args.session_id,
        "status": RunStatus.RUNNING.value,
        "workflow_id": workflow_id,
        "start_operation_id": operation_id,
        "recovery_state": "start_pending",
        "events": [{
            "operation_id": operation_id,
            "outcome": "intent_persisted",
            "attempt": 0,
            "detail": "caller process persisted canonical identity before Temporal start",
        }],
    }
    _write_record(Path(args.state_file), record)

    driver = await _connect_driver(args)
    try:
        # This branch is for process-level acceptance. A proxy can inject
        # RST after forwarding StartWorkflow to Temporal but before response.
        ref = await driver.start_run(
            runtime_run_id=run_id,
            session_id=args.session_id,
        )
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
        _write_record(Path(args.state_file), record)
        print(json.dumps({"phase": "acknowledged", **record}, sort_keys=True))
    finally:
        close = getattr(driver.client, "close", None)
        if close:
            maybe = close()
            if asyncio.iscoroutine(maybe):
                await maybe


async def _reconcile(args) -> None:
    """Reattach to a confirmed existing workflow. Never starts an absent/unknown workflow."""
    state_path = Path(args.state_file)
    record = _read_record(state_path)
    run_id = str(record["run_id"])
    operation_id = deterministic_start_operation_id(run_id)
    if record.get("start_operation_id") != operation_id:
        raise RuntimeError("persisted start operation identity mismatch")

    driver = await _connect_driver(args)
    try:
        ref = await driver.reconcile_start(
            runtime_run_id=run_id,
            session_id=str(record["session_id"]),
            operation_id=operation_id,
        )
        record.setdefault("events", []).append({
            "operation_id": operation_id,
            "outcome": "unknown",
            "attempt": len(record.get("events", [])),
            "detail": "restart process began reconciliation without assuming the previous ACK outcome",
        })
        if ref is None:
            record["recovery_state"] = "unknown"
            record["events"].append({
                "operation_id": operation_id,
                "outcome": "unknown",
                "attempt": len(record["events"]),
                "detail": "provider did not confirm an execution; no replacement workflow started",
            })
            _write_record(state_path, record)
            raise RuntimeError("workflow outcome remains unknown; refusing a second start")

        record["workflow_ref"] = {
            "provider": ref.provider,
            "workflow_id": ref.workflow_id,
            "run_id": ref.run_id,
        }
        record["recovery_state"] = "attached"
        record["events"].append({
            "operation_id": operation_id,
            "outcome": "attached",
            "attempt": len(record["events"]),
            "detail": f"attached existing workflow_id={ref.workflow_id}; provider={ref.provider}; temporal_run_id={ref.run_id}",
        })
        _write_record(state_path, record)
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
