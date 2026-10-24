from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest
from hpack import Decoder
from temporalio.api.enums.v1 import EventType
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.testing import WorkflowEnvironment


async def _start_cached_time_skipping():
    """Reuse the Temporal test-server executable and retry transient rate limits."""
    download_dir = Path.home() / ".cache" / "temporal-test-server"
    download_dir.mkdir(parents=True, exist_ok=True)
    for attempt in range(4):
        try:
            return await WorkflowEnvironment.start_time_skipping(
                download_dest_dir=str(download_dir)
            )
        except RuntimeError as exc:
            if "429 Too Many Requests" not in str(exc) or attempt == 3:
                raise
            await asyncio.sleep(2 ** attempt)
    raise AssertionError("unreachable Temporal test-server retry state")
from temporalio.worker import Worker

from cloud_agent_runtime.temporal_workflow import AgentRunWorkflow


_CLIENT_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
_START_WORKFLOW_PATH = "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"


class _HTTP2FrameParser:
    """Incremental HTTP/2 frame parser; decodes client HPACK headers only."""

    def __init__(self, *, client_direction: bool) -> None:
        self.client_direction = client_direction
        self.buffer = bytearray()
        self.preface_checked = not client_direction
        self.decoder = Decoder()
        self.header_blocks: dict[int, bytearray] = {}

    @staticmethod
    def _header_fragment(payload: bytes, flags: int) -> bytes:
        offset = 0
        padding = 0
        if flags & 0x08:  # PADDED
            if not payload:
                raise ValueError("invalid padded HTTP/2 HEADERS frame")
            padding = payload[0]
            offset = 1
        if flags & 0x20:  # PRIORITY
            offset += 5
        end = len(payload) - padding
        if end < offset:
            raise ValueError("invalid HTTP/2 HEADERS padding/priority")
        return payload[offset:end]

    def _decode_completed_headers(self, stream_id: int, block: bytes):
        decoded = self.decoder.decode(block, raw=False)
        headers = dict(decoded)
        path = headers.get(":path", "")
        if path.endswith("/StartWorkflowExecution"):
            return [(stream_id, path)]
        return []

    def feed(self, data: bytes) -> tuple[list[tuple[int, int, bytes]], list[tuple[int, str]]]:
        self.buffer.extend(data)
        if self.client_direction and not self.preface_checked:
            if len(self.buffer) < len(_CLIENT_PREFACE):
                return [], []
            if bytes(self.buffer[:len(_CLIENT_PREFACE)]) != _CLIENT_PREFACE:
                raise ValueError("expected HTTP/2 client connection preface")
            del self.buffer[:len(_CLIENT_PREFACE)]
            self.preface_checked = True

        frames: list[tuple[int, int, bytes]] = []
        start_requests: list[tuple[int, str]] = []
        while len(self.buffer) >= 9:
            payload_length = int.from_bytes(self.buffer[0:3], "big")
            frame_size = 9 + payload_length
            if len(self.buffer) < frame_size:
                break
            raw_frame = bytes(self.buffer[:frame_size])
            del self.buffer[:frame_size]
            frame_type = raw_frame[3]
            flags = raw_frame[4]
            stream_id = int.from_bytes(raw_frame[5:9], "big") & 0x7FFFFFFF
            payload = raw_frame[9:]

            if self.client_direction:
                if frame_type == 0x1:  # HEADERS
                    fragment = self._header_fragment(payload, flags)
                    if flags & 0x04:  # END_HEADERS
                        start_requests.extend(
                            self._decode_completed_headers(stream_id, fragment)
                        )
                    else:
                        self.header_blocks[stream_id] = bytearray(fragment)
                elif frame_type == 0x9 and stream_id in self.header_blocks:  # CONTINUATION
                    self.header_blocks[stream_id].extend(payload)
                    if flags & 0x04:
                        block = bytes(self.header_blocks.pop(stream_id))
                        start_requests.extend(
                            self._decode_completed_headers(stream_id, block)
                        )
            frames.append((stream_id, frame_type, raw_frame))
        return frames, start_requests


class _TemporalStartAckDropProxy:
    """Forward an HTTP/2 StartWorkflow request but discard its response frames."""

    def __init__(self, target: str) -> None:
        host, port = target.rsplit(":", 1)
        self.target_host = host.strip("[]")
        self.target_port = int(port)
        self.server: asyncio.AbstractServer | None = None
        self.request_forwarded = asyncio.Event()
        self.response_dropped = asyncio.Event()
        self.events: list[dict[str, object]] = []
        self.errors: list[str] = []
        self._writers: set[asyncio.StreamWriter] = set()

    @property
    def address(self) -> str:
        if self.server is None or not self.server.sockets:
            raise RuntimeError("proxy has not been started")
        host, port = self.server.sockets[0].getsockname()[:2]
        return f"{host}:{port}"

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for writer in tuple(self._writers):
            writer.close()
        if self._writers:
            await asyncio.gather(
                *(writer.wait_closed() for writer in tuple(self._writers)),
                return_exceptions=True,
            )

    async def _handle(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                self.target_host, self.target_port
            )
        except Exception as exc:
            self.errors.append(f"upstream connect failed: {type(exc).__name__}: {exc}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        self._writers.update((client_writer, upstream_writer))
        request_parser = _HTTP2FrameParser(client_direction=True)
        response_parser = _HTTP2FrameParser(client_direction=False)
        blocked_stream_ids: set[int] = set()

        async def client_to_temporal() -> None:
            while True:
                data = await client_reader.read(65536)
                if not data:
                    return
                _, matched = request_parser.feed(data)
                # Mark the stream before forwarding the bytes. If Temporal
                # responds immediately, the response task already knows to drop it.
                for stream_id, path in matched:
                    blocked_stream_ids.add(stream_id)
                upstream_writer.write(data)
                await upstream_writer.drain()
                for stream_id, path in matched:
                    self.events.append({
                        "event": "request_forwarded",
                        "method": path,
                        "stream_id": stream_id,
                        "timestamp": time.time(),
                    })
                    self.request_forwarded.set()

        async def temporal_to_client() -> None:
            while True:
                data = await upstream_reader.read(65536)
                if not data:
                    return
                frames, _ = response_parser.feed(data)
                for stream_id, frame_type, raw_frame in frames:
                    if stream_id in blocked_stream_ids:
                        self.events.append({
                            "event": "response_frame_dropped",
                            "frame_type": frame_type,
                            "stream_id": stream_id,
                            "timestamp": time.time(),
                        })
                        self.response_dropped.set()
                        continue
                    client_writer.write(raw_frame)
                await client_writer.drain()

        tasks = {
            asyncio.create_task(client_to_temporal()),
            asyncio.create_task(temporal_to_client()),
        }
        try:
            _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                    self.errors.append(f"proxy relay failed: {type(result).__name__}: {result}")
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for writer in (client_writer, upstream_writer):
                writer.close()
            await asyncio.gather(
                client_writer.wait_closed(),
                upstream_writer.wait_closed(),
                return_exceptions=True,
            )
            self._writers.discard(client_writer)
            self._writers.discard(upstream_writer)


async def _wait_for_running_workflow(client, workflow_id: str, timeout: float = 15.0):
    handle = client.get_workflow_handle(workflow_id)
    deadline = asyncio.get_running_loop().time() + timeout
    last_error: Exception | None = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            description = await handle.describe()
            status = getattr(description, "status", None)
            status_name = getattr(status, "name", str(status))
            if status_name == "RUNNING":
                return handle, description
        except Exception as exc:
            last_error = exc
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"Temporal did not expose RUNNING workflow {workflow_id}; "
        f"last_error={last_error!r}"
    )


def test_tcp_proxy_drops_temporal_start_ack_then_sigkill_and_reconcile(tmp_path) -> None:
    async def scenario() -> None:
        db_path = tmp_path / "ack-drop.sqlite"
        task_queue = f"p0-ack-drop-{uuid4()}"
        run_id = f"p0-ack-drop-{uuid4()}"
        workflow_id = f"agent-run-{run_id}"
        operation_id = f"agent-run-start:{run_id}"

        async with await _start_cached_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[AgentRunWorkflow],
            ):
                proxy = _TemporalStartAckDropProxy(
                    env.client.service_client.config.target_host
                )
                await proxy.start()
                caller: asyncio.subprocess.Process | None = None
                try:
                    caller = await asyncio.create_subprocess_exec(
                        sys.executable,
                        "-m",
                        "cloud_agent_runtime.recovery_probe",
                        "start-await-ack",
                        "--sqlite-path",
                        str(db_path),
                        "--run-id",
                        run_id,
                        "--temporal-address",
                        proxy.address,
                        "--namespace",
                        "default",
                        "--task-queue",
                        task_queue,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )

                    await asyncio.wait_for(proxy.request_forwarded.wait(), timeout=15)
                    handle, description = await _wait_for_running_workflow(
                        env.client, workflow_id, timeout=15
                    )
                    await asyncio.wait_for(proxy.response_dropped.wait(), timeout=15)
                    assert any(
                        event["event"] == "request_forwarded"
                        and event["method"] == _START_WORKFLOW_PATH
                        for event in proxy.events
                    ), f"proxy did not log forwarded StartWorkflow: {proxy.events!r}"
                    assert any(
                        event["event"] == "response_frame_dropped"
                        for event in proxy.events
                    ), f"proxy did not log a dropped response: {proxy.events!r}"
                    assert caller.returncode is None, (
                        "caller returned although StartWorkflow response frames were dropped"
                    )

                    with sqlite3.connect(db_path) as connection:
                        rows = connection.execute(
                            "SELECT run_id, payload FROM runs"
                        ).fetchall()
                    assert len(rows) == 1, "expected one canonical Run before SIGKILL"
                    persisted_run_id, payload_text = rows[0]
                    persisted = json.loads(payload_text)
                    assert persisted_run_id == run_id
                    assert persisted["run_id"] == run_id
                    assert persisted["recovery_operation_id"] == operation_id
                    assert persisted["recovery_state"] == "start_pending"
                    assert persisted["workflow_ref"] is None

                    caller.send_signal(signal.SIGKILL)
                    caller_stdout, caller_stderr = await asyncio.wait_for(
                        caller.communicate(), timeout=10
                    )
                    assert caller.returncode == -signal.SIGKILL, (
                        f"expected SIGKILL exit; returncode={caller.returncode}; "
                        f"stdout={caller_stdout.decode(errors='replace')}; "
                        f"stderr={caller_stderr.decode(errors='replace')}"
                    )

                    reconcile = await asyncio.create_subprocess_exec(
                        sys.executable,
                        "-m",
                        "cloud_agent_runtime.recovery_probe",
                        "reconcile-sqlite",
                        "--sqlite-path",
                        str(db_path),
                        "--run-id",
                        run_id,
                        "--temporal-address",
                        env.client.service_client.config.target_host,
                        "--namespace",
                        "default",
                        "--task-queue",
                        task_queue,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    recovered_stdout, recovered_stderr = await asyncio.wait_for(
                        reconcile.communicate(), timeout=30
                    )
                    assert reconcile.returncode == 0, (
                        f"reconcile failed with {reconcile.returncode}; "
                        f"stdout={recovered_stdout.decode(errors='replace')}; "
                        f"stderr={recovered_stderr.decode(errors='replace')}"
                    )
                    recovered = json.loads(recovered_stdout.decode())
                    assert recovered["run_id"] == run_id
                    assert recovered["recovery_operation_id"] == operation_id
                    assert recovered["recovery_state"] == "attached"
                    assert recovered["workflow_ref"]["workflow_id"] == workflow_id

                    description = await handle.describe()
                    assert getattr(description.status, "name", "") == "RUNNING"
                    observed_temporal_run_id = getattr(description, "run_id", None)
                    assert isinstance(observed_temporal_run_id, str) and observed_temporal_run_id, (
                        "Temporal Describe must expose the original execution run ID"
                    )
                    assert recovered["workflow_ref"]["run_id"] == observed_temporal_run_id, (
                        "recovered WorkflowRef must retain the described Temporal execution run ID"
                    )
                    history = await handle.fetch_history()
                    events = list(getattr(history, "events", []))
                    if not events and getattr(history, "history", None) is not None:
                        events = list(getattr(history.history, "events", []))
                    started = [
                        event for event in events
                        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
                    ]
                    assert len(started) == 1, (
                        f"expected exactly one WorkflowExecutionStarted event, got {len(started)}"
                    )

                    from temporalio.common import WorkflowIDReusePolicy

                    with pytest.raises(WorkflowAlreadyStartedError):
                        await env.client.start_workflow(
                            AgentRunWorkflow.run,
                            {
                                "runtime_run_id": run_id,
                                "session_id": persisted["session_id"],
                            },
                            id=workflow_id,
                            task_queue=task_queue,
                            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                        )

                    evidence = {
                        "scenario": "tcp_grpc_start_response_drop_then_sigkill_and_reconcile",
                        "run_id": run_id,
                        "start_operation_id": operation_id,
                        "workflow_id": workflow_id,
                        "temporal_status_after_recovery": getattr(description.status, "name", str(description.status)),
                        "temporal_execution_run_id": observed_temporal_run_id,
                        "workflow_execution_started_event_count": len(started),
                        "duplicate_start": "rejected",
                        "caller_exit": "SIGKILL",
                        "proxy_events": proxy.events,
                    }
                    evidence_dir = Path(os.environ.get("P0_RECOVERY_EVIDENCE_DIR", str(tmp_path)))
                    evidence_dir.mkdir(parents=True, exist_ok=True)
                    evidence_path = evidence_dir / "temporal-ack-drop-evidence.json"
                    evidence_path.write_text(
                        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    print(f"TEMPORAL_ACK_DROP_EVIDENCE={evidence_path}")
                finally:
                    if caller is not None and caller.returncode is None:
                        caller.kill()
                        await asyncio.wait_for(caller.wait(), timeout=10)
                    await proxy.close()

    asyncio.run(scenario())
