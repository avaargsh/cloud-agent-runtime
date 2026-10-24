from __future__ import annotations

import os
import subprocess
import sys


def test_temporal_workflow_import_does_not_load_http_compute_client() -> None:
    # Importing a Temporal workflow happens inside the deterministic workflow
    # sandbox. Infrastructure-only HTTP modules must stay outside that import
    # graph; package-level convenience exports must not eagerly load them.
    script = """
import sys
import cloud_agent_runtime.temporal_workflow
assert "cloud_agent_runtime.management_compute_client" not in sys.modules
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        timeout=15,
    )
    assert completed.returncode == 0, (
        "Temporal workflow import eagerly loaded the Management HTTP adapter; "
        "keep infrastructure exports lazy.\n"
        f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    )
