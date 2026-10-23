from __future__ import annotations

import argparse
import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from .temporal_workflow import AgentRunWorkflow


async def run_worker(
    *,
    target: str,
    namespace: str,
    task_queue: str,
    max_cached_workflows: int | None = None,
) -> None:
    client = await Client.connect(
        target,
        namespace=namespace,
    )

    worker_options = {}
    if max_cached_workflows is not None:
        if (
            isinstance(max_cached_workflows, bool)
            or not isinstance(max_cached_workflows, int)
            or max_cached_workflows < 0
        ):
            raise ValueError("max_cached_workflows must be a non-negative integer")
        worker_options["max_cached_workflows"] = max_cached_workflows

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[AgentRunWorkflow],
        **worker_options,
    )
    await worker.run()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="cloud-agent-runtime-worker"
    )
    parser.add_argument(
        "--target",
        default=os.environ.get(
            "TEMPORAL_ADDRESS",
            "localhost:7233",
        ),
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get(
            "TEMPORAL_NAMESPACE",
            "default",
        ),
    )
    parser.add_argument(
        "--task-queue",
        default=os.environ.get(
            "TEMPORAL_TASK_QUEUE",
            "agent-runs",
        ),
    )
    parser.add_argument(
        "--max-cached-workflows",
        type=int,
        default=None,
        help="Override Temporal workflow cache capacity; 0 forces history replay.",
    )
    args = parser.parse_args()

    asyncio.run(
        run_worker(
            target=args.target,
            namespace=args.namespace,
            task_queue=args.task_queue,
            max_cached_workflows=args.max_cached_workflows,
        )
    )


if __name__ == "__main__":
    main()
