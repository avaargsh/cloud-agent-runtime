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
) -> None:
    client = await Client.connect(
        target,
        namespace=namespace,
    )

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[AgentRunWorkflow],
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
    args = parser.parse_args()

    asyncio.run(
        run_worker(
            target=args.target,
            namespace=args.namespace,
            task_queue=args.task_queue,
        )
    )


if __name__ == "__main__":
    main()
