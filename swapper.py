import argparse
import asyncio
import sys
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.config import TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE
from shared.targets import DEFAULT_TARGET, add_target_arg, task_queue, workflow_id
from workflows.swap_instance import SwapInstance


async def main(env_name: str, new_version: str, target: str = DEFAULT_TARGET):
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    wf_id = workflow_id(target, "swap", env_name, new_version)
    try:
        handle = await client.start_workflow(
            SwapInstance.run,
            args=[env_name, new_version],
            id=wf_id,
            task_queue=task_queue(target),
        )
    except WorkflowAlreadyStartedError:
        print(
            f"Swap '{wf_id}' is already running. Wait for it to finish before re-running.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Started swap workflow: {handle.id}")
    result = await handle.result()
    print(f"Result: {result}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Roll an environment's compute to a new app version.")
    parser.add_argument("env", nargs="?", default="ephemeral-test-01", help="environment name")
    parser.add_argument("version", nargs="?", default="v2", help="new app version")
    add_target_arg(parser)
    args = parser.parse_args()
    asyncio.run(main(args.env, args.version, args.target))
