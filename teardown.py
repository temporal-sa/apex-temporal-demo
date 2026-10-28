import argparse
import asyncio
import sys
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.config import TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE
from shared.targets import DEFAULT_TARGET, add_target_arg, task_queue, workflow_id
from workflows.teardown_instance import TeardownInstance


async def main(env_name: str, target: str = DEFAULT_TARGET):
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    wf_id = workflow_id(target, "teardown", env_name)
    try:
        handle = await client.start_workflow(
            TeardownInstance.run,
            env_name,
            id=wf_id,
            task_queue=task_queue(target),
        )
    except WorkflowAlreadyStartedError:
        print(
            f"Teardown '{wf_id}' is already running. Wait for it to finish before re-running.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Started teardown: {handle.id}")
    result = await handle.result()
    print(f"Result: {result}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tear down an environment.")
    parser.add_argument("env", nargs="?", default="ephemeral-test-01", help="environment name")
    add_target_arg(parser)
    args = parser.parse_args()
    asyncio.run(main(args.env, args.target))
