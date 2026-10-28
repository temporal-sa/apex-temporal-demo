import argparse
import asyncio
import sys
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowHandle
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError

from approve import MODULES, format_plan, provision_workflow_id
from shared.config import TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE
from shared.targets import DEFAULT_TARGET, add_target_arg, task_queue, workflow_id
from workflows.deploy_test_instance import DeployTestInstance
from workflows.provision_module import DEFAULT_APPROVAL_TIMEOUT_SECONDS, ProvisionModule

# DeployTestInstance.status phases that mean the deploy is still on its way up.
IN_PROGRESS = {None, "starting", "provisioning-network", "provisioning-compute", "provisioning-app", "health-check"}


async def print_url_when_healthy(handle: WorkflowHandle) -> None:
    """With a TTL, the result only arrives after teardown, so ask the running
    workflow for the URL instead and print it as soon as the instance is up."""
    while True:
        try:
            state = await handle.query(DeployTestInstance.status)
        except RPCError:
            state = {}  # no worker yet, or it's restarting; ask again
        if state.get("phase") not in IN_PROGRESS:
            break
        await asyncio.sleep(5)
    if state["phase"] != "ttl-wait":
        return  # rolled back or already tearing down; handle.result() below reports it
    print(f"Healthy: {state['url']} (tears down at {state['expires_at']})")
    print(f"  extend 10 min: temporal workflow signal --workflow-id {handle.id} --name extend_ttl --input 600")
    print(f"  tear down now: temporal workflow signal --workflow-id {handle.id} --name teardown_now")
    print(f"  status:        temporal workflow query --workflow-id {handle.id} --name status")


async def print_approval_requests(
    client: Client, env_name: str, poll_seconds: float = 2, target: str = DEFAULT_TARGET
) -> None:
    """Print each module plan as it starts waiting for approval, with the
    commands to approve or reject it. Runs until cancelled."""
    seen: set[tuple[str, int]] = set()  # (module, approval round)
    while True:
        for module in MODULES:
            handle = client.get_workflow_handle(provision_workflow_id(env_name, module, target))
            try:
                # Skip closed runs (e.g. from an earlier deploy of this env).
                if (await handle.describe()).status != WorkflowExecutionStatus.RUNNING:
                    continue
                state = await handle.query(ProvisionModule.plan_summary)
            except RPCError:
                continue  # not started yet, or no worker; ask again
            key = (module, state["round"])
            if state["status"] != "awaiting-approval" or key in seen:
                continue
            seen.add(key)
            print(f"Approval needed:\n{format_plan(state)}")
            flag = "" if target == DEFAULT_TARGET else f" --target {target}"
            print(f'  approve: python approve.py {env_name} {module}{flag} --approve --reason "..."')
            print(f'  reject:  python approve.py {env_name} {module}{flag} --reject --reason "..."')
        await asyncio.sleep(poll_seconds)


async def main(
    env_name: str,
    ttl_seconds: int,
    fail_app_deploy: bool,
    require_approval: bool = False,
    approval_timeout: int = DEFAULT_APPROVAL_TIMEOUT_SECONDS,
    target: str = DEFAULT_TARGET,
):
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    wf_id = workflow_id(target, "deploy", env_name)
    args = [env_name, ttl_seconds, True, fail_app_deploy]
    if require_approval:
        args += [True, approval_timeout]
    try:
        handle = await client.start_workflow(
            DeployTestInstance.run,
            args=args,
            id=wf_id,
            task_queue=task_queue(target),
        )
    except WorkflowAlreadyStartedError:
        print(
            f"Workflow '{wf_id}' is already running. "
            f"Wait for it to finish, tear down '{env_name}' first, or pick a different env name.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Started workflow: {handle.id}")
    watcher = (
        asyncio.create_task(print_approval_requests(client, env_name, target=target)) if require_approval else None
    )
    try:
        if ttl_seconds > 0:
            await print_url_when_healthy(handle)
        result = await handle.result()
    finally:
        if watcher:
            watcher.cancel()
    print(f"Result: {result}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision an ephemeral test environment.")
    parser.add_argument(
        "env", nargs="?", default="ephemeral-test-01",
        help="environment name; names starting with 'chaos-' pause 30s after network for a kill -9 demo",
    )
    parser.add_argument(
        "--ttl", type=int, default=0, metavar="SECONDS",
        help="tear the environment down automatically after this many seconds (default: keep it)",
    )
    parser.add_argument(
        "--fail-app-deploy", action="store_true",
        help="make the app module fail during apply, after network and compute are built, to demo saga rollback",
    )
    parser.add_argument(
        "--require-approval", action="store_true",
        help="wait for approve.py before applying each module plan that has changes",
    )
    parser.add_argument(
        "--approval-timeout", type=int, default=DEFAULT_APPROVAL_TIMEOUT_SECONDS, metavar="SECONDS",
        help="with --require-approval, fail and roll back if a plan isn't decided in time (default: 24h)",
    )
    add_target_arg(parser)
    args = parser.parse_args()
    asyncio.run(
        main(
            args.env, args.ttl, args.fail_app_deploy, args.require_approval, args.approval_timeout, args.target
        )
    )
