import argparse
import asyncio
import sys
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.service import RPCError

from shared.config import TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE
from shared.targets import DEFAULT_TARGET, add_target_arg, workflow_id
from workflows.provision_module import ProvisionModule

MODULES = ["network", "compute", "app"]


def provision_workflow_id(env_name: str, module: str, target: str = DEFAULT_TARGET) -> str:
    # Matches the child workflow IDs DeployTestInstance uses.
    return workflow_id(target, "provision", module, env_name)


def format_plan(state: dict) -> str:
    """Render a ProvisionModule.plan_summary result for a terminal."""
    lines = [f"{state['module']}/{state['env']}: {state['status']}"]
    summary = state.get("summary")
    if summary:
        again = f", round {state['round']}" if state["round"] > 1 else ""
        lines.append(
            f"  plan: {summary['add']} to add, {summary['change']} to change, "
            f"{summary['destroy']} to destroy{again}"
        )
        lines += [f"    {r}" for r in summary["resources"]]
    if state.get("reason"):
        lines.append(f"  reason: {state['reason']}")
    return "\n".join(lines)


async def show(client: Client, env_name: str, module: str, target: str = DEFAULT_TARGET) -> dict:
    handle = client.get_workflow_handle(provision_workflow_id(env_name, module, target))
    return await handle.query(ProvisionModule.plan_summary)


async def decide(
    client: Client, env_name: str, module: str, approved: bool, reason: str, target: str = DEFAULT_TARGET
) -> str:
    handle = client.get_workflow_handle(provision_workflow_id(env_name, module, target))
    return await handle.execute_update(ProvisionModule.decide, args=[approved, reason])


async def main(env_name: str, module: str, approved: bool | None, reason: str, target: str = DEFAULT_TARGET):
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    wf_id = provision_workflow_id(env_name, module, target)
    try:
        print(format_plan(await show(client, env_name, module, target)))
        if approved is None:
            return
        result = await decide(client, env_name, module, approved, reason, target)
    except WorkflowUpdateFailedError as e:
        # The validator rejected it, e.g. the module isn't waiting for approval.
        print(f"Decision not accepted: {e.cause}", file=sys.stderr)
        sys.exit(1)
    except RPCError as e:
        print(f"Can't reach workflow '{wf_id}': {e}", file=sys.stderr)
        sys.exit(1)
    print(f"{module}/{env_name}: {result}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Show, approve, or reject a module plan waiting for approval "
        "(deploys started with --require-approval)."
    )
    parser.add_argument("env", help="environment name")
    parser.add_argument("module", choices=MODULES)
    decision = parser.add_mutually_exclusive_group()
    decision.add_argument("--approve", dest="approved", action="store_true", help="apply the plan")
    decision.add_argument("--reject", dest="approved", action="store_false", help="fail the deploy and roll back")
    parser.set_defaults(approved=None)  # neither flag: just show the plan
    parser.add_argument("--reason", default="", help="recorded in the workflow history with the decision")
    add_target_arg(parser)
    args = parser.parse_args()
    asyncio.run(main(args.env, args.module, args.approved, args.reason, args.target))
