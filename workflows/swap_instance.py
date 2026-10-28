from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from activities.health import http_health_check
    from activities.terraform import read_terraform_outputs
    from shared.targets import target_for_queue, workflow_id

from workflows.provision_module import ProvisionModule


READ_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=5,
)

HEALTH_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=12,
)


@workflow.defn
class SwapInstance:
    """
    Roll the compute module to a new app_version. The TF apply replaces
    the EC2 (user_data_replace_on_change). Health check gates success.
    """

    @workflow.run
    async def run(self, env_name: str, new_app_version: str) -> dict:
        workflow.logger.info(f"SwapInstance: {env_name} -> {new_app_version}")

        # ApplicationError (not RuntimeError) so a logical failure ends the
        # workflow as Failed instead of retrying the workflow task forever.
        #
        # Compute needs the subnet and security group from the existing
        # network. Read them from state rather than re-planning the network.
        net_outputs = await workflow.execute_activity(
            read_terraform_outputs,
            args=["network", env_name],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=READ_RETRY,
        )
        if not net_outputs.get("subnet_id"):
            raise ApplicationError(f"swap: no network state for '{env_name}'; provision it first")

        compute_inputs = {
            "env_name": env_name,
            "app_version": new_app_version,
            "subnet_id": net_outputs["subnet_id"],
            "security_group_id": net_outputs.get("security_group_id"),
        }
        new_compute = await workflow.execute_child_workflow(
            ProvisionModule.run,
            args=["compute", env_name, compute_inputs],
            id=workflow_id(target_for_queue(workflow.info().task_queue), "swap-compute", env_name, new_app_version),
        )

        new_ip = new_compute.get("public_ip")
        if not new_ip:
            raise ApplicationError("swap: compute did not return public_ip")

        url = f"http://{new_ip}/"
        # Raises on a non-2xx response, so returning at all means healthy.
        health = await workflow.execute_activity(
            http_health_check,
            args=[url],
            start_to_close_timeout=timedelta(seconds=15),
            schedule_to_close_timeout=timedelta(minutes=5),
            retry_policy=HEALTH_RETRY,
        )
        url = health.url  # see DeployTestInstance: may resolve to a local port

        return {
            "env": env_name,
            "app_version": new_app_version,
            "public_ip": new_ip,
            "url": url,
            "status": "swapped-healthy",
        }
