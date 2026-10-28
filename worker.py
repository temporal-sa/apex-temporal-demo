import argparse
import asyncio
import logging
import os
import sys
from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logging.getLogger("temporalio").setLevel(logging.ERROR)

from shared.config import FLOCI_ENDPOINT, TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TF_STATE_BUCKET
from shared.targets import DEFAULT_TARGET, METRICS_PORTS, TARGETS, task_queue
from workflows.deploy_test_instance import DeployTestInstance
from workflows.provision_module import ProvisionModule
from workflows.swap_instance import SwapInstance
from workflows.teardown_instance import TeardownInstance
from activities import floci, sim
from activities import terraform as tf
from activities.terraform import (
    run_terraform_plan,
    run_terraform_apply,
    run_terraform_destroy,
    read_terraform_outputs,
    simulate_long_step,
)
from activities.health import http_health_check

ACTIVITIES = {
    "aws": [
        run_terraform_plan,
        run_terraform_apply,
        run_terraform_destroy,
        read_terraform_outputs,
        simulate_long_step,
        http_health_check,
    ],
    # Real Terraform against Floci; the health check resolves Floci's port forward.
    "local": [
        run_terraform_plan,
        run_terraform_apply,
        run_terraform_destroy,
        read_terraform_outputs,
        simulate_long_step,
        floci.floci_health_check,
    ],
    # Same activity names, simulated: no Terraform, no AWS.
    "sim": [
        sim.sim_plan,
        sim.sim_apply,
        sim.sim_destroy,
        sim.sim_read_outputs,
        simulate_long_step,
        sim.sim_health_check,
    ],
}


async def main(target: str, port: int):
    if target == "aws" and not TF_STATE_BUCKET:
        sys.exit("TF_STATE_BUCKET is not set; copy .env.example to .env and fill it in")
    if target == "local":
        tf.use_target("local")
        try:
            floci.ensure_state_bucket()
        except Exception as e:
            sys.exit(f"Can't reach Floci at {FLOCI_ENDPOINT} ({type(e).__name__}). "
                     f"Start it with: docker compose --profile local up -d floci")
    runtime = Runtime(
        telemetry=TelemetryConfig(
            metrics=PrometheusConfig(bind_address=f"0.0.0.0:{port}"),
        )
    )
    client = await Client.connect(
        TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE, runtime=runtime
    )
    queue = task_queue(target)
    worker = Worker(
        client,
        task_queue=queue,
        workflows=[DeployTestInstance, ProvisionModule, SwapInstance, TeardownInstance],
        activities=ACTIVITIES[target],
    )
    print(f"Worker [{target}] on task queue: {queue}, metrics: http://localhost:{port}/metrics")
    await worker.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the worker for one deploy target.")
    parser.add_argument(
        "--target", choices=TARGETS, default=os.getenv("APEX_TARGET", DEFAULT_TARGET),
        help="aws: real Terraform and AWS; local: real Terraform against Floci in Docker; "
        "sim: simulated, no AWS needed (default: $APEX_TARGET or aws)",
    )
    parser.add_argument(
        "--metrics-port", type=int, metavar="PORT",
        help="Prometheus metrics port (default: 9090 aws, 9093 local, 9092 sim); set it to run a second worker",
    )
    args = parser.parse_args()
    asyncio.run(main(args.target, args.metrics_port or METRICS_PORTS[args.target]))
