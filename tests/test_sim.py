"""The real workflows against the simulated activities, on the sim target's own
task queue, so the `@sim` workflow IDs are exercised end to end."""
import asyncio
import uuid
from datetime import timedelta

import pytest
from temporalio.client import Client, WorkflowFailureError
from temporalio.worker import Worker

import approve
from activities import sim
from activities.terraform import simulate_long_step
from shared import targets
from workflows.deploy_test_instance import DeployTestInstance
from workflows.provision_module import ProvisionModule
from workflows.swap_instance import SwapInstance
from workflows.teardown_instance import TeardownInstance

SIM_ACTIVITIES = [
    sim.sim_plan,
    sim.sim_apply,
    sim.sim_destroy,
    sim.sim_read_outputs,
    simulate_long_step,
    sim.sim_health_check,
]
TQ = targets.task_queue("sim")


@pytest.fixture
def client(temporal_env) -> Client:
    return temporal_env.client


@pytest.fixture(autouse=True)
def sim_env(tmp_path, monkeypatch):
    monkeypatch.setattr(sim, "STATE_DIR", tmp_path)
    monkeypatch.setenv("SIM_SPEED", "0")
    monkeypatch.delenv("SIM_FAIL", raising=False)
    return tmp_path


@pytest.fixture
async def worker(client):
    async with Worker(
        client,
        task_queue=TQ,
        workflows=[DeployTestInstance, ProvisionModule, SwapInstance, TeardownInstance],
        activities=SIM_ACTIVITIES,
    ):
        yield


def new_env(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


async def deploy(client: Client, env: str, require_approval: bool = False):
    args = [env, 0, True, False] + ([True, 3600] if require_approval else [])
    return await client.start_workflow(
        DeployTestInstance.run, args=args, id=targets.workflow_id("sim", "deploy", env), task_queue=TQ
    )


def test_target_helpers():
    assert targets.task_queue("aws") == targets.TASK_QUEUE_NONPROD
    assert targets.task_queue("sim") == f"{targets.TASK_QUEUE_NONPROD}-sim"
    assert targets.target_for_queue(targets.task_queue("sim")) == "sim"
    assert targets.target_for_queue("some-test-queue") == "aws"
    assert targets.workflow_id("aws", "provision", "network", "e1") == "provision-network-e1"
    assert targets.workflow_id("sim", "provision", "network", "e1") == "provision-network-e1@sim"
    with pytest.raises(ValueError):
        targets.task_queue("gcp")


async def test_deploy_then_teardown(client, worker, sim_env, monkeypatch):
    env = new_env("sim")
    result = await (await deploy(client, env)).result()

    assert result["status"] == "healthy"
    assert result["public_ip"].startswith("198.51.100.")
    assert {p.name for p in (sim_env / env).glob("*.json")} == {"network.json", "compute.json", "app.json"}
    # Children were named for the sim target.
    child = await client.get_workflow_handle(f"provision-network-{env}@sim").describe()
    assert child.task_queue == TQ

    async def teardown(run: int):
        return await client.execute_workflow(
            TeardownInstance.run, env, id=targets.workflow_id("sim", "teardown", env, str(run)), task_queue=TQ
        )

    assert (await teardown(1))["modules"] == {m: "destroyed" for m in ["app", "compute", "network"]}
    assert not (sim_env / env).exists()
    # Idempotent: a second teardown finds empty state and skips everything.
    assert set((await teardown(2))["modules"].values()) == {"skipped (state empty)"}


async def test_injected_failure_rolls_back(client, worker, sim_env, monkeypatch):
    monkeypatch.setenv("SIM_FAIL", "compute:apply")
    env = new_env("sim-fail")
    with pytest.raises(WorkflowFailureError):
        await (await deploy(client, env)).result()
    # Compute failed after its resources appeared; the rollback destroyed it and network.
    assert not list(sim_env.glob(f"{env}/*.json"))
    status = await client.get_workflow_handle(targets.workflow_id("sim", "deploy", env)).query(
        DeployTestInstance.status
    )
    assert status["phase"] == "rolled-back"


async def test_failed_app_deploy_rolls_back_network_and_compute(client, worker, sim_env):
    env = new_env("sim-app")
    handle = await client.start_workflow(
        DeployTestInstance.run, args=[env, 0, True, True], id=targets.workflow_id("sim", "deploy", env), task_queue=TQ
    )
    with pytest.raises(WorkflowFailureError):
        await handle.result()
    app = await client.get_workflow_handle(f"provision-app-{env}@sim").describe()
    assert app.status.name == "FAILED"
    status = await handle.query(DeployTestInstance.status)
    assert status["modules"] == ["network", "compute"]
    assert status["phase"] == "rolled-back"
    assert not list(sim_env.glob(f"{env}/*.json"))


async def test_flaky_step_retries_then_succeeds(client, worker, monkeypatch):
    monkeypatch.setenv("SIM_FAIL", "network:plan:flaky")
    env = new_env("sim-flaky")
    assert (await (await deploy(client, env)).result())["status"] == "healthy"
    # Retries don't add history events; the started event records the final attempt.
    history = await client.get_workflow_handle(f"provision-network-{env}@sim").fetch_history()
    started = [e for e in history.events if e.HasField("activity_task_started_event_attributes")]
    assert started[0].activity_task_started_event_attributes.attempt == sim.FLAKY_ATTEMPTS + 1


async def test_rejecting_a_plan_rolls_back(client, worker, sim_env):
    env = new_env("sim-approve")
    handle = await deploy(client, env, require_approval=True)

    async def wait_for(module: str) -> dict:
        for _ in range(100):
            try:
                state = await approve.show(client, env, module, "sim")
                if state["status"] == "awaiting-approval":
                    return state
            except Exception:
                pass
            await asyncio.sleep(0.1)
        raise AssertionError(f"{module} never waited for approval")

    plan = await wait_for("network")
    assert plan["summary"]["add"] == 6
    assert "+ aws_vpc.this" in plan["summary"]["resources"]
    assert await approve.decide(client, env, "network", True, "looks good", "sim") == "approved"
    await wait_for("compute")
    assert await approve.decide(client, env, "compute", False, "not today", "sim") == "rejected"

    with pytest.raises(WorkflowFailureError):
        await handle.result()
    assert not list(sim_env.glob(f"{env}/*.json"))


async def test_swap_replaces_the_instance(client, worker):
    env = new_env("sim-swap")
    before = await (await deploy(client, env)).result()
    after = await client.execute_workflow(
        SwapInstance.run,
        args=[env, "v2"],
        id=targets.workflow_id("sim", "swap", env, "v2"),
        task_queue=TQ,
        execution_timeout=timedelta(minutes=2),
    )
    assert after["status"] == "swapped-healthy"
    assert after["public_ip"] != before["public_ip"]
