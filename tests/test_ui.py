"""The UI's API against the real workflows and the sim activities: deploy with
approval, approve and reject through the API, and the fault toggles."""
import asyncio

import httpx
import pytest
from temporalio.worker import Worker

import ui
from activities import sim
from tests.test_sim import SIM_ACTIVITIES, TQ, new_env
from workflows.deploy_test_instance import DeployTestInstance
from workflows.provision_module import ProvisionModule
from workflows.swap_instance import SwapInstance
from workflows.teardown_instance import TeardownInstance


@pytest.fixture(autouse=True)
def sim_env(tmp_path, monkeypatch):
    monkeypatch.setattr(sim, "STATE_DIR", tmp_path)
    monkeypatch.setenv("SIM_SPEED", "0")
    monkeypatch.delenv("SIM_FAIL", raising=False)
    return tmp_path


@pytest.fixture
async def api(temporal_env):
    app = ui.create_app(temporal_env.client)
    async with Worker(
        temporal_env.client,
        task_queue=TQ,
        workflows=[DeployTestInstance, ProvisionModule, SwapInstance, TeardownInstance],
        activities=SIM_ACTIVITIES,
    ):
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://ui") as client:
                yield client


async def poll(api, env: str, done, timeout: float = 20) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        r = await api.get(f"/api/sim/envs/{env}")
        if r.status_code == 200 and done(d := r.json()):
            return d
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out; last: {r.status_code} {r.text[:500]}")
        await asyncio.sleep(0.1)


def awaiting(module: str):
    return lambda d: any(m["module"] == module and m["detail"] == "awaiting-approval" for m in d["modules"])


async def test_config_and_worker(api):
    config = (await api.get("/api/config")).json()
    assert config["targets"] == ["aws", "local", "sim"]
    assert config["task_queues"]["sim"] == TQ
    assert (await api.get("/api/gcp/envs")).status_code == 400
    # An option the server doesn't know is refused, not silently ignored.
    assert (await api.post("/api/sim/deploy", json={"env": "x", "fail_after_network": True})).status_code == 422


async def test_approve_then_reject_rolls_back(api, sim_env):
    env = new_env("ui")
    r = await api.post("/api/sim/deploy", json={"env": env, "require_approval": True})
    assert r.json()["workflow_id"] == f"deploy-{env}@sim"
    assert (await api.post("/api/sim/deploy", json={"env": env})).status_code == 409

    d = await poll(api, env, awaiting("network"))
    network = next(m for m in d["modules"] if m["module"] == "network")
    assert network["plan"]["summary"]["add"] == 6
    r = await api.post(f"/api/sim/envs/{env}/modules/network/decide", json={"approved": True, "reason": "ok"})
    assert r.json() == {"result": "approved"}
    # Deciding twice is refused by the update validator.
    r = await api.post(f"/api/sim/envs/{env}/modules/network/decide", json={"approved": True})
    assert r.status_code == 409

    await poll(api, env, awaiting("compute"))
    await api.post(f"/api/sim/envs/{env}/modules/compute/decide", json={"approved": False, "reason": "no"})
    d = await poll(api, env, lambda d: d["status"] == "failed")
    assert d["phase"] == "rolled-back"
    assert "plan rejected: no" in d["error"]
    kinds = {r["kind"] for r in d["related"]}
    assert {"deploy", "provision", "rollback"} <= kinds
    assert not list(sim_env.glob(f"{env}/*.json"))

    envs = (await api.get("/api/sim/envs")).json()
    assert any(e["env"] == env and e["status"] == "failed" for e in envs)


async def test_fault_toggle_reaches_the_worker(api, sim_env):
    r = await api.put("/api/sim/faults", json={"on": ["compute:apply"]})
    assert [p["id"] for p in r.json()["presets"] if p["on"]] == ["compute:apply"]
    assert (await api.put("/api/sim/faults", json={"on": ["bogus"]})).status_code == 400

    env = new_env("ui-fault")
    await api.post("/api/sim/deploy", json={"env": env})
    d = await poll(api, env, lambda d: d["status"] == "failed")
    assert "simulated failure for compute" in d["error"]
    await api.put("/api/sim/faults", json={"on": []})
