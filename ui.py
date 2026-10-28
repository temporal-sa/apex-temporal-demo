"""Demo control panel: a small web UI over the same workflows the CLIs drive.

    python ui.py                 # http://localhost:8000

Browsers can't speak Temporal's gRPC API, so this FastAPI app wraps the
Temporal client: it starts workflows, reads them with the `status` and
`plan_summary` queries, sends approvals with the `decide` update, and the TTL
signals. Nothing here holds state of its own; refresh the page at any point.
"""
import argparse
import asyncio
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowHandle,
    WorkflowUpdateFailedError,
)
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from activities import sim
from approve import MODULES, provision_workflow_id
from shared.config import TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE
from shared.targets import TARGETS, task_queue, workflow_id
from workflows.deploy_test_instance import DeployTestInstance
from workflows.provision_module import DEFAULT_APPROVAL_TIMEOUT_SECONDS, ProvisionModule
from workflows.swap_instance import SwapInstance
from workflows.teardown_instance import TeardownInstance

TEMPORAL_UI_URL = os.getenv("TEMPORAL_UI_URL", "http://localhost:8233")
INDEX = Path(__file__).resolve().parent / "ui" / "index.html"

# A query on a closed workflow replays it on a worker; don't hang the page
# when that worker is down.
QUERY_TIMEOUT = timedelta(seconds=3)

# Fault presets the UI can toggle on the sim worker (see activities/sim.py).
SIM_FAULTS = {
    "health:check:flaky": "Instance slow to boot: health check retries until it answers",
    "network:apply:flaky": "Network apply is flaky: fails twice, then retries succeed",
    "compute:apply": "Compute apply fails after creating resources: saga rollback",
    "health:check": "Health check fails: rollback after everything is built",
}


class Body(BaseModel):
    # Refuse unknown fields instead of dropping them: a page and server out of
    # sync (e.g. a renamed option) must fail loudly, not start a different deploy.
    model_config = ConfigDict(extra="forbid")


def create_app(client: Client | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.client = client or await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
        yield

    app = FastAPI(title="Apex demo", lifespan=lifespan)

    def c() -> Client:
        return app.state.client

    def check_target(target: str) -> str:
        if target not in TARGETS:
            raise HTTPException(400, f"unknown target {target!r}")
        return target

    @app.get("/")
    async def index():
        return FileResponse(INDEX)

    @app.get("/api/config")
    async def config():
        return {
            "targets": list(TARGETS),
            "modules": MODULES,
            "namespace": c().namespace,
            "temporal_ui": TEMPORAL_UI_URL,
            "task_queues": {t: task_queue(t) for t in TARGETS},
            "default_approval_timeout": DEFAULT_APPROVAL_TIMEOUT_SECONDS,
        }

    @app.get("/api/{target}/worker")
    async def worker_status(target: str):
        """Whether a worker polled this target's queue recently. An idle worker
        long-polls for up to ~70s, so 'last seen' can lag a crash by that much."""
        queue = task_queue(check_target(target))
        resp = await c().workflow_service.describe_task_queue(
            DescribeTaskQueueRequest(
                namespace=c().namespace,
                task_queue=TaskQueue(name=queue),
                task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
            )
        )
        now = datetime.now(timezone.utc)
        ages = [(now - p.last_access_time.ToDatetime(timezone.utc)).total_seconds() for p in resp.pollers]
        last = min(ages) if ages else None
        return {
            "task_queue": queue,
            "online": last is not None and last < 75,
            "last_seen_seconds": round(last) if last is not None else None,
            "pollers": len(resp.pollers),
        }

    @app.get("/api/{target}/envs")
    async def list_envs(target: str):
        """Environments with a deploy workflow on this target, newest first."""
        queue = task_queue(check_target(target))
        seen, envs = set(), []
        query = f"WorkflowType = 'DeployTestInstance' AND TaskQueue = '{queue}'"
        async for wf in c().list_workflows(query, limit=50):
            if wf.id in seen:
                continue
            seen.add(wf.id)
            envs.append({
                "env": _env_from_deploy_id(wf.id, target),
                "workflow_id": wf.id,
                "status": _status(wf.status),
                "started": _iso(wf.start_time),
            })
        return envs

    @app.get("/api/{target}/envs/{env}")
    async def env_detail(target: str, env: str):
        check_target(target)
        deploy = c().get_workflow_handle(workflow_id(target, "deploy", env))
        try:
            desc = await deploy.describe()
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                raise HTTPException(404, f"no deploy for {env!r} on {target}")
            raise
        status = await _query(deploy, DeployTestInstance.status) or {}
        health_retry = next((p for p in _retries(desc) if p["activity"] == "http_health_check"), None)
        modules = await asyncio.gather(
            *(_module_state(c(), env, m, target, desc.start_time) for m in MODULES)
        )
        return {
            "env": env,
            "target": target,
            "workflow_id": deploy.id,
            "run_id": desc.run_id,
            "status": _status(desc.status),
            "started": _iso(desc.start_time),
            "closed": _iso(desc.close_time),
            "phase": status.get("phase"),
            "url": status.get("url"),
            "expires_at": status.get("expires_at"),
            "built": status.get("modules", []),
            "health_retry": health_retry,
            "modules": modules,
            "error": await _failure(deploy) if desc.status == WorkflowExecutionStatus.FAILED else None,
            "related": await _related(c(), env, target, desc.start_time),
        }

    class Deploy(Body):
        env: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,40}$")
        require_approval: bool = False
        approval_timeout: int = DEFAULT_APPROVAL_TIMEOUT_SECONDS
        ttl: int = 0
        fail_app_deploy: bool = False

    @app.post("/api/{target}/deploy")
    async def deploy(target: str, body: Deploy):
        args = [body.env, body.ttl, True, body.fail_app_deploy]
        if body.require_approval:
            args += [True, body.approval_timeout]
        return await _start(
            c(), DeployTestInstance.run, args, workflow_id(check_target(target), "deploy", body.env), target
        )

    class Decision(Body):
        approved: bool
        reason: str = ""

    @app.post("/api/{target}/envs/{env}/modules/{module}/decide")
    async def decide(target: str, env: str, module: str, body: Decision):
        if module not in MODULES:
            raise HTTPException(400, f"unknown module {module!r}")
        handle = c().get_workflow_handle(provision_workflow_id(env, module, check_target(target)))
        try:
            result = await handle.execute_update(ProvisionModule.decide, args=[body.approved, body.reason])
        except WorkflowUpdateFailedError as e:
            # The validator rejected it, e.g. it's no longer waiting for approval.
            raise HTTPException(409, str(e.cause))
        return {"result": result}

    class Extend(Body):
        seconds: int = Field(gt=0, le=24 * 3600)

    @app.post("/api/{target}/envs/{env}/extend")
    async def extend(target: str, env: str, body: Extend):
        await c().get_workflow_handle(workflow_id(check_target(target), "deploy", env)).signal(
            DeployTestInstance.extend_ttl, body.seconds
        )
        return {"ok": True}

    @app.post("/api/{target}/envs/{env}/teardown-now")
    async def teardown_now(target: str, env: str):
        await c().get_workflow_handle(workflow_id(check_target(target), "deploy", env)).signal(
            DeployTestInstance.teardown_now
        )
        return {"ok": True}

    @app.post("/api/{target}/envs/{env}/teardown")
    async def teardown(target: str, env: str):
        return await _start(c(), TeardownInstance.run, [env], workflow_id(check_target(target), "teardown", env), target)

    class Swap(Body):
        version: str = Field(pattern=r"^[A-Za-z0-9._-]{1,20}$")

    @app.post("/api/{target}/envs/{env}/swap")
    async def swap(target: str, env: str, body: Swap):
        wf_id = workflow_id(check_target(target), "swap", env, body.version)
        return await _start(c(), SwapInstance.run, [env, body.version], wf_id, target)

    @app.get("/api/sim/faults")
    async def get_faults():
        active = set(sim.fault_entries())
        return {"presets": [{"id": k, "label": v, "on": k in active} for k, v in SIM_FAULTS.items()]}

    class Faults(Body):
        on: list[str]

    @app.put("/api/sim/faults")
    async def put_faults(body: Faults):
        unknown = set(body.on) - set(SIM_FAULTS)
        if unknown:
            raise HTTPException(400, f"unknown faults: {sorted(unknown)}")
        sim.write_faults(body.on)
        return await get_faults()

    return app


async def _start(client: Client, run, args: list, wf_id: str, target: str) -> dict:
    try:
        handle = await client.start_workflow(run, args=args, id=wf_id, task_queue=task_queue(target))
    except WorkflowAlreadyStartedError:
        raise HTTPException(409, f"{wf_id} is already running")
    return {"workflow_id": handle.id, "run_id": handle.result_run_id}


async def _query(handle: WorkflowHandle, query):
    try:
        return await handle.query(query, rpc_timeout=QUERY_TIMEOUT)
    except Exception:
        return None  # no worker to answer, or not started yet


async def _module_state(client: Client, env: str, module: str, target: str, since: datetime | None) -> dict:
    handle = client.get_workflow_handle(provision_workflow_id(env, module, target))
    state = {"module": module, "workflow_id": handle.id, "status": "pending", "detail": None, "plan": None, "retry": None}
    try:
        desc = await handle.describe()
    except RPCError:
        return state
    # Same ID is reused across deploys of one env; ignore a run from an earlier deploy.
    if since and desc.start_time and desc.start_time < since:
        return state
    state["status"] = _status(desc.status)
    state["retry"] = next(iter(_retries(desc)), None)
    plan = await _query(handle, ProvisionModule.plan_summary)
    if plan:
        state["detail"] = plan["status"]
        state["plan"] = plan
    return state


def _retries(desc) -> list[dict]:
    """Pending activities that have already failed at least once: the retry
    policy at work. Retries don't add history events, so describe is the only
    place to see them."""
    out = []
    for p in desc.raw_description.pending_activities:
        if p.attempt > 1 or p.HasField("last_failure"):
            out.append({
                "activity": p.activity_type.name,
                "attempt": p.attempt,
                "last_failure": p.last_failure.message if p.HasField("last_failure") else None,
                "next_attempt": _iso(p.next_attempt_schedule_time.ToDatetime(timezone.utc))
                if p.HasField("next_attempt_schedule_time") else None,
            })
    return out


async def _failure(handle: WorkflowHandle) -> str | None:
    try:
        await handle.result(follow_runs=False, rpc_timeout=QUERY_TIMEOUT)
    except WorkflowFailureError as e:
        # Innermost cause carries the real message (e.g. "plan rejected: ...").
        err, msgs = e.cause, []
        while err is not None:
            msgs.append(getattr(err, "message", str(err)))
            err = err.__cause__
        return msgs[-1] if msgs else str(e)
    except Exception:
        return None
    return None


def _related_patterns(env: str, target: str) -> list[tuple[str, re.Pattern]]:
    e, sfx = re.escape(env), "" if target == "aws" else re.escape(f"@{target}")
    return [
        ("deploy", re.compile(rf"^deploy-{e}{sfx}$")),
        ("provision", re.compile(rf"^provision-(network|compute|app)-{e}{sfx}$")),
        ("teardown", re.compile(rf"^teardown(-ttl)?-{e}{sfx}$")),
        ("rollback", re.compile(rf"^rollback-{e}-\d+{sfx}$")),
        ("swap", re.compile(rf"^swap(-compute)?-{e}-[A-Za-z0-9._-]+{sfx}$")),
    ]


async def _related(client: Client, env: str, target: str, since: datetime | None) -> list[dict]:
    """Every workflow for this env since the current deploy started: children,
    rollback, teardowns, swaps. Newest first."""
    patterns = _related_patterns(env, target)
    out = []
    async for wf in client.list_workflows(f"TaskQueue = '{task_queue(target)}'", limit=200):
        kind = next((k for k, p in patterns if p.match(wf.id)), None)
        if kind is None or (since and wf.start_time and wf.start_time < since):
            continue
        out.append({
            "kind": kind,
            "workflow_id": wf.id,
            "run_id": wf.run_id,
            "type": wf.workflow_type,
            "status": _status(wf.status),
            "started": _iso(wf.start_time),
        })
    return sorted(out, key=lambda r: r["started"] or "", reverse=True)


def _env_from_deploy_id(wf_id: str, target: str) -> str:
    base = wf_id.removesuffix(f"@{target}") if target != "aws" else wf_id
    return base.removeprefix("deploy-")


def _status(status: WorkflowExecutionStatus | None) -> str:
    return status.name.lower().replace("_", "-") if status else "unknown"


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


app = create_app()

if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser(description="Run the demo control panel.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    print(f"Apex demo UI: http://{args.host}:{args.port}  (Temporal at {TEMPORAL_ADDRESS})")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
