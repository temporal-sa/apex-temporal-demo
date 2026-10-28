"""Simulated activities for the `sim` target: no Terraform, no AWS.

Each one is registered under the same name as its real counterpart and returns
the same types, so the workflows can't tell the difference. Retries, saga
rollback, approvals, TTL timers, and crash recovery all behave exactly as they
do against AWS.

Simulated state lives in JSON files under .sim_state/<env>/<module>.json (saved
plans next to them as <module>.tfplan), so it survives a worker `kill -9` the
way S3 state does.

Each module takes about 10 seconds (plan plus apply) and so does the health
check, so every step is visible in a live demo.

Knobs (worker environment):
  SIM_SPEED         multiplies every simulated delay (default 1; 0 = instant)
  SIM_FAIL          comma-separated failures to inject:
                      <module>:<step>        fails every attempt, non-retryable
                      <module>:<step>:flaky  fails the first 2 attempts, then passes
                    module is network, compute, app, or health; step is plan,
                    apply, or destroy (health only has `check`).
                    e.g. SIM_FAIL=compute:apply  or  SIM_FAIL=network:apply:flaky,health:check
                    health:check:flaky is an instance slow to boot.

The deploy's `fail_app_deploy` option needs no knob: it passes
simulate_failure=true to the app module, which fails during apply here just
as the real Terraform module's provisioner does.

Failures can also be toggled while the worker runs: entries in
.sim_state/faults (same format, one per line or comma-separated) are read on
every attempt and added to SIM_FAIL. The demo UI writes this file.
"""
import asyncio
import hashlib
import json
import os
import shutil
from pathlib import Path

from temporalio import activity
from temporalio.exceptions import ApplicationError

from activities.health import HealthResult
from activities.terraform import ApplyResult, PlanResult

REPO_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = REPO_ROOT / ".sim_state"

FLAKY_ATTEMPTS = 2

# What each module "builds", mirroring terraform/modules/*.
RESOURCES = {
    "network": [
        "aws_internet_gateway.this",
        "aws_route_table.public",
        "aws_route_table_association.public",
        "aws_security_group.web",
        "aws_subnet.public",
        "aws_vpc.this",
    ],
    "compute": ["aws_instance.web"],
    "app": ["null_resource.app_marker"],
}

# Simulated durations in seconds (before SIM_SPEED). Plan plus apply is at
# least 10s per module so each step stays on screen long enough to talk to.
PLAN_SECONDS = 3
APPLY_SECONDS = {"network": 8, "compute": 9, "app": 7}
DESTROY_SECONDS = {"network": 4, "compute": 4, "app": 2}
HEALTH_SECONDS = 10
# The workflows give the health check 15s per attempt; stay inside that.
HEALTH_MAX_SECONDS = 12


def _speed() -> float:
    return float(os.getenv("SIM_SPEED", "1"))


def fault_entries() -> list[str]:
    """Active fault entries: SIM_FAIL plus the faults file."""
    path = STATE_DIR / "faults"
    spec = os.getenv("SIM_FAIL", "") + "," + (path.read_text().replace("\n", ",") if path.exists() else "")
    return [e for e in (e.strip() for e in spec.split(",")) if e]


def write_faults(entries: list[str]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    (STATE_DIR / "faults").write_text("\n".join(entries) + ("\n" if entries else ""))


def _injected(module: str, step: str) -> str | None:
    """'always', 'flaky', or None for this module/step, from fault_entries()."""
    for entry in fault_entries():
        parts = entry.split(":")
        if parts[:2] == [module, step]:
            return "flaky" if parts[2:] == ["flaky"] else "always"
    return None


def _maybe_fail(module: str, step: str) -> None:
    mode = _injected(module, step)
    attempt = activity.info().attempt
    if mode == "always":
        raise ApplicationError(
            f"terraform {step} failed: simulated failure for {module} (SIM_FAIL)",
            type="SimulatedFailure",
            non_retryable=True,
        )
    if mode == "flaky" and attempt <= FLAKY_ATTEMPTS:
        raise ApplicationError(
            f"terraform {step} failed: simulated transient failure for {module} "
            f"(attempt {attempt} of {FLAKY_ATTEMPTS} that fail)",
            type="TerraformError",
        )


async def _work(seconds: float, label: str, max_seconds: float | None = None) -> None:
    """Sleep while heartbeating, like a long terraform command."""
    remaining = seconds * _speed()
    if max_seconds is not None:
        remaining = min(remaining, max_seconds)
    while remaining > 0:
        activity.heartbeat(label)
        step = min(1.0, remaining)
        await asyncio.sleep(step)
        remaining -= step


def _state_path(module: str, env: str) -> Path:
    return STATE_DIR / env / f"{module}.json"


def _read_state(module: str, env: str) -> dict | None:
    path = _state_path(module, env)
    return json.loads(path.read_text()) if path.exists() else None


def _write_state(module: str, env: str, state: dict) -> None:
    path = _state_path(module, env)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(path)  # atomic, so a kill -9 never leaves half a file


def _fake_id(*parts: str) -> str:
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:17]


def _desired(module: str, tf_vars: dict) -> dict:
    """The attributes that, when changed, force a replace (like user_data)."""
    if module in ("compute", "app"):
        return {"app_version": tf_vars.get("app_version") or "v1"}
    return {}


def _outputs(module: str, env: str, attrs: dict) -> dict:
    if module == "network":
        return {
            "vpc_id": f"vpc-{_fake_id(env, 'vpc')}",
            "subnet_id": f"subnet-{_fake_id(env, 'subnet')}",
            "security_group_id": f"sg-{_fake_id(env, 'sg')}",
        }
    if module == "compute":
        h = _fake_id(env, "instance", attrs["app_version"])
        # 198.51.100.0/24 is reserved for documentation: obviously not real.
        return {"instance_id": f"i-{h}", "public_ip": f"198.51.100.{int(h, 16) % 250 + 4}"}
    return {"app_version": attrs["app_version"]}


def _diff(module: str, tf_vars: dict, state: dict | None) -> tuple[dict, list[str]]:
    """Desired attributes and the plan's resource changes against state."""
    desired = _desired(module, tf_vars)
    if state is None:
        return desired, [f"+ {r}" for r in RESOURCES[module]]
    if state["attrs"] != desired:
        return desired, [f"-/+ {r}" for r in RESOURCES[module]]
    return desired, []


def _summary(changes: list[str], desired: dict) -> dict:
    replaced = sum(c.startswith("-/+") for c in changes)
    added = sum(c.startswith("+") for c in changes)
    return {
        "add": added + replaced,
        "change": 0,
        "destroy": replaced,
        "resources": sorted(changes, key=lambda c: c.split(" ", 1)[1]),
        "fingerprint": hashlib.sha256(json.dumps([changes, desired], sort_keys=True).encode()).hexdigest()[:16],
    }


@activity.defn(name="run_terraform_plan")
async def sim_plan(module: str, env: str, tf_vars: dict, summarize: bool = False) -> PlanResult:
    await _work(PLAN_SECONDS, f"plan {module}")
    _maybe_fail(module, "plan")
    state = _read_state(module, env)
    desired, changes = _diff(module, tf_vars, state)
    summary = _summary(changes, desired)
    plan_path = STATE_DIR / env / f"{module}.tfplan"
    if changes:
        plan_path.parent.mkdir(parents=True, exist_ok=True)
        plan_path.write_text(json.dumps({"desired": desired, "summary": summary}))
    return PlanResult(
        module=module,
        env=env,
        plan_path=str(plan_path),
        has_changes=bool(changes),
        diff_summary=f"Plan: {summary['add']} to add, {summary['change']} to change, {summary['destroy']} to destroy.",
        current_outputs=state["outputs"] if state else {},
        summary=summary if summarize and changes else None,
    )


@activity.defn(name="run_terraform_apply")
async def sim_apply(
    module: str, env: str, tf_vars: dict, plan_path: str, approved_summary: dict | None = None
) -> ApplyResult:
    # Like the real apply: a retry, or a missing plan file, re-plans from
    # current state, and in approval mode that re-plan must match what was approved.
    if activity.info().attempt > 1 or not Path(plan_path).exists():
        desired, changes = _diff(module, tf_vars, _read_state(module, env))
        new_summary = _summary(changes, desired)
        if approved_summary is not None and changes and new_summary != approved_summary:
            raise ApplicationError(
                f"re-planned {module}/{env} no longer matches the approved plan",
                new_summary,
                plan_path,
                type="PlanChangedError",
                non_retryable=True,
            )
    desired = _desired(module, tf_vars)

    # Resources come into existence partway through, so an apply that fails
    # (or is killed) after the midpoint leaves state for the rollback to destroy.
    half = APPLY_SECONDS[module] / 2
    await _work(half, f"apply {module}")
    _write_state(module, env, {"attrs": desired, "outputs": _outputs(module, env, desired)})
    await _work(half, f"apply {module}")
    _maybe_fail(module, "apply")
    if str(tf_vars.get("simulate_failure", "")).lower() == "true":
        # Same message and type as the real module's local-exec failure.
        raise ApplicationError(
            "terraform apply failed: Error: local-exec provisioner error: Simulated app deploy failure",
            type="AppDeployError",
            non_retryable=True,
        )

    Path(plan_path).unlink(missing_ok=True)
    return ApplyResult(module=module, env=env, outputs=_read_state(module, env)["outputs"])


@activity.defn(name="read_terraform_outputs")
async def sim_read_outputs(module: str, env: str) -> dict:
    state = _read_state(module, env)
    return state["outputs"] if state else {}


@activity.defn(name="run_terraform_destroy")
async def sim_destroy(module: str, env: str, tf_vars: dict) -> None:
    if _read_state(module, env) is None:
        return  # nothing to destroy: same as terraform destroy on empty state
    await _work(DESTROY_SECONDS[module], f"destroy {module}")
    _maybe_fail(module, "destroy")
    _state_path(module, env).unlink(missing_ok=True)
    (STATE_DIR / env / f"{module}.tfplan").unlink(missing_ok=True)
    # Like the real destroy removing its workspace: once nothing is left, drop the env dir.
    env_dir = STATE_DIR / env
    if not any(env_dir.glob("*.json")):
        shutil.rmtree(env_dir, ignore_errors=True)


@activity.defn(name="http_health_check")
async def sim_health_check(url: str) -> HealthResult:
    activity.logger.info(f"GET {url} (simulated)")
    await _work(HEALTH_SECONDS, "health", max_seconds=HEALTH_MAX_SECONDS)
    mode = _injected("health", "check")
    attempt = activity.info().attempt
    if mode == "flaky" and attempt <= FLAKY_ATTEMPTS:
        # An instance still booting: nginx isn't answering yet.
        raise ApplicationError(
            f"Instance still booting (simulated): {url} not answering yet, retrying "
            f"(attempt {attempt} of {FLAKY_ATTEMPTS} that fail)",
            type="InstanceBooting",
        )
    _maybe_fail("health", "check")
    return HealthResult(url=url, status_code=200)
