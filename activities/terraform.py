import asyncio
import hashlib
import json
import os
import shlex
import shutil
import signal
from dataclasses import dataclass
from pathlib import Path

from temporalio import activity
from temporalio.exceptions import ApplicationError

from shared.config import FLOCI_ENDPOINT, LOCAL_STATE_BUCKET, TF_STATE_BUCKET, TF_STATE_REGION
from shared.idempotency import idempotency_key

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULES_DIR = REPO_ROOT / "terraform" / "modules"
WORKDIR_BASE = REPO_ROOT / "terraform" / "_workspaces"
# The local target keeps its own workspaces and copies these overrides in.
LOCAL_WORKDIR_BASE = REPO_ROOT / "terraform" / "_workspaces_local"
LOCAL_OVERRIDES_DIR = REPO_ROOT / "terraform" / "local"
OVERRIDE_ENDPOINT = "http://localhost:4566"

# "aws" or "local": which backend these activities run Terraform against. A
# worker serves one target, and worker.py sets this once at startup.
_target = "aws"


def use_target(target: str) -> None:
    global _target
    if target not in ("aws", "local"):
        raise ValueError(f"terraform activities don't run against {target!r}")
    _target = target

# How long plan, apply, and destroy wait for another run's S3 state lock
# (backend `use_lockfile = true`) before failing. Lock failures are retryable,
# so the activity's retry policy keeps waiting after that.
LOCK_TIMEOUT = "2m"

# Stderr fragments for failures that no retry can fix. Anything else (lock
# contention, a stale saved plan, AWS throttling) stays retryable.
PERMANENT_ERRORS = {
    "TerraformConfigError": (
        "Error: Invalid",
        "Error: Unsupported",
        "No value for required variable",
        "Reference to undeclared",
    ),
    "AwsCredentialsError": (
        "No valid credential sources",
        "ExpiredToken",
        "InvalidClientTokenId",
        "AccessDenied",
        "UnauthorizedOperation",
    ),
    "StateBucketError": ("NoSuchBucket",),
    "AwsQuotaError": ("VpcLimitExceeded", "InstanceLimitExceeded", "VcpuLimitExceeded"),
    # The app module's provisioner failed (e.g. the simulate_failure demo hook).
    "AppDeployError": ("local-exec provisioner error",),
}

@dataclass
class PlanResult:
    module: str
    env: str
    plan_path: str
    has_changes: bool
    diff_summary: str
    current_outputs: dict
    # Only set when the plan was requested with summarize=True (approval mode).
    summary: dict | None = None


@dataclass
class ApplyResult:
    module: str
    env: str
    outputs: dict


def _workdir(module: str, env: str) -> Path:
    """Per-(module, env) workspace dir. Sync module source on every call so .tf
    edits propagate to existing envs without manual cleanup. For the local
    target, also copy in the Floci overrides (see terraform/local/README.md)."""
    src = MODULES_DIR / module
    dst = (LOCAL_WORKDIR_BASE if _target == "local" else WORKDIR_BASE) / env / module
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        if item.is_file():
            shutil.copy2(item, dst / item.name)
    if _target == "local":
        for override in _local_overrides(module):
            text = override.read_text().replace(OVERRIDE_ENDPOINT, FLOCI_ENDPOINT)
            (dst / override.name).write_text(text)
    return dst


def _local_overrides(module: str) -> list[Path]:
    files = [LOCAL_OVERRIDES_DIR / "backend_override.tf"]
    if 'provider "aws"' in (MODULES_DIR / module / "main.tf").read_text():
        files.append(LOCAL_OVERRIDES_DIR / "aws_provider_override.tf")
    specific = LOCAL_OVERRIDES_DIR / f"{module}_override.tf"
    if specific.exists():
        files.append(specific)
    return files


async def _run(
    cmd: list[str], cwd: Path, heartbeat: bool = False
) -> tuple[int, str, str]:
    """Run a subprocess, optionally heartbeating Temporal while it runs."""
    activity.logger.info(f"$ {' '.join(shlex.quote(c) for c in cmd)} (cwd={cwd})")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(cwd),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        # Plain-text output, so errors read cleanly in the Temporal UI and
        # _tf_error can match on them.
        env={**os.environ, "TF_IN_AUTOMATION": "1", "TF_CLI_ARGS": "-no-color"},
    )

    async def _heartbeat_loop():
        while True:
            activity.heartbeat("running")
            await asyncio.sleep(20)

    hb_task = asyncio.create_task(_heartbeat_loop()) if heartbeat else None
    try:
        stdout_b, stderr_b = await proc.communicate()
    except asyncio.CancelledError:
        # The activity was cancelled (only heartbeating calls hear about it).
        # Interrupt terraform so it finishes in-flight resource calls, saves
        # state, and releases the lock, instead of running on as an orphan.
        if proc.returncode is None:
            activity.logger.warning(f"cancelled; interrupting {cmd[0]} {cmd[1]}")
            proc.send_signal(signal.SIGINT)
            await proc.communicate()
        raise
    finally:
        if hb_task:
            hb_task.cancel()

    assert proc.returncode is not None
    return proc.returncode, stdout_b.decode(), stderr_b.decode()


def _tf_error(step: str, stderr: str) -> ApplicationError:
    """Turn a failed terraform command into a typed, correctly retryable error."""
    message = f"terraform {step} failed: {stderr.strip()}"
    if _target == "local" and FLOCI_ENDPOINT.split("//")[-1] in stderr and "request send failed" in stderr:
        # Still retryable: Floci may just be restarting.
        message = (
            f"Floci isn't responding at {FLOCI_ENDPOINT}; check `docker logs apex-floci` "
            f"(restart it with `docker restart apex-floci` if it's stuck starting). {message}"
        )
        return ApplicationError(message, type="FlociUnavailable")
    for error_type, fragments in PERMANENT_ERRORS.items():
        if any(f in stderr for f in fragments):
            return ApplicationError(message, type=error_type, non_retryable=True)
    return ApplicationError(message, type="TerraformError")


def _backend_args(module: str, env: str) -> list[str]:
    key = f"apex-temporal-demo/{env}/{module}.tfstate"
    bucket, region = (LOCAL_STATE_BUCKET, "us-east-1") if _target == "local" else (TF_STATE_BUCKET, TF_STATE_REGION)
    return [
        f"-backend-config=bucket={bucket}",
        f"-backend-config=key={key}",
        f"-backend-config=region={region}",
    ]


def _tfvars(tf_vars: dict) -> list[str]:
    out = []
    for k, v in tf_vars.items():
        if v is None:
            continue
        out.extend(["-var", f"{k}={v}"])
    return out


async def _init(module: str, env: str, wd: Path, heartbeat: bool = False) -> None:
    # -reconfigure: the backend key never moves, so a workspace initialized
    # with older backend settings can safely pick up the current ones.
    rc, _, err = await _run(
        ["terraform", "init", "-input=false", "-reconfigure", *_backend_args(module, env)],
        wd,
        heartbeat=heartbeat,
    )
    if rc != 0:
        raise _tf_error("init", err)


async def _plan(wd: Path, tf_vars: dict, plan_file: str, heartbeat: bool = False) -> tuple[bool, str]:
    """Write a saved plan to plan_file. Returns (has_changes, diff summary)."""
    rc, out, err = await _run(
        [
            "terraform",
            "plan",
            "-input=false",
            "-detailed-exitcode",
            f"-lock-timeout={LOCK_TIMEOUT}",
            "-out",
            plan_file,
            *_tfvars(tf_vars),
        ],
        wd,
        heartbeat=heartbeat,
    )
    if rc == 1:
        raise _tf_error("plan", err)
    return rc == 2, "\n".join(out.splitlines()[-15:])


# Cap on resource addresses kept in a plan summary, so a large plan can't bloat
# workflow history. The counts and fingerprint still cover every change.
MAX_SUMMARY_ADDRESSES = 50

_ACTION_SYMBOLS = {"create": "+", "update": "~", "delete": "-", "replace": "-/+"}
_ACTION_COUNTS = {"create": "add", "update": "change", "delete": "destroy"}


def _summarize_plan(plan_json: dict, idempotency_key: str) -> dict:
    """Small, comparable summary of a `terraform show -json` plan: add/change/
    destroy counts, the changed resource addresses, and a fingerprint of the
    full changes. The fingerprint ignores the idempotency key, which differs on
    every plan, so two plans of the same change from the same state match."""
    counts = {"add": 0, "change": 0, "destroy": 0}
    resources: list[str] = []
    changes = []
    for rc in plan_json.get("resource_changes", []):
        actions = rc["change"]["actions"]
        if actions in (["no-op"], ["read"]):
            continue
        if "create" in actions and "delete" in actions:
            kind = "replace"
            counts["add"] += 1
            counts["destroy"] += 1
        else:
            kind = actions[0]
            if kind in _ACTION_COUNTS:
                counts[_ACTION_COUNTS[kind]] += 1
        # Anything else (e.g. "forget") is still listed and fingerprinted.
        resources.append(f"{_ACTION_SYMBOLS.get(kind, kind)} {rc['address']}")
        changes.append([rc["address"], actions, *(rc["change"].get(k) for k in ("before", "after", "after_unknown"))])
    canonical = json.dumps(sorted(changes, key=lambda c: c[0]), sort_keys=True)
    if idempotency_key:
        canonical = canonical.replace(idempotency_key, "<idempotency_key>")
    return {
        **counts,
        "resources": sorted(resources, key=lambda r: r.split(" ", 1)[1])[:MAX_SUMMARY_ADDRESSES],
        "fingerprint": hashlib.sha256(canonical.encode()).hexdigest()[:16],
    }


async def _show_summary(wd: Path, plan_file: str, idempotency_key: str, heartbeat: bool = False) -> dict:
    rc, out, err = await _run(["terraform", "show", "-json", plan_file], wd, heartbeat=heartbeat)
    if rc != 0:
        raise _tf_error("show", err)
    return _summarize_plan(json.loads(out), idempotency_key)


def _plan_file(key: str) -> str:
    # One plan file per activity, so two workflows planning the same module
    # can't overwrite each other's plan before it's applied.
    return f"tfplan-{key[:12]}.bin"


@activity.defn
async def run_terraform_plan(module: str, env: str, tf_vars: dict, summarize: bool = False) -> PlanResult:
    key = idempotency_key(f"plan:{module}:{env}")
    activity.logger.info(f"plan idempotency key: {key}")

    wd = _workdir(module, env)
    plan_file = _plan_file(key)

    await _init(module, env, wd)
    has_changes, diff_summary = await _plan(wd, {**tf_vars, "idempotency_key": key}, plan_file)
    summary = await _show_summary(wd, plan_file, key) if summarize and has_changes else None

    # Always read current outputs so downstream modules can chain even when plan is a no-op.
    current_outputs: dict = {}
    rc_o, out_o, _ = await _run(["terraform", "output", "-json"], wd)
    if rc_o == 0 and out_o.strip():
        try:
            raw = json.loads(out_o)
            current_outputs = {k: v["value"] for k, v in raw.items()}
        except Exception:
            pass

    return PlanResult(
        module=module,
        env=env,
        plan_path=str(wd / plan_file),
        has_changes=has_changes,
        diff_summary=diff_summary,
        current_outputs=current_outputs,
        summary=summary,
    )


@activity.defn
async def run_terraform_apply(
    module: str, env: str, tf_vars: dict, plan_path: str, approved_summary: dict | None = None
) -> ApplyResult:
    """Apply the saved plan. With approved_summary (approval mode), a re-plan
    must match what was approved; otherwise this raises a non-retryable
    PlanChangedError carrying the new summary and plan path, so the workflow
    can ask for approval again instead of applying an unapproved plan."""
    key = idempotency_key(f"apply:{module}:{env}")
    activity.logger.info(f"apply idempotency key: {key} (plan_path={plan_path})")

    wd = _workdir(module, env)

    # A retry can't trust the saved plan: a partial apply leaves it stale, and
    # a retry on another host won't have the file. Plan again from current state.
    attempt = activity.info().attempt
    if attempt > 1 or not Path(plan_path).exists():
        activity.logger.info(f"re-planning {module}/{env} before apply (attempt {attempt})")
        await _init(module, env, wd, heartbeat=True)
        plan_path = str(wd / _plan_file(key))
        await _plan(wd, {**tf_vars, "idempotency_key": key}, plan_path, heartbeat=True)
        if approved_summary is not None:
            new_summary = await _show_summary(wd, plan_path, key, heartbeat=True)
            if new_summary != approved_summary:
                raise ApplicationError(
                    f"re-planned {module}/{env} no longer matches the approved plan",
                    new_summary,
                    plan_path,
                    type="PlanChangedError",
                    non_retryable=True,
                )

    rc, out, err = await _run(
        [
            "terraform",
            "apply",
            "-input=false",
            "-auto-approve",
            f"-lock-timeout={LOCK_TIMEOUT}",
            plan_path,
        ],
        wd,
        heartbeat=True,
    )
    if rc != 0:
        raise _tf_error("apply", err)
    Path(plan_path).unlink(missing_ok=True)

    rc, out, err = await _run(["terraform", "output", "-json"], wd)
    if rc != 0:
        raise _tf_error("output", err)

    raw = json.loads(out)
    outputs = {k: v["value"] for k, v in raw.items()}
    return ApplyResult(module=module, env=env, outputs=outputs)


@activity.defn
async def read_terraform_outputs(module: str, env: str) -> dict:
    key = idempotency_key(f"outputs:{module}:{env}")
    activity.logger.info(f"read_terraform_outputs idempotency key: {key}")

    wd = _workdir(module, env)
    await _init(module, env, wd)

    rc, out, err = await _run(["terraform", "output", "-json"], wd)
    if rc != 0:
        raise _tf_error("output", err)

    if not out.strip():
        return {}
    raw = json.loads(out)
    return {k: v["value"] for k, v in raw.items()}


@activity.defn
async def run_terraform_destroy(module: str, env: str, tf_vars: dict) -> None:
    key = idempotency_key(f"destroy:{module}:{env}")
    activity.logger.info(f"destroy idempotency key: {key}")

    wd = _workdir(module, env)
    # Heartbeat through init too: a fresh workspace downloads providers, which
    # can outlast the 60s heartbeat timeout on its own.
    await _init(module, env, wd, heartbeat=True)

    merged_vars = {"env_name": env, **tf_vars, "idempotency_key": key}
    rc, out, err = await _run(
        [
            "terraform",
            "destroy",
            "-input=false",
            "-auto-approve",
            f"-lock-timeout={LOCK_TIMEOUT}",
            *_tfvars(merged_vars),
        ],
        wd,
        heartbeat=True,
    )
    if rc != 0:
        raise _tf_error("destroy", err)

    shutil.rmtree(wd, ignore_errors=True)


@activity.defn
async def simulate_long_step(label: str, seconds: int = 30) -> str:
    """Heartbeat for `seconds` so a chaos demo has a clean window to kill the worker."""
    activity.logger.info(f"simulate_long_step: {label} ({seconds}s)")
    for i in range(seconds):
        activity.heartbeat(f"{label} tick {i + 1}/{seconds}")
        await asyncio.sleep(1)
    return f"{label}:done"
