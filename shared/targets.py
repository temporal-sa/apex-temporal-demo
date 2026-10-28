"""Deploy targets. Each target has its own task queue and its own worker, so
work started for one target can never run against another.

- aws: real Terraform against real AWS (the original demo).
- local: real Terraform against Floci, a local AWS emulator in Docker. EC2
  instances are containers, so nginx really serves the page on localhost.
- sim: no Terraform, no AWS. Simulated activities with the same names and
  return types, so every workflow behaves exactly as it does against AWS.

Workflow IDs carry the target as an `@<target>` suffix (none for aws, so AWS
IDs are unchanged), letting the same env name exist on several targets.
"""
import os

from shared.config import TASK_QUEUE_NONPROD

TARGETS = ("aws", "local", "sim")
DEFAULT_TARGET = "aws"

# Each worker serves Prometheus metrics on its own port, so workers for
# different targets can run side by side.
METRICS_PORTS = {"aws": 9090, "local": 9093, "sim": 9092}


def task_queue(target: str) -> str:
    if target not in TARGETS:
        raise ValueError(f"unknown target {target!r}; expected one of {', '.join(TARGETS)}")
    return TASK_QUEUE_NONPROD if target == "aws" else f"{TASK_QUEUE_NONPROD}-{target}"


def target_for_queue(queue: str) -> str:
    """The target a task queue belongs to. Workflows use this on their own
    task queue to name child workflows; children inherit the parent's queue.
    Any other queue (e.g. a test's throwaway queue) names IDs like aws does."""
    for target in TARGETS:
        if task_queue(target) == queue:
            return target
    return DEFAULT_TARGET


def workflow_id(target: str, *parts: str) -> str:
    """`workflow_id("sim", "deploy", "env-01")` -> "deploy-env-01@sim"."""
    base = "-".join(parts)
    return base if target == "aws" else f"{base}@{target}"


def add_target_arg(parser) -> None:
    """The --target flag every CLI shares, defaulting to $APEX_TARGET."""
    parser.add_argument(
        "--target", choices=TARGETS, default=os.getenv("APEX_TARGET", DEFAULT_TARGET),
        help="which worker runs it: aws (real AWS), local (Floci), or sim (simulated) (default: $APEX_TARGET or aws)",
    )
