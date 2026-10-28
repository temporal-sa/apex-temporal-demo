"""Helpers for the `local` target: Terraform against Floci (a local AWS
emulator) in Docker.

Floci runs each EC2 instance as a container and forwards a port its security
group opens (80) to a host port it picks from 30000 up, through a socat
sidecar named floci-aws-ec2-fwd-<instance-id>-<port>. That host port isn't in
the EC2 API, so the local compute module reports the instance as
`<instance-id>.floci.localhost` (terraform/local/compute_override.tf) and the
health check below asks Docker where port 80 ended up.
"""
import asyncio
import re
import shutil
from urllib.parse import urlparse

from temporalio import activity
from temporalio.exceptions import ApplicationError

from activities.health import HealthResult, http_health_check
from shared.config import FLOCI_ENDPOINT, LOCAL_STATE_BUCKET

INSTANCE_HOST = re.compile(r"^(i-[0-9a-f]+)\.floci\.localhost$")


async def published_port(instance_id: str, port: int = 80) -> int | None:
    """The host port Floci forwards to the instance's `port`, or None if the
    forwarder isn't up yet (it starts once the instance is running)."""
    if not shutil.which("docker"):
        raise ApplicationError("docker CLI not found on PATH; the local target needs it", type="DockerMissing",
                               non_retryable=True)
    proc = await asyncio.create_subprocess_exec(
        "docker", "ps", "--filter", f"name=-fwd-{instance_id}-{port}$", "--format", "{{.Ports}}",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"docker ps failed: {err.decode().strip()}")
    # e.g. "0.0.0.0:30000->80/tcp, [::]:30000->80/tcp"
    m = re.search(rf":(\d+)->{port}/tcp", out.decode())
    return int(m[1]) if m else None


def resolve_instance_url(url: str, host_port: int) -> str:
    parsed = urlparse(url)
    return parsed._replace(netloc=f"localhost:{host_port}").geturl()


@activity.defn(name="http_health_check")
async def floci_health_check(url: str) -> HealthResult:
    """The regular health check, after resolving a Floci instance name to the
    host port that reaches it. Returns the URL that actually answered, so the
    workflow can report a link that works from a browser."""
    m = INSTANCE_HOST.match(urlparse(url).hostname or "")
    if not m:
        return await http_health_check(url)
    instance_id = m[1]
    port = await published_port(instance_id)
    if port is None:
        raise ApplicationError(
            f"Floci hasn't published port 80 for {instance_id} yet (instance still starting)",
            type="InstanceBooting",
        )
    return await http_health_check(resolve_instance_url(url, port))


def ensure_state_bucket() -> None:
    """Create the local target's Terraform state bucket in Floci if missing."""
    import boto3
    from botocore.config import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=FLOCI_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 2}, connect_timeout=3),
    )
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if LOCAL_STATE_BUCKET not in existing:
        s3.create_bucket(Bucket=LOCAL_STATE_BUCKET)
