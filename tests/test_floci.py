"""The local (Floci) target without Floci: which overrides land in each
workspace, which state bucket it uses, and how the health check finds the
port Floci forwards to an instance (against a fake `docker` and a local server)."""
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

import activities.terraform as tf
from activities import floci


@pytest.fixture
def local_tf(tmp_path, monkeypatch):
    monkeypatch.setattr(tf, "WORKDIR_BASE", tmp_path / "_workspaces")
    monkeypatch.setattr(tf, "LOCAL_WORKDIR_BASE", tmp_path / "_workspaces_local")
    monkeypatch.setattr(tf, "FLOCI_ENDPOINT", "http://floci.test:4599")
    tf.use_target("local")
    yield tmp_path
    tf.use_target("aws")


def test_local_workspace_gets_floci_overrides(local_tf):
    compute = tf._workdir("compute", "e1")
    assert compute.parent.parent == local_tf / "_workspaces_local"
    assert {p.name for p in compute.glob("*.tf")} == {
        "main.tf", "backend_override.tf", "aws_provider_override.tf", "compute_override.tf",
    }
    provider = (compute / "aws_provider_override.tf").read_text()
    assert 'ec2 = "http://floci.test:4599"' in provider and "localhost:4566" not in provider

    network = tf._workdir("network", "e1")
    assert "compute_override.tf" not in {p.name for p in network.glob("*.tf")}

    # The app module has no aws provider, and an override with no matching
    # provider block would be an error.
    app = tf._workdir("app", "e1")
    assert {p.name for p in app.glob("*.tf")} == {"main.tf", "backend_override.tf"}


def test_local_target_uses_its_own_state_bucket(local_tf):
    args = tf._backend_args("network", "e1")
    assert f"-backend-config=bucket={tf.LOCAL_STATE_BUCKET}" in args
    assert "-backend-config=region=us-east-1" in args


def test_aws_target_is_unchanged(tmp_path, monkeypatch):
    monkeypatch.setattr(tf, "WORKDIR_BASE", tmp_path / "_workspaces")
    monkeypatch.setattr(tf, "TF_STATE_BUCKET", "real-bucket")
    wd = tf._workdir("compute", "e1")
    assert wd.parent.parent == tmp_path / "_workspaces"
    assert {p.name for p in wd.glob("*.tf")} == {"main.tf"}
    assert "-backend-config=bucket=real-bucket" in tf._backend_args("compute", "e1")


def test_floci_connection_errors_name_floci(local_tf):
    stderr = ('Error: Failed to get existing workspaces: ... request send failed, '
              'Get "http://floci.test:4599/apex-local-state?list-type=2": read: connection reset by peer')
    err = tf._tf_error("init", stderr)
    assert err.type == "FlociUnavailable" and not err.non_retryable
    assert "docker logs apex-floci" in err.message


def test_use_target_rejects_sim():
    with pytest.raises(ValueError):
        tf.use_target("sim")


@pytest.fixture
def fake_docker(tmp_path, monkeypatch):
    """A `docker` that prints whatever ports are in $FAKE_DOCKER_PORTS."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    script = bin_dir / "docker"
    script.write_text('#!/bin/sh\nprintf "%s" "$FAKE_DOCKER_PORTS"\n')
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    return lambda ports: monkeypatch.setenv("FAKE_DOCKER_PORTS", ports)


async def test_published_port_parses_docker_ps(fake_docker):
    fake_docker("0.0.0.0:30001->80/tcp, [::]:30001->80/tcp")
    assert await floci.published_port("i-abc123") == 30001
    fake_docker("")  # forwarder not started yet
    assert await floci.published_port("i-abc123") is None


class OkHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def server_port():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), OkHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv.server_port
    srv.shutdown()


async def test_health_check_resolves_instance_name(fake_docker, server_port):
    fake_docker(f"0.0.0.0:{server_port}->80/tcp")
    result = await ActivityEnvironment().run(floci.floci_health_check, "http://i-abc123.floci.localhost/")
    assert result.url == f"http://localhost:{server_port}/"
    assert result.status_code == 200


async def test_health_check_retries_until_port_is_published(fake_docker):
    fake_docker("")
    with pytest.raises(ApplicationError) as err:
        await ActivityEnvironment().run(floci.floci_health_check, "http://i-abc123.floci.localhost/")
    assert err.value.type == "InstanceBooting" and not err.value.non_retryable


async def test_health_check_passes_plain_urls_through(server_port):
    result = await ActivityEnvironment().run(floci.floci_health_check, f"http://127.0.0.1:{server_port}/")
    assert result.url == f"http://127.0.0.1:{server_port}/"
