import asyncio
from datetime import datetime, timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError, is_cancelled_exception

with workflow.unsafe.imports_passed_through():
    from activities.health import http_health_check
    from activities.terraform import simulate_long_step
    from shared.targets import target_for_queue, workflow_id

from workflows.provision_module import DEFAULT_APPROVAL_TIMEOUT_SECONDS, ProvisionModule
from workflows.teardown_instance import TeardownInstance


HEALTH_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=10,
    non_retryable_error_types=[],
)


@workflow.defn
class DeployTestInstance:
    """
    Parent: orchestrates network -> compute -> app via ProvisionModule child,
    then runs an HTTP health check directly as the success signal.

    Saga rollback: any failure during provisioning or the health check triggers
    a compensating teardown of whatever made it into state, including a module
    that failed partway through its apply. Teardown is idempotent and skips
    empty state, so it is always safe to run. Cancelling the workflow triggers
    the same teardown, including during the TTL wait, and the run still ends
    as Canceled. Disable with `rollback_on_failure=False` for triage workflows
    where the partial state is the diagnostic.

    With `require_approval`, each module whose plan has changes waits for a
    human decision before it is applied (see ProvisionModule). A rejection or
    timeout fails that module, which triggers the same rollback.

    The `status` query reports progress and the instance URL while the
    workflow runs. During a TTL wait, the `extend_ttl` signal pushes the
    teardown back and `teardown_now` ends the wait early.
    """

    def __init__(self) -> None:
        self._env: str | None = None
        self._phase = "starting"
        self._modules: list[str] = []
        self._url: str | None = None
        self._expires_at: datetime | None = None
        self._teardown_requested = False

    @workflow.query
    def status(self) -> dict:
        return {
            "env": self._env,
            "phase": self._phase,
            "modules": self._modules,
            "url": self._url,
            "expires_at": self._expires_at.isoformat() if self._expires_at else None,
        }

    @workflow.signal
    def extend_ttl(self, seconds: int) -> None:
        if self._expires_at is None:
            workflow.logger.warning("extend_ttl ignored: no TTL is counting down")
            return
        self._expires_at += timedelta(seconds=seconds)

    @workflow.signal
    def teardown_now(self) -> None:
        self._teardown_requested = True

    @workflow.run
    async def run(
        self,
        env_name: str,
        ttl_seconds: int = 0,
        rollback_on_failure: bool = True,
        fail_app_deploy: bool = False,
        require_approval: bool = False,
        approval_timeout_seconds: int = DEFAULT_APPROVAL_TIMEOUT_SECONDS,
    ) -> dict:
        workflow.logger.info(f"DeployTestInstance: {env_name}")
        self._env = env_name
        target = target_for_queue(workflow.info().task_queue)
        # Appended to each child's args only when set, so a deploy without
        # approval starts its children exactly as before.
        approval_args = [True, approval_timeout_seconds] if require_approval else []

        try:
            # 1) Network
            self._phase = "provisioning-network"
            net_outputs = await workflow.execute_child_workflow(
                ProvisionModule.run,
                args=["network", env_name, {"env_name": env_name}, *approval_args],
                id=workflow_id(target, "provision", "network", env_name),
            )
            self._modules.append("network")

            # Chaos hook: pause between network and compute so a `kill -9` window exists.
            if env_name.startswith("chaos-"):
                await workflow.execute_activity(
                    simulate_long_step,
                    args=["pre-compute", 30],
                    start_to_close_timeout=timedelta(minutes=2),
                    heartbeat_timeout=timedelta(seconds=10),
                )

            # 2) Compute (depends on network outputs)
            self._phase = "provisioning-compute"
            compute_inputs = {
                "env_name": env_name,
                "subnet_id": net_outputs.get("subnet_id"),
                "security_group_id": net_outputs.get("security_group_id"),
            }
            compute_outputs = await workflow.execute_child_workflow(
                ProvisionModule.run,
                args=["compute", env_name, compute_inputs, *approval_args],
                id=workflow_id(target, "provision", "compute", env_name),
            )
            self._modules.append("compute")

            # 3) App marker module (placeholder for future app deploy logic).
            # With fail_app_deploy, the app module's apply fails on purpose
            # after network and compute are built, to demo the saga rollback.
            self._phase = "provisioning-app"
            app_inputs = {"env_name": env_name, "app_version": "v1"}
            if fail_app_deploy:
                app_inputs["simulate_failure"] = "true"
            await workflow.execute_child_workflow(
                ProvisionModule.run,
                args=["app", env_name, app_inputs, *approval_args],
                id=workflow_id(target, "provision", "app", env_name),
            )
            self._modules.append("app")

            # 4) Health check the running instance. The activity raises on a
            # non-2xx response, so returning at all means healthy.
            public_ip = compute_outputs.get("public_ip")
            if not public_ip:
                raise RuntimeError("compute module did not return public_ip")

            url = f"http://{public_ip}/"
            self._phase = "health-check"
            self._url = url
            health = await workflow.execute_activity(
                http_health_check,
                args=[url],
                start_to_close_timeout=timedelta(seconds=15),
                schedule_to_close_timeout=timedelta(minutes=5),
                retry_policy=HEALTH_RETRY,
            )
            # The URL that answered: the same one on AWS, but on the local
            # target the instance name resolves to a forwarded localhost port.
            url = self._url = health.url
            self._phase = "healthy"

            result = {
                "env": env_name,
                "public_ip": public_ip,
                "url": url,
                "status": "healthy",
            }

            if ttl_seconds > 0:
                await self._wait_for_ttl(ttl_seconds)
                self._phase = "tearing-down"
                await workflow.execute_child_workflow(
                    TeardownInstance.run,
                    env_name,
                    id=workflow_id(target, "teardown-ttl", env_name),
                )
                self._phase = "torn-down"
                result["torn_down_at"] = workflow.now().isoformat()

            return result

        # asyncio.CancelledError is a BaseException, so `except Exception` alone
        # would skip the teardown when the run is cancelled during a timer.
        except (asyncio.CancelledError, Exception) as e:
            cancelled = is_cancelled_exception(e)
            if rollback_on_failure:
                reason = "cancelled" if cancelled else f"failed ({type(e).__name__}: {e})"
                workflow.logger.warning(
                    f"deploy {reason}; rolling back "
                    f"(completed modules: {self._modules or 'none'})"
                )
                await self._rollback(target, env_name)
            # Re-raise a cancellation as-is so the run ends as Canceled, not Failed.
            if cancelled:
                raise
            # Re-raise as ApplicationError so the workflow execution terminates
            # as Failed. A plain Python exception is interpreted as a Workflow
            # Task failure, which Temporal retries forever, which is not what we want
            # for a logical "this deploy failed and was rolled back" outcome.
            if isinstance(e, ApplicationError):
                raise
            raise ApplicationError(str(e), type=type(e).__name__) from e

    async def _wait_for_ttl(self, ttl_seconds: int) -> None:
        """Durable TTL wait. Survives worker restarts like workflow.sleep, but
        wakes early for `extend_ttl` (to re-arm) and `teardown_now` (to stop)."""
        self._expires_at = workflow.now() + timedelta(seconds=ttl_seconds)
        self._phase = "ttl-wait"
        workflow.logger.info(f"TTL set: tearing down at {self._expires_at.isoformat()}")
        while not self._teardown_requested:
            remaining = self._expires_at - workflow.now()
            if remaining <= timedelta(0):
                break
            deadline = self._expires_at
            try:
                await workflow.wait_condition(
                    lambda: self._teardown_requested or self._expires_at != deadline,
                    timeout=remaining,
                )
            except asyncio.TimeoutError:
                pass  # loop re-checks the deadline, which an extension may have moved
        workflow.logger.info(
            "teardown requested" if self._teardown_requested else "TTL expired, triggering teardown"
        )

    async def _rollback(self, target: str, env_name: str) -> None:
        self._phase = "rolling-back"
        try:
            await workflow.execute_child_workflow(
                TeardownInstance.run,
                env_name,
                id=workflow_id(target, "rollback", env_name, str(int(workflow.now().timestamp()))),
            )
            self._phase = "rolled-back"
        except Exception as rollback_err:
            # Surface the original deploy failure, not the rollback's.
            self._phase = "rollback-failed"
            workflow.logger.error(
                f"rollback failed, resources may be orphaned; "
                f"run `python teardown.py {env_name}`: {rollback_err}"
            )
