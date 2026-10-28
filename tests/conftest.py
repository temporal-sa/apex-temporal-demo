import shutil

import pytest_asyncio
from temporalio.testing import WorkflowEnvironment


@pytest_asyncio.fixture(scope="session")
async def temporal_env():
    """A throwaway Temporal dev server for the whole run. Uses the `temporal`
    CLI on PATH if present; otherwise the SDK downloads one on first use."""
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal")
    ) as env:
        yield env
