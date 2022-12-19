import logging
import os
import subprocess
from asyncio import wait_for
from time import monotonic

import pytest

from arrlio import backends
from tests import tasks

logger = logging.getLogger("arrlio")
logger.setLevel(logging.ERROR)


@pytest.mark.skip
class TestPerf:
    count = 500

    @pytest.mark.parametrize(
        "backend",
        [
            # backends.local,
            backends.rabbitmq,
            # backends.redis,
        ],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_perf_arrlio(self, backend, app):
        app.backend.config.results_queue_mode = "shared"
        url = app.backend.config.url[-1].get_secret_value()
        cmd = subprocess.run(["which", "python"], capture_output=True).stdout.decode().strip()
        ps = subprocess.Popen(
            [cmd, "tests/worker.py"],
            cwd=".",
            env={"ARRLIO_RMQ_BACKEND_URL": url},
        )
        try:

            hello_world = tasks.hello_world

            t0 = monotonic()

            for _ in range(self.count):
                ar = await app.send_task(
                    hello_world,
                    # extra={"result_queue_mode": "shared"},
                )
                assert await wait_for(ar.get(), 2) == "Hello World!"

            print(monotonic() - t0)

        finally:
            ps.terminate()

    @pytest.mark.parametrize(
        "backend",
        [
            backends.rabbitmq,
        ],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_perf_celery(self, backend, app):
        host = app.backend.config.url[-1].host
        cmd = subprocess.run(["which", "celery"], capture_output=True).stdout.decode().strip()
        ps = subprocess.Popen(
            [cmd, "-A", "tests.celery_tasks", "worker", "--loglevel=ERROR", "--concurrency=1"],
            cwd=".",
            env={"RABBITMQ_HOST": host},
        )
        os.environ["RABBITMQ_HOST"] = host
        try:
            from tests.celery_tasks import hello_world

            t0 = monotonic()

            for _ in range(self.count):
                ar = hello_world.delay()
                assert ar.get(timeout=2) == "Hello World!"

            print(monotonic() - t0)

        finally:
            ps.terminate()
