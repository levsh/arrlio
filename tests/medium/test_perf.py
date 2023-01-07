import logging
import os
import subprocess
from asyncio import wait_for
from time import monotonic

import pytest

from tests import tasks


@pytest.mark.skip
class TestPerf:
    count = 500

    @pytest.mark.parametrize(
        "params",
        [
            # {"backend": {"module": "arrlio.backends.rabbitmq"}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "shared"}}},
        ],
        indirect=True,
        ids=[
            # "       rabbitmq",
            "rabbitmq:shared",
        ],
    )
    @pytest.mark.asyncio
    async def test_perf_arrlio(self, params):
        backend, app = params

        logger = logging.getLogger("arrlio")
        logger.setLevel(logging.ERROR)

        url = app.backend.config.url[-1].get_secret_value()
        cmd = subprocess.run(["which", "python"], capture_output=True).stdout.decode().strip()
        ps = subprocess.Popen(
            [cmd, "tests/worker.py"],
            cwd=".",
            env={"ARRLIO_RABBITMQ_URL": url},
        )
        try:

            hello_world = tasks.hello_world

            t0 = monotonic()

            ars = [await app.send_task(hello_world) for _ in range(self.count)]
            for ar in ars:
                assert await wait_for(ar.get(), 2) == "Hello World!"

            print(monotonic() - t0)

        finally:
            ps.terminate()

    # @pytest.mark.skip
    @pytest.mark.parametrize(
        "params",
        [
            {"backend": {"module": "arrlio.backends.rabbitmq"}},
        ],
        indirect=True,
        ids=[
            "       rabbitmq",
        ],
    )
    @pytest.mark.asyncio
    async def test_perf_celery(self, params):
        backend, app = params

        logger = logging.getLogger("arrlio")
        logger.setLevel(logging.ERROR)

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

            ars = [hello_world.delay() for _ in range(self.count)]
            for ar in ars:
                assert ar.get(timeout=2) == "Hello World!"

            print(monotonic() - t0)

        finally:
            ps.terminate()
