import logging
import os
import subprocess
from asyncio import wait_for
from time import monotonic

import pytest

from tests import tasks


# @pytest.mark.skip
class TestPerf:
    count = 1000

    @pytest.mark.parametrize(
        "params",
        [
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "common"}}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "direct_reply_to"}}},
        ],
        indirect=True,
        ids=[
            "         rabbitmq:common",
            "rabbitmq:direct_reply_to",
        ],
    )
    @pytest.mark.asyncio
    async def test_perf_arrlio(self, params):
        backend, app = params

        logger = logging.getLogger("arrlio")
        logger.setLevel(logging.ERROR)

        url = app.backend.config.url[-1].get_secret_value()
        cmd = subprocess.run(["pipenv", "run", "which", "python"], capture_output=True).stdout.decode().strip()
        cwd = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..")
        ps = subprocess.Popen(
            [cmd, "tests/worker.py"],
            cwd=cwd,
            env={"ARRLIO_RABBITMQ_URL": url, "PYTHONPATH": f"$PYTHONPATH:{cwd}"},
        )
        try:

            hello_world = tasks.hello_world

            print()

            # A
            t0 = monotonic()
            for _ in range(self.count):
                ar = await app.send_task(hello_world)
                assert await wait_for(ar.get(), 5) == "Hello World!"
            print(f"A: {monotonic() - t0}")

            # B
            t0 = monotonic()
            ars = [await app.send_task(hello_world) for _ in range(self.count)]
            for ar in ars:
                assert await wait_for(ar.get(), 5) == "Hello World!"
            print(f"B: {monotonic() - t0}")

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
            "                  celery",
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

            print()

            # A
            t0 = monotonic()
            for _ in range(self.count):
                ar = hello_world.delay()
                assert ar.get(timeout=5) == "Hello World!"
            print(f"A: {monotonic() - t0}")

            # B
            t0 = monotonic()
            ars = [hello_world.delay() for _ in range(self.count)]
            for ar in ars:
                assert ar.get(timeout=5) == "Hello World!"
            print(f"B: {monotonic() - t0}")

        finally:
            ps.terminate()
