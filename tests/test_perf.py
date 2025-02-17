import asyncio
import json
import logging
import os
import subprocess

from asyncio import wait_for
from time import monotonic

import pytest


logger = logging.getLogger("arrlio")
logger.setLevel(logging.ERROR)


from tests import tasks


# @pytest.mark.skip
class TestPerf:
    count = 1000

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {
                    "module": "arrlio.backends.brokers.rabbitmq",
                    "config": {
                        # "serializer": {"module": "arrlio.serializers.msgpack"},
                    },
                },
                "result_backend": {
                    "module": "arrlio.backends.result_backends.rabbitmq",
                    "config": {
                        "reply_to_mode": "common_queue",
                        # "serializer": {"module": "arrlio.serializers.msgpack"},
                    },
                },
            },
            {
                "broker": {
                    "module": "arrlio.backends.brokers.rabbitmq",
                    "config": {
                        # "serializer": {"module": "arrlio.serializers.msgpack"},
                    },
                },
                "result_backend": {
                    "module": "arrlio.backends.result_backends.rabbitmq",
                    "config": {
                        "reply_to_mode": "direct_reply_to",
                        # "serializer": {"module": "arrlio.serializers.msgpack"},
                    },
                },
            },
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

        url = app.broker.config.url[-1].get_secret_value()
        cmd = subprocess.run(["poetry", "run", "which", "python"], capture_output=True).stdout.decode().strip()
        cwd = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..")
        ps = subprocess.Popen(
            [cmd, "arrlio/tests/worker.py"],
            cwd=cwd,
            env={
                "ARRLIO_RABBITMQ_URL": json.dumps([f"{url}"]),
                # "ARRLIO_SERIALIZER_MODULE": "arrlio.serializers.msgpack",
                "PYTHONPATH": f"$PYTHONPATH:{cwd}",
            },
        )
        await asyncio.sleep(1)
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
            {"broker": {"module": "arrlio.backends.brokers.rabbitmq"}},
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

        host = app.broker.config.url[-1].host
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
