import asyncio
import logging

import arrlio
import pytest

from tests.medium import tasks


logger = logging.getLogger("arrlio")
logger.setLevel(logging.DEBUG)


pytestmark = pytest.mark.asyncio


class TestArrlio:
    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_task_default(self, backend, client, executor):
        await executor.run()
        ar = await client.call(tasks.hello_world)
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_task_args_kwds(self, backend, client, executor):
        await executor.run()
        ar = await client.call(tasks.echo, args=(1, 2), kwds={"3": 3, "4": 4})
        res = await asyncio.wait_for(ar.get(), 5)
        assert res == ((1, 2), {"3": 3, "4": 4}) or res == [[1, 2], {"3": 3, "4": 4}]

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_task_custom_queue(self, backend, client, executor):
        executor.config.task_queues = ["queue1", "queue2"]
        await executor.run()
        ar = await client.call(tasks.hello_world, queue="queue1")
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"
        ar = await client.call(tasks.hello_world, queue="queue2")
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
        ],
        indirect=True,
    )
    async def test_task_priority(self, backend, client, executor):
        executor.config.pool_size = 1
        await executor.run()
        await client.call(tasks.sleep, args=(0.5,), priority=10)
        aw1 = (await client.call(tasks.sleep, args=(1,), priority=1)).get()
        aw2 = (await client.call(tasks.sleep, args=(1,), priority=2)).get()
        done, pending = await asyncio.wait_for(asyncio.wait({aw1, aw2}, return_when=asyncio.FIRST_COMPLETED), 5)
        assert {t.get_coro() for t in done} == {aw2}
        assert {t.get_coro() for t in pending} == {aw1}

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_lost_connection(self, backend, client, executor):
        await executor.run()
        await asyncio.sleep(1)
        backend.container.stop()
        await asyncio.sleep(3)
        backend.container.start()
        await asyncio.sleep(1)
        ar = await client.call(tasks.hello_world)
        assert await asyncio.wait_for(ar.get(), 10) == "Hello World!"

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_task_timeout(self, backend, client, executor):
        await executor.run()
        ar = await client.call(tasks.sleep, args=(3600,), timeout=1)
        with pytest.raises(arrlio.TaskError):
            await asyncio.wait_for(ar.get(), 5)

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            # "arrlio.backend.rabbitmq",
            # "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_task_no_result(self, backend, client, executor):
        await executor.run()
        ar = await client.call(tasks.noresult)
        with pytest.raises(arrlio.TaskNoResultError):
            await asyncio.wait_for(ar.get(), 5)

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_task_result_timeout(self, backend, client, executor):
        await executor.run()
        ar = await client.call(tasks.hello_world, result_ttl=1)
        await asyncio.sleep(3)
        with pytest.raises((arrlio.TaskNoResultError, asyncio.TimeoutError)):
            await asyncio.wait_for(ar.get(), 2)

    # @pytest.mark.parametrize("backend", ["rabbitmq"], indirect=True)
    # async def test_task_ack_late(self, backend, app):
    #     await app.run()
    #     ar = await app.send_task(tasks.ack_late)
    #     await asyncio.sleep(1)
    #     await app.stop()
    #     await app.run()
    #     await asyncio.wait_for(ar.get(), 5)
    #     assert tasks.ack_late.func.counter == 2
