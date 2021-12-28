import asyncio
import logging
import re

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
    async def test_task_default(self, backend, task_producer, task_consumer):
        await task_consumer.consume()

        ar = await task_producer.send(tasks.hello_world)
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

        ar = await task_producer.send("hello_world")
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
    async def test_task_not_found(self, backend, task_producer, task_consumer):
        await task_consumer.consume()
        with pytest.raises(arrlio.exc.TaskError):
            ar = await task_producer.send("invalid")
            await ar.get()

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_task_args_kwds(self, backend, task_producer, task_consumer):
        await task_consumer.consume()
        ar = await task_producer.send(tasks.echo, args=(1, 2), kwds={"3": 3, "4": 4})
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
    async def test_task_custom_queue(self, backend, task_producer, task_consumer):
        task_consumer.config.queues = ["queue1", "queue2"]
        await task_consumer.consume()
        ar = await task_producer.send(tasks.hello_world, queue="queue1")
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"
        ar = await task_producer.send(tasks.hello_world, queue="queue2")
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
        ],
        indirect=True,
    )
    async def test_task_priority(self, backend, task_producer, task_consumer):
        task_consumer.config.pool_size = 1
        await task_consumer.consume()
        await task_producer.send(tasks.sleep, args=(0.5,), priority=10)
        aw1 = (await task_producer.send(tasks.sleep, args=(1,), priority=1)).get()
        aw2 = (await task_producer.send(tasks.sleep, args=(1,), priority=2)).get()
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
    async def test_lost_connection(self, backend, task_producer, task_consumer):
        await task_consumer.consume()
        await asyncio.sleep(1)
        backend.container.stop()
        await asyncio.sleep(3)
        backend.container.start()
        await asyncio.sleep(1)
        ar = await task_producer.send(tasks.hello_world)
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
    async def test_task_timeout(self, backend, task_producer, task_consumer):
        await task_consumer.consume()
        ar = await task_producer.send(tasks.sleep, args=(3600,), timeout=1)
        with pytest.raises(arrlio.TaskError):
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
    async def test_task_thread(self, backend, task_producer, task_consumer):
        await task_consumer.consume()

        ar = await task_producer.send(tasks.thread_name)
        assert re.match("^Thread-[0-9]*", await asyncio.wait_for(ar.get(), 5))

        ar = await task_producer.send("thread_name")
        assert re.match("^Thread-[0-9]*", await asyncio.wait_for(ar.get(), 5))

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_task_no_result(self, backend, task_producer, task_consumer):
        await task_consumer.consume()

        ar = await task_producer.send(tasks.noresult)
        with pytest.raises(arrlio.TaskNoResultError):
            await asyncio.wait_for(ar.get(), 5)

        ar = await task_producer.send(tasks.hello_world, result_return=False)
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
    async def test_task_result_timeout(self, backend, task_producer, task_consumer):
        await task_consumer.consume()
        ar = await task_producer.send(tasks.hello_world, result_ttl=1)
        await asyncio.sleep(3)
        with pytest.raises((arrlio.TaskNoResultError, asyncio.TimeoutError)):
            await asyncio.wait_for(ar.get(), 2)

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_message(self, backend, message_producer, message_consumer):
        if backend.module == "arrlio.backend.rabbitmq":
            async with message_consumer.backend.channel_ctx() as channel:
                exchange = arrlio.settings.MESSAGE_EXCHANGE
                await channel.exchange_declare(
                    exchange,
                    exchange_type="direct",
                    durable=False,
                    auto_delete=True,
                )
                for queue in message_consumer.config.queues:
                    await channel.queue_declare(
                        queue,
                        durable=False,
                        auto_delete=True,
                    )
                    await channel.queue_bind(queue, exchange, routing_key=queue)

        flag = asyncio.Future()

        async def on_message(message):
            nonlocal flag
            flag.set_result(message == "Hello!")

        message_consumer.on_message = on_message

        await message_consumer.consume()
        await message_producer.send("Hello!", routing_key="arrlio.messages")

        assert (await asyncio.wait_for(flag, 1))
