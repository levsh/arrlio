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
    async def test_task_default(self, backend, producer, consumer):
        await consumer.consume_tasks()

        ar = await producer.send_task(tasks.hello_world)
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

        ar = await producer.send_task("hello_world")
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
    async def test_sync_task_(self, backend, producer, consumer):
        await consumer.consume_tasks()

        ar = await producer.send_task(tasks.sync_task)
        assert await asyncio.wait_for(ar.get(), 5) == "Hello from sync_task!"

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_task_not_found(self, backend, producer, consumer):
        await consumer.consume_tasks()
        with pytest.raises(arrlio.exc.TaskError):
            ar = await producer.send_task("invalid")
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
    async def test_task_args_kwds(self, backend, producer, consumer):
        await consumer.consume_tasks()
        ar = await producer.send_task(tasks.echo, args=(1, 2), kwds={"3": 3, "4": 4})
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
    async def test_task_custom_queue(self, backend, producer, consumer):
        consumer.config.task_queues = ["queue1", "queue2"]
        await consumer.consume_tasks()
        ar = await producer.send_task(tasks.hello_world, queue="queue1")
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"
        ar = await producer.send_task(tasks.hello_world, queue="queue2")
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
        ],
        indirect=True,
    )
    async def test_task_priority(self, backend, producer, consumer):
        consumer.config.pool_size = 1
        await consumer.consume_tasks()
        await producer.send_task(tasks.sleep, args=(0.5,), priority=10)
        aw1 = (await producer.send_task(tasks.sleep, args=(1,), priority=1)).get()
        aw2 = (await producer.send_task(tasks.sleep, args=(1,), priority=2)).get()
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
    async def test_lost_connection(self, backend, producer, consumer):
        await consumer.consume_tasks()
        await asyncio.sleep(1)
        backend.container.stop()
        await asyncio.sleep(3)
        backend.container.start()
        await asyncio.sleep(1)
        ar = await producer.send_task(tasks.hello_world)
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
    async def test_task_timeout(self, backend, producer, consumer):
        await consumer.consume_tasks()
        ar = await producer.send_task(tasks.sleep, args=(3600,), timeout=1)
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
    async def test_task_thread(self, backend, producer, consumer):
        await consumer.consume_tasks()

        ar = await producer.send_task(tasks.thread_name)
        assert re.match("^Thread-[0-9]*", await asyncio.wait_for(ar.get(), 5))

        ar = await producer.send_task("thread_name")
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
    async def test_task_no_result(self, backend, producer, consumer):
        await consumer.consume_tasks()

        ar = await producer.send_task(tasks.noresult)
        with pytest.raises(arrlio.TaskNoResultError):
            await asyncio.wait_for(ar.get(), 5)

        ar = await producer.send_task(tasks.hello_world, result_return=False)
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
    async def test_task_result_timeout(self, backend, producer, consumer):
        await consumer.consume_tasks()
        ar = await producer.send_task(tasks.hello_world, result_ttl=1)
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
    async def test_message(self, backend, producer, consumer):
        if backend.module == "arrlio.backend.rabbitmq":
            async with consumer.backend.channel_ctx() as channel:
                exchange = arrlio.settings.MESSAGE_EXCHANGE
                await channel.exchange_declare(
                    exchange,
                    exchange_type="direct",
                    durable=False,
                    auto_delete=True,
                )
                for queue in consumer.config.message_queues:
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

        consumer.on_message = on_message

        await consumer.consume_messages()
        await producer.send_message("Hello!", routing_key="arrlio.messages")

        assert await asyncio.wait_for(flag, 1)

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            "arrlio.backend.rabbitmq",
            "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_graph(self, backend, producer, consumer):
        await consumer.consume_tasks()

        graph = arrlio.Graph("Test")
        graph.add_node("A", tasks.add_one, root=True)
        graph.add_node("B", tasks.add_one)
        graph.add_node("C", tasks.add_one)
        graph.add_edge("A", "B")
        graph.add_edge("B", "C")

        ars = await producer.send_graph(graph, args=(0,))
        assert await asyncio.wait_for(ars["A"].get(), 1) == 1
        assert await asyncio.wait_for(ars["B"].get(), 1) == 2
        assert await asyncio.wait_for(ars["C"].get(), 1) == 3

    @pytest.mark.parametrize(
        "backend",
        [
            "arrlio.backend.local",
            # "arrlio.backend.rabbitmq",
            # "arrlio.backend.redis",
        ],
        indirect=True,
    )
    async def test_events(self, backend, producer, consumer):
        ev = asyncio.Event()
        ev.clear()

        async def on_event(event):
            if event.type == "task done" and event.data["status"] is True:
                ev.set()

        await consumer.consume_tasks()
        await consumer.consume_events(on_event)

        ar = await producer.send_task(tasks.hello_world, events=True)
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"
        await asyncio.wait_for(ev.wait(), 1)
