import asyncio
import re

import pytest

import arrlio

from arrlio import exceptions
from arrlio.backends import brokers
from tests import tasks


TIMEOUT = 5


class TestArrlio:
    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {
                    "module": "arrlio.backends.result_backends.rabbitmq",
                    "config": {"reply_to_mode": "direct_reply_to"},
                },
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "rabbitmq:direct_reply_to",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_basic(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        for _ in range(3):
            ar = await app.send_task(tasks.hello_world)
            assert await asyncio.wait_for(ar.get(), TIMEOUT) == "Hello World!"
            assert await asyncio.wait_for(ar.get(), TIMEOUT) == "Hello World!"

        for _ in range(3):
            ar = await app.send_task("hello_world")
            assert await asyncio.wait_for(ar.get(), TIMEOUT) == "Hello World!"
            assert await asyncio.wait_for(ar.get(), TIMEOUT) == "Hello World!"

        if isinstance(app.broker, brokers.rabbitmq.Broker):
            for _ in range(3):
                ar = await app.send_task("hello_world", headers={"rabbitmq:reply_to": "amq.rabbitmq.reply-to"})
                assert await asyncio.wait_for(ar.get(), TIMEOUT) == "Hello World!"
                assert await asyncio.wait_for(ar.get(), TIMEOUT) == "Hello World!"

        for _ in range(3):
            ar = await app.send_task(tasks.sync_task)
            assert await asyncio.wait_for(ar.get(), TIMEOUT) == "Hello from sync_task!"
            assert await asyncio.wait_for(ar.get(), TIMEOUT) == "Hello from sync_task!"

        for task in (tasks.async_xrange, tasks.xrange):
            for _ in range(3):
                results = []
                ar = await app.send_task(task, args=(3,))
                async for x in ar:
                    results.append(x)
                assert results == [0, 1, 2]
                results = []
                async for x in ar:
                    results.append(x)
                assert results == []

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {
                    "module": "arrlio.backends.result_backends.rabbitmq",
                    "config": {"reply_to_mode": "direct_reply_to"},
                },
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "rabbitmq:direct_reply_to",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_not_found(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        for _ in range(2):
            with pytest.raises(exceptions.TaskError):
                ar = await app.send_task("invalid")
                await ar.get()
            with pytest.raises(exceptions.TaskError):
                await ar.get()

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {
                    "module": "arrlio.backends.result_backends.rabbitmq",
                    "config": {"reply_to_mode": "direct_reply_to"},
                },
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "rabbitmq:direct_reply_to",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_custom(self, params):
        backend, app = params

        app.config.task_queues = ["queue1", "queue2"]

        await app.consume_tasks()
        await asyncio.sleep(1)

        for _ in range(3):
            ar = await app.send_task(tasks.echo, args=(1, 2), kwds={"3": 3, "4": 4}, queue="queue1")
            res = await asyncio.wait_for(ar.get(), TIMEOUT)
            assert res == ((1, 2), {"3": 3, "4": 4}) or res == [[1, 2], {"3": 3, "4": 4}]
            assert res == ((1, 2), {"3": 3, "4": 4}) or res == [[1, 2], {"3": 3, "4": 4}]

        for task in (tasks.async_xrange, tasks.xrange):
            for _ in range(3):
                results = []
                ar = await app.send_task(task, queue="queue2", args=(3,))
                async for x in ar:
                    results.append(x)
                assert results == [0, 1, 2]
                results = []
                async for x in ar:
                    results.append(x)
                assert results == []

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {
                    "module": "arrlio.backends.brokers.local",
                    "config": {"pool_size": 1},
                },
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
            },
        ],
        indirect=True,
        ids=[
            "                   local",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_priority(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        for _ in range(3):
            await app.send_task(tasks.sleep, args=(0.5,), priority=5, ack_late=True)
            t1 = asyncio.create_task((await app.send_task(tasks.sleep, args=(1,), priority=1, ack_late=True)).get())
            t2 = asyncio.create_task((await app.send_task(tasks.sleep, args=(1,), priority=2, ack_late=True)).get())
            done, pending = await asyncio.wait_for(asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED), TIMEOUT)
            assert done == {t2}
            assert pending == {t1}
            await t1

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {
                    "module": "arrlio.backends.result_backends.rabbitmq",
                    "config": {"queue_durable": True},
                },
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
        ],
        indirect=True,
        ids=[
            "           rabbitmq",
        ],
    )
    @pytest.mark.asyncio
    async def test_lost_connection(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        for task in (tasks.async_xrange, tasks.xrange):
            for _ in range(3):
                ar = await app.send_task(task, args=(3,), kwds={"sleep": 1}, thread=True)
                await asyncio.sleep(1)
                backend.stop()
                await asyncio.sleep(3)
                backend.start()
                await asyncio.sleep(1)

                results = []
                async for x in ar:
                    results.append(x)
                assert results == [0, 1, 2]

                ar = await app.send_task(tasks.hello_world)
                assert await asyncio.wait_for(ar.get(), TIMEOUT) == "Hello World!"

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {
                    "module": "arrlio.backends.result_backends.rabbitmq",
                    "config": {"reply_to_mode": "direct_reply_to"},
                },
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "rabbitmq:direct_reply_to",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_timeout(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        for _ in range(3):
            ar = await app.send_task(tasks.sleep, args=(3600,), timeout=1)
            with pytest.raises(exceptions.TaskError):
                await asyncio.wait_for(ar.get(), TIMEOUT)

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {
                    "module": "arrlio.backends.result_backends.rabbitmq",
                    "config": {"reply_to_mode": "direct_reply_to"},
                },
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "rabbitmq:direct_reply_to",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_thread(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        for _ in range(3):
            ar = await app.send_task(tasks.thread_name)
            assert re.match("^Thread-[0-9]*", await asyncio.wait_for(ar.get(), TIMEOUT))

            ar = await app.send_task("thread_name")
            assert re.match("^Thread-[0-9]*", await asyncio.wait_for(ar.get(), TIMEOUT))

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {
                    "module": "arrlio.backends.result_backends.rabbitmq",
                    "config": {"reply_to_mode": "direct_reply_to"},
                },
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "rabbitmq:direct_reply_to",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_no_result(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        ar = await app.send_task(tasks.noresult)
        with pytest.raises(exceptions.TaskResultError):
            await asyncio.wait_for(ar.get(), TIMEOUT)

        ar = await app.send_task(tasks.hello_world, result_return=False)
        with pytest.raises(exceptions.TaskResultError):
            await asyncio.wait_for(ar.get(), TIMEOUT)

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {
                    "module": "arrlio.backends.result_backends.rabbitmq",
                    "config": {"reply_to_mode": "direct_reply_to"},
                },
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "rabbitmq:direct_reply_to",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_result_timeout(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        for _ in range(3):
            ar = await app.send_task(tasks.hello_world, result_ttl=1)
            await asyncio.sleep(3)
            with pytest.raises((exceptions.TaskResultError, asyncio.TimeoutError)):
                await asyncio.wait_for(ar.get(), 2)

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
                "plugins": [{"module": "arrlio.plugins.events"}],
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
                "plugins": [{"module": "arrlio.plugins.events"}],
            },
        ],
        indirect=True,
        ids=[
            "   local",
            "rabbitmq",
        ],
    )
    @pytest.mark.asyncio
    async def test_events(self, params):
        backend, app = params

        ev = asyncio.Event()
        ev.clear()

        async def on_event(event):
            if event.type == "task.done" and event.data["result"].exc is None:
                ev.set()

        await app.consume_tasks()
        await app.consume_events("test", on_event)
        await asyncio.sleep(1)

        ar = await app.send_task(tasks.hello_world, events=True)
        assert await asyncio.wait_for(ar.get(), TIMEOUT) == "Hello World!"
        await asyncio.wait_for(ev.wait(), 1)

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
        ],
        indirect=True,
        ids=[
            "   local",
            "rabbitmq",
        ],
    )
    @pytest.mark.asyncio
    async def test_graph(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        # basic

        graph = arrlio.Graph("Test")
        graph.add_node("A", tasks.add_one, root=True)
        graph.add_node("B", tasks.add_one)
        graph.add_node("C", tasks.add_one)
        graph.add_edge("A", "B")
        graph.add_edge("B", "C")

        ars = await app.send_graph(graph, args=(0,))

        assert await asyncio.wait_for(ars["A"].get(), TIMEOUT) == 1
        assert await asyncio.wait_for(ars["B"].get(), TIMEOUT) == 2
        assert await asyncio.wait_for(ars["C"].get(), TIMEOUT) == 3

        # xrange

        for task in (tasks.xrange, tasks.async_xrange):
            for i in range(10):
                graph = arrlio.Graph(f"Test {i}")
                graph.add_node("A", task, root=True)
                graph.add_node("B", task)
                graph.add_node("C", task)
                graph.add_node("D", task)
                graph.add_node("E", task)
                graph.add_edge("A", "B")
                graph.add_edge("B", "C")
                graph.add_edge("C", "D")
                graph.add_edge("D", "E")

                ars = await app.send_graph(graph, args=(5,))

                actual = []
                async for result in ars["A"]:
                    actual.append(result)
                assert sorted(actual) == sorted([0, 1, 2, 3, 4])

                actual = []
                async for result in ars["B"]:
                    actual.append(result)
                assert sorted(actual) == sorted([0, 0, 1, 0, 1, 2, 0, 1, 2, 3])

                actual = []
                async for result in ars["C"]:
                    actual.append(result)
                assert sorted(actual) == sorted([0, 0, 0, 1, 0, 0, 1, 0, 1, 2])

                actual = []
                async for result in ars["D"]:
                    actual.append(result)
                assert sorted(actual) == sorted([0, 0, 0, 0, 1])

                actual = []
                async for result in ars["E"]:
                    actual.append(result)
                assert sorted(actual) == sorted([0])

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
        ],
        indirect=True,
        ids=[
            "   local",
            "rabbitmq",
        ],
    )
    @pytest.mark.asyncio
    async def test_graph_complex(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        graph = arrlio.Graph("Test")
        graph.add_node("A", tasks.compare, root=True)
        graph.add_node("B", tasks.logger_info)
        graph.add_node("C", tasks.logger_info)
        graph.add_edge("A", "B", routes="true")
        graph.add_edge("A", "C", routes="false")

        ###

        ars = await app.send_graph(graph, args=(0, 0))

        assert await asyncio.wait_for(ars["A"].get(), 1) is True
        assert await asyncio.wait_for(ars["B"].get(), 1) is None
        with pytest.raises(exceptions.TaskClosedError):
            await asyncio.wait_for(ars["C"].get(), TIMEOUT)

        ###

        ars = await app.send_graph(graph, args=(0, 1))

        assert await asyncio.wait_for(ars["A"].get(), 1) is False
        with pytest.raises(exceptions.TaskClosedError):
            await asyncio.wait_for(ars["B"].get(), 1)
        assert await asyncio.wait_for(ars["C"].get(), 1) is None

    @pytest.mark.parametrize(
        "params",
        [
            {
                "broker": {"module": "arrlio.backends.brokers.local"},
                "result_backend": {"module": "arrlio.backends.result_backends.local"},
                "event_backend": {"module": "arrlio.backends.event_backends.local"},
            },
            {
                "broker": {"module": "arrlio.backends.brokers.rabbitmq"},
                "result_backend": {"module": "arrlio.backends.result_backends.rabbitmq"},
                "event_backend": {"module": "arrlio.backends.event_backends.rabbitmq"},
            },
        ],
        indirect=True,
        ids=[
            "   local",
            "rabbitmq",
        ],
    )
    @pytest.mark.asyncio
    async def test_param_loads_result_dumps(self, params):
        backend, app = params

        await app.consume_tasks()
        await asyncio.sleep(1)

        ar = await app.send_task(tasks.loads_dumps, args=[tasks.LoadsDumps(x=1)])
        if isinstance(app.broker, brokers.local.Broker):
            assert await asyncio.wait_for(ar.get(), 1) == tasks.LoadsDumps(x=1)
        else:
            assert await asyncio.wait_for(ar.get(), 1) == {"x": 1}
