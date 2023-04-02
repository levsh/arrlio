import asyncio
import re

import pytest

import arrlio
from arrlio import backends
from tests import tasks


class TestArrlio:
    @pytest.mark.parametrize(
        "params",
        [
            {"backend": {"module": "arrlio.backends.local"}},
            {"backend": {"module": "arrlio.backends.rabbitmq"}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "single_use"}}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "direct_reply_to"}}},
            {"backend": {"module": "arrlio.backends.redis"}},
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "     rabbitmq:single_use",
            "rabbitmq:direct_reply_to",
            "                   redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_basic(self, params):
        backend, app = params

        await app.consume_tasks()

        for _ in range(2):
            ar = await app.send_task(tasks.hello_world)
            assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"
            assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

        for _ in range(2):
            ar = await app.send_task("hello_world")
            assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"
            assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

        if isinstance(app.backend, backends.rabbitmq.Backend):
            for _ in range(2):
                ar = await app.send_task("hello_world", extra={"rabbitmq:result_queue_mode": "single_use"})
                assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"
                assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

            for _ in range(2):
                ar = await app.send_task("hello_world", extra={"rabbitmq:result_queue_mode": "direct_reply_to"})
                assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"
                assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

        for _ in range(2):
            ar = await app.send_task(tasks.sync_task)
            assert await asyncio.wait_for(ar.get(), 5) == "Hello from sync_task!"
            assert await asyncio.wait_for(ar.get(), 5) == "Hello from sync_task!"

        for task in (tasks.async_xrange, tasks.xrange):
            for _ in range(2):
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
            {"backend": {"module": "arrlio.backends.local"}},
            {"backend": {"module": "arrlio.backends.rabbitmq"}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "single_use"}}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "direct_reply_to"}}},
            {"backend": {"module": "arrlio.backends.redis"}},
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "     rabbitmq:single_use",
            "rabbitmq:direct_reply_to",
            "                   redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_not_found(self, params):
        backend, app = params

        await app.consume_tasks()
        with pytest.raises(arrlio.exc.TaskError):
            ar = await app.send_task("invalid")
            await ar.get()
        with pytest.raises(arrlio.exc.TaskError):
            await ar.get()

    @pytest.mark.parametrize(
        "params",
        [
            {"backend": {"module": "arrlio.backends.local"}},
            {"backend": {"module": "arrlio.backends.rabbitmq"}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "single_use"}}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "direct_reply_to"}}},
            {"backend": {"module": "arrlio.backends.redis"}},
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "     rabbitmq:single_use",
            "rabbitmq:direct_reply_to",
            "                   redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_custom(self, params):
        backend, app = params

        app.config.task_queues = ["queue1", "queue2"]
        await app.consume_tasks()

        for _ in range(2):
            ar = await app.send_task(tasks.echo, args=(1, 2), kwds={"3": 3, "4": 4}, queue="queue1")
            res = await asyncio.wait_for(ar.get(), 5)
            assert res == ((1, 2), {"3": 3, "4": 4}) or res == [[1, 2], {"3": 3, "4": 4}]
            assert res == ((1, 2), {"3": 3, "4": 4}) or res == [[1, 2], {"3": 3, "4": 4}]

        for _ in range(2):
            ar = await app.send_task(tasks.bind_true, queue="queue1")
            res = await asyncio.wait_for(ar.get(), 5)
            res = await asyncio.wait_for(ar.get(), 5)

        for task in (tasks.async_xrange, tasks.xrange):
            for _ in range(2):
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
            {"backend": {"module": "arrlio.backends.local", "config": {"pool_size": 1}}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"pool_size": 1}}},
            {
                "backend": {
                    "module": "arrlio.backends.rabbitmq",
                    "config": {"pool_size": 1, "results_queue_mode": "single_use"},
                }
            },
            # {"backend": {"module": "arrlio.backends.redis", "config": {"pool_size": 1}}},
        ],
        indirect=True,
        ids=[
            "              local",
            "           rabbitmq",
            "rabbitmq:single_use",
            # "            redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_priority(self, params):
        backend, app = params

        await app.consume_tasks()

        for _ in range(2):
            await app.send_task(tasks.sleep, args=(0.5,), priority=10, ack_late=True)
            t1 = asyncio.create_task((await app.send_task(tasks.sleep, args=(1,), priority=1, ack_late=True)).get())
            t2 = asyncio.create_task((await app.send_task(tasks.sleep, args=(1,), priority=2, ack_late=True)).get())
            done, pending = await asyncio.wait_for(asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED), 5)
            assert done == {t2}
            assert pending == {t1}
            await t1

    @pytest.mark.parametrize(
        "params",
        [
            {
                "backend": {
                    "module": "arrlio.backends.rabbitmq",
                    "config": {
                        "tasks_queue_durable": True,
                        "results_common_queue_durable": True,
                    },
                }
            },
            {
                "backend": {
                    "module": "arrlio.backends.rabbitmq",
                    "config": {
                        "tasks_queue_durable": True,
                        "results_queue_mode": "single_use",
                        "results_single_use_queue_durable": True,
                    },
                }
            },
            {"backend": {"module": "arrlio.backends.redis"}},
        ],
        indirect=True,
        ids=[
            "           rabbitmq",
            "rabbitmq:single_use",
            "              redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_lost_connection(self, params):
        backend, app = params

        await app.consume_tasks()

        for task in (tasks.async_xrange, tasks.xrange):
            for _ in range(1):
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
                assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"

    @pytest.mark.parametrize(
        "params",
        [
            {"backend": {"module": "arrlio.backends.local"}},
            {"backend": {"module": "arrlio.backends.rabbitmq"}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "single_use"}}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "direct_reply_to"}}},
            {"backend": {"module": "arrlio.backends.redis"}},
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "     rabbitmq:single_use",
            "rabbitmq:direct_reply_to",
            "                   redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_timeout(self, params):
        backend, app = params

        await app.consume_tasks()

        for _ in range(2):
            ar = await app.send_task(tasks.sleep, args=(3600,), timeout=1)
            with pytest.raises(arrlio.exc.TaskError):
                await asyncio.wait_for(ar.get(), 5)

    @pytest.mark.parametrize(
        "params",
        [
            {"backend": {"module": "arrlio.backends.local"}},
            {"backend": {"module": "arrlio.backends.rabbitmq"}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "single_use"}}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "direct_reply_to"}}},
            {"backend": {"module": "arrlio.backends.redis"}},
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "     rabbitmq:single_use",
            "rabbitmq:direct_reply_to",
            "                   redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_thread(self, params):
        backend, app = params

        await app.consume_tasks()

        for _ in range(2):
            ar = await app.send_task(tasks.thread_name)
            assert re.match("^Thread-[0-9]*", await asyncio.wait_for(ar.get(), 5))

            ar = await app.send_task("thread_name")
            assert re.match("^Thread-[0-9]*", await asyncio.wait_for(ar.get(), 5))

    @pytest.mark.parametrize(
        "params",
        [
            {"backend": {"module": "arrlio.backends.local"}},
            {"backend": {"module": "arrlio.backends.rabbitmq"}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "single_use"}}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "direct_reply_to"}}},
            {"backend": {"module": "arrlio.backends.redis"}},
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "     rabbitmq:single_use",
            "rabbitmq:direct_reply_to",
            "                   redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_no_result(self, params):
        backend, app = params

        await app.consume_tasks()

        ar = await app.send_task(tasks.noresult)
        with pytest.raises(arrlio.exc.TaskResultError):
            await asyncio.wait_for(ar.get(), 5)

        ar = await app.send_task(tasks.hello_world, result_return=False)
        with pytest.raises(arrlio.exc.TaskResultError):
            await asyncio.wait_for(ar.get(), 5)

    @pytest.mark.parametrize(
        "params",
        [
            {"backend": {"module": "arrlio.backends.local"}},
            {"backend": {"module": "arrlio.backends.rabbitmq"}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "single_use"}}},
            {"backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "direct_reply_to"}}},
            {"backend": {"module": "arrlio.backends.redis"}},
        ],
        indirect=True,
        ids=[
            "                   local",
            "                rabbitmq",
            "     rabbitmq:single_use",
            "rabbitmq:direct_reply_to",
            "                   redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_task_result_timeout(self, params):
        backend, app = params

        await app.consume_tasks()

        for _ in range(2):
            ar = await app.send_task(tasks.hello_world, result_ttl=1)
            await asyncio.sleep(3)
            with pytest.raises((arrlio.exc.TaskResultError, asyncio.TimeoutError)):
                await asyncio.wait_for(ar.get(), 2)

    @pytest.mark.parametrize(
        "params",
        [
            {"backend": {"module": "arrlio.backends.local"}},
            {"backend": {"module": "arrlio.backends.rabbitmq"}},
            {"backend": {"module": "arrlio.backends.redis"}},
        ],
        indirect=True,
        ids=[
            "          local",
            "       rabbitmq",
            "          redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_message(self, params):
        backend, app = params

        if isinstance(app.backend, backends.rabbitmq.Backend):
            channel = await app.backend._conn.channel()
            exchange = arrlio.settings.MESSAGE_EXCHANGE
            await channel.exchange_declare(
                exchange,
                exchange_type="direct",
                durable=False,
                auto_delete=True,
            )
            for queue in app.config.message_queues:
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

        await app.consume_messages(on_message)
        await app.send_message("Hello!", routing_key="arrlio.messages")

        assert await asyncio.wait_for(flag, 1)

    @pytest.mark.parametrize(
        "params",
        [
            {
                "backend": {"module": "arrlio.backends.local"},
                "plugins": [{"module": "arrlio.plugins.events"}],
            },
            {
                "backend": {"module": "arrlio.backends.rabbitmq"},
                "plugins": [{"module": "arrlio.plugins.events"}],
            },
            {
                "backend": {"module": "arrlio.backends.redis"},
                "plugins": [{"module": "arrlio.plugins.events"}],
            },
        ],
        indirect=True,
        ids=[
            "          local",
            "       rabbitmq",
            "          redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_events(self, params):
        backend, app = params

        ev = asyncio.Event()
        ev.clear()

        async def on_event(event):
            if event.type == "task:done" and event.data["status"]["exc"] is None:
                ev.set()

        await app.consume_tasks()
        await app.consume_events("test", on_event)

        ar = await app.send_task(tasks.hello_world, events=True)
        assert await asyncio.wait_for(ar.get(), 5) == "Hello World!"
        await asyncio.wait_for(ev.wait(), 1)

    @pytest.mark.parametrize(
        "params",
        [
            {
                "backend": {"module": "arrlio.backends.local"},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
            {
                "backend": {"module": "arrlio.backends.rabbitmq"},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
            {
                "backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "single_use"}},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
            {
                "backend": {"module": "arrlio.backends.redis"},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
        ],
        indirect=True,
        ids=[
            "                    local",
            "                 rabbitmq",
            "      rabbitmq:single_use",
            "                    redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_graph(self, params):
        backend, app = params

        await app.consume_tasks()

        # basic

        graph = arrlio.Graph("Test")
        graph.add_node("A", tasks.add_one, root=True)
        graph.add_node("B", tasks.add_one)
        graph.add_node("C", tasks.add_one)
        graph.add_edge("A", "B")
        graph.add_edge("B", "C")

        ars = await app.send_graph(graph, args=(0,))

        assert await asyncio.wait_for(ars["A"].get(), 5) == 1
        assert await asyncio.wait_for(ars["B"].get(), 5) == 2
        assert await asyncio.wait_for(ars["C"].get(), 5) == 3

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
                "backend": {"module": "arrlio.backends.local"},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
            {
                "backend": {"module": "arrlio.backends.rabbitmq"},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
            {
                "backend": {"module": "arrlio.backends.rabbitmq", "config": {"results_queue_mode": "single_use"}},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
            {
                "backend": {"module": "arrlio.backends.redis"},
                "plugins": [
                    {"module": "arrlio.plugins.events"},
                    {"module": "arrlio.plugins.graphs"},
                ],
            },
        ],
        indirect=True,
        ids=[
            "                    local",
            "                 rabbitmq",
            "      rabbitmq:single_use",
            "                    redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_graph_complex(self, params):
        backend, app = params

        await app.consume_tasks()

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
        with pytest.raises(arrlio.exc.TaskClosedError):
            await asyncio.wait_for(ars["C"].get(), 5)

        ###

        ars = await app.send_graph(graph, args=(0, 1))

        assert await asyncio.wait_for(ars["A"].get(), 1) is False
        with pytest.raises(arrlio.exc.TaskClosedError):
            await asyncio.wait_for(ars["B"].get(), 1)
        assert await asyncio.wait_for(ars["C"].get(), 1) is None

    @pytest.mark.parametrize(
        "params",
        [
            {"backend": {"module": "arrlio.backends.local"}},
            {"backend": {"module": "arrlio.backends.rabbitmq"}},
            {"backend": {"module": "arrlio.backends.redis"}},
        ],
        indirect=True,
        ids=[
            "          local",
            "       rabbitmq",
            "          redis",
        ],
    )
    @pytest.mark.asyncio
    async def test_param_loads_result_dumps(self, params):
        backend, app = params

        await app.consume_tasks()

        ar = await app.send_task(tasks.loads_dumps, args=[tasks.LoadsDumps(x=1)])
        if isinstance(app.backend, backends.local.Backend):
            assert await asyncio.wait_for(ar.get(), 1) == tasks.LoadsDumps(x=1)
        else:
            assert await asyncio.wait_for(ar.get(), 1) == {"x": 1}
