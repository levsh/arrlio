import asyncio
from contextlib import asynccontextmanager
from types import ModuleType
from unittest import mock

import pytest

import arrlio
from arrlio import App, Config, TaskResult, exceptions
from arrlio.plugins import base
from arrlio.plugins.events import Config as EventsPluginConfig
from arrlio.plugins.events import Config as GraphsPluginConfig
from arrlio.plugins.events import Plugin as EventsPlugin
from arrlio.plugins.graphs import Plugin as GraphsPlugin
from tests import tasks


class TestEventsPlugin:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        app = App(Config())
        try:
            plugin = EventsPlugin(app, EventsPluginConfig())
            try:
                assert plugin.name == "arrlio.events"
                await plugin.on_init()
            finally:
                await plugin.on_close()
        finally:
            await app.close()

    @pytest.mark.asyncio
    async def test_on_task_received(self, cleanup):
        app = App(Config())
        try:
            plugin = EventsPlugin(app, EventsPluginConfig())
            try:
                task_instance = tasks.hello_world.instantiate()
                with mock.patch.object(app, "send_event") as mock_send_event:
                    await plugin.on_task_received(task_instance)
                    mock_send_event.assert_not_awaited()

                task_instance = tasks.hello_world.instantiate(events=True)
                with mock.patch.object(app, "send_event") as mock_send_event:
                    await plugin.on_task_received(task_instance)
                    mock_send_event.assert_awaited_once()
                    event = mock_send_event.call_args.args[0]
                    assert event.type == "task.received"
                    assert event.data == {"task:id": task_instance.task_id}
            finally:
                await plugin.on_close()
        finally:
            await app.close()

    @pytest.mark.asyncio
    async def test_on_task_done(self, cleanup):
        app = App(Config())
        try:
            plugin = EventsPlugin(app, EventsPluginConfig())
            try:
                task_instance = tasks.hello_world.instantiate()
                task_result = await app.executor(task_instance).__anext__()
                with mock.patch.object(app, "send_event") as mock_send_event:
                    await plugin.on_task_done(task_instance, task_result)
                    mock_send_event.assert_not_awaited()

                task_instance = tasks.hello_world.instantiate(events=True)
                with mock.patch.object(app, "send_event") as mock_send_event:
                    await plugin.on_task_done(task_instance, task_result)
                    mock_send_event.assert_awaited_once()
                    event = mock_send_event.call_args.args[0]
                    assert event.type == "task.done"
                    assert event.data == {
                        "task:id": task_instance.task_id,
                        "result": TaskResult(res="Hello World!", exc=None, trb=None, idx=None, routes=None),
                    }
            finally:
                await plugin.on_close()
        finally:
            await app.close()


class TestGraphsPlugin:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        app = App(Config())
        try:
            plugin = GraphsPlugin(app, GraphsPluginConfig())
            try:
                assert plugin.name == "arrlio.graphs"

                with pytest.raises(exceptions.ArrlioError):
                    await plugin.on_init()

                with mock.patch("arrlio.core.App.consume_events") as mock_consume_events:
                    app._plugins["arrlio.events"] = mock.MagicMock()
                    await plugin.on_init()
                    mock_consume_events.assert_awaited_once_with(
                        "arrlio.graphs",
                        plugin._on_event,
                        event_types=["graph.task.send", "graph.task.done"],
                    )
            finally:
                await plugin.on_close()
        finally:
            await app.close()

    @pytest.mark.asyncio
    async def test_on_task_done(self, cleanup):
        app = App(Config())
        try:
            plugin = GraphsPlugin(app, GraphsPluginConfig())
            try:
                task_instance = tasks.hello_world.instantiate()
                task_result = await app.executor(task_instance).__anext__()
                with mock.patch.object(app, "send_event") as mock_send_event:
                    await plugin.on_task_done(task_instance, task_result)
                    mock_send_event.assert_not_awaited()

                graph = arrlio.Graph("Test")
                graph_id = "id"
                graph_app_id = "app_id"
                graph_call_id = "call_id"
                task_instance = tasks.xrange.instantiate(
                    extra={
                        "graph:graph": graph,
                        "graph:id": graph_id,
                        "graph:app_id": graph_app_id,
                        "graph:call_id": graph_call_id,
                    }
                )
                with mock.patch.object(app, "send_event") as mock_send_event:
                    await plugin.on_task_done(task_instance, task_result)
                    mock_send_event.assert_awaited_once()
                    event = mock_send_event.call_args.args[0]
                    assert event.type == "graph.task.done"
                    assert event.data == {
                        "task:id": task_instance.task_id,
                        "graph:id": graph_id,
                        "graph:app_id": graph_app_id,
                        "graph:call_id": graph_call_id,
                    }, event.data
            finally:
                await plugin.on_close()
        finally:
            await app.close()

    @pytest.mark.asyncio
    async def test__init_graph(self, cleanup):
        graph = arrlio.Graph("Test")
        graph.add_node("A", tasks.xrange, root=True)
        graph.add_node("B", tasks.xrange)
        graph.add_edge("A", "B")

        app = App(Config())
        try:
            plugin = GraphsPlugin(app, GraphsPluginConfig())
            try:
                new_graph = plugin._init_graph(graph)
                assert new_graph.name == graph.name
                assert new_graph.edges == graph.edges
                assert new_graph.roots == graph.roots
                for _, (_, node_kwds) in new_graph.nodes.items():
                    assert node_kwds["task_id"]
            finally:
                await plugin.on_close()
        finally:
            await app.close()


class TestPlugin:
    @pytest.mark.asyncio
    async def test_task_context(self, cleanup):
        ev = asyncio.Event()

        class TestConfig(base.Config):
            pass

        class TestPlugin(base.Plugin):
            @property
            def name(self) -> str:
                return "arrlio.tests"

            @asynccontextmanager
            async def task_context(self, task_instance):
                self.app.context["x"] = "y"
                yield

            async def on_task_done(self, task_instance, task_result):
                if "task_instance" in self.app.context and self.app.context["x"] == "y":
                    ev.set()

        module = ModuleType("test")
        module.Config = TestConfig
        module.Plugin = TestPlugin

        app = App(Config(plugins=[{"module": module}]))
        try:
            await app.init()
            await app.consume_tasks()
            ar = await app.send_task("hello_world")
            await asyncio.wait_for(ar.get(), 5)
            assert ev.is_set()
        finally:
            await app.close()
