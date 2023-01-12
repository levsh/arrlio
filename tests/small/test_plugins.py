import asyncio
from contextlib import asynccontextmanager
from types import ModuleType
from unittest import mock

import pytest

import arrlio
from arrlio import App, Config, TaskResult
from arrlio.plugins import base
from arrlio.plugins.events import Config as EventsPluginConfig
from arrlio.plugins.events import Plugin as EventsPlugin
from arrlio.plugins.graphs import Plugin as GraphsPlugin
from tests import tasks


class TestEventsPlugin:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        app = App(Config())
        try:
            plugin = EventsPlugin(app, EventsPluginConfig())
            assert plugin.name == "arrlio.events"
            await plugin.on_init()
            await plugin.on_close()
        finally:
            await app.close()

    @pytest.mark.asyncio
    async def test_on_task_received(self, cleanup):
        app = App(Config())
        try:
            plugin = EventsPlugin(app, EventsPluginConfig())

            task_instance = tasks.hello_world.instantiate()
            with mock.patch.object(app, "send_event") as mock_send_event:
                await plugin.on_task_received(task_instance)
                mock_send_event.assert_not_awaited()

            task_instance = tasks.hello_world.instantiate(events=True)
            with mock.patch.object(app, "send_event") as mock_send_event:
                await plugin.on_task_received(task_instance)
                mock_send_event.assert_awaited_once()
                event = mock_send_event.call_args.args[0]
                assert event.type == "task:received"
                assert event.data == {"task_id": task_instance.data.task_id}
        finally:
            await app.close()

    @pytest.mark.asyncio
    async def test_on_task_done(self, cleanup):
        app = App(Config())
        try:
            plugin = EventsPlugin(app, EventsPluginConfig())

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
                assert event.type == "task:done"
                assert event.data == {
                    "task_id": task_instance.data.task_id,
                    "status": TaskResult(res="Hello World!", exc=None, trb=None, routes=None),
                }
        finally:
            await app.close()


class TestGraphsPlugin:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        app = App(Config())
        try:
            plugin = GraphsPlugin(app, EventsPluginConfig())
            assert plugin.name == "arrlio.graphs"

            with pytest.raises(arrlio.exc.ArrlioError):
                await plugin.on_init()

            with mock.patch("arrlio.core.App.consume_events") as mock_consume_events:
                app._plugins["arrlio.events"] = mock.MagicMock()
                await plugin.on_init()
                mock_consume_events.assert_awaited_once_with(
                    "arrlio.graphs",
                    plugin._on_event,
                    event_types=["graph:task:send", "graph:task:done"],
                )

            await plugin.on_close()
        finally:
            await app.close()


class TestPlugin:
    @pytest.mark.asyncio
    async def test_task_context(self):
        ev = asyncio.Event()

        class _Config(base.Config):
            pass

        class _Plugin(base.Plugin):
            @property
            def name(self) -> str:
                return "arrlio.tests"

            @asynccontextmanager
            async def task_context(self, task_instance):
                self.app.context.set({"x": "y"})
                yield

            async def on_task_done(self, task_result, *args, **kwds):
                if self.app.context.get() == {"x": "y"}:
                    ev.set()

        module = ModuleType("test")
        module.Config = _Config
        module.Plugin = _Plugin

        app = App(Config(plugins=[{"module": module}]))
        await app.init()
        await app.consume_tasks()
        ar = await app.send_task("hello_world")
        await asyncio.wait_for(ar.get(), 5)
        assert ev.is_set()
