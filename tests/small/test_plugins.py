from unittest import mock

import pytest

from arrlio import App, Config
from arrlio.plugins.events import Plugin as EventsPlugin
from tests import tasks


class TestEventsPlugin:
    @pytest.mark.asyncio
    async def test_init(self, cleanup):
        app = App(Config())
        try:
            plugin = EventsPlugin(app)
            assert plugin.name == "events"
            await plugin.on_init()
            await plugin.on_close()
        finally:
            await app.close()

    @pytest.mark.asyncio
    async def test_on_task_received(self, cleanup):
        app = App(Config())
        try:
            plugin = EventsPlugin(app)
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
    async def test_on_task_don(self, cleanup):
        app = App(Config())
        try:
            plugin = EventsPlugin(app)
            task_instance = tasks.hello_world.instantiate()
            task_result = await app.executor(task_instance)
            with mock.patch.object(app, "send_event") as mock_send_event:
                await plugin.on_task_done(task_instance, task_result)
                mock_send_event.assert_not_awaited()
            task_instance = tasks.hello_world.instantiate(events=True)
            with mock.patch.object(app, "send_event") as mock_send_event:
                await plugin.on_task_done(task_instance, task_result)
                mock_send_event.assert_awaited_once()
                event = mock_send_event.call_args.args[0]
                assert event.type == "task:done"
                assert event.data == {"task_id": task_instance.data.task_id, "status": True}
        finally:
            await app.close()
