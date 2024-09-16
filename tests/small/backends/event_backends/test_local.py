import asyncio

from datetime import datetime

import pytest

from arrlio.backends.event_backends import local
from arrlio.models import Event


class TestConfig:
    def test__init(self, cleanup):
        config = local.Config()
        assert config.id


class TestEventBackend:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        event_backend = local.EventBackend(local.Config())
        await event_backend.close()

    @pytest.mark.asyncio
    async def test_str(self, cleanup):
        event_backend = local.EventBackend(local.Config(id="abc"))
        try:
            assert str(event_backend).startswith("EventBackend[local#abc]")
        finally:
            await event_backend.close()

    @pytest.mark.asyncio
    async def test_repr(self, cleanup):
        event_backend = local.EventBackend(local.Config(id="abc"))
        try:
            assert repr(event_backend).startswith("EventBackend[local#abc]")
        finally:
            await event_backend.close()

    @pytest.mark.asyncio
    async def test_with(self, cleanup):
        event_backend = local.EventBackend(local.Config())
        async with event_backend:
            pass

    @pytest.mark.asyncio
    async def test_send_event(self, cleanup):
        event_backend = local.EventBackend(local.Config())
        event = Event(type="ev_type", dt=datetime.now(), data={})
        await event_backend.send_event(event)
        assert event_backend._events[event.event_id] == event

        await event_backend.close()

    @pytest.mark.asyncio
    async def test_consume_events(self, cleanup):
        event_backend = local.EventBackend(local.Config())
        event = Event(type="ev_type", dt=datetime.now(), data={})
        fut = asyncio.Future()

        async def on_event(*args):
            assert args == (event,)
            fut.set_result(True)

        await event_backend.consume_events("test", on_event)
        await event_backend.send_event(event)

        await asyncio.wait_for(fut, 1)

        await event_backend.close()

    @pytest.mark.asyncio
    async def test_task_on_closed_event_backend(self, cleanup):
        event_backend = local.EventBackend(local.Config())
        await event_backend.close()

        async def on_task(*args):
            pass

        with pytest.raises(Exception):
            await event_backend.consume_tasks(["queue"], on_task)
