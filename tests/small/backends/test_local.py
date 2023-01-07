import asyncio
from datetime import datetime
from unittest import mock

import pytest

from arrlio import serializers
from arrlio.backends import local
from arrlio.exc import TaskNoResultError
from arrlio.models import Event, Message, Task


class TestConfig:
    def test__init(self, cleanup):
        config = local.Config()
        assert config.serializer.module == serializers.nop
        assert config.id == local.BACKEND_ID


class TestBackend:
    def test__init(self, cleanup):
        backend = local.Backend(local.Config())
        assert isinstance(backend.serializer, serializers.nop.Serializer)

    def test_str(self, cleanup):
        backend = local.Backend(local.Config())
        assert str(backend)

    def test_repr(self, cleanup):
        backend = local.Backend(local.Config())
        assert repr(backend)

    @pytest.mark.asyncio
    async def test_with(self, cleanup):
        backend = local.Backend(local.Config())
        async with backend:
            pass

    @pytest.mark.asyncio
    async def test_shared(self, cleanup):
        assert local.Backend._Backend__shared == {}
        backend1 = local.Backend(local.Config())

        assert backend1._refs == 1

        backend2 = local.Backend(local.Config())
        assert backend1._refs == 2
        assert backend2._refs == 2
        assert id(backend1._task_queues) == id(backend1._task_queues)
        assert id(backend1._message_queues) == id(backend1._message_queues)
        assert id(backend1._results) == id(backend1._results)

        backend3 = local.Backend(local.Config(id="custom"))
        assert backend1._refs == 2
        assert backend2._refs == 2
        assert backend3._refs == 1
        assert id(backend1._task_queues) != id(backend3._task_queues)
        assert id(backend1._message_queues) != id(backend3._message_queues)
        assert id(backend1._results) != id(backend3._results)

        await backend1.close()
        await backend1.close()
        del backend1
        assert local.Backend._Backend__shared["arrlio"]["refs"] == 1
        assert backend2._refs == 1
        assert local.Backend._Backend__shared["custom"]["refs"] == 1
        assert backend3._refs == 1

        await backend2.close()
        await backend2.close()
        del backend2
        assert "arrlio" not in local.Backend._Backend__shared
        assert local.Backend._Backend__shared["custom"]["refs"] == 1
        assert backend3._refs == 1

        await backend3.close()
        await backend3.close()
        del backend3
        assert "custom" not in local.Backend._Backend__shared

    @pytest.mark.asyncio
    async def test_send_task(self, cleanup):
        backend = local.Backend(local.Config())
        task_instance = Task(None, "test_send_task").instantiate(queue="queue", result_return=True)
        await backend.send_task(task_instance)
        assert backend._task_queues["queue"].qsize() == 1
        await backend.send_task(task_instance)
        assert backend._task_queues["queue"].qsize() == 2

        await backend.close()

    @pytest.mark.asyncio
    async def test_consume_tasks(self, cleanup):
        backend = local.Backend(local.Config())
        task_instance = Task(None, "test_consume_task").instantiate(queue="queue")
        fut = asyncio.Future()

        async def on_task(*args):
            assert args == (task_instance,)
            fut.set_result(True)

        await backend.consume_tasks(["queue"], on_task)
        await backend.send_task(task_instance)

        await asyncio.wait_for(fut, 1)

        await backend.close()

    @pytest.mark.asyncio
    async def test_push_pop_task_result(self, cleanup):
        backend = local.Backend(local.Config())
        task_instance = Task(None, "test_push_pop_task_result").instantiate(
            queue="queue",
            result_return=True,
            result_ttl=100,
        )
        result = mock.MagicMock()

        await backend.push_task_result(task_instance, result)
        assert await backend.pop_task_result(task_instance).__anext__() == result
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(backend.pop_task_result(task_instance).__anext__(), 0.5)
        assert task_instance.data.task_id not in backend._results

        task_instance = Task(None, "test_push_pop_task_result", result_return=False).instantiate(queue="queue")
        result = mock.MagicMock()

        await backend.push_task_result(task_instance, result)
        assert task_instance.data.task_id not in backend._results
        with pytest.raises(TaskNoResultError):
            await backend.pop_task_result(task_instance).__anext__()

        await backend.close()

    @pytest.mark.asyncio
    async def test_pop_task_result_timeout(self, cleanup):
        backend = local.Backend(local.Config())
        task_instance = Task(None, "test_pop_task_result").instantiate(
            queue="queue",
            result_return=True,
            result_ttl=1,
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(backend.pop_task_result(task_instance).__anext__(), 0.1)

        await backend.close()

    @pytest.mark.asyncio
    async def test_send_message(self, cleanup):
        backend = local.Backend(local.Config())
        message = Message({}, exchange="queue")
        await backend.send_message(message)
        assert backend._message_queues["queue"].qsize() == 1
        await backend.send_message(message)
        assert backend._message_queues["queue"].qsize() == 2

        await backend.close()

    @pytest.mark.asyncio
    async def test_consume_messages(self, cleanup):
        backend = local.Backend(local.Config())
        message = Message({}, exchange="queue")
        fut = asyncio.Future()

        async def on_message(*args):
            assert args == (message,)
            fut.set_result(True)

        await backend.consume_messages(["queue"], on_message)
        await backend.send_message(message)

        await asyncio.wait_for(fut, 1)

        await backend.close()

    @pytest.mark.asyncio
    async def test_send_event(self, cleanup):
        backend = local.Backend(local.Config())
        event = Event(type="ev_type", dt=datetime.now(), data={})
        await backend.send_event(event)
        assert backend._events[event.event_id] == event

        await backend.close()

    @pytest.mark.asyncio
    async def test_consume_events(self, cleanup):
        backend = local.Backend(local.Config())
        event = Event(type="ev_type", dt=datetime.now(), data={})
        fut = asyncio.Future()

        async def on_event(*args):
            assert args == (event,)
            fut.set_result(True)

        await backend.consume_events("test", on_event)
        await backend.send_event(event)

        await asyncio.wait_for(fut, 1)

        await backend.close()

    @pytest.mark.asyncio
    async def test__cancel_all_tasks(self, cleanup):
        backend = local.Backend(local.Config())
        task = asyncio.create_task(asyncio.sleep(10))
        backend._backend_tasks["test"].add(task)
        backend._cancel_all_backend_tasks()
        assert task.cancelled

    @pytest.mark.asyncio
    async def test_task_on_closed_backend(self, cleanup):
        backend = local.Backend(local.Config())
        await backend.close()

        async def on_task(*args):
            pass

        with pytest.raises(Exception):
            await backend.consume_tasks(["queue"], on_task)
