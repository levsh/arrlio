import asyncio
import datetime
from unittest import mock

import pytest

from arrlio import TaskNoResultError
from arrlio.backends import local
from arrlio.models import Event, Message, Task, TaskData, TaskInstance
from arrlio.serializers import nop


class TestBackendConfig:
    def test_init(self):
        config = local.BackendConfig()
        assert config.serializer == nop
        assert config.name == local.BACKEND_NAME

    def test_init_custom(self):
        def serializer_factory():
            return nop.Serializer()

        config = local.BackendConfig(serializer=serializer_factory)
        assert config.serializer == serializer_factory
        assert config.name == local.BACKEND_NAME


class TestBackend:
    def test_init(self):
        backend = local.Backend(local.BackendConfig())
        assert isinstance(backend.serializer, nop.Serializer)

    def test_init_custom(self):
        backend = local.Backend(local.BackendConfig(serializer=lambda: nop.Serializer()))
        assert isinstance(backend.serializer, nop.Serializer)

    def test_str(self):
        backend = local.Backend(local.BackendConfig())
        assert str(backend)

    def test_repr(self):
        backend = local.Backend(local.BackendConfig())
        assert repr(backend)

    @pytest.mark.asyncio
    async def test_shared(self):
        backend1 = local.Backend(local.BackendConfig())

        assert backend1._refs == 1

        backend2 = local.Backend(local.BackendConfig())
        assert backend1._refs == 2
        assert backend2._refs == 2
        assert id(backend1._task_queues) == id(backend1._task_queues)
        assert id(backend1._message_queues) == id(backend1._message_queues)
        assert id(backend1._results) == id(backend1._results)

        backend3 = local.Backend(local.BackendConfig(name="custom"))
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
    async def test_send_task(self):
        backend = local.Backend(local.BackendConfig())
        task_instance = TaskInstance(
            task=Task(None, "test_send_task"),
            data=TaskData(queue="queue", result_return=True),
        )
        await backend.send_task(task_instance)
        assert backend._task_queues["queue"].qsize() == 1
        await backend.send_task(task_instance)
        assert backend._task_queues["queue"].qsize() == 2

        await backend.close()

    @pytest.mark.asyncio
    async def test_consume_tasks(self):
        backend = local.Backend(local.BackendConfig())
        task_instance = TaskInstance(task=Task(None, "test_consume_task"), data=TaskData(queue="queue"))
        fut = asyncio.Future()

        async def on_task(*args):
            assert args == (task_instance,)
            fut.set_result(True)

        await backend.consume_tasks(["queue"], on_task)
        await backend.send_task(task_instance)

        await asyncio.wait_for(fut, 1)

        await backend.close()

    @pytest.mark.asyncio
    async def test_push_pop_task_result(self):
        backend = local.Backend(local.BackendConfig())
        task_instance = TaskInstance(
            task=Task(None, "test_push_pop_task_result"),
            data=TaskData(queue="queue", result_return=True, result_ttl=1),
        )
        result = mock.MagicMock()
        backend._results[task_instance.data.task_id] = [asyncio.Event(), None]

        await backend.push_task_result(task_instance, result)
        assert await backend.pop_task_result(task_instance) == result
        assert task_instance.data.task_id not in backend._results

        task_instance = TaskInstance(
            task=Task(None, "test_push_pop_task_result", result_return=False),
            data=TaskData(queue="queue"),
        )
        result = mock.MagicMock()

        await backend.push_task_result(task_instance, result)
        assert task_instance.data.task_id not in backend._results
        with pytest.raises(TaskNoResultError):
            await backend.pop_task_result(task_instance)

        await backend.close()

    @pytest.mark.asyncio
    async def test_pop_task_result(self):
        backend = local.Backend(local.BackendConfig())
        task_instance = TaskInstance(
            task=Task(None, "test_pop_task_result"),
            data=TaskData(queue="queue", result_return=True, result_ttl=1),
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(backend.pop_task_result(task_instance), 0.1)

        await backend.close()

    @pytest.mark.asyncio
    async def test_send_message(self):
        backend = local.Backend(local.BackendConfig())
        message = Message({}, exchange="queue")
        await backend.send_message(message)
        assert backend._message_queues["queue"].qsize() == 1
        await backend.send_message(message)
        assert backend._message_queues["queue"].qsize() == 2

        await backend.close()

    @pytest.mark.asyncio
    async def test_consume_messages(self):
        backend = local.Backend(local.BackendConfig())
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
    async def test_push_event(self):
        backend = local.Backend(local.BackendConfig())
        task_instance = TaskInstance(
            task=Task(None, "test_push_event"),
            data=TaskData(queue="queue", events=True, event_ttl=10),
        )
        event = Event(type="ev_type", datetime=datetime.datetime.now(), data={})
        await backend.push_event(task_instance, event)
        assert backend._events[event.event_id] == event

        await backend.close()

    @pytest.mark.asyncio
    async def test_consume_events(self):
        backend = local.Backend(local.BackendConfig())
        task_instance = TaskInstance(
            task=Task(None, "test_push_event"),
            data=TaskData(queue="queue", events=True, event_ttl=10),
        )
        event = Event(type="ev_type", datetime=datetime.datetime.now(), data={})
        fut = asyncio.Future()

        async def on_event(*args):
            assert args == (event,)
            fut.set_result(True)

        await backend.consume_events(on_event)
        await backend.push_event(task_instance, event)

        await asyncio.wait_for(fut, 1)

        await backend.close()

    @pytest.mark.asyncio
    async def test__cancel_tasks(self):
        backend = local.Backend(local.BackendConfig())
        task = asyncio.create_task(asyncio.sleep(10))
        backend._tasks.add(task)
        backend._cancel_tasks()
        assert task.cancelled

    @pytest.mark.asyncio
    async def test_task_on_closed_backend(self):
        backend = local.Backend(local.BackendConfig())
        await backend.close()

        async def on_task(*args):
            pass

        with pytest.raises(Exception):
            await backend.consume_tasks(["queue"], on_task)
