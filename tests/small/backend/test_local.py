import asyncio
from unittest import mock

import pytest

from arrlio import TaskNoResultError
from arrlio.backend import local
from arrlio.models import Task, TaskData, TaskInstance
from arrlio.serializer.nop import Nop


pytestmark = pytest.mark.asyncio


def test_BackendConfig():
    config = local.BackendConfig()
    assert config.serializer == Nop
    assert config.name == local.BACKEND_NAME


async def test_Backend():
    backend1 = local.Backend(local.BackendConfig())

    assert str(backend1)
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
    assert backend1._refs == 1
    assert backend2._refs == 1
    assert backend3._refs == 1

    await backend2.close()
    assert backend1._refs is None
    assert backend2._refs is None
    assert backend3._refs == 1

    await backend3.close()
    assert backend3._refs is None


async def test_Backend_send_task():
    backend = local.Backend(local.BackendConfig())
    task_instance = TaskInstance(task=Task(None, "test_send_task"), data=TaskData(queue="queue"))
    await backend.send_task(task_instance)
    assert backend._task_queues["queue"].qsize() == 1
    await backend.send_task(task_instance, encrypt=True)
    assert backend._task_queues["queue"].qsize() == 2

    await backend.close()


async def test_Backend_consume_tasks():
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


async def test_Backend_push_pop_task_result():
    backend = local.Backend(local.BackendConfig())
    task_instance = TaskInstance(
        task=Task(None, "test_push_pop_task_result"),
        data=TaskData(queue="queue", result_ttl=1),
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
