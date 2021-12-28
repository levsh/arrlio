import uuid

import pytest
from arrlio import models, settings


pytestmark = pytest.mark.asyncio


def test_TaskData():
    task_data = models.TaskData()
    assert isinstance(task_data.task_id, uuid.UUID)
    assert task_data.args == ()
    assert task_data.kwds == {}
    assert task_data.queue is None
    assert task_data.priority is None
    assert task_data.timeout is None
    assert task_data.ttl is None
    assert task_data.ack_late is None
    assert task_data.result_ttl is None
    assert task_data.result_return is None
    assert task_data.result_encrypt is None
    assert task_data.thread is None

    task_data.queue = "default"


async def test_Task():
    def foo():
        return "Foo!"

    async def bar(*args, **kwds):
        return "Bar!"

    task = models.Task(foo, "foo")
    assert task.func == foo
    assert task.name == "foo"
    assert task.bind == settings.TASK_BIND
    assert task.queue == settings.TASK_QUEUE
    assert task.priority == settings.TASK_PRIORITY
    assert task.timeout == settings.TASK_TIMEOUT
    assert task.ttl == settings.TASK_TTL
    assert task.ack_late == settings.TASK_ACK_LATE
    assert task.result_ttl == settings.RESULT_TTL
    assert task.result_return == settings.RESULT_RETURN
    assert task.result_encrypt == settings.RESULT_ENCRYPT
    assert task.thread is None

    task_instance = task.instatiate()
    assert isinstance(task_instance.data.task_id, uuid.UUID)
    assert task_instance.data.args == ()
    assert task_instance.data.kwds == {}
    assert task_instance.data.queue == task.queue
    assert task_instance.data.priority == task.priority
    assert task_instance.data.timeout == task.timeout
    assert task_instance.data.ttl == task.ttl
    assert task_instance.data.ack_late == task.ack_late
    assert task_instance.data.result_ttl == task.result_ttl
    assert task_instance.data.result_return == task.result_return
    assert task_instance.data.result_encrypt == task.result_encrypt
    assert task_instance.data.thread is None

    assert await task() == "Foo!"

    task_instance = models.Task(bar, "bar").instatiate(
        models.TaskData(
            task_id='e67b80b9-a9f0-4ff1-89e8-0beb70993ffd',
            args=[1],
            kwds={"a": "a"},
            queue="abc",
            priority=5,
            timeout=10,
            ttl=20,
            ack_late=False,
            result_ttl=30,
            result_return=False,
            result_encrypt=True,
            thread=True,
        )
    )
    assert task_instance.task.func == bar
    assert task_instance.task.name == "bar"
    assert task_instance.task.bind == settings.TASK_BIND
    assert task_instance.task.queue == settings.TASK_QUEUE
    assert task_instance.task.priority == settings.TASK_PRIORITY
    assert task_instance.task.timeout == settings.TASK_TIMEOUT
    assert task_instance.task.ttl == settings.TASK_TTL
    assert task_instance.task.ack_late == settings.TASK_ACK_LATE
    assert task_instance.task.result_ttl == settings.RESULT_TTL
    assert task_instance.task.result_return == settings.RESULT_RETURN
    assert task_instance.task.result_encrypt == settings.RESULT_ENCRYPT
    assert task_instance.task.thread is None

    assert task_instance.data.task_id == uuid.UUID('e67b80b9-a9f0-4ff1-89e8-0beb70993ffd')
    assert task_instance.data.args == (1,)
    assert task_instance.data.kwds == {"a": "a"}
    assert task_instance.data.queue == "abc"
    assert task_instance.data.priority == 5
    assert task_instance.data.timeout == 10
    assert task_instance.data.ttl == 20
    assert task_instance.data.ack_late is False
    assert task_instance.data.result_ttl == 30
    assert task_instance.data.result_return is False
    assert task_instance.data.result_encrypt is True
    assert task_instance.data.thread is True

    assert await task_instance() == "Bar!"

    assert await models.Task(bar, "bar", bind=True)() == "Bar!"
