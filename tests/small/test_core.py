from unittest import mock

import pytest

from arrlio import MessageProducer, MessageProducerConfig, TaskProducer, TaskProducerConfig, settings, task
from arrlio.core import AsyncResult


pytestmark = pytest.mark.asyncio


def test_task():
    @task
    async def foo():
        pass

    @task(bind=True)
    async def bar():
        pass


async def test_TaskProducer():
    config = TaskProducerConfig()
    producer = TaskProducer(config)
    assert producer.config == config
    assert isinstance(producer.backend, config.backend.Backend)

    await producer.close()


async def test_MessageProducer():
    config = MessageProducerConfig()
    producer = MessageProducer(config)
    assert producer.config == config
    assert isinstance(producer.backend, config.backend.Backend)

    await producer.close()


async def test_TaskProducer_send():
    config = TaskProducerConfig()
    producer = TaskProducer(config)

    with mock.patch.object(producer.backend, "send_task") as mock_send_task:
        ar = await producer.send("foo")
        mock_send_task.assert_awaited_once()
        task_instance = mock_send_task.call_args[0][0]
        assert task_instance.task.func is None
        assert task_instance.task.name == "foo"
        assert task_instance.task.bind == settings.TASK_BIND
        assert task_instance.task.queue == settings.TASK_QUEUE
        assert task_instance.task.priority == settings.TASK_PRIORITY
        assert task_instance.task.timeout == settings.TASK_TIMEOUT
        assert task_instance.task.ttl == settings.TASK_TTL
        assert task_instance.task.ack_late == settings.TASK_ACK_LATE
        assert task_instance.task.result_ttl == settings.RESULT_TTL
        assert task_instance.task.result_return == settings.RESULT_RETURN
        assert task_instance.task.result_encrypt == settings.RESULT_ENCRYPT
        assert task_instance.data.args == ()
        assert task_instance.data.kwds == {}
        assert task_instance.data.queue == task_instance.task.queue
        assert task_instance.data.priority == task_instance.task.priority
        assert task_instance.data.timeout == task_instance.task.timeout
        assert task_instance.data.ttl == task_instance.task.ttl
        assert task_instance.data.ack_late == task_instance.task.ack_late
        assert task_instance.data.result_ttl == task_instance.task.result_ttl
        assert task_instance.data.result_return == task_instance.task.result_return
        assert task_instance.data.result_encrypt == task_instance.task.result_encrypt

        assert isinstance(ar, AsyncResult)
        assert ar.task_instance == task_instance

    await producer.close()
