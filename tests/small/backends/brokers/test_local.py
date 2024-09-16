import asyncio

from datetime import datetime
from unittest import mock

import pytest

from arrlio.backends.brokers import local
from arrlio.exceptions import TaskResultError
from arrlio.models import Event, Task


class TestConfig:
    def test__init(self, cleanup):
        config = local.Config()
        assert config.id


class TestBroker:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        broker = local.Broker(local.Config())
        await broker.close()

    @pytest.mark.asyncio
    async def test_str(self, cleanup):
        broker = local.Broker(local.Config(id="abc"))
        try:
            assert str(broker).startswith("Broker[local#abc]")
        finally:
            await broker.close()

    @pytest.mark.asyncio
    async def test_repr(self, cleanup):
        broker = local.Broker(local.Config(id="abc"))
        try:
            assert repr(broker).startswith("Broker[local#abc]")
        finally:
            await broker.close()

    @pytest.mark.asyncio
    async def test_with(self, cleanup):
        broker = local.Broker(local.Config())
        async with broker:
            pass

    @pytest.mark.asyncio
    async def test_send_task(self, cleanup):
        broker = local.Broker(local.Config())
        task_instance = Task(None, "test_send_task").instantiate(queue="queue", result_return=True)
        await broker.send_task(task_instance)
        assert broker._queues["queue"].qsize() == 1
        await broker.send_task(task_instance)
        assert broker._queues["queue"].qsize() == 2
        await broker.close()

    @pytest.mark.asyncio
    async def test_consume_tasks(self, cleanup):
        broker = local.Broker(local.Config())
        task_instance = Task(None, "test_consume_task").instantiate(queue="queue")
        fut = asyncio.Future()

        async def on_task(*args):
            assert args == (task_instance,)
            fut.set_result(True)

        await broker.consume_tasks(["queue"], on_task)
        await broker.send_task(task_instance)

        await asyncio.wait_for(fut, 1)
        await broker.close()

    @pytest.mark.asyncio
    async def test_task_on_closed_broker(self, cleanup):
        broker = local.Broker(local.Config())
        await broker.close()

        async def on_task(*args):
            pass

        with pytest.raises(Exception):
            await broker.consume_tasks(["queue"], on_task)
