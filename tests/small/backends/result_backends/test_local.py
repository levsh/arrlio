import asyncio

from unittest import mock

import pytest

from arrlio.backends.result_backends import local
from arrlio.exceptions import TaskResultError
from arrlio.models import Task


class TestConfig:
    def test__init(self, cleanup):
        config = local.Config()
        assert config.id


class TestResultBackend:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        result_backend = local.ResultBackend(local.Config())
        await result_backend.close()

    @pytest.mark.asyncio
    async def test_str(self, cleanup):
        result_backend = local.ResultBackend(local.Config(id="abc"))
        try:
            assert str(result_backend).startswith("ResultBackend[local#abc]")
        finally:
            await result_backend.close()

    @pytest.mark.asyncio
    async def test_repr(self, cleanup):
        result_backend = local.ResultBackend(local.Config(id="abc"))
        try:
            assert repr(result_backend).startswith("ResultBackend[local#abc]")
        finally:
            await result_backend.close()

    @pytest.mark.asyncio
    async def test_with(self, cleanup):
        result_backend = local.ResultBackend(local.Config())
        async with result_backend:
            pass

    @pytest.mark.asyncio
    async def test_push_pop_task_result(self, cleanup):
        result_backend = local.ResultBackend(local.Config())
        task_instance = Task(None, "test_push_pop_task_result").instantiate(
            queue="queue",
            result_return=True,
            result_ttl=100,
        )
        result = mock.MagicMock()

        await result_backend.push_task_result(result, task_instance)
        assert await result_backend.pop_task_result(task_instance).__anext__() == result
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(result_backend.pop_task_result(task_instance).__anext__(), 0.5)
        assert task_instance.task_id not in result_backend._results

        task_instance = Task(None, "test_push_pop_task_result", result_return=False).instantiate(queue="queue")
        result = mock.MagicMock()

        await result_backend.push_task_result(result, task_instance)
        assert task_instance.task_id not in result_backend._results
        with pytest.raises(TaskResultError):
            await result_backend.pop_task_result(task_instance).__anext__()

        await result_backend.close()

    @pytest.mark.asyncio
    async def test_pop_task_result_timeout(self, cleanup):
        result_backend = local.ResultBackend(local.Config())
        task_instance = Task(None, "test_pop_task_result").instantiate(
            queue="queue",
            result_return=True,
            result_ttl=1,
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(result_backend.pop_task_result(task_instance).__anext__(), 0.1)

        await result_backend.close()

    @pytest.mark.asyncio
    async def test_task_on_closed_result_backend(self, cleanup):
        result_backend = local.ResultBackend(local.Config())
        await result_backend.close()

        async def on_task(*args):
            pass

        with pytest.raises(Exception):
            await result_backend.consume_tasks(["queue"], on_task)
