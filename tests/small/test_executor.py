from unittest import mock

import pytest

from arrlio.executor import Config, Executor
from tests import tasks


class TestExecutor:
    @pytest.mark.asyncio
    async def test_execute(self, cleanup):
        executor = Executor(Config())

        result = await executor.execute(tasks.hello_world.instantiate())
        assert result.exc is None
        assert result.res == "Hello World!"
        assert result.trb is None
        assert result.routes is None

        result = await executor.execute(tasks.sync_task.instantiate())
        assert result.exc is None
        assert result.res == "Hello from sync_task!"
        assert result.trb is None
        assert result.routes is None

        result = await executor.execute(tasks.zero_division.instantiate())
        assert result.exc is not None
        assert isinstance(result.exc, ZeroDivisionError)
        assert result.res is None
        assert result.trb is not None
        assert result.routes is None

    @pytest.mark.asyncio
    async def test_execute_in_thread(self, cleanup):
        executor = Executor(Config())

        result = await executor.execute_in_thread(tasks.hello_world.instantiate())
        assert result.exc is None
        assert result.res == "Hello World!"
        assert result.trb is None
        assert result.routes is None

        result = await executor.execute_in_thread(tasks.sync_task.instantiate())
        assert result.exc is None
        assert result.res == "Hello from sync_task!"
        assert result.trb is None
        assert result.routes is None

        result = await executor.execute_in_thread(tasks.zero_division.instantiate())
        assert result.exc is not None
        assert isinstance(result.exc, ZeroDivisionError)
        assert result.res is None
        assert result.trb is not None
        assert result.routes is None

    @pytest.mark.asyncio
    async def test_call(self, cleanup):
        executor = Executor(Config())

        with mock.patch.object(executor, "execute") as mock_execute:
            task_instance = tasks.hello_world.instantiate()
            await executor(task_instance)
            mock_execute.assert_awaited_once_with(task_instance)

        with mock.patch.object(executor, "execute_in_thread") as mock_execute:
            task_instance = tasks.hello_world.instantiate(thread=True)
            await executor(task_instance)
            mock_execute.assert_awaited_once_with(task_instance)
