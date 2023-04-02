import pytest

from arrlio.executor import Config, Executor
from tests import tasks


class TestExecutor:
    @pytest.mark.asyncio
    async def test_execute(self, cleanup):
        executor = Executor(Config())

        async for result in executor.execute(tasks.hello_world.instantiate()):
            assert result.exc is None
            assert result.res == "Hello World!"
            assert result.trb is None
            assert result.routes is None

        async for result in executor.execute(tasks.sync_task.instantiate()):
            assert result.exc is None
            assert result.res == "Hello from sync_task!"
            assert result.trb is None
            assert result.routes is None

        async for result in executor.execute(tasks.zero_division.instantiate()):
            assert result.exc is not None
            assert isinstance(result.exc, ZeroDivisionError)
            assert result.res is None
            assert result.trb is not None
            assert result.routes is None

    @pytest.mark.asyncio
    async def test_execute_in_thread(self, cleanup):
        executor = Executor(Config())

        async for result in executor.execute_in_thread(tasks.hello_world.instantiate()):
            assert result.exc is None
            assert result.res == "Hello World!"
            assert result.trb is None
            assert result.routes is None

        async for result in executor.execute_in_thread(tasks.sync_task.instantiate()):
            assert result.exc is None
            assert result.res == "Hello from sync_task!"
            assert result.trb is None
            assert result.routes is None

        async for result in executor.execute_in_thread(tasks.zero_division.instantiate()):
            assert result.exc is not None
            assert isinstance(result.exc, ZeroDivisionError)
            assert result.res is None
            assert result.trb is not None
            assert result.routes is None

    @pytest.mark.asyncio
    async def test_call(self, cleanup):
        executor = Executor(Config())

        # async xrange
        actual = []
        async for res in executor(tasks.async_xrange.instantiate(args=(3,))):
            actual.append(res.res)
        assert actual == [0, 1, 2]

        # xrange
        actual = []
        async for res in executor(tasks.xrange.instantiate(args=(3,))):
            actual.append(res.res)
        assert actual == [0, 1, 2]
