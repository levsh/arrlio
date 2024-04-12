import asyncio
import datetime
import json
import uuid

import pytest

from arrlio import task, utils


@pytest.mark.asyncio
async def test_wait_for():
    async def foo():
        await asyncio.sleep(0.1)
        return "foo"

    assert await utils.wait_for(foo(), 2) == "foo"

    flag = asyncio.Event()

    async def foo():
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            flag.set()

    with pytest.raises(asyncio.TimeoutError):
        await utils.wait_for(foo(), 0.1)
    await asyncio.sleep(0)
    assert flag.is_set()


@pytest.mark.asyncio
async def test_retry():
    counter = 0

    @utils.retry(retry_timeouts=[], exc_filter=lambda e: False)
    async def foo():
        nonlocal counter
        counter += 1
        raise KeyError

    with pytest.raises(KeyError):
        await foo()
    assert counter == 1

    counter = 0

    @utils.retry(retry_timeouts=[0, 0], exc_filter=lambda e: False)
    async def foo():
        nonlocal counter
        counter += 1
        raise KeyError

    with pytest.raises(KeyError):
        await foo()
    assert counter == 1

    counter = 0

    @utils.retry(retry_timeouts=[0, 0], exc_filter=lambda e: isinstance(e, KeyError))
    async def foo():
        nonlocal counter
        counter += 1
        raise KeyError

    with pytest.raises(KeyError):
        await foo()
    assert counter == 3


@pytest.mark.asyncio
async def test_retry_generator():
    counter = 0

    @utils.retry(retry_timeouts=[], exc_filter=lambda e: False)
    async def foo():
        nonlocal counter
        counter += 1
        raise KeyError
        yield

    with pytest.raises(KeyError):
        async for _ in foo():
            pass
    assert counter == 1

    counter = 0

    @utils.retry(retry_timeouts=[0, 0], exc_filter=lambda e: False)
    async def foo():
        nonlocal counter
        counter += 1
        raise KeyError
        yield

    with pytest.raises(KeyError):
        async for _ in foo():
            pass
    assert counter == 1

    counter = 0

    @utils.retry(retry_timeouts=[0, 0], exc_filter=lambda e: isinstance(e, KeyError))
    async def foo():
        nonlocal counter
        counter += 1
        raise KeyError
        yield

    with pytest.raises(KeyError):
        async for _ in foo():
            pass
    assert counter == 3


def test_ExtendedJSONEncoder():
    assert (
        json.dumps(
            "a",
            cls=utils.ExtendedJSONEncoder,
        )
        == '"a"'
    )
    assert (
        json.dumps(
            datetime.datetime(2021, 1, 1),
            cls=utils.ExtendedJSONEncoder,
        )
        == '"2021-01-01T00:00:00"'
    )
    assert (
        json.dumps(
            uuid.UUID("ea47d0af-c6b2-45d0-9a05-6bd1e34aa58c"),
            cls=utils.ExtendedJSONEncoder,
        )
        == '"ea47d0af-c6b2-45d0-9a05-6bd1e34aa58c"'
    )

    @task
    def foo():
        pass

    assert json.dumps(foo, cls=utils.ExtendedJSONEncoder) == (
        """{\"func\": \"test_utils.foo\", \"name\": \"test_utils.foo\", """
        """\"queue\": \"arrlio.tasks\", \"priority\": 1, \"timeout\": 300, \"ttl\": 300, """
        """\"ack_late\": false, \"result_ttl\": 300, \"result_return\": true, """
        """\"thread\": null, \"events\": false, \"event_ttl\": 300, \"extra\": {}}"""
    )

    class C:
        pass

    with pytest.raises(TypeError):
        json.dumps(C(), cls=utils.ExtendedJSONEncoder)


def test_LoopIter():
    it = utils.LoopIter([0, 1, 2])
    for _ in range(3):
        assert next(it) == 0
        assert next(it) == 1
        assert next(it) == 2
        with pytest.raises(StopIteration):
            next(it)
