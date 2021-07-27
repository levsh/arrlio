import datetime
import json
import uuid

import pytest

from arrlio import utils


pytestmark = pytest.mark.asyncio


class C:
    pass


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
    with pytest.raises(TypeError):
        json.dumps(C(), cls=utils.ExtendedJSONEncoder)


async def test_AsyncRetry():
    async def foo(a, b=None):
        pass

    await utils.AsyncRetry()(foo, 1)

    counter = 0

    async def bar():
        nonlocal counter
        counter += 1
        1 / 0

    with pytest.raises(ZeroDivisionError):
        await utils.AsyncRetry()(bar)
    assert counter == 1

    counter = 0
    with pytest.raises(ZeroDivisionError):
        await utils.AsyncRetry(retry_timeouts=[0, 0], exc_filter=lambda e: isinstance(e, ZeroDivisionError))(bar)
    assert counter == 3
