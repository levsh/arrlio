import datetime
import json
import uuid

import pytest

from arrlio import task, utils


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

    @task
    def foo():
        pass

    assert json.dumps(foo, cls=utils.ExtendedJSONEncoder) == (
        """{\"func\": \"test_utils.foo\", \"name\": \"test_utils.foo\", \"bind\": false, """
        """\"queue\": \"arrlio.tasks\", \"priority\": 1, \"timeout\": 300, \"ttl\": 300, \"encrypt\": null, """
        """\"ack_late\": false, \"result_ttl\": 300, \"result_return\": true, \"result_encrypt\": null, """
        """\"thread\": null, \"events\": false, \"event_ttl\": 300}"""
    )

    with pytest.raises(TypeError):
        json.dumps(C(), cls=utils.ExtendedJSONEncoder)


@pytest.mark.asyncio
async def test_retry():
    async def foo(a, b=None):
        pass

    await utils.retry()(foo)(1)

    counter = 0

    async def bar():
        nonlocal counter
        counter += 1
        1 / 0

    with pytest.raises(ZeroDivisionError):
        await utils.retry()(bar)()
    assert counter == 1

    counter = 0
    with pytest.raises(ZeroDivisionError):
        await utils.retry(retry_timeouts=[0, 0], exc_filter=lambda e: isinstance(e, ZeroDivisionError))(bar)()
    assert counter == 3
