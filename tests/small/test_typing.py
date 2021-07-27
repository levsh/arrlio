import types

import pydantic
import pytest

from arrlio import typing


def test_BackendType():

    class M(pydantic.BaseModel):
        backend: typing.BackendT

    m = M(backend="arrlio.backend.local")
    assert isinstance(m.backend, types.ModuleType)

    from arrlio.backend import rabbitmq
    m = M(backend=rabbitmq)
    assert isinstance(m.backend, types.ModuleType)

    with pytest.raises(ValueError):
        m = M(backend=0)
