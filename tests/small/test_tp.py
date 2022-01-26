import types

import pydantic
import pytest

from arrlio import tp


def test_BackendType():

    class M(pydantic.BaseModel):
        backend: tp.BackendT

    m = M(backend="arrlio.backends.local")
    assert isinstance(m.backend, types.ModuleType)

    from arrlio.backends import rabbitmq
    m = M(backend=rabbitmq)
    assert isinstance(m.backend, types.ModuleType)

    with pytest.raises(ValueError):
        m = M(backend=0)
