import types

import pydantic
import pytest

from arrlio import tp


def test_SecretAnyUrl():
    class M(pydantic.BaseModel):
        url: tp.SecretAnyUrl

    m = M(url="http://user:pass@localhost")
    assert str(m.url) == "http://***:***@localhost"
    assert repr(m.url) == "SecretAnyUrl('http://***:***@localhost')"
    assert m.url.get_secret_value() == "http://user:pass@localhost"
    assert m.url.user == "user"
    assert m.url.password == "pass"

    assert M(url="http://user:pass@localhost") == M(url="http://user:pass@localhost")

    assert hash(m.url) == hash(m.url.get_secret_value())


def test_RedisDsn():
    class M(pydantic.BaseModel):
        url: tp.RedisDsn

    M(url="redis://localhost")
    M(url="rediss://localhost")


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
