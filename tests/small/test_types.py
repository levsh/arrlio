import pydantic
import pytest

from arrlio import backends
from arrlio.types import BackendModule, SecretAmqpDsn, SecretAnyUrl


def test_SecretAnyUrl():
    class M(pydantic.BaseModel):
        url: SecretAnyUrl

    m = M(url="http://user:pass@localhost")
    assert str(m.url) == "http://***:***@localhost"
    assert repr(m.url) == "SecretAnyUrl('http://***:***@localhost')"
    assert m.url.get_secret_value() == "http://user:pass@localhost"
    assert m.url.user == "user"
    assert m.url.password == "pass"

    assert M(url="http://user:pass@localhost") == M(url="http://user:pass@localhost")

    assert hash(m.url) == hash(m.url.get_secret_value())


def test_SecretAmqpDsn():
    class M(pydantic.BaseModel):
        url: SecretAmqpDsn

    M(url="amqp://guest:guest@localhost")

    with pytest.raises(ValueError):
        M(url="amqp://localhost")

    with pytest.raises(ValueError):
        M(url="http://localhost")

    assert str(M(url="amqp://guest:guest@localhost")) == "url=SecretAmqpDsn('amqp://***:***@localhost')"


def test_BackendModule():
    class M(pydantic.BaseModel):
        backend: BackendModule

    m = M(backend="arrlio.backends.local")
    assert m.backend == backends.local

    m = M(backend="arrlio.backends.rabbitmq")
    assert m.backend == backends.rabbitmq

    m = M(backend=backends.local)
    assert m.backend == backends.local

    m = M(backend=backends.rabbitmq)
    assert m.backend == backends.rabbitmq

    with pytest.raises(ValueError):
        m = M(backend="abc")
