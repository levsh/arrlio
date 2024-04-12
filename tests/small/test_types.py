import pydantic
import pydantic_settings
import pytest

from arrlio import backends
from arrlio.types import BackendModule, SecretAmqpDsn, SecretAnyUrl


def test_SecretAnyUrl():
    url = SecretAnyUrl("http://example.org")
    assert url.scheme == "http"
    assert url.username is None
    assert url.password is None
    assert url.host == "example.org"
    assert str(url) == "http://example.org/"
    assert repr(url) == "SecretAnyUrl('http://example.org/')"

    url = SecretAnyUrl("http://user:pass@example.org")
    assert url.scheme == "http"
    assert url.username == pydantic.SecretStr("user")
    assert url.password == pydantic.SecretStr("pass")
    assert url.host == "example.org"
    assert str(url) == "http://***:***@example.org/"
    assert repr(url) == "SecretAnyUrl('http://***:***@example.org/')"

    class S(pydantic_settings.BaseSettings):
        url: SecretAnyUrl

    for T in [S]:
        for url in [
            "http://user:pass@example.org",
            SecretAnyUrl("http://user:pass@example.org"),
            pydantic.AnyUrl("http://user:pass@example.org"),
        ]:
            m = T(url=url)
            assert m.url.scheme == "http"
            assert m.url.username == pydantic.SecretStr("user")
            assert m.url.password == pydantic.SecretStr("pass")
            assert m.url.host == "example.org"
            assert str(m.url) == "http://***:***@example.org/"
            assert repr(m.url) == "SecretAnyUrl('http://***:***@example.org/')"


def test_SecretAmqpDsn():
    url = SecretAmqpDsn("amqp://example.org")
    assert url.scheme == "amqp"
    assert url.username is None
    assert url.password is None
    assert url.host == "example.org"
    assert str(url) == "amqp://example.org"
    assert repr(url) == "SecretAnyUrl('amqp://example.org')"

    url = SecretAnyUrl("amqp://user:pass@example.org")
    assert url.scheme == "amqp"
    assert url.username == pydantic.SecretStr("user")
    assert url.password == pydantic.SecretStr("pass")
    assert url.host == "example.org"
    assert str(url) == "amqp://***:***@example.org"
    assert repr(url) == "SecretAnyUrl('amqp://***:***@example.org')"

    class S(pydantic_settings.BaseSettings):
        url: SecretAmqpDsn

    for T in [S]:
        for url in [
            "amqp://user:pass@example.org",
            SecretAnyUrl("amqp://user:pass@example.org"),
            pydantic.AnyUrl("amqp://user:pass@example.org"),
        ]:
            m = T(url=url)
            assert m.url.scheme == "amqp"
            assert m.url.username == pydantic.SecretStr("user")
            assert m.url.password == pydantic.SecretStr("pass")
            assert m.url.host == "example.org"
            assert str(m.url) == "amqp://***:***@example.org"
            assert repr(m.url) == "SecretAnyUrl('amqp://***:***@example.org')"


def test_BackendModule():
    class M(pydantic_settings.BaseSettings):
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
