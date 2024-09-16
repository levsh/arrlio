import pydantic
import pydantic_settings
import pytest

from arrlio.backends import brokers, event_backends, result_backends
from arrlio.types import BrokerModule, EventBackendModule, ResultBackendModule, SecretAmqpDsn, SecretAnyUrl


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


def test_BrokerModule():
    class M(pydantic_settings.BaseSettings):
        broker: BrokerModule

    m = M(broker="arrlio.backends.brokers.local")
    assert m.broker == brokers.local

    m = M(broker="arrlio.backends.brokers.rabbitmq")
    assert m.broker == brokers.rabbitmq

    m = M(broker=brokers.local)
    assert m.broker == brokers.local

    m = M(broker=brokers.rabbitmq)
    assert m.broker == brokers.rabbitmq

    with pytest.raises(ValueError):
        m = M(broker="abc")


def test_ResultBackendModule():
    class M(pydantic_settings.BaseSettings):
        result_backend: ResultBackendModule

    m = M(result_backend="arrlio.backends.result_backends.local")
    assert m.result_backend == result_backends.local

    m = M(result_backend="arrlio.backends.result_backends.rabbitmq")
    assert m.result_backend == result_backends.rabbitmq

    m = M(result_backend=result_backends.local)
    assert m.result_backend == result_backends.local

    m = M(result_backend=result_backends.rabbitmq)
    assert m.result_backend == result_backends.rabbitmq

    with pytest.raises(ValueError):
        m = M(result_backend="abc")


def test_EventBackendModule():
    class M(pydantic_settings.BaseSettings):
        event_backend: EventBackendModule

    m = M(event_backend="arrlio.backends.event_backends.local")
    assert m.event_backend == event_backends.local

    m = M(event_backend="arrlio.backends.event_backends.rabbitmq")
    assert m.event_backend == event_backends.rabbitmq

    m = M(event_backend=event_backends.local)
    assert m.event_backend == event_backends.local

    m = M(event_backend=event_backends.rabbitmq)
    assert m.event_backend == event_backends.rabbitmq

    with pytest.raises(ValueError):
        m = M(event_backend="abc")
