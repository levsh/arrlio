import asyncio

from unittest import mock

import pytest

from arrlio import serializers
from arrlio.backends.event_backends import rabbitmq


@pytest.fixture
def mock_connect():
    with mock.patch("aiormq.connect") as m:
        mock_conn = mock.AsyncMock()
        mock_conn.is_closed = False
        mock_conn.closing = asyncio.Future()
        m.return_value = mock_conn
        yield m


class TestConfig:
    def test__init(self, cleanup):
        config = rabbitmq.Config()
        assert config.id
        assert config.serializer.module == serializers.json
        assert config.url.get_secret_value() == rabbitmq.URL
        assert config.timeout == rabbitmq.TIMEOUT
        assert config.push_retry_timeouts
        assert config.pull_retry_timeout
        assert config.exchange == rabbitmq.EXCHANGE
        assert config.exchange_durable == rabbitmq.EXCHANGE_DURABLE
        assert config.queue_type == rabbitmq.QUEUE_TYPE
        assert config.queue_durable == rabbitmq.QUEUE_DURABLE
        assert config.queue_exclusive == rabbitmq.QUEUE_EXCLUSIVE
        assert config.queue_auto_delete == rabbitmq.QUEUE_AUTO_DELETE
        assert config.queue == rabbitmq.QUEUE
        assert config.prefetch_count == rabbitmq.PREFETCH_COUNT

    def test__init_custom(self, cleanup):
        config = rabbitmq.Config(
            id="id",
            url="amqps://admin@example.com",
            timeout=123,
            push_retry_timeouts=[2],
            pull_retry_timeout=3,
            exchange="events_exchange",
            exchange_durable=True,
            queue_type="quorum",
            queue_durable=False,
            queue_exclusive=True,
            queue_auto_delete=False,
            queue="events_queue",
            prefetch_count=20,
        )
        assert config.serializer.module == serializers.json
        assert config.id == "id"
        assert config.url.get_secret_value() == "amqps://admin@example.com"
        assert config.timeout == 123
        assert next(iter(config.push_retry_timeouts)) == 2
        assert config.pull_retry_timeout == 3
        assert config.exchange == "events_exchange"
        assert config.exchange_durable is True
        assert config.queue_type == "quorum"
        assert config.queue_durable is False
        assert config.queue_exclusive is True
        assert config.queue_auto_delete is False
        assert config.queue == "events_queue"
        assert config.prefetch_count == 20


class TestEventBackend:
    @pytest.mark.asyncio
    async def test__init(self, mock_connect, cleanup):
        event_backend = rabbitmq.EventBackend(rabbitmq.Config())
        try:
            assert isinstance(event_backend.serializer, serializers.json.Serializer)
            assert event_backend._conn is not None
        finally:
            await event_backend.close()

    @pytest.mark.asyncio
    async def test_str(self, mock_connect, cleanup):
        event_backend = rabbitmq.EventBackend(rabbitmq.Config(id="abc"))
        try:
            assert str(event_backend).startswith("EventBackend[rabbitmq#abc][Connection[localhost]")
        finally:
            await event_backend.close()

    @pytest.mark.asyncio
    async def test_repr(self, mock_connect, cleanup):
        event_backend = rabbitmq.EventBackend(rabbitmq.Config(id="abc"))
        try:
            assert repr(event_backend).startswith("EventBackend[rabbitmq#abc][Connection[localhost]")
        finally:
            await event_backend.close()
