import asyncio

from unittest import mock

import pytest

from arrlio import serializers
from arrlio.backends.result_backends import rabbitmq


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
        assert config.result_ttl == rabbitmq.RESULT_TTL
        assert config.reply_to_mode == rabbitmq.REPLY_TO_MODE
        assert config.queue_prefix == rabbitmq.QUEUE_PREFIX
        assert config.queue_durable == rabbitmq.QUEUE_DURABLE
        assert config.queue_type == rabbitmq.QUEUE_TYPE
        assert config.prefetch_count == rabbitmq.PREFETCH_COUNT

    def test__init_custom(self, cleanup):
        config = rabbitmq.Config(
            id="id",
            url="amqps://admin@example.com",
            timeout=123,
            push_retry_timeouts=[2],
            pull_retry_timeout=3,
            result_ttl=10,
            reply_to_mode="direct_reply_to",
            queue_prefix="results.",
            queue_durable=True,
            queue_type="quorum",
        )
        assert config.serializer.module == serializers.json
        assert config.id == "id"
        assert config.url.get_secret_value() == "amqps://admin@example.com"
        assert config.timeout == 123
        assert next(iter(config.push_retry_timeouts)) == 2
        assert config.pull_retry_timeout == 3
        assert config.result_ttl == 10
        assert config.reply_to_mode == "direct_reply_to"
        assert config.queue_prefix == "results."
        assert config.queue_durable is True
        assert config.queue_type == "quorum"


class TestResultBackend:
    @pytest.mark.asyncio
    async def test__init(self, mock_connect, cleanup):
        result_backend = rabbitmq.ResultBackend(rabbitmq.Config())
        try:
            assert isinstance(result_backend.serializer, serializers.json.Serializer)
            assert result_backend._conn is not None
        finally:
            await result_backend.close()

    def test__init_custom(self):
        result_backend = rabbitmq.ResultBackend(
            rabbitmq.Config(
                serializer=rabbitmq.SerializerConfig(
                    module=serializers.msgpack,
                    config=serializers.msgpack.Config(),
                )
            )
        )
        assert isinstance(result_backend.serializer, serializers.msgpack.Serializer)

    @pytest.mark.asyncio
    async def test_str(self, mock_connect, cleanup):
        result_backend = rabbitmq.ResultBackend(rabbitmq.Config(id="abc"))
        try:
            assert str(result_backend).startswith("ResultBackend[rabbitmq#abc][Connection[localhost]")
        finally:
            await result_backend.close()

    @pytest.mark.asyncio
    async def test_repr(self, mock_connect, cleanup):
        result_backend = rabbitmq.ResultBackend(rabbitmq.Config(id="abc"))
        try:
            assert repr(result_backend).startswith("ResultBackend[rabbitmq#abc][Connection[localhost]")
        finally:
            await result_backend.close()
