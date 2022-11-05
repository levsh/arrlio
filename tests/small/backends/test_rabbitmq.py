import asyncio
from unittest import mock

import pytest

from arrlio.backends import rabbitmq
from arrlio.serializers.json import Serializer


class TestBackendConfig:
    def test_init(self, cleanup):
        config = rabbitmq.BackendConfig()
        assert config.name == rabbitmq.BACKEND_NAME
        assert config.serializer == Serializer
        assert config.url.get_secret_value() == rabbitmq.URL
        assert config.timeout == rabbitmq.TIMEOUT
        assert config.retry_timeouts == rabbitmq.RETRY_TIMEOUTS
        assert config.verify_ssl is True
        assert config.tasks_exchange == rabbitmq.TASKS_EXCHANGE
        assert config.tasks_queue_type == rabbitmq.TASKS_QUEUE_TYPE
        assert config.tasks_queue_durable == rabbitmq.TASKS_QUEUE_DURABLE
        assert config.tasks_queue_ttl == rabbitmq.TASKS_QUEUE_TTL
        assert config.tasks_prefetch_count == rabbitmq.TASKS_PREFETCH_COUNT
        assert config.events_exchange == rabbitmq.EVENTS_EXCHANGE
        assert config.events_queue_type == rabbitmq.EVENTS_QUEUE_TYPE
        assert config.events_queue_durable == rabbitmq.EVENTS_QUEUE_DURABLE
        assert config.events_queue == rabbitmq.EVENTS_QUEUE
        assert config.events_queue_ttl == rabbitmq.EVENTS_QUEUE_TTL
        assert config.events_prefetch_count == rabbitmq.EVENTS_PREFETCH_COUNT
        assert config.messages_prefetch_count == rabbitmq.MESSAGES_PREFETCH_COUNT

    def test_init_custom(self, cleanup):
        def serializer_factory():
            return Serializer()

        config = rabbitmq.BackendConfig(
            serializer=serializer_factory,
            name="Custom Name",
            url="amqps://admin@example.com",
            timeout=123,
            retry_timeouts=[1, 2, 3],
            verify_ssl=False,
            tasks_exchange="Tasks custom Exchange",
            tasks_queue_type="quorum",
            tasks_queue_durable=False,
            tasks_queue_ttl=321,
            tasks_prefetch_count=10,
            events_exchange="Events custom Exchange",
            events_queue_type="quorum",
            events_queue_durable=False,
            events_queue="Events queue",
            events_queue_ttl=333,
            events_prefetch_count=20,
            messages_prefetch_count=30,
        )
        assert config.serializer == serializer_factory
        assert config.name == "Custom Name"
        assert config.url.get_secret_value() == "amqps://admin@example.com"
        assert config.timeout == 123
        assert config.retry_timeouts == [1, 2, 3]
        assert config.verify_ssl is False
        assert config.tasks_exchange == "Tasks custom Exchange"
        assert config.tasks_queue_type == "quorum"
        assert config.tasks_queue_durable is False
        assert config.tasks_queue_ttl == 321
        assert config.tasks_prefetch_count == 10
        assert config.events_exchange == "Events custom Exchange"
        assert config.events_queue_type == "quorum"
        assert config.events_queue_durable is False
        assert config.events_queue == "Events queue"
        assert config.events_queue_ttl == 333
        assert config.events_prefetch_count == 20
        assert config.messages_prefetch_count == 30


class TestRMQConnection:
    def test_init(self, cleanup):
        conn = rabbitmq.RMQConnection("amqp://admin@example.com")
        assert isinstance(conn.url, rabbitmq.AmqpDsn)
        assert conn.url.get_secret_value() == "amqp://admin@example.com"
        assert conn._retry_timeouts is None
        assert conn._exc_filter is None
        assert conn._key is not None
        assert conn._key in conn._RMQConnection__shared
        assert conn._shared["id"] == 1
        assert conn._shared["objs"] == 1
        assert conn in conn._shared
        assert conn._shared[conn] == {
            "on_open": {},
            "on_lost": {},
            "on_close": {},
        }
        assert conn._id == 1
        assert conn._supervisor_task is None
        assert conn._closed is None
        assert conn._conn is None
        assert str(conn) == "RMQConnection#1[example.com:None]"
        assert repr(conn) == "RMQConnection#1[example.com:None]"

    @pytest.mark.asyncio
    async def test_connect(self, cleanup):
        conn = rabbitmq.RMQConnection("amqp://admin@example.com")
        try:
            with mock.patch("aiormq.connect") as mock_connect:
                mock_conn = mock.AsyncMock()
                mock_conn.is_closed = False
                mock_conn.closing = asyncio.Future()
                mock_connect.return_value = mock_conn
                await conn.open()
                mock_connect.assert_awaited_once_with("amqp://admin@example.com")
                assert conn._supervisor_task is not None
                assert conn.is_open is True
                assert conn.is_closed is False
                assert conn._conn is not None
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_channel(self, cleanup):
        conn = rabbitmq.RMQConnection("amqp://admin@example.com")
        try:
            with mock.patch("aiormq.connect") as mock_connect:
                mock_conn = mock.AsyncMock()
                mock_conn.is_closed = False
                mock_conn.closing = asyncio.Future()
                mock_connect.return_value = mock_conn
                await conn.open()
                async with conn.channel_ctx():
                    pass
        finally:
            await conn.close()


class TestBackend:
    @pytest.mark.asyncio
    async def test_init(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.BackendConfig())
        try:
            assert isinstance(backend.serializer, Serializer)
            assert backend._task_consumers == {}
            assert backend._message_consumers == {}
            assert backend._events_consumer == []
            assert backend._Backend__conn is not None
        finally:
            await backend.close()

    # def test_init_custom(self):
    #     backend = local.Backend(local.BackendConfig(serializer=lambda: nop.Serializer()))
    #     assert isinstance(backend.serializer, nop.Serializer)

    @pytest.mark.asyncio
    async def test_str(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.BackendConfig())
        try:
            assert str(backend) == "RMQBackend[RMQConnection#1[localhost:None]]"
        finally:
            await backend.close()

    @pytest.mark.asyncio
    async def test_repr(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.BackendConfig())
        try:
            assert repr(backend) == "RMQBackend[RMQConnection#1[localhost:None]]"
        finally:
            await backend.close()
