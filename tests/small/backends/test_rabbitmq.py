import asyncio
from unittest import mock

import pytest

from arrlio import serializers
from arrlio.backends import rabbitmq


class TestConfig:
    def test__init(self, cleanup):
        config = rabbitmq.Config()
        assert config.id
        assert config.serializer.module == serializers.json
        assert config.url.get_secret_value() == rabbitmq.URL
        assert config.timeout == rabbitmq.TIMEOUT
        assert config.verify_ssl is True
        assert config.pool_size == rabbitmq.POOL_SIZE
        assert config.conn_retry_timeouts is None
        assert config.push_retry_timeouts
        assert config.pull_retry_timeouts
        assert config.tasks_exchange == rabbitmq.TASKS_EXCHANGE
        assert config.tasks_exchange_durable == rabbitmq.TASKS_EXCHANGE_DURABLE
        assert config.tasks_queue_type == rabbitmq.TASKS_QUEUE_TYPE
        assert config.tasks_queue_durable == rabbitmq.TASKS_QUEUE_DURABLE
        assert config.tasks_queue_ttl == rabbitmq.TASKS_QUEUE_TTL
        assert config.tasks_prefetch_count == rabbitmq.TASKS_PREFETCH_COUNT
        assert config.events_exchange == rabbitmq.EVENTS_EXCHANGE
        assert config.events_exchange_durable == rabbitmq.EVENTS_EXCHANGE_DURABLE
        assert config.events_queue_type == rabbitmq.EVENTS_QUEUE_TYPE
        assert config.events_queue_durable == rabbitmq.EVENTS_QUEUE_DURABLE
        assert config.events_queue_prefix == rabbitmq.EVENTS_QUEUE_PREFIX
        assert config.events_queue_ttl == rabbitmq.EVENTS_QUEUE_TTL
        assert config.events_prefetch_count == rabbitmq.EVENTS_PREFETCH_COUNT
        assert config.messages_prefetch_count == rabbitmq.MESSAGES_PREFETCH_COUNT
        assert config.results_queue_mode == rabbitmq.RESULTS_QUEUE_MODE
        assert config.results_queue_prefix == rabbitmq.RESULTS_QUEUE_PREFIX
        assert config.results_shared_queue_durable == rabbitmq.RESULTS_SHARED_QUEUE_DURABLE
        assert config.results_shared_queue_ttl == rabbitmq.RESULTS_SHARED_QUEUE_TTL
        assert config.results_shared_queue_type == rabbitmq.RESULTS_SHARED_QUEUE_TYPE
        assert config.results_single_queue_durable == rabbitmq.RESULTS_SINGLE_QUEUE_DURABLE
        assert config.results_single_queue_ttl == rabbitmq.RESULTS_SINGLE_QUEUE_TTL
        assert config.results_single_queue_type == rabbitmq.RESULTS_SINGLE_QUEUE_TYPE

    def test__init_custom(self, cleanup):
        config = rabbitmq.Config(
            id="id",
            url="amqps://admin@example.com",
            timeout=123,
            verify_ssl=False,
            pool_size=50,
            conn_retry_timeouts=[1],
            push_retry_timeouts=[2],
            pull_retry_timeouts=[3],
            tasks_exchange="tasks_exchange",
            tasks_exchange_durable=True,
            tasks_queue_type="quorum",
            tasks_queue_durable=False,
            tasks_queue_ttl=321,
            tasks_prefetch_count=10,
            events_exchange="events_exchange",
            events_exchange_durable=True,
            events_queue_type="quorum",
            events_queue_durable=False,
            events_queue_prefix="events_queue_prefix",
            events_queue_ttl=333,
            events_prefetch_count=20,
            messages_prefetch_count=30,
            results_queue_mode="shared",
            results_queue_prefix="results.",
            results_shared_queue_durable=True,
            results_shared_queue_ttl=10,
            results_shared_queue_type="quorum",
        )
        assert config.serializer.module == serializers.json
        assert config.id == "id"
        assert config.url.get_secret_value() == "amqps://admin@example.com"
        assert config.timeout == 123
        assert config.verify_ssl is False
        assert config.pool_size == 50
        assert next(iter(config.conn_retry_timeouts)) == 1
        assert next(iter(config.push_retry_timeouts)) == 2
        assert next(iter(config.pull_retry_timeouts)) == 3
        assert config.tasks_exchange == "tasks_exchange"
        assert config.tasks_exchange_durable is True
        assert config.tasks_queue_type == "quorum"
        assert config.tasks_queue_durable is False
        assert config.tasks_queue_ttl == 321
        assert config.tasks_prefetch_count == 10
        assert config.events_exchange == "events_exchange"
        assert config.events_exchange_durable is True
        assert config.events_queue_type == "quorum"
        assert config.events_queue_durable is False
        assert config.events_queue_prefix == "events_queue_prefix"
        assert config.events_queue_ttl == 333
        assert config.events_prefetch_count == 20
        assert config.messages_prefetch_count == 30
        assert config.results_queue_mode == "shared"
        assert config.results_queue_prefix == "results."
        assert config.results_shared_queue_durable is True
        assert config.results_shared_queue_ttl == 10
        assert config.results_shared_queue_type == "quorum"


class TestRMQConnection:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        conn = rabbitmq.RMQConnection("amqp://admin@example.com")
        assert isinstance(conn.url, rabbitmq.AmqpDsn)
        assert conn.url.get_secret_value() == "amqp://admin@example.com"
        assert conn._retry_timeouts is None
        assert conn._exc_filter is not None
        assert conn._RMQConnection__key is not None
        assert conn._RMQConnection__key in conn._RMQConnection__shared
        assert conn._shared["id"] == 1
        assert conn._shared["objs"] == 1
        assert conn in conn._shared
        assert conn._shared[conn] == {
            "on_open_order": {},
            "on_open": {},
            "on_lost": {},
            "on_lost_order": {},
            "on_close": {},
            "on_close_order": {},
            "callback_tasks": set(),
        }
        assert conn._id == 1
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
                await conn.channel()
        finally:
            await conn.close()


class TestBackend:
    @pytest.mark.asyncio
    async def test__init(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            assert isinstance(backend.serializer, serializers.json.Serializer)
            assert backend._task_consumers == {}
            assert backend._message_consumers == {}
            assert backend._events_consumer == []
            assert backend._Backend__conn is not None
        finally:
            await backend.close()

    # def test__init_custom(self):
    #     backend = local.Backend(local.Config(serializer=lambda: nop.Serializer()))
    #     assert isinstance(backend.serializer, nop.Serializer)

    @pytest.mark.asyncio
    async def test_str(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            assert str(backend) == "RMQBackend[RMQConnection#1[localhost:None]]"
        finally:
            await backend.close()

    @pytest.mark.asyncio
    async def test_repr(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            assert repr(backend) == "RMQBackend[RMQConnection#1[localhost:None]]"
        finally:
            await backend.close()
