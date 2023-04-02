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
        assert config.results_common_queue_durable == rabbitmq.RESULTS_COMMON_QUEUE_DURABLE
        assert config.results_common_queue_ttl == rabbitmq.RESULTS_COMMON_QUEUE_TTL
        assert config.results_common_queue_type == rabbitmq.RESULTS_COMMON_QUEUE_TYPE
        assert config.results_single_use_queue_durable == rabbitmq.RESULTS_SINGLE_USE_QUEUE_DURABLE
        assert config.results_single_use_queue_type == rabbitmq.RESULTS_SINGLE_USE_QUEUE_TYPE

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
            results_queue_mode="common",
            results_queue_prefix="results.",
            results_common_queue_durable=True,
            results_common_queue_ttl=10,
            results_common_queue_type="quorum",
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
        assert config.results_queue_mode == "common"
        assert config.results_queue_prefix == "results."
        assert config.results_common_queue_durable is True
        assert config.results_common_queue_ttl == 10
        assert config.results_common_queue_type == "quorum"


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
            "on_open_ordered": {},
            "on_open": {},
            "on_lost": {},
            "on_lost_ordered": {},
            "on_close": {},
            "on_close_ordered": {},
            "callback_tasks": set(),
        }
        assert conn._id == 1
        assert conn._conn is None
        assert str(conn) == "RMQConnection#1[example.com]"
        assert repr(conn) == "RMQConnection#1[example.com]"

    @pytest.mark.asyncio
    async def test__init_custom(self, cleanup):
        conn = rabbitmq.RMQConnection(["amqp://admin@rabbitmq1.com", "amqp://admin@rabbitmq2.com"])
        try:
            assert isinstance(conn.url, rabbitmq.AmqpDsn)
            assert conn.url.get_secret_value() == "amqp://admin@rabbitmq1.com"
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_callbacks(self, cleanup):
        on_open_cb_flag = False
        on_close_cb_flag = False

        def on_open_cb():
            nonlocal on_open_cb_flag
            on_open_cb_flag = not on_open_cb_flag

        def on_close_cb():
            nonlocal on_close_cb_flag
            on_close_cb_flag = not on_close_cb_flag

        def cb_with_exception():
            raise Exception

        conn = rabbitmq.RMQConnection("amqp://admin@example.com")
        try:
            conn.set_callback("on_open", "test", on_open_cb)
            assert conn._shared[conn]["on_open"]["test"] == (on_open_cb, None)
            conn.set_callback("on_close", "test", on_close_cb)
            assert conn._shared[conn]["on_close"]["test"] == (on_close_cb, None)

            await conn._execute_callbacks("on_open")
            assert on_open_cb_flag is True
            assert on_close_cb_flag is False

            conn.remove_callback("on_open", "test")
            assert conn._shared[conn]["on_open"] == {}
            assert conn._shared[conn]["on_close"]["test"] == (on_close_cb, None)

            await conn._execute_callbacks("on_close")
            assert on_open_cb_flag is True
            assert on_close_cb_flag is True

            conn.remove_callbacks(cancel=True)
            assert conn._shared[conn]["on_open"] == {}
            assert conn._shared[conn]["on_close"] == {}
            assert conn._shared[conn]["callback_tasks"] == set()

            conn.set_callback("on_open", "excpetion", cb_with_exception)
            await conn._execute_callbacks("on_open")
        finally:
            await conn.close()

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

    @pytest.mark.asyncio
    async def test_str(self, cleanup):
        conn = rabbitmq.RMQConnection("amqp://admin@example.com")
        try:
            assert str(conn) == "RMQConnection#1[example.com]"
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_repr(self, cleanup):
        conn = rabbitmq.RMQConnection("amqp://admin@example.com")
        try:
            assert repr(conn) == "RMQConnection#1[example.com]"
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
            assert backend._events_consumer == ()
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
            assert str(backend) == "RMQBackend[RMQConnection#1[localhost]]"
        finally:
            await backend.close()

    @pytest.mark.asyncio
    async def test_repr(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            assert repr(backend) == "RMQBackend[RMQConnection#1[localhost]]"
        finally:
            await backend.close()

    @pytest.mark.asyncio
    async def test__declare(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            with mock.patch.object(backend._Backend__conn, "channel"):
                await backend._declare()
        finally:
            await backend.close()

    @pytest.mark.asyncio
    async def test__consume_results(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            mock_channel = mock.AsyncMock()
            mock_channel.is_closed = False
            backend._conn_open_ev.set()
            with mock.patch.object(backend._Backend__conn, "new_channel", return_value=mock_channel):
                await backend._consume_results()
                assert backend._common_results_consumer
                assert backend._direct_reply_to_consumer
                assert backend._direct_reply_to_channel
                await backend._stop_consume_results()
                assert backend._common_results_consumer == ()
                assert backend._direct_reply_to_consumer == ()
                assert backend._direct_reply_to_channel is None
        finally:
            await backend.close()

    @pytest.mark.asyncio
    async def test_on_connection_open(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            mock_channel = mock.AsyncMock()
            mock_channel.is_closed = False
            with mock.patch.object(backend._Backend__conn, "channel"), mock.patch.object(
                backend._Backend__conn, "new_channel", return_value=mock_channel
            ):
                await backend._on_connection_open()
                assert backend._conn_open_ev.is_set()
        finally:
            await backend.close()

    @pytest.mark.asyncio
    async def test_on_connection_lost(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            mock_channel = mock.AsyncMock()
            mock_channel.is_closed = True
            backend._conn_open_ev.set()
            with mock.patch.object(backend._Backend__conn, "open"), mock.patch.object(
                backend._Backend__conn, "channel"
            ), mock.patch.object(backend._Backend__conn, "new_channel", return_value=mock_channel):
                await backend._on_connection_lost()
                assert not backend._conn_open_ev.is_set()
        finally:
            await backend.close()

    @pytest.mark.asyncio
    async def test_on_connection_close(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            await backend._on_connection_close()
            assert not backend._conn_open_ev.is_set()
            assert not backend._declared
            assert not backend._task_consumers
            assert not backend._message_consumers
            assert not backend._events_consumer
        finally:
            await backend.close()

    @pytest.mark.asyncio
    async def test__declare_events(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            mock_channel = mock.AsyncMock()
            mock_channel.is_closed = False
            backend._conn_open_ev.set()
            with mock.patch.object(backend._Backend__conn, "channel", return_value=mock_channel):
                await backend._declare_events()
        finally:
            await backend.close()

    @pytest.mark.asyncio
    async def test__declare_tasks_queue(self, cleanup):
        backend = rabbitmq.Backend(rabbitmq.Config())
        try:
            mock_channel = mock.AsyncMock()
            mock_channel.is_closed = False
            backend._conn_open_ev.set()
            with mock.patch.object(backend._Backend__conn, "channel", return_value=mock_channel):
                await backend._declare_tasks_queue("test_queue")
                assert "test_queue" in backend._declared
        finally:
            await backend.close()
