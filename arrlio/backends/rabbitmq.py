import asyncio
import itertools
import logging
import ssl
from asyncio import FIRST_COMPLETED, create_task, get_event_loop, wait
from collections.abc import Hashable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from functools import partial
from inspect import isasyncgenfunction, iscoroutine, iscoroutinefunction, isgeneratorfunction
from ssl import SSLContext
from typing import Any, AsyncGenerator, Callable, Coroutine, Optional
from uuid import UUID

import aiormq
import aiormq.exceptions
import yarl
from pydantic import Field, PositiveInt
from pydantic_settings import SettingsConfigDict

from arrlio import settings
from arrlio.backends import base
from arrlio.exceptions import TaskClosedError, TaskResultError
from arrlio.models import Event, TaskInstance, TaskResult
from arrlio.settings import ENV_PREFIX
from arrlio.types import TASK_MAX_PRIORITY, SecretAmqpDsn, SerializerModule, Timeout
from arrlio.utils import LoopIter, is_debug_level, is_info_level, retry

logger = logging.getLogger("arrlio.backends.rabbitmq")

BasicProperties = aiormq.spec.Basic.Properties


class QueueType(StrEnum):
    CLASSIC = "classic"
    QUORUM = "quorum"


class ResultQueueMode(StrEnum):
    DIRECT_REPLY_TO = "direct_reply_to"
    COMMON = "common"


SERIALIZER = "arrlio.serializers.json"

URL = "amqp://guest:guest@localhost"
VERIFY_SSL = True
TIMEOUT = 15
CONNECT_TIMEOUT = 15

PUSH_RETRY_TIMEOUTS = [5, 5, 5, 5]  # pylint: disable=invalid-name
PULL_RETRY_TIMEOUTS = itertools.repeat(5)  # pylint: disable=invalid-name

TASKS_EXCHANGE = "arrlio"
TASKS_EXCHANGE_DURABLE = False
TASKS_QUEUE_TYPE = QueueType.CLASSIC
TASKS_QUEUE_DURABLE = False
TASKS_QUEUE_AUTO_DELETE = True
TASKS_TTL = 600
TASKS_PREFETCH_COUNT = 1

EVENTS_EXCHANGE = "arrlio.events"
EVENTS_EXCHANGE_DURABLE = False
EVENTS_QUEUE_TYPE = QueueType.CLASSIC
EVENTS_QUEUE_DURABLE = False
EVENTS_QUEUE_AUTO_DELETE = True
EVENTS_QUEUE = "arrlio.events"
EVENTS_TTL = 600
EVENTS_PREFETCH_COUNT = 10

RESULTS_QUEUE_MODE = ResultQueueMode.COMMON
RESULTS_QUEUE_PREFIX = "arrlio."
RESULTS_QUEUE_AUTO_DELETE = True
RESULTS_QUEUE_DURABLE = False
RESULTS_QUEUE_TYPE = QueueType.CLASSIC
RESULTS_TTL = 600
RESULTS_PREFETCH_COUNT = 10


class Connection:
    """RabbitMQ connection."""

    __shared: dict = {}

    def __init__(self, urls: list[str], ssl_context: SSLContext | None = None):
        self._urls = urls
        self._urls_iter = LoopIter(self._urls)

        self.url = next(self._urls_iter)

        self._ssl_context = ssl_context

        self._open_task: asyncio.Task | asyncio.Future = asyncio.Future()
        self._open_task.set_result(None)

        self._closed: asyncio.Future = asyncio.Future()
        self._watcher_task: asyncio.Task | None = None

        self._key: tuple = (get_event_loop(), tuple(sorted(self._urls)))

        if self._key not in self.__shared:
            self.__shared[self._key] = {
                "refs": 0,
                "objs": 0,
                "conn": None,
                "connect_lock": asyncio.Lock(),
            }

        shared: dict = self.__shared[self._key]
        shared["objs"] += 1
        shared[self] = {
            "on_open": {},
            "on_lost": {},
            "on_close": {},
            "callback_tasks": {"on_open": {}, "on_lost": {}, "on_close": {}},
        }
        self._shared = shared

        self._channel: aiormq.Channel = None

    def __del__(self):
        if self._key:
            if self._conn and not self.is_closed:
                logger.warning("%s unclosed", self)
            shared = self._shared
            shared["objs"] -= 1
            if self in shared:
                shared.pop(self, None)
            if shared["objs"] == 0:
                self.__shared.pop(self._key, None)

    @property
    def _conn(self) -> aiormq.Connection:
        return self._shared["conn"]

    @_conn.setter
    def _conn(self, value: aiormq.Connection):
        self._shared["conn"] = value

    @property
    def _refs(self) -> int:
        return self._shared["refs"]

    @_refs.setter
    def _refs(self, value: int):
        self._shared["refs"] = value

    def set_callback(self, tp: str, name: Hashable, callback: Callable):
        if shared := self._shared.get(self):
            if tp not in shared:
                raise ValueError("invalid callback type")
            shared[tp][name] = callback

    async def _execute_callbacks(self, tp: str, reraise: bool | None = None):
        async def fn(name, callback):
            logger.debug("%s execute callback %s[%s]", self, tp, name)

            self._shared[self]["callback_tasks"][tp][name] = asyncio.current_task()
            try:
                if iscoroutinefunction(callback):
                    await callback()
                else:
                    res = callback()
                    if iscoroutine(res):
                        await res
            except Exception as e:
                logger.exception("%s callback %s[%s] %s", self, tp, name, callback)
                if reraise:
                    raise e
            finally:
                self._shared[self]["callback_tasks"][tp].pop(name, None)

        for name, callback in tuple(self._shared[self][tp].items()):
            await create_task(fn(name, callback))

    def remove_callback(self, tp: str, name: Hashable, cancel: bool | None = None):
        if shared := self._shared.get(self):
            if tp not in shared:
                raise ValueError("invalid callback type")
            if name in shared[tp]:
                del shared[tp][name]
            if cancel:
                task = shared["callback_tasks"][tp].get(name)
                if task:
                    task.cancel()

    def remove_callbacks(self, cancel: bool | None = None):
        if self in self._shared:
            if cancel:
                for tp in ("on_open", "on_lost", "on_close"):
                    for task in self._shared[self]["callback_tasks"][tp].values():
                        task.cancel()
            self._shared[self] = {
                "on_open": {},
                "on_lost": {},
                "on_close": {},
                "callback_tasks": {"on_open": {}, "on_lost": {}, "on_close": {}},
            }

    def __str__(self):
        return f"{self.__class__.__name__}[{yarl.URL(self.url).host}]"

    def __repr__(self):
        return self.__str__()

    @property
    def is_open(self) -> bool:
        return self._watcher_task is not None and not (self.is_closed or self._conn is None or self._conn.is_closed)

    @property
    def is_closed(self) -> bool:
        return self._closed.done()

    async def _watcher(self):
        try:
            await wait([self._conn.closing, self._closed], return_when=FIRST_COMPLETED)
        except Exception as e:
            logger.warning("%s %s %s", self, e.__class__, e)

        self._watcher_task = None

        if not self._closed.done():
            logger.warning("%s connection lost", self)
            await self._channel.close()
            self._refs -= 1
            await self._execute_callbacks("on_lost")

    async def _connect(self):
        while not self.is_closed:
            connect_timeout = yarl.URL(self.url).query.get("connection_timeout")
            if connect_timeout is not None:
                connect_timeout = int(connect_timeout) / 1000
            else:
                connect_timeout = CONNECT_TIMEOUT
            try:
                logger.info("%s connecting[timeout=%s]...", self, connect_timeout)

                async with asyncio.timeout(connect_timeout):
                    self._conn = await aiormq.connect(self.url, context=self._ssl_context)
                self._urls_iter.reset()
                break
            except (asyncio.TimeoutError, ConnectionError, aiormq.exceptions.ConnectionClosed) as e:
                try:
                    url = next(self._urls_iter)
                except StopIteration:
                    raise e
                logger.warning("%s %s %s", self, e.__class__, e)
                self.url = url

        logger.info("%s connected", self)

    async def open(self):
        if self.is_open:
            return

        if self.is_closed:
            self._closed = asyncio.Future()

        async with self._shared["connect_lock"]:
            if self._conn is None or self._conn.is_closed:
                self._open_task = create_task(self._connect())
                await self._open_task

            if self._watcher_task is None:
                self._refs += 1
                self._watcher_task = create_task(self._watcher())
                try:
                    await self._execute_callbacks("on_open", reraise=True)
                except Exception as e:
                    logger.exception(e)
                    await self.close()
                    raise e

    async def close(self):
        if self.is_closed:
            return

        if not self._open_task.done():
            self._open_task.cancel()
            self._open_task = asyncio.Future()

        if self._conn:
            await self._execute_callbacks("on_close")

        self._closed.set_result(None)

        self._refs = max(0, self._refs - 1)
        if self._refs == 0:
            if self._conn:
                await self._conn.close()
                self._conn = None
                logger.info("%s close underlying connection", self)

        self.remove_callbacks(cancel=True)

        if self._watcher_task:
            await self._watcher_task
            self._watcher_task = None

        logger.info("%s closed", self)

    async def new_channel(self) -> aiormq.Channel:
        await self.open()
        return await self._conn.channel()

    async def channel(self) -> aiormq.Channel:
        if self._channel is None or self._channel.is_closed:
            await self.open()
            if self._channel is None or self._channel.is_closed:
                self._channel = await self.new_channel()
        return self._channel


@dataclass(frozen=True)
class Exchange:
    name: str = ""
    type: str = "direct"
    durable: bool = False
    auto_delete: bool = False
    timeout: int | None = None
    conn: Connection = None
    conn_factory: Callable = field(default=None, repr=False)

    def __post_init__(self):
        if all((self.conn, self.conn_factory)):
            raise Exception("conn and conn_factory are incompatible")
        if not any((self.conn, self.conn_factory)):
            raise Exception("conn or conn_factory is requried")
        if self.conn_factory:
            object.__setattr__(self, "conn", self.conn_factory())

    async def close(self, delete: bool | None = None, timeout: int | None = None):
        logger.debug("Close %s", self)
        try:
            if self.conn_factory:
                self.conn.remove_callbacks(cancel=True)
            else:
                self.conn.remove_callback("on_open", f"on_open_exchange_{self.name}_declare", cancel=True)
            if delete and self.name != "":
                channel = await self.conn.channel()
                try:
                    await channel.exchange_delete(self.name, timeout=timeout or self.timeout)
                except aiormq.exceptions.AMQPError:
                    pass
        finally:
            if self.conn_factory:
                await self.conn.close()

    async def declare(
        self,
        timeout: int | None = None,
        restore: bool | None = None,
        force: bool | None = None,
    ):
        if self.name == "":
            return

        if is_debug_level():
            logger.debug("Declare[force=%s, restore=%s] %s", force, restore, self)

        async def fn():
            channel = await self.conn.channel()
            await channel.exchange_declare(
                self.name,
                exchange_type=self.type,
                durable=self.durable,
                auto_delete=self.auto_delete,
                timeout=timeout or self.timeout,
            )

        async def on_error(e):  # pylint: disable=unused-argument
            channel = await self.conn.channel()
            await channel.exchange_delete(self.name)

        if force:
            await retry(
                retry_timeouts=[0],
                exc_filter=lambda e: isinstance(e, aiormq.ChannelPreconditionFailed),
                on_error=on_error,
            )(fn)()
        else:
            await fn()

        if restore:
            self.conn.set_callback(
                "on_open",
                f"on_open_exchange_{self.name}_declare",
                partial(self.declare, timeout=timeout),
            )

    async def publish(
        self,
        data: bytes,
        routing_key: str,
        properties: dict | None = None,
        timeout: int | None = None,
    ):
        channel = await self.conn.channel()

        if is_debug_level():
            logger.debug(
                "Exchange[name='%s'] channel[%s] publish[routing_key='%s'] %s",
                self.name,
                channel,
                routing_key,
                data if not settings.LOG_SANITIZE else "<hiden>",
            )

        await channel.basic_publish(
            data,
            exchange=self.name,
            routing_key=routing_key,
            properties=BasicProperties(**(properties or {})),
            timeout=timeout or self.timeout,
        )


@dataclass(frozen=True)
class Consumer:
    channel: aiormq.Channel
    consumer_tag: int

    async def close(self):
        logger.debug("Close %s", self)
        await self.channel.close()


@dataclass(frozen=True)
class Queue:
    name: str
    type: QueueType = QueueType.CLASSIC
    durable: bool = False
    auto_delete: bool = False
    prefetch_count: int = 1
    max_priority: int = TASK_MAX_PRIORITY
    expires: int | None = None
    msg_ttl: int | None = None
    timeout: int | None = None
    conn: Connection = None
    conn_factory: Callable = field(default=None, repr=False)
    consumer: Consumer | None = None
    bindings: list[tuple[Exchange, str]] = field(default_factory=list, init=False)

    def __post_init__(self):
        if all((self.conn, self.conn_factory)):
            raise Exception("conn and conn_factory are incompatible")
        if not any((self.conn, self.conn_factory)):
            raise Exception("conn or conn_factory is requried")
        if self.conn_factory:
            object.__setattr__(self, "conn", self.conn_factory())
        self.conn.set_callback(
            "on_lost",
            f"on_lost_queue_{self.name}_cleanup_consumer",
            lambda: object.__setattr__(self, "consumer", None),
        )
        self.conn.set_callback(
            "on_close",
            f"on_close_queue_{self.name}_cleanup_consumer",
            lambda: object.__setattr__(self, "consumer", None),
        )

    async def close(self, delete: bool | None = None, timeout: int | None = None):
        logger.debug("Close %s", self)
        try:
            if self.conn_factory:
                self.conn.remove_callbacks(cancel=True)
            else:
                self.conn.remove_callback("on_open", f"on_open_queue_{self.name}_declare", cancel=True)
                self.conn.remove_callback("on_lost", f"on_lost_queue_{self.name}_consume", cancel=True)
                self.conn.remove_callback("on_lost", f"on_lost_queue_{self.name}_cleanup_consumer", cancel=True)
                self.conn.remove_callback("on_close", f"on_close_queue_{self.name}_cleanup_consumer", cancel=True)
                for exchange, routing_key in self.bindings:
                    self.conn.remove_callback(
                        "on_open",
                        f"on_open_queue_{self.name}_bind_{exchange.name}_{routing_key}",
                        cancel=True,
                    )
            if delete:
                channel = await self.conn.channel()
                try:
                    await channel.queue_delete(self.name, timeout=timeout or self.timeout)
                except aiormq.exceptions.AMQPError:
                    pass
        finally:
            if self.conn_factory:
                await self.conn.close()

    async def declare(self, timeout: int | None = None, restore: bool | None = None, force: bool | None = None):
        if is_debug_level():
            logger.debug("Declare[force=%s, restore=%s] %s", force, restore, self)

        async def fn():
            channel = await self.conn.channel()
            arguments = {
                "x-queue-type": self.type,
                "x-max-priority": self.max_priority,
            }
            if self.expires:
                arguments["x-expires"] = int(self.expires) * 1000
            if self.msg_ttl:
                arguments["x-message-ttl"] = int(self.msg_ttl) * 1000
            await channel.queue_declare(
                self.name,
                durable=self.durable,
                auto_delete=self.auto_delete,
                arguments=arguments,
                timeout=timeout or self.timeout,
            )

        async def on_error(e):  # pylint: disable=unused-argument
            channel = await self.conn.channel()
            await channel.queue_delete(self.name)

        if force:
            await retry(
                retry_timeouts=[0],
                exc_filter=lambda e: isinstance(e, aiormq.ChannelPreconditionFailed),
                on_error=on_error,
            )(fn)()
        else:
            await fn()

        if restore:
            self.conn.set_callback(
                "on_open",
                f"on_open_queue_{self.name}_declare",
                partial(self.declare, timeout=timeout),
            )

    async def bind(
        self,
        exchange: Exchange,
        routing_key: str,
        timeout: int | None = None,
        restore: bool | None = None,
    ):
        if is_debug_level():
            logger.debug(
                "Bind queue '%s' to exchange '%s' with routing_key '%s'",
                self.name,
                exchange.name,
                routing_key,
            )

        channel = await self.conn.channel()
        await channel.queue_bind(
            self.name,
            exchange.name,
            routing_key=routing_key,
            timeout=timeout or self.timeout,
        )

        if not (exchange, routing_key) in self.bindings:
            self.bindings.append((exchange, routing_key))

        if restore:
            self.conn.set_callback(
                "on_open",
                f"on_open_queue_{self.name}_bind_{exchange.name}_{routing_key}",
                partial(self.bind, exchange, routing_key, timeout=timeout),
            )

    async def unbind(self, exchange: Exchange, routing_key: str, timeout: int | None = None):
        if is_debug_level():
            logger.debug(
                "Unbind queue '%s' from exchange '%s' for routing_key '%s'",
                self.name,
                exchange.name,
                routing_key,
            )

        if (exchange, routing_key) in self.bindings:
            self.bindings.remove((exchange, routing_key))

            channel = await self.conn.channel()
            await channel.queue_unbind(
                self.name,
                exchange.name,
                routing_key=routing_key,
                timeout=timeout or self.timeout,
            )

            self.conn.remove_callback(
                "on_open",
                f"on_open_queue_{self.name}_bind_{exchange.name}_{routing_key}",
                cancel=True,
            )

    async def consume(
        self,
        callback: Callable[[aiormq.Channel, aiormq.abc.DeliveredMessage], Coroutine],
        prefetch_count: int | None = None,
        timeout: int | None = None,
    ):
        if self.consumer is None:
            channel = await self.conn.new_channel()
            await channel.basic_qos(
                prefetch_count=prefetch_count or self.prefetch_count,
                timeout=timeout or self.timeout,
            )

            object.__setattr__(
                self,
                "consumer",
                Consumer(
                    channel=channel,
                    consumer_tag=(
                        await channel.basic_consume(
                            self.name,
                            partial(callback, channel),
                            timeout=timeout or self.timeout,
                        )
                    ).consumer_tag,
                ),
            )
            if is_debug_level():
                logger.info("Consuming %s", self)
            self.conn.set_callback(
                "on_lost",
                f"on_lost_queue_{self.name}_consume",
                partial(
                    retry(retry_timeouts=itertools.repeat(5), exc_filter=lambda e: True)(self.consume),
                    callback,
                    prefetch_count=prefetch_count,
                    timeout=timeout,
                ),
            )

        return self.consumer

    async def stop_consume(self, timeout: int | None = None):
        logger.debug("Stop consume %s", self)
        self.conn.remove_callback("on_lost", f"on_lost_queue_{self.name}_consume", cancel=True)
        if self.consumer and not self.consumer.channel.is_closed:
            await self.consumer.channel.basic_cancel(self.consumer.consumer_tag, timeout=timeout)
            await self.consumer.close()
            object.__setattr__(self, "consumer", None)


class SerializerConfig(base.SerializerConfig):
    module: SerializerModule = SERIALIZER


class Config(base.Config):
    """RabbitMQ backend config."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RABBITMQ_")

    serializer: SerializerConfig = Field(default_factory=SerializerConfig)
    url: SecretAmqpDsn | list[SecretAmqpDsn] = Field(default_factory=lambda: URL)
    """See amqp [spec](https://www.rabbitmq.com/uri-spec.html)."""
    timeout: Optional[Timeout] = Field(default_factory=lambda: TIMEOUT)
    verify_ssl: Optional[bool] = Field(default_factory=lambda: True)
    # push_retry_timeouts: Optional[RetryTimeout] = Field(default_factory=lambda: PUSH_RETRY_TIMEOUTS)
    # pull_retry_timeouts: Optional[RetryTimeout] = Field(default_factory=lambda: PULL_RETRY_TIMEOUTS)
    push_retry_timeouts: Optional[Any] = Field(default_factory=lambda: PUSH_RETRY_TIMEOUTS)
    pull_retry_timeouts: Optional[Any] = Field(default_factory=lambda: PULL_RETRY_TIMEOUTS)
    tasks_exchange: str = Field(default_factory=lambda: TASKS_EXCHANGE)
    tasks_exchange_durable: bool = Field(default_factory=lambda: TASKS_EXCHANGE_DURABLE)
    tasks_queue_type: QueueType = Field(default_factory=lambda: TASKS_QUEUE_TYPE)
    tasks_queue_durable: bool = Field(default_factory=lambda: TASKS_QUEUE_DURABLE)
    tasks_queue_auto_delete: bool = Field(default_factory=lambda: TASKS_QUEUE_AUTO_DELETE)
    tasks_ttl: Optional[Timeout] = Field(default_factory=lambda: TASKS_TTL)
    tasks_prefetch_count: Optional[PositiveInt] = Field(default_factory=lambda: TASKS_PREFETCH_COUNT)
    events_exchange: str = Field(default_factory=lambda: EVENTS_EXCHANGE)
    events_exchange_durable: bool = Field(default_factory=lambda: EVENTS_EXCHANGE_DURABLE)
    events_queue_type: QueueType = Field(default_factory=lambda: EVENTS_QUEUE_TYPE)
    events_queue_durable: bool = Field(default_factory=lambda: EVENTS_QUEUE_DURABLE)
    events_queue_auto_delete: bool = Field(default_factory=lambda: EVENTS_QUEUE_AUTO_DELETE)
    events_queue: str = Field(default_factory=lambda: EVENTS_QUEUE)
    events_ttl: Optional[Timeout] = Field(default_factory=lambda: EVENTS_TTL)
    events_prefetch_count: Optional[PositiveInt] = Field(default_factory=lambda: EVENTS_PREFETCH_COUNT)
    results_queue_mode: ResultQueueMode = Field(default_factory=lambda: RESULTS_QUEUE_MODE)
    results_queue_prefix: str = Field(default_factory=lambda: RESULTS_QUEUE_PREFIX)
    """.. note:: Only valid for `ResultQueueMode.COMMON`."""
    results_queue_durable: bool = Field(default_factory=lambda: RESULTS_QUEUE_DURABLE)
    """.. note:: Only valid for `ResultQueueMode.COMMON`."""
    results_queue_auto_delete: bool = Field(default_factory=lambda: RESULTS_QUEUE_AUTO_DELETE)
    """.. note:: Only valid for `ResultQueueMode.COMMON`."""
    results_queue_type: QueueType = Field(default_factory=lambda: RESULTS_QUEUE_TYPE)
    """.. note:: Only valid for `ResultQueueMode.COMMON`."""
    results_ttl: Optional[Timeout] = Field(default_factory=lambda: RESULTS_TTL)
    """.. note:: Only valid for `ResultQueueMode.COMMON`."""
    results_prefetch_count: Optional[PositiveInt] = Field(default_factory=lambda: RESULTS_PREFETCH_COUNT)

    def get_ssl_context(self) -> ssl.SSLContext | None:
        url = self.url[0] if isinstance(self.url, list) else self.url  # pylint: disable=unsubscriptable-object

        if url.scheme != "amqps":  # pylint: disable=no-member
            return

        url = yarl.URL(url.get_secret_value())  # pylint: disable=no-member
        query = url.query

        context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            capath=query.get("capath"),
            cafile=query.get("cafile"),
        )

        cert = query.get("certfile")
        if cert:
            keyfile = query.get("keyfile")
            context.load_cert_chain(
                cert,
                keyfile=keyfile,
                password=self.certpass.get_secret_value() if self.certpass else None,
            )

        verify = query.get("no_verify_ssl", "0") == "0"
        if not verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        return context


def exc_filter(e) -> bool:
    return isinstance(
        e,
        (
            aiormq.AMQPConnectionError,
            ConnectionError,
            asyncio.TimeoutError,
            TimeoutError,
        ),
    )


class Backend(base.Backend):
    def __init__(self, config: Config):
        """
        Args:
            config: backend `Config`.
        """

        super().__init__(config)

        self._conn: Connection = self._connection_factory()
        self._conn.set_callback("on_open", "on_conn_open_first_time", self._on_conn_open_first_time)
        self._conn.set_callback("on_open", "on_conn_open", self._on_conn_open)

        self._default_exchange: Exchange = Exchange(conn=self._conn)
        self._tasks_exchange: Exchange = Exchange(
            config.tasks_exchange,
            conn=self._conn,
            durable=config.tasks_exchange_durable,
            auto_delete=not config.tasks_exchange_durable,
            timeout=config.timeout,
        )
        self._task_queues: dict[str, Queue] = {}

        # self._semaphore = asyncio.Semaphore(value=config.pool_size)

        self._results_queue: Queue = Queue(
            f"{config.results_queue_prefix}results.{config.id}",
            conn=self._conn,
            type=config.results_queue_type,
            durable=config.results_queue_durable,
            auto_delete=config.results_queue_auto_delete,
            prefetch_count=config.results_prefetch_count,
            # expires=config.results_ttl,
            # msg_ttl=config.results_ttl,
            timeout=config.timeout,
        )
        self._results_storage: dict[UUID, tuple[asyncio.Event, list[TaskResult]]] = {}

        self._direct_reply_to_consumer: tuple[aiormq.Channel, aiormq.spec.Basic.ConsumeOk] = ()

        self._events_exchange: Exchange = Exchange(
            config.events_exchange,
            conn=self._conn,
            type="topic",
            durable=config.events_exchange_durable,
            auto_delete=not config.events_exchange_durable,
            timeout=config.timeout,
        )
        self._events_queue: Queue = Queue(
            config.events_queue,
            conn=self._conn,
            type=config.events_queue_type,
            durable=config.events_queue_durable,
            auto_delete=self.config.events_queue_auto_delete,
            prefetch_count=config.events_prefetch_count,
            # expires=config.events_ttl,
            # msg_ttl=config.events_ttl,
            timeout=config.timeout,
        )
        self._event_callbacks: dict[str, tuple[Callable[[Event], Any], list[str] | None]] = {}

        self._send_task = retry(
            msg=f"{self} action send_task",
            retry_timeouts=config.push_retry_timeouts,
            exc_filter=exc_filter,
        )(self._send_task)

        self._push_task_result_ack_late = retry(
            msg=f"{self} action push_task_result",
            retry_timeouts=self.config.push_retry_timeouts,
            exc_filter=lambda e: isinstance(e, asyncio.TimeoutError),
        )(self._push_task_result)

        self._push_task_result = retry(
            msg=f"{self} action push_task_result",
            retry_timeouts=self.config.push_retry_timeouts,
            exc_filter=exc_filter,
        )(self._push_task_result)

        self._send_event = retry(
            msg=f"{self} action send_event",
            retry_timeouts=config.push_retry_timeouts,
            exc_filter=exc_filter,
        )(self._send_event)

    def __str__(self):
        return f"Backend[{self._tasks_exchange.conn}]"

    def __repr__(self):
        return self.__str__()

    def _connection_factory(self):
        urls = self.config.url
        if not isinstance(urls, list):
            urls = [urls]
        return Connection([url.get_secret_value() for url in urls], ssl_context=self.config.get_ssl_context())

    async def _on_conn_open_first_time(self):
        await self._tasks_exchange.declare(restore=True, force=True)
        await self._results_queue.declare(restore=True, force=True)
        await self._results_queue.bind(self._tasks_exchange, self._results_queue.name, restore=True)
        await self._results_queue.consume(
            lambda *args, **kwds: self._create_internal_task(
                "on_result_message",
                lambda: self._on_result_message(*args, **kwds),
            )
            and None
        )

        self._conn.remove_callback("on_open", "on_conn_open_first_time")

    async def _on_conn_open(self):
        channel = await self._tasks_exchange.conn.channel()

        if is_debug_level():
            logger.debug("%s channel[%s] start consuming results queue '%s'", self, channel, "amq.rabbitmq.reply-to")

        self._direct_reply_to_consumer = (
            channel,
            await channel.basic_consume(
                "amq.rabbitmq.reply-to",
                partial(self._on_result_message, channel, no_ack=True),
                no_ack=True,
                timeout=self.config.timeout,
            ),
        )

    def _allocate_results_storage(self, task_id: UUID) -> tuple:
        if task_id not in self._results_storage:
            self._results_storage[task_id] = (asyncio.Event(), [])

        return self._results_storage[task_id]

    def _cleanup_results_storage(self, task_id: UUID):
        self._results_storage.pop(task_id, None)

    async def _on_result_message(
        self,
        channel: aiormq.Channel,
        message: aiormq.abc.DeliveredMessage,
        no_ack: bool | None = None,
    ):
        try:
            properties: aiormq.spec.Basic.Properties = message.header.properties
            task_id: UUID = UUID(properties.message_id)

            task_result: TaskResult = self.serializer.loads_task_result(message.body)

            if not no_ack:
                await channel.basic_ack(message.delivery.delivery_tag)

            if is_debug_level():
                logger.debug(
                    "%s channel[%s] got result for task %s\n%s",
                    self,
                    channel,
                    task_id,
                    task_result.pretty_repr(sanitize=settings.LOG_SANITIZE),
                )

            storage = self._allocate_results_storage(task_id)
            storage[1].append(task_result)
            storage[0].set()

            if expiration := properties.expiration:
                get_event_loop().call_later(
                    int(expiration) / 1000,
                    lambda *args: self._cleanup_results_storage(task_id),
                )

        except Exception as e:
            logger.exception(e)

    async def _ensure_task_queue(self, name: str) -> Queue:
        if name not in self._task_queues:
            queue = Queue(
                name,
                conn=self._conn,
                type=self.config.tasks_queue_type,
                durable=self.config.tasks_queue_durable,
                auto_delete=self.config.tasks_queue_auto_delete,
                prefetch_count=self.config.tasks_prefetch_count,
                max_priority=TASK_MAX_PRIORITY,
                # expires=self.config.tasks_ttl,
                # msg_ttl=self.config.tasks_ttl,
                timeout=self.config.timeout,
            )
            await queue.declare(restore=True)
            await queue.bind(self._tasks_exchange, name, timeout=self.config.timeout, restore=True)
            self._task_queues[name] = queue

        return self._task_queues[name]

    async def _on_task_message(self, callback, channel: aiormq.Channel, message: aiormq.abc.DeliveredMessage):
        task_instance = None
        try:
            if is_debug_level():
                logger.debug("%s got raw message %s", self, message.body if not settings.LOG_SANITIZE else "<hiden>")

            task_instance = self.serializer.loads_task_instance(message.body)

            task_instance.extra["rabbitmq:reply_to"] = message.header.properties.reply_to

            if is_info_level():
                logger.info(
                    "%s got task\n%s",
                    self,
                    task_instance.pretty_repr(sanitize=settings.LOG_SANITIZE),
                )

            if not task_instance.ack_late:
                await channel.basic_ack(message.delivery.delivery_tag)

            await callback(task_instance)

            if task_instance.ack_late:
                await channel.basic_ack(message.delivery.delivery_tag)

        except Exception as e:
            logger.exception(e)
            try:
                exchange = self._tasks_exchange
                routing_key = message.header.properties.reply_to
                if routing_key.startswith("amq.rabbitmq.reply-to."):
                    exchange = self._default_exchange
                await exchange.publish(
                    self.serializer.dumps_task_result(TaskResult(res=None, exc=e, trb=None), task_instance),
                    routing_key=routing_key,
                    properties={
                        "delivery_mode": 2,
                        "message_id": message.header.properties.message_id,
                        "timestamp": datetime.now(tz=timezone.utc),
                    },
                    timeout=self.config.timeout,
                )
            except Exception as ee:
                logger.exception(ee)

    async def close(self):
        await super().close()
        await self._tasks_exchange.close()
        for queue in self._task_queues.values():
            await queue.close()
        await self._results_queue.close(delete=True)
        await self._conn.close()

    def _reply_to(self, task_instance: TaskInstance) -> str:
        reply_to = task_instance.extra.get("rabbitmq:reply_to")
        if reply_to is None:
            if self.config.results_queue_mode == ResultQueueMode.COMMON:
                reply_to = self._results_queue.name
            else:
                reply_to = "amq.rabbitmq.reply-to"
        return reply_to

    async def _send_task(self, task_instance: TaskInstance, **kwds):  # pylint: disable=method-hidden
        reply_to = self._reply_to(task_instance)
        task_instance.extra["rabbitmq:reply_to"] = reply_to
        data: bytes = self.serializer.dumps_task_instance(task_instance)

        await self._ensure_task_queue(task_instance.queue)

        await self._tasks_exchange.publish(
            data,
            routing_key=task_instance.queue,
            properties={
                "delivery_mode": 2,
                "message_id": f"{task_instance.task_id}",
                "timestamp": datetime.now(tz=timezone.utc),
                "expiration": f"{int(task_instance.ttl * 1000)}" if task_instance.ttl is not None else None,
                "priority": task_instance.priority,
                "reply_to": reply_to,
                "correlation_id": f"{task_instance.task_id}",
            },
        )

    async def send_task(self, task_instance: TaskInstance, **kwds):
        await self._create_internal_task("send_task", lambda: self._send_task(task_instance, **kwds))

    async def close_task(self, task_instance: TaskInstance, idx: tuple[str, int] | None = None):
        if is_debug_level():
            logger.debug("%s close task %s[%s]", self, task_instance.name, task_instance.task_id)

        if "rabbitmq:reply_to" not in task_instance.extra:
            task_instance.extra["rabbitmq:reply_to"] = self._reply_to(task_instance)

        await self.push_task_result(
            TaskResult(exc=TaskClosedError(task_instance.task_id), idx=idx),
            task_instance,
        )

    async def consume_tasks(self, queues: list[str], callback: Callable[[TaskInstance], Coroutine]):
        for queue_name in queues:
            queue: Queue = await self._ensure_task_queue(queue_name)
            if not queue.consumer:
                await queue.consume(
                    lambda *args, **kwds: self._create_internal_task(
                        "on_task_message",
                        lambda: self._on_task_message(callback, *args, **kwds),
                    )
                    and None
                )

    async def stop_consume_tasks(self, queues: list[str] | None = None):
        queues = queues if queues is not None else list(self._task_queues.keys())
        for queue_name in queues:
            if not (queue := self._task_queues.get(queue_name)):
                continue
            if queue.consumer:
                await queue.stop_consume()
            self._task_queues.pop(queue_name)

    async def _result_routing(self, task_instance: TaskInstance) -> tuple[Exchange, str]:
        exchange = self._tasks_exchange
        routing_key = task_instance.extra["rabbitmq:reply_to"]
        if routing_key.startswith("amq.rabbitmq.reply-to."):
            exchange = self._default_exchange

        return exchange, routing_key

    async def _push_task_result(
        self,
        task_result: TaskResult,
        task_instance: TaskInstance,
    ):  # pylint: disable=method-hidden
        exchange, routing_key = await self._result_routing(task_instance)

        if is_debug_level():
            logger.debug(
                "%s push result for task %s[%s] into exchange '%s' with routing_key '%s'\n%s",
                self,
                task_instance.name,
                task_instance.task_id,
                exchange.name,
                routing_key,
                task_result.pretty_repr(sanitize=settings.LOG_SANITIZE),
            )

        await exchange.publish(
            self.serializer.dumps_task_result(task_result, task_instance),
            routing_key=routing_key,
            properties={
                "delivery_mode": 2,
                "message_id": f"{task_instance.task_id}",
                "timestamp": datetime.now(tz=timezone.utc),
                "expiration": f"{int(task_instance.result_ttl * 1000)}"
                if task_instance.result_ttl is not None
                else None,
                "correlation_id": f"{task_instance.task_id}",
            },
            timeout=self.config.timeout,
        )

    async def push_task_result(self, task_result: TaskResult, task_instance: TaskInstance):
        if not task_instance.result_return:
            return

        if task_instance.ack_late:
            await self._create_internal_task(
                "push_task_result",
                lambda: self._push_task_result_ack_late(task_result, task_instance),
            )
        else:
            await self._create_internal_task(
                "push_task_result",
                lambda: self._push_task_result(task_result, task_instance),
            )

    async def _pop_task_results(self, task_instance: TaskInstance):
        task_id: UUID = task_instance.task_id

        async def fn():
            func = task_instance.func

            if task_instance.extra.get("arrlio:closable") or isasyncgenfunction(func) or isgeneratorfunction(func):
                while not self.is_closed:
                    if task_id not in self._results_storage:
                        raise TaskResultError("result expired")

                    ev, results = self._results_storage[task_id]
                    await ev.wait()
                    ev.clear()

                    while results:
                        task_result: TaskResult = results.pop(0)

                        if isinstance(task_result.exc, TaskClosedError):
                            yield task_result
                            return

                        if is_debug_level():
                            logger.debug(
                                "%s pop result for task %s[%s]\n%s",
                                self,
                                task_instance.name,
                                task_id,
                                task_result.pretty_repr(sanitize=settings.LOG_SANITIZE),
                            )

                        yield task_result

            else:
                ev, results = self._results_storage[task_id]
                await ev.wait()
                ev.clear()

                if is_debug_level():
                    logger.debug(
                        "%s pop result for task %s[%s]\n%s",
                        self,
                        task_instance.name,
                        task_id,
                        results[0].pretty_repr(sanitize=settings.LOG_SANITIZE),
                    )

                yield results.pop(0)

        __anext__ = fn().__anext__

        self._allocate_results_storage(task_id)

        idx_data: dict[str, int] = {}

        try:
            while not self.is_closed:
                task_result: TaskResult = await self._create_internal_task("pop_task_result", __anext__)
                idx = task_result.idx
                if idx:
                    idx_0, idx_1 = idx
                    if idx_0 not in idx_data:
                        idx_data[idx_0] = idx_1 - 1
                    if idx_1 <= idx_data[idx_0]:
                        continue
                    idx_data[idx_0] += 1
                    if idx_1 > idx_data[idx_0]:
                        raise TaskResultError(f"unexpected result index, expect {idx_data[idx_0]}, got {idx_1}")
                if not isinstance(task_result.exc, TaskClosedError):
                    yield task_result

        except StopAsyncIteration:
            return

        finally:
            self._cleanup_results_storage(task_id)

    async def pop_task_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        if not task_instance.result_return:
            raise TaskResultError("try to pop result for task with result_return=False")

        async for task_result in self._pop_task_results(task_instance):
            yield task_result

    async def _send_event(self, event: Event):  # pylint: disable=method-hidden
        await self._events_exchange.publish(
            self.serializer.dumps_event(event),
            routing_key="events",
            properties={
                "delivery_mode": 2,
                "timestamp": datetime.now(tz=timezone.utc),
                "expiration": f"{int(event.ttl * 1000)}" if event.ttl is not None else None,
            },
        )

    async def send_event(self, event: Event):
        if is_debug_level():
            logger.debug("%s put event\n%s", self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

        await self._create_internal_task("send_event", lambda: self._send_event(event))

    async def consume_events(
        self,
        callback_id: str,
        callback: Callable[[Event], Any],
        event_types: list[str] | None = None,
    ):
        self._event_callbacks[callback_id] = (callback, event_types)

        if self._events_queue.consumer:
            return

        async def on_message(channel: aiormq.Channel, message: aiormq.abc.DeliveredMessage):
            try:
                event: Event = self.serializer.loads_event(message.body)

                if is_debug_level():
                    logger.debug("%s got event\n%s", self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

                await channel.basic_ack(message.delivery.delivery_tag)

                for callback, event_types in self._event_callbacks.values():
                    if event_types is not None and event.type not in event_types:
                        continue
                    if iscoroutinefunction(callback):
                        self._create_internal_task("event_callback", partial(callback, event))
                    else:
                        try:
                            callback(event)
                        except Exception as e:
                            logger.exception(e)

            except Exception as e:
                logger.exception(e)

        await self._events_exchange.declare(restore=True, force=True)
        await self._events_queue.declare(restore=True, force=True)
        await self._events_queue.bind(self._events_exchange, routing_key="events", restore=True)
        await self._events_queue.consume(lambda *args, **kwds: create_task(on_message(*args, **kwds)) and None)

    async def stop_consume_events(self, callback_id: str | None = None):
        if callback_id:
            self._event_callbacks.pop(callback_id, None)
            if self._event_callbacks:
                return

        self._event_callbacks = {}
        await self._events_queue.stop_consume()
