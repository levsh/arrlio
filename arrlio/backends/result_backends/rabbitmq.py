import asyncio
import logging

from asyncio import get_event_loop
from datetime import datetime, timezone
from functools import partial
from inspect import isasyncgenfunction, isgeneratorfunction
from itertools import repeat
from typing import AsyncGenerator, Optional
from uuid import UUID, uuid4

import aiormq
import aiormq.exceptions

from pydantic import Field, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict
from rmqaio import Connection, Exchange, Queue, QueueType, SimpleExchange

from arrlio import configs, settings
from arrlio.abc import AbstractResultBackend
from arrlio.backends.rabbitmq import (
    PULL_RETRY_TIMEOUT,
    PUSH_RETRY_TIMEOUTS,
    TIMEOUT,
    URL,
    ReplyToMode,
    connection_factory,
    exc_filter,
)
from arrlio.exceptions import TaskClosedError, TaskResultError
from arrlio.models import Shared, TaskInstance, TaskResult
from arrlio.settings import ENV_PREFIX
from arrlio.types import SecretAmqpDsn, Timeout
from arrlio.utils import AioTasksRunner, Closable, is_debug_level, retry


logger = logging.getLogger("arrlio.backends.result_backends.rabbitmq")


REPLY_TO_MODE = ReplyToMode.COMMON_QUEUE
"""ResultBackend reply to mode."""

EXCHANGE = "arrlio"
"""ResultBackend exchange name."""

EXCHANGE_DURABLE = False
"""ResultBackend exchange durable option."""

QUEUE_PREFIX = "arrlio."
"""ResultBackend queue prefix."""

QUEUE_TYPE = QueueType.CLASSIC
"""ResultBackend queue type."""

QUEUE_DURABLE = False
"""ResultBackend queue `durable` option."""

QUEUE_EXCLUSIVE = True
"""ResultBackend queue `excusive` option."""

QUEUE_AUTO_DELETE = False
"""ResultBackend queue `auto-delete` option."""

PREFETCH_COUNT = 10
"""Results prefetch count."""


class SerializerConfig(configs.SerializerConfig):
    """RabbitMQ result backend serializer config."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RABBITMQ_RESULT_BACKEND_SERIALIZER_")


class Config(BaseSettings):
    """
    RabbitMQ `ResultBackend` config.

    Attributes:
        id: `ResultBackend` Id.
        url: RabbitMQ URL. See amqp [spec](https://www.rabbitmq.com/uri-spec.html).
            [Default][arrlio.backends.rabbitmq.URL].
        timeout: Network operation timeout in seconds.
            [Default][arrlio.backends.rabbitmq.TIMEOUT].
        push_retry_timeouts: Push operation retry timeouts(sequence of seconds).
            [Default][arrlio.backends.rabbitmq.PUSH_RETRY_TIMEOUTS].
        pull_retry_timeouts: Pull operation retry timeout in seconds.
            [Default][arrlio.backends.rabbitmq.PULL_RETRY_TIMEOUT].
        serializer: Config for Serializer.
        reply_to_mode: Reply to mode.
            [Default][arrlio.backends.result_backends.rabbitmq.REPLY_TO_MODE].
        exchange: Exchange name.
            [Default][arrlio.backends.result_backends.rabbitmq.EXCHANGE].
            !!! note "Only valid for `ReplyToMode.COMMON_QUEUE`."
        exchange_durable: Exchange durable option.
            [Default][arrlio.backends.result_backends.rabbitmq.EXCHANGE_DURABLE].
            !!! note "Only valid for `ReplyToMode.COMMON_QUEUE`."
        queue_prefix: Results queue prefix.
            [Default][arrlio.backends.result_backends.rabbitmq.QUEUE_PREFIX].
            !!! note "Only valid for `ReplyToMode.COMMON_QUEUE`."
        queue_type: Events queue type.
            [Default][arrlio.backends.result_backends.rabbitmq.QUEUE_TYPE].
            !!! note "Only valid for `ReplyToMode.COMMON_QUEUE`."
        queue_durable: Queue durable option.
            [Default][arrlio.backends.result_backends.rabbitmq.QUEUE_DURABLE].
            !!! note "Only valid for `ReplyToMode.COMMON_QUEUE`."
        queue_exclusive: Queue exclusive option.
            [Default][arrlio.backends.result_backends.rabbitmq.QUEUE_EXCLUSIVE].
            !!! note "Only valid for `ReplyToMode.COMMON_QUEUE`."
        queue_auto_delete: Queue auto delete option.
            [Default][arrlio.backends.result_backends.rabbitmq.QUEUE_AUTO_DELETE].
            !!! note "Only valid for `ReplyToMode.COMMON_QUEUE`."
        prefetch_count: Results prefetch count.
            [Default][arrlio.backends.result_backends.rabbitmq.PREFETCH_COUNT].
            !!! note "Only valid for `ReplyToMode.COMMON_QUEUE`."
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RABBITMQ_RESULT_BACKEND_")

    id: str = Field(default_factory=lambda: f"{uuid4().hex[-4:]}")
    url: SecretAmqpDsn | list[SecretAmqpDsn] = Field(default_factory=lambda: URL)
    timeout: Optional[Timeout] = Field(default_factory=lambda: TIMEOUT)
    push_retry_timeouts: Optional[list[Timeout]] = Field(default_factory=lambda: PUSH_RETRY_TIMEOUTS)
    pull_retry_timeout: Timeout = Field(default_factory=lambda: PULL_RETRY_TIMEOUT)
    serializer: SerializerConfig = Field(default_factory=SerializerConfig)
    reply_to_mode: ReplyToMode = Field(default_factory=lambda: REPLY_TO_MODE)
    exchange: str = Field(default_factory=lambda: EXCHANGE)
    exchange_durable: bool = Field(default_factory=lambda: EXCHANGE_DURABLE)
    queue_prefix: str = Field(default_factory=lambda: QUEUE_PREFIX)
    queue_type: QueueType = Field(default_factory=lambda: QUEUE_TYPE)
    queue_durable: bool = Field(default_factory=lambda: QUEUE_DURABLE)
    queue_exclusive: bool = Field(default_factory=lambda: QUEUE_EXCLUSIVE)
    queue_auto_delete: bool = Field(default_factory=lambda: QUEUE_AUTO_DELETE)
    prefetch_count: PositiveInt = Field(default_factory=lambda: PREFETCH_COUNT)


class ResultBackend(Closable, AbstractResultBackend):
    """
    RabbitMQ `ResultBackend`.

    Args:
        config: result backend config.
    """

    def __init__(self, config: Config):

        super().__init__()

        self.config = config

        self._internal_tasks_runner = AioTasksRunner()

        self.serializer = config.serializer.module.Serializer(config.serializer.config)

        self._conn: Connection = connection_factory(config.url)
        self._conn.set_callback("on_open", "on_conn_open_once", self._on_conn_open_once)
        self._conn.set_callback("on_open", "on_conn_open", self._on_conn_open)

        self._direct_reply_to_consumer: tuple[
            aiormq.abc.AbstractChannel,
            aiormq.spec.Basic.ConsumeOk,
        ] = None  # type: ignore

        self._default_exchange = SimpleExchange(conn=self._conn)

        self._exchange = Exchange(
            config.exchange,
            conn=self._conn,
            durable=config.exchange_durable,
            auto_delete=not config.exchange_durable,
            timeout=config.timeout,
        )

        self._queue: Queue = Queue(
            f"{config.queue_prefix}results.{self.config.id}",
            conn=self._conn,
            type=config.queue_type,
            durable=config.queue_durable,
            exclusive=config.queue_exclusive,
            auto_delete=config.queue_auto_delete,
            prefetch_count=config.prefetch_count,
            timeout=config.timeout,
        )

        self._storage: dict[UUID, tuple[asyncio.Event, list[TaskResult]]] = {}

        self._push_task_result_ack_late_with_retry = retry(
            msg=f"{self} action push_task_result",
            retry_timeouts=self.config.push_retry_timeouts,
            exc_filter=lambda e: isinstance(e, asyncio.TimeoutError),
        )(self._push_task_result)

        self._push_task_result_with_retry = retry(
            msg=f"{self} action push_task_result",
            retry_timeouts=self.config.push_retry_timeouts,
            exc_filter=exc_filter,
        )(self._push_task_result)

    def __str__(self):
        return f"ResultBackend[rabbitmq#{self.config.id}][{self._conn}]"

    def __repr__(self):
        return self.__str__()

    async def init(self):
        await retry(
            msg=f"{self} init error",
            retry_timeouts=repeat(5),
            exc_filter=exc_filter,
        )(self._conn.open)()

    async def close(self):
        await super().close()
        await self._queue.close(delete=True)
        await self._conn.close()

    def _get_reply_to(self, task_instance: TaskInstance) -> str | None:
        reply_to = task_instance.headers.get("rabbitmq:reply_to")
        if reply_to is None:
            if self.config.reply_to_mode == ReplyToMode.COMMON_QUEUE:
                reply_to = self._queue.name
            elif self.config.reply_to_mode == ReplyToMode.DIRECT_REPLY_TO:
                reply_to = "amq.rabbitmq.reply-to"
        return reply_to

    def make_headers(self, task_instance: TaskInstance) -> dict:
        return {"rabbitmq:reply_to": self._get_reply_to(task_instance)}

    def make_shared(self, task_instance: TaskInstance) -> Shared:
        shared = Shared()
        if task_instance.headers.get("rabbitmq:reply_to") == "amq.rabbitmq.reply-to":
            shared["rabbitmq:conn"] = self._conn
        return shared

    async def _on_conn_open_once(self):
        await self._exchange.declare(restore=True, force=True)
        await self._queue.declare(restore=True, force=True)
        await self._queue.bind(self._exchange, self._queue.name, restore=True)

        logger.info("%s start consuming results queue %s", self, self._queue)

        await self._queue.consume(
            lambda *args, **kwds: self._internal_tasks_runner.create_task(
                "on_result_message",
                lambda: self._on_result_message(*args, **kwds),
            )
            and None,
            retry_timeout=self.config.pull_retry_timeout,
        )

        self._conn.remove_callback("on_open", "on_conn_open_once")

    async def _on_conn_open(self):
        channel = await self._conn.channel()

        logger.info(
            "%s channel[%s] start consuming results queue '%s'",
            self,
            channel,
            "amq.rabbitmq.reply-to",
        )

        self._direct_reply_to_consumer = (
            channel,
            await channel.basic_consume(
                "amq.rabbitmq.reply-to",
                partial(self._on_result_message, channel, no_ack=True),
                no_ack=True,
                timeout=self.config.timeout,
            ),
        )

    def _allocate_storage(self, task_id: UUID) -> tuple:
        if task_id not in self._storage:
            self._storage[task_id] = (asyncio.Event(), [])
        return self._storage[task_id]

    async def allocate_storage(self, task_instance: TaskInstance):
        self._allocate_storage(task_instance.task_id)

    def _cleanup_storage(self, task_id: UUID):
        self._storage.pop(task_id, None)

    async def _on_result_message(
        self,
        channel: aiormq.abc.AbstractChannel,
        message: aiormq.abc.DeliveredMessage,
        no_ack: bool | None = None,
    ):
        try:
            properties: aiormq.spec.Basic.Properties = message.header.properties
            task_id: UUID = UUID(properties.message_id)

            task_result: TaskResult = self.serializer.loads_task_result(message.body, properties.headers)

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

            ev, task_results = self._allocate_storage(task_id)

            task_results.append(task_result)
            ev.set()

            if expiration := properties.expiration:
                get_event_loop().call_later(
                    int(expiration) / 1000,
                    lambda *args: self._cleanup_storage(task_id),
                )

        except Exception as e:
            logger.exception(e)

    async def _get_result_routing(self, task_instance: TaskInstance) -> tuple[SimpleExchange | Exchange, str]:
        exchange_name = task_instance.headers.get("rabbitmq:reply_to.exchange", self._exchange.name)
        if exchange_name == self._exchange.name:
            exchange = self._exchange
        else:
            exchange = SimpleExchange(exchange_name, conn=self._conn)

        routing_key = task_instance.headers["rabbitmq:reply_to"]
        if routing_key.startswith("amq.rabbitmq.reply-to."):
            exchange = self._default_exchange

        return exchange, routing_key

    async def _push_task_result(
        self,
        task_result: TaskResult,
        task_instance: TaskInstance,
    ):
        exchange, routing_key = await self._get_result_routing(task_instance)

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

        data, headers = self.serializer.dumps_task_result(task_result, task_instance=task_instance)

        properties = {
            "delivery_mode": 2,
            "message_type": "arrlio:result",
            "headers": headers,
            "message_id": f"{task_instance.task_id}",
            "correlation_id": f"{task_instance.task_id}",
            "timestamp": datetime.now(tz=timezone.utc),
        }

        if self.serializer.content_type is not None:
            properties["content_type"] = self.serializer.content_type

        if task_instance.result_ttl is not None:
            properties["expiration"] = f"{int(task_instance.result_ttl * 1000)}"

        # if task_instance.headers.get("app_id"):
        #     properties["app_id"] = task_instance.headers["app_id"]

        await exchange.publish(
            data,
            routing_key=routing_key,
            properties=properties,
            timeout=self.config.timeout,
        )

    async def push_task_result(self, task_result: TaskResult, task_instance: TaskInstance):
        if not task_instance.result_return:
            return

        if task_instance.ack_late:
            await self._internal_tasks_runner.create_task(
                "push_task_result",
                lambda: self._push_task_result_ack_late_with_retry(task_result, task_instance),
            )
        else:
            await self._internal_tasks_runner.create_task(
                "push_task_result",
                lambda: self._push_task_result_with_retry(task_result, task_instance),
            )

    async def _pop_task_results(self, task_instance: TaskInstance):
        task_id = task_instance.task_id

        async def fn():
            func = task_instance.func

            if task_instance.headers.get("arrlio:closable") or isasyncgenfunction(func) or isgeneratorfunction(func):
                while not self.is_closed:
                    if task_id not in self._storage:
                        raise TaskResultError("Result expired")

                    ev, results = self._storage[task_id]
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
                ev, results = self._storage[task_id]
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

        self._allocate_storage(task_id)

        idx_data: dict[str, int] = {}

        try:
            while not self.is_closed:
                task_result: TaskResult = await self._internal_tasks_runner.create_task("pop_task_result", __anext__)
                idx = task_result.idx
                if idx:
                    idx_0, idx_1 = idx
                    if idx_0 not in idx_data:
                        idx_data[idx_0] = idx_1 - 1
                    if idx_1 <= idx_data[idx_0]:
                        continue
                    idx_data[idx_0] += 1
                    if idx_1 > idx_data[idx_0]:
                        raise TaskResultError(f"Unexpected result index, expect {idx_data[idx_0]}, got {idx_1}")
                if not isinstance(task_result.exc, TaskClosedError):
                    yield task_result

        except StopAsyncIteration:
            return

        finally:
            self._cleanup_storage(task_id)

    async def pop_task_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        if not task_instance.result_return:
            raise TaskResultError("Try to pop result for task with result_return=False")

        async for task_result in self._pop_task_results(task_instance):
            yield task_result

    async def close_task(self, task_instance: TaskInstance, idx: tuple[str, int] | None = None):
        if is_debug_level():
            logger.debug("%s close task %s[%s]", self, task_instance.name, task_instance.task_id)

        if "rabbitmq:reply_to" not in task_instance.headers:
            task_instance.headers["rabbitmq:reply_to"] = self._get_reply_to(task_instance)

        await self.push_task_result(
            TaskResult(exc=TaskClosedError(task_instance.task_id), idx=idx),
            task_instance,
        )
