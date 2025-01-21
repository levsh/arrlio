import logging
import re

from asyncio import create_task
from datetime import datetime, timezone
from functools import partial
from inspect import iscoroutinefunction
from itertools import repeat
from typing import Any, Callable, Optional
from uuid import uuid4

import aiormq
import aiormq.exceptions

from pydantic import Field, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict
from rmqaio import Connection, Exchange, ExchangeType, Queue, QueueType

from arrlio import configs, settings
from arrlio.abc import AbstractEventBackend
from arrlio.backends.rabbitmq import (
    PULL_RETRY_TIMEOUT,
    PUSH_RETRY_TIMEOUTS,
    TIMEOUT,
    URL,
    connection_factory,
    exc_filter,
)
from arrlio.exceptions import ArrlioError
from arrlio.models import Event
from arrlio.settings import ENV_PREFIX
from arrlio.types import SecretAmqpDsn, Timeout
from arrlio.utils import AioTasksRunner, Closable, event_type_to_regex, is_debug_level, retry


logger = logging.getLogger("arrlio.backends.event_backend.rabbitmq")


EXCHANGE = "arrlio.events"
"""EventBackend exchange name."""

EXCHANGE_DURABLE = False
"""EventBackend exchange durable option."""

QUEUE = "arrlio.events"
"""EventBackend queue name."""

QUEUE_TYPE = QueueType.CLASSIC
"""EventBackend queue type."""

QUEUE_DURABLE = False
"""EventBackend queue `durable` option."""

QUEUE_EXCLUSIVE = False
"""EventBackend queue `excusive` option."""

QUEUE_AUTO_DELETE = True
"""EventBackend queue `auto-delete` option."""

PREFETCH_COUNT = 10
"""Events prefetch count."""


BasicProperties = aiormq.spec.Basic.Properties


class SerializerConfig(configs.SerializerConfig):
    """RabbitMQ `EventBackend` serializer config."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RABBITMQ_EVENT_BACKEND_SERIALIZER_")


class Config(BaseSettings):
    """
    RabbitMQ `EventBackend` config.

    Attributes:
        id: `EventBackend` Id.
        url: RabbitMQ URL. See amqp [spec](https://www.rabbitmq.com/uri-spec.html).
            [Default][arrlio.backends.rabbitmq.URL].
        timeout: Network operation timeout in seconds.
            [Default][arrlio.backends.rabbitmq.TIMEOUT].
        push_retry_timeouts: Push operation retry timeouts(sequence of seconds).
            [Default][arrlio.backends.rabbitmq.PUSH_RETRY_TIMEOUTS].
        pull_retry_timeouts: Pull operation retry timeout in seconds.
            [Default][arrlio.backends.rabbitmq.PULL_RETRY_TIMEOUT].
        serializer: Config for Serializer.
        exchange: Exchange name.
            [Default][arrlio.backends.event_backends.rabbitmq.EXCHANGE].
        exchange_durable: Exchange durable option.
            [Default][arrlio.backends.event_backends.rabbitmq.EXCHANGE_DURABLE].
        queue: Events queue name.
            [Default][arrlio.backends.event_backends.rabbitmq.QUEUE].
        queue_type: Events queue type.
            [Default][arrlio.backends.event_backends.rabbitmq.QUEUE_TYPE].
        queue_durable: Queue durable option.
            [Default][arrlio.backends.event_backends.rabbitmq.QUEUE_DURABLE].
        queue_exclusive: Queue exclusive option.
            [Default][arrlio.backends.event_backends.rabbitmq.QUEUE_EXCLUSIVE].
        queue_auto_delete: Queue auto delete option.
            [Default][arrlio.backends.event_backends.rabbitmq.QUEUE_AUTO_DELETE].
        prefetch_count: Events prefetch count.
            [Default][arrlio.backends.event_backends.rabbitmq.PREFETCH_COUNT].
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RABBITMQ_EVENT_BACKEND_")

    id: str = Field(default_factory=lambda: f"{uuid4().hex[-4:]}")
    url: SecretAmqpDsn | list[SecretAmqpDsn] = Field(default_factory=lambda: URL)
    timeout: Optional[Timeout] = Field(default_factory=lambda: TIMEOUT)
    push_retry_timeouts: Optional[list[Timeout]] = Field(default_factory=lambda: PUSH_RETRY_TIMEOUTS)
    pull_retry_timeout: Timeout = Field(default_factory=lambda: PULL_RETRY_TIMEOUT)
    serializer: SerializerConfig = Field(default_factory=SerializerConfig)
    exchange: str = Field(default_factory=lambda: EXCHANGE)
    exchange_durable: bool = Field(default_factory=lambda: EXCHANGE_DURABLE)
    queue: str = Field(default_factory=lambda: QUEUE)
    queue_type: QueueType = Field(default_factory=lambda: QUEUE_TYPE)
    queue_durable: bool = Field(default_factory=lambda: QUEUE_DURABLE)
    queue_exclusive: bool = Field(default_factory=lambda: QUEUE_EXCLUSIVE)
    queue_auto_delete: bool = Field(default_factory=lambda: QUEUE_AUTO_DELETE)
    prefetch_count: PositiveInt = Field(default_factory=lambda: PREFETCH_COUNT)


class EventBackend(Closable, AbstractEventBackend):
    """
    RabbitMQ `EventBackend`.

    Args:
        config: RabbitMQ `EventBackend` config.
    """

    def __init__(self, config: Config):
        super().__init__()

        self.config = config

        self._internal_tasks_runner = AioTasksRunner()

        self.serializer = config.serializer.module.Serializer(config.serializer.config)

        self._conn: Connection = connection_factory(config.url)

        self._exchange: Exchange = Exchange(
            config.exchange,
            conn=self._conn,
            type=ExchangeType.TOPIC,
            durable=config.exchange_durable,
            auto_delete=not config.exchange_durable,
            timeout=config.timeout,
        )

        self._queue: Queue = Queue(
            config.queue,
            conn=self._conn,
            type=config.queue_type,
            durable=config.queue_durable,
            exclusive=config.queue_exclusive,
            auto_delete=self.config.queue_auto_delete,
            prefetch_count=config.prefetch_count,
            timeout=config.timeout,
        )
        self._callbacks: dict[str, tuple[Callable[[Event], Any], list[str], list]] = {}

        self._send_event_with_retry = retry(
            msg=f"{self} action send_event",
            retry_timeouts=config.push_retry_timeouts,
            exc_filter=exc_filter,
        )(self._send_event)

    def __str__(self):
        return f"EventBackend[rabbitmq#{self.config.id}][{self._conn}]"

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

    async def _send_event(self, event: Event):
        data, headers = self.serializer.dumps_event(event)
        await self._exchange.publish(
            data,
            routing_key=event.type,
            properties={
                "delivery_mode": 2,
                "timestamp": datetime.now(tz=timezone.utc),
                "expiration": f"{int(event.ttl * 1000)}" if event.ttl is not None else None,
                "headers": headers,
            },
        )

    async def send_event(self, event: Event):
        if is_debug_level():
            logger.debug("%s send event\n%s", self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

        await self._internal_tasks_runner.create_task("send_event", lambda: self._send_event_with_retry(event))

    async def consume_events(
        self,
        callback_id: str,
        callback: Callable[[Event], Any],
        event_types: list[str] | None = None,
    ):
        if callback_id in self._callbacks:
            raise ArrlioError(
                (
                    f"callback_id '{callback_id}' already in use for consuming "
                    f"'{self._callbacks[callback_id][1]}' event_types"
                )
            )

        event_types = event_types or ["#"]

        self._callbacks[callback_id] = (
            callback,
            event_types,
            [re.compile(event_type_to_regex(event_type)) for event_type in event_types],
        )

        async def on_message(channel: aiormq.Channel, message: aiormq.abc.DeliveredMessage):
            try:
                event: Event = self.serializer.loads_event(
                    message.body,
                    message.header.properties.headers,
                )

                if is_debug_level():
                    logger.debug("%s got event\n%s", self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

                await channel.basic_ack(message.delivery.delivery_tag)

                for callback, event_types, patterns in self._callbacks.values():
                    if event_types is not None and not any(pattern.match(event.type) for pattern in patterns):
                        continue
                    if iscoroutinefunction(callback):
                        self._internal_tasks_runner.create_task("callback", partial(callback, event))
                    else:
                        try:
                            callback(event)
                        except Exception as e:
                            logger.exception(e)

            except Exception as e:
                logger.exception(e)

        if not self._queue.consumer:
            await self._exchange.declare(restore=True, force=True)
            await self._queue.declare(restore=True, force=True)

        for event_type in event_types:
            await self._queue.bind(self._exchange, event_type, restore=True)

        # TODO
        logger.info(
            "%s start consuming events[callback_id=%s, event_types=%s]",
            self,
            callback_id,
            event_types,
        )

        await self._queue.consume(
            lambda *args, **kwds: create_task(on_message(*args, **kwds)) and None,
            retry_timeout=self.config.pull_retry_timeout,
        )

    async def stop_consume_events(self, callback_id: str | None = None):
        logger.info("%s stop consuming events[callback_id=%s]", self, callback_id)

        if callback_id:
            if callback_id not in self._callbacks:
                return
            callback, event_types, regex = self._callbacks.pop(callback_id)
            for event_type in set(event_types) - {x for v in self._callbacks.values() for x in v[1]}:
                await self._queue.unbind(self._exchange, event_type)
            if not self._callbacks:
                await self._queue.stop_consume()
        else:
            for callback, event_types, regex in self._callbacks.values():
                for event_type in event_types:
                    await self._queue.unbind(self._exchange, event_type)
            self._callbacks = {}
            await self._queue.stop_consume()
