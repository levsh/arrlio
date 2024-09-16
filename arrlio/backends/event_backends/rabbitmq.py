import logging
import re

from asyncio import create_task
from datetime import datetime, timezone
from functools import partial
from inspect import iscoroutinefunction
from typing import Any, Callable, Optional
from uuid import uuid4

import aiormq
import aiormq.exceptions

from pydantic import Field, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict
from rmqaio import Connection, Exchange, Queue, QueueType

from arrlio import settings
from arrlio.abc import AbstractEventBackend
from arrlio.backends.rabbitmq import (
    PULL_RETRY_TIMEOUT,
    PUSH_RETRY_TIMEOUTS,
    TIMEOUT,
    URL,
    connection_factory,
    exc_filter,
)
from arrlio.configs import SerializerConfig
from arrlio.exceptions import ArrlioError
from arrlio.models import Event
from arrlio.settings import ENV_PREFIX
from arrlio.types import SecretAmqpDsn, Timeout
from arrlio.utils import AioTasksRunner, Closable, event_type_to_regex, is_debug_level, retry


logger = logging.getLogger("arrlio.backends.event_backend.rabbitmq")


EXCHANGE = "arrlio.events"
EXCHANGE_DURABLE = False
QUEUE_TYPE = QueueType.CLASSIC
QUEUE_DURABLE = False
QUEUE_AUTO_DELETE = True
QUEUE = "arrlio.events"
EVENT_TTL = 600
PREFETCH_COUNT = 10


BasicProperties = aiormq.spec.Basic.Properties


class SerializerConfig(SerializerConfig):  # pylint: disable=function-redefined
    """RabbitMQ event backend serializer config."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RABBITMQ_EVENT_BACKEND_SERIALIZER_")


class Config(BaseSettings):
    """RabbitMQ event backend config."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RABBITMQ_EVENT_BACKEND_")

    id: str = Field(default_factory=lambda: f"{uuid4().hex[-4:]}")
    url: SecretAmqpDsn | list[SecretAmqpDsn] = Field(default_factory=lambda: URL)
    """See amqp [spec](https://www.rabbitmq.com/uri-spec.html)."""
    timeout: Optional[Timeout] = Field(default_factory=lambda: TIMEOUT)
    push_retry_timeouts: Optional[list[Timeout]] = Field(default_factory=lambda: PUSH_RETRY_TIMEOUTS)
    pull_retry_timeout: Optional[Timeout] = Field(default_factory=lambda: PULL_RETRY_TIMEOUT)
    serializer: SerializerConfig = Field(default_factory=SerializerConfig)
    event_ttl: Optional[Timeout] = Field(default_factory=lambda: EVENT_TTL)
    exchange: str = Field(default_factory=lambda: EXCHANGE)
    exchange_durable: bool = Field(default_factory=lambda: EXCHANGE_DURABLE)
    queue_type: QueueType = Field(default_factory=lambda: QUEUE_TYPE)
    queue_durable: bool = Field(default_factory=lambda: QUEUE_DURABLE)
    queue_auto_delete: bool = Field(default_factory=lambda: QUEUE_AUTO_DELETE)
    queue: str = Field(default_factory=lambda: QUEUE)
    prefetch_count: PositiveInt = Field(default_factory=lambda: PREFETCH_COUNT)


class EventBackend(Closable, AbstractEventBackend):
    """RabbitMQ event backend."""

    def __init__(self, config: Config):
        """
        Args:
            config: RabbitMQ event backend config.
        """

        super().__init__()

        self.config = config
        self._internal_tasks_runner = AioTasksRunner()

        self.serializer = config.serializer.module.Serializer(config.serializer.config)

        self._conn: Connection = connection_factory(config.url)

        self._exchange: Exchange = Exchange(
            config.exchange,
            conn=self._conn,
            type="topic",
            durable=config.exchange_durable,
            auto_delete=not config.exchange_durable,
            timeout=config.timeout,
        )
        self._queue: Queue = Queue(
            config.queue,
            conn=self._conn,
            type=config.queue_type,
            durable=config.queue_durable,
            auto_delete=self.config.queue_auto_delete,
            prefetch_count=config.prefetch_count,
            # expires=config.events_ttl,
            # msg_ttl=config.events_ttl,
            timeout=config.timeout,
        )
        self._callbacks: dict[str, tuple[Callable[[Event], Any], list[str], list]] = {}

        self._send_event = retry(
            msg=f"{self} action send_event",
            retry_timeouts=config.push_retry_timeouts,
            exc_filter=exc_filter,
        )(self._send_event)

    def __str__(self):
        return f"EventBackend[rabbitmq#{self.config.id}][{self._conn}]"

    def __repr__(self):
        return self.__str__()

    async def init(self):
        await self._conn.open()

    async def close(self):
        await super().close()

    async def _send_event(self, event: Event):  # pylint: disable=method-hidden
        await self._exchange.publish(
            self.serializer.dumps_event(event),
            routing_key=event.type,
            properties={
                "delivery_mode": 2,
                "timestamp": datetime.now(tz=timezone.utc),
                "expiration": f"{int(event.ttl * 1000)}" if event.ttl is not None else None,
            },
        )

    async def send_event(self, event: Event):
        if is_debug_level():
            logger.debug("%s put event\n%s", self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

        await self._internal_tasks_runner.create_task("send_event", lambda: self._send_event(event))

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
                event: Event = self.serializer.loads_event(message.body)

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

        # TODO  pylint: disable=fixme
        await self._queue.consume(
            lambda *args, **kwds: create_task(on_message(*args, **kwds)) and None,
            retry_timeout=self.config.pull_retry_timeout,
        )

    async def stop_consume_events(self, callback_id: str | None = None):
        if callback_id:
            if callback_id not in self._callbacks:
                return
            _, event_types, _ = self._callbacks.pop(callback_id)
            for event_type in set(event_types) - {x for v in self._callbacks.values() for x in v[1]}:
                await self._queue.unbind(self._exchange, event_type)
            if not self._callbacks:
                await self._queue.stop_consume()
        else:
            for _, event_types, _ in self._callbacks.values():
                for event_type in event_types:
                    await self._queue.unbind(self._exchange, event_type)
            self._callbacks = {}
            await self._queue.stop_consume()
