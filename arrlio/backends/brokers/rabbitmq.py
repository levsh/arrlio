import logging

from datetime import datetime, timezone
from itertools import repeat
from typing import Callable, Coroutine, Optional
from uuid import uuid4

import aiormq
import aiormq.exceptions

from pydantic import Field, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict
from rmqaio import Connection, Exchange, Queue, QueueType

from arrlio import gettext, settings
from arrlio.abc import AbstractBroker
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
from arrlio.models import TaskInstance
from arrlio.settings import ENV_PREFIX
from arrlio.types import TASK_MAX_PRIORITY, SecretAmqpDsn, Timeout
from arrlio.utils import AioTasksRunner, Closable, is_debug_level, retry


_ = gettext.gettext


logger = logging.getLogger("arrlio.backends.brokers.rabbitmq")


EXCHANGE = "arrlio"
"""Broker exchange name."""


EXCHANGE_DURABLE = False
"""Broker exchange durable option."""

QUEUE_TYPE = QueueType.CLASSIC
"""Broker queue type."""

QUEUE_DURABLE = False
"""Broker queue `durable` option."""

QUEUE_EXCLUSIVE = False
"""Broker queue `excusive` option."""

QUEUE_AUTO_DELETE = True
"""Broker queue `auto-delete` option."""

PREFETCH_COUNT = 1
"""Tasks prefetch count."""


BasicProperties = aiormq.spec.Basic.Properties


class SerializerConfig(SerializerConfig):
    """RabbitMQ `Broker` serializer config."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RABBITMQ_BROKER_SERIALIZER_")


class Config(BaseSettings):
    """
    RabbitMQ `Broker` config.

    Attributes:
        id: `Broker` Id.
            Default:
            ```
            f"{uuid4().hex[-4:]}"
            ```
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
            [Default][arrlio.backends.brokers.rabbitmq.EXCHANGE].
        exchange_durable: Exchange durable option.
            [Default][arrlio.backends.brokers.rabbitmq.EXCHANGE_DURABLE].
        queue_type: Tasks queue type.
            [Default][arrlio.backends.brokers.rabbitmq.QUEUE_TYPE].
        queue_durable: Queue durable option.
            [Default][arrlio.backends.brokers.rabbitmq.QUEUE_DURABLE].
        queue_exclusive: Queue exclusive option.
            [Default][arrlio.backends.brokers.rabbitmq.QUEUE_EXCLUSIVE].
        queue_auto_delete: Queue auto delete option.
            [Default][arrlio.backends.brokers.rabbitmq.QUEUE_AUTO_DELETE].
        prefetch_count: Tasks prefetch count.
            [Default][arrlio.backends.brokers.rabbitmq.PREFETCH_COUNT].
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RABBITMQ_BROKER_")

    id: str = Field(default_factory=lambda: f"{uuid4().hex[-4:]}")
    url: SecretAmqpDsn | list[SecretAmqpDsn] = Field(default_factory=lambda: URL)
    timeout: Optional[Timeout] = Field(default_factory=lambda: TIMEOUT)
    push_retry_timeouts: Optional[list[Timeout]] = Field(default_factory=lambda: PUSH_RETRY_TIMEOUTS)
    pull_retry_timeout: Optional[Timeout] = Field(default_factory=lambda: PULL_RETRY_TIMEOUT)
    serializer: SerializerConfig = Field(default_factory=SerializerConfig)
    exchange: str = Field(default_factory=lambda: EXCHANGE)
    exchange_durable: bool = Field(default_factory=lambda: EXCHANGE_DURABLE)
    queue_type: QueueType = Field(default_factory=lambda: QUEUE_TYPE)
    queue_durable: bool = Field(default_factory=lambda: QUEUE_DURABLE)
    queue_exclusive: bool = Field(default_factory=lambda: QUEUE_EXCLUSIVE)
    queue_auto_delete: bool = Field(default_factory=lambda: QUEUE_AUTO_DELETE)
    prefetch_count: PositiveInt = Field(default_factory=lambda: PREFETCH_COUNT)


class Broker(Closable, AbstractBroker):
    """
    RabbitMQ `Broker`.

    Args:
        config: `Broker` config.
    """

    def __init__(self, config: Config):
        super().__init__()

        self.config = config

        self._internal_tasks_runner = AioTasksRunner()

        self.serializer = config.serializer.module.Serializer(config.serializer.config)

        self._conn: Connection = connection_factory(config.url)
        self._conn.set_callback("on_open", "on_conn_open_once", self._on_conn_open_once)

        self._default_exchange = Exchange(conn=self._conn)

        self._exchange = Exchange(
            config.exchange,
            conn=self._conn,
            durable=config.exchange_durable,
            auto_delete=not config.exchange_durable,
            timeout=config.timeout,
        )

        self._queues: dict[str, Queue] = {}

        self._send_task = retry(
            msg=f"{self} action send_task",
            retry_timeouts=config.push_retry_timeouts,
            exc_filter=exc_filter,
        )(self._send_task)

    def __str__(self):
        return f"Broker[rabbitmq#{self.config.id}][{self._conn}]"

    def __repr__(self):
        return self.__str__()

    async def init(self):
        await retry(
            msg=f"{self} init error",
            retry_timeouts=repeat(5),
            exc_filter=exc_filter,
        )(self._conn.open)()

    async def close(self):
        await self._exchange.close()
        for queue in self._queues.values():
            await queue.close()
        await super().close()

    async def _on_conn_open_once(self):
        await self._exchange.declare(restore=True, force=True)
        self._conn.remove_callback("on_open", "on_conn_open_once")

    async def _ensure_queue(self, name: str) -> Queue:
        if name not in self._queues:
            queue = Queue(
                name,
                conn=self._conn,
                type=self.config.queue_type,
                durable=self.config.queue_durable,
                exclusive=self.config.queue_exclusive,
                auto_delete=self.config.queue_auto_delete,
                prefetch_count=self.config.prefetch_count,
                max_priority=TASK_MAX_PRIORITY,
                timeout=self.config.timeout,
            )
            await queue.declare(restore=True)
            await queue.bind(self._exchange, name, timeout=self.config.timeout, restore=True)
            self._queues[name] = queue

        return self._queues[name]

    async def _on_task_message(
        self,
        callback,
        channel: aiormq.abc.AbstractChannel,
        message: aiormq.abc.DeliveredMessage,
    ):
        try:
            if is_debug_level():
                logger.debug(_("%s got raw message %s"), self, message.body if not settings.LOG_SANITIZE else "<hiden>")

            task_instance = self.serializer.loads_task_instance(
                message.body,
                message.header.properties.headers,
            )

            reply_to = message.header.properties.reply_to
            if reply_to is not None:
                task_instance.headers["rabbitmq:reply_to"] = reply_to

            if is_debug_level():
                logger.debug(
                    _("%s got task\n%s"),
                    self,
                    task_instance.pretty_repr(sanitize=settings.LOG_SANITIZE),
                )

            if not task_instance.ack_late:
                await channel.basic_ack(message.delivery.delivery_tag)

            await callback(task_instance)

            if task_instance.ack_late:
                await channel.basic_ack(message.delivery.delivery_tag)

        except Exception:
            logger.exception(message.header.properties)

    async def _send_task(self, task_instance: TaskInstance, **kwds):
        if is_debug_level():
            logger.debug(
                _("%s send task\n%s"),
                self,
                task_instance.pretty_repr(sanitize=settings.LOG_SANITIZE),
            )

        data, headers = self.serializer.dumps_task_instance(task_instance)
        task_headers = task_instance.headers

        reply_to = task_headers.get("rabbitmq:reply_to")

        # await self._ensure_queue(task_instance.queue)

        properties = {
            "delivery_mode": 2,
            "message_type": "arrlio:task",
            "headers": headers,
            "message_id": f"{task_instance.task_id}",
            "correlation_id": f"{task_instance.task_id}",
            "reply_to": reply_to,
            "timestamp": datetime.now(tz=timezone.utc),
            "priority": min(task_instance.priority, TASK_MAX_PRIORITY),
        }

        if self.serializer.content_type is not None:
            properties["content_type"] = self.serializer.content_type

        if task_instance.ttl is not None:
            properties["expiration"] = f"{int(task_instance.ttl * 1000)}"

        if task_headers.get("app_id"):
            properties["app_id"] = task_headers["app_id"]

        if reply_to == "amq.rabbitmq.reply-to":
            if not task_instance.shared.get("rabbitmq:conn"):
                raise ArrlioError("missing 'rabbitmq:conn' shared")
            channel = await task_instance.shared["rabbitmq:conn"].channel()
            await channel.basic_publish(
                data,
                exchange=self._exchange.name,
                routing_key=task_instance.queue,
                properties=BasicProperties(**properties),
                timeout=self._exchange.timeout,
            )
        else:
            await self._exchange.publish(
                data,
                routing_key=task_instance.queue,
                properties=properties,
            )

    async def send_task(self, task_instance: TaskInstance, **kwds):
        await self._internal_tasks_runner.create_task("send_task", lambda: self._send_task(task_instance, **kwds))

    async def consume_tasks(self, queues: list[str], callback: Callable[[TaskInstance | Exception], Coroutine]):
        for queue_name in queues:
            queue = await self._ensure_queue(queue_name)
            if not queue.consumer:
                logger.info(_("%s start consuming tasks queue '%s'"), self, queue.name)
                await queue.consume(
                    lambda *args, **kwds: self._internal_tasks_runner.create_task(
                        "on_task_message",
                        lambda: self._on_task_message(callback, *args, **kwds),
                    )
                    and None,
                    retry_timeout=self.config.pull_retry_timeout,
                )

    async def stop_consume_tasks(self, queues: list[str] | None = None):
        queues = queues if queues is not None else list(self._queues.keys())
        for queue_name in queues:
            if not (queue := self._queues.get(queue_name)):
                continue
            if queue.consumer:
                logger.info(_("%s stop consuming tasks queue '%s'"), self, queue_name)
                await queue.stop_consume()
            self._queues.pop(queue_name)
