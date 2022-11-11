import asyncio
import contextlib
import functools
import itertools
import logging
from datetime import datetime, timezone
from enum import Enum
from functools import partial
from inspect import iscoroutinefunction
from typing import AsyncGenerator, Callable, Dict, Hashable, Iterable, List, Optional, Tuple, Union
from uuid import UUID

import aiormq
import yarl
from pydantic import BaseModel, Field

from arrlio import core
from arrlio.backends import base
from arrlio.exc import TaskNoResultError
from arrlio.models import Event, Message, TaskData, TaskInstance, TaskResult
from arrlio.settings import ENV_PREFIX
from arrlio.tp import AmqpDsn, AsyncCallableT, ExceptionFilterT, PositiveIntT, PriorityT, SerializerT, TimeoutT
from arrlio.utils import InfIter, retry

logger = logging.getLogger("arrlio.backends.rabbitmq")


class QueueType(str, Enum):
    CLASSIC = "classic"
    QUORUM = "quorum"


BACKEND_NAME: str = "arrlio"
SERIALIZER: str = "arrlio.serializers.json"
URL: str = "amqp://guest:guest@localhost"
TIMEOUT: int = 10
CONN_RETRY_TIMEOUTS: Iterable[int] = [0]
PUSH_RETRY_TIMEOUTS: Iterable[int] = [5, 5, 5, 5]
PULL_RETRY_TIMEOUTS: Iterable[int] = itertools.repeat(5)
VERIFY_SSL: bool = True
TASKS_EXCHANGE: str = "arrlio"
TASKS_QUEUE_TYPE: QueueType = QueueType.CLASSIC
TASKS_QUEUE_DURABLE: bool = False
TASKS_QUEUE_TTL: int = None
TASKS_PREFETCH_COUNT: int = 1
EVENTS_EXCHANGE: str = "arrlio"
EVENTS_QUEUE_TYPE: QueueType = QueueType.CLASSIC
EVENTS_QUEUE_DURABLE: bool = False
EVENTS_QUEUE: str = "arrlio.events"
EVENTS_QUEUE_TTL: int = None
EVENTS_PREFETCH_COUNT: int = 1
MESSAGES_PREFETCH_COUNT: int = 1


class BackendConfig(base.BackendConfig):
    name: Optional[str] = Field(default_factory=lambda: BACKEND_NAME)
    serializer: SerializerT = Field(default_factory=lambda: SERIALIZER)
    url: Union[AmqpDsn, List[AmqpDsn]] = Field(default_factory=lambda: URL)
    timeout: Optional[TimeoutT] = Field(default_factory=lambda: TIMEOUT)
    conn_retry_timeouts: Optional[Iterable] = Field(default_factory=lambda: CONN_RETRY_TIMEOUTS)
    push_retry_timeouts: Optional[Iterable] = Field(default_factory=lambda: PUSH_RETRY_TIMEOUTS)
    pull_retry_timeouts: Optional[Iterable] = Field(default_factory=lambda: PULL_RETRY_TIMEOUTS)
    verify_ssl: Optional[bool] = Field(default_factory=lambda: True)
    tasks_exchange: str = Field(default_factory=lambda: TASKS_EXCHANGE)
    tasks_queue_type: QueueType = Field(default_factory=lambda: TASKS_QUEUE_TYPE)
    tasks_queue_durable: bool = Field(default_factory=lambda: TASKS_QUEUE_DURABLE)
    tasks_queue_ttl: Optional[PositiveIntT] = Field(default_factory=lambda: TASKS_QUEUE_TTL)
    tasks_prefetch_count: Optional[PositiveIntT] = Field(default_factory=lambda: TASKS_PREFETCH_COUNT)
    events_exchange: str = Field(default_factory=lambda: EVENTS_EXCHANGE)
    events_queue_type: QueueType = Field(default_factory=lambda: EVENTS_QUEUE_TYPE)
    events_queue_durable: bool = Field(default_factory=lambda: EVENTS_QUEUE_DURABLE)
    events_queue: str = Field(default_factory=lambda: EVENTS_QUEUE)
    events_queue_ttl: Optional[PositiveIntT] = Field(default_factory=lambda: EVENTS_QUEUE_TTL)
    events_prefetch_count: Optional[PositiveIntT] = Field(default_factory=lambda: EVENTS_PREFETCH_COUNT)
    messages_prefetch_count: Optional[PositiveIntT] = Field(default_factory=lambda: MESSAGES_PREFETCH_COUNT)

    class Config:
        env_prefix = f"{ENV_PREFIX}RMQ_BACKEND_"


class RMQConnection:
    __shared: dict = {}

    def __init__(
        self,
        url: Union[Union[AmqpDsn, str], List[Union[AmqpDsn, str]]],
        retry_timeouts: Iterable[int] = None,
        exc_filter: ExceptionFilterT = None,
    ):
        url: List[AmqpDsn] = self._normalize_url(url)
        self._url_iter = InfIter(url)

        self.url: AmqpDsn = next(self._url_iter)

        self._retry_timeouts = retry_timeouts
        self._exc_filter = exc_filter

        self._open_task: asyncio.Task = asyncio.create_task(asyncio.sleep(0))
        self._supervisor_task: asyncio.Task = asyncio.create_task(asyncio.sleep(0))

        self._closed: asyncio.Future = asyncio.Future()

        key = (
            asyncio.get_event_loop(),
            tuple(sorted(u.get_secret_value() for u in url)),
        )
        self.__key = key

        if key not in self.__shared:
            self.__shared[key] = {
                "id": 0,
                "refs": 0,
                "objs": 0,
                "conn": None,
            }

        self._shared = self.__shared[key]
        shared = self._shared

        shared["id"] += 1
        shared["objs"] += 1
        shared[self] = {
            "on_open": {},
            "on_lost": {},
            "on_close": {},
            "tasks": set(),
        }

        self._id = shared["id"]

    def _normalize_url(self, url: Union[Union[AmqpDsn, str], List[Union[AmqpDsn, str]]]) -> List[AmqpDsn]:
        if not isinstance(url, list):
            url = [url]

        for i, u in enumerate(url):
            if isinstance(u, str):

                class T(BaseModel):
                    u: AmqpDsn

                url[i] = T(u=u).u

        return url

    def __del__(self):
        if self._conn and not self.is_closed:
            logger.warning("%s: unclosed", self)
        shared = self._shared
        shared["objs"] -= 1
        if self in shared:
            del shared[self]
        if shared["objs"] == 0:
            del self.__shared[self.__key]

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

    def add_callback(self, tp: Hashable, name: Hashable, cb: Callable):
        shared = self._shared.get(self)
        if shared:
            shared[tp][name] = cb

    def remove_callback(self, tp: Hashable, name: Hashable):
        shared = self._shared.get(self)
        if shared and name in shared[tp]:
            del shared[tp][name]

    def remove_callbacks(self, cancel: bool = None):
        if cancel:
            for task in self._shared[self]["tasks"]:
                task.cancel()
        self._shared.pop(self, None)

    def __str__(self):
        return f"{self.__class__.__name__}#{self._id}[{self.url.host}:{self.url.port}]"

    def __repr__(self):
        return self.__str__()

    @property
    def is_open(self) -> bool:
        return not self.is_closed and self._conn is not None and not self._conn.is_closed

    @property
    def is_closed(self) -> bool:
        return self._closed.done()

    async def _execute_callbacks(self, tp: Hashable):
        shared = self._shared

        async def fn():
            for callback in shared[self][tp].values():
                try:
                    if iscoroutinefunction(callback):
                        await callback()
                    else:
                        callback()
                except Exception as e:
                    logger.error("%s: callback '%s' %s error: %s %s", self, tp, callback, e.__class__, e)

        task: asyncio.Task = asyncio.create_task(fn())
        shared[self]["tasks"].add(task)
        try:
            await task
        finally:
            if self in shared:
                shared[self]["tasks"].discard(task)

    async def _supervisor(self):
        try:
            await asyncio.wait([self._conn.closing, self._closed], return_when=asyncio.FIRST_COMPLETED)
        except Exception as e:
            logger.warning("%s: %s %s", self, e.__class__, e)
        if not self._closed.done():
            logger.warning("%s: connection lost", self)
            self._refs -= 1
            await self._execute_callbacks("on_lost")

    async def _connect(self):
        connect_timeout = yarl.URL(self.url).query.get("connection_timeout")
        if connect_timeout is not None:
            connect_timeout = int(connect_timeout) / 1000

        while True:
            try:
                logger.info("%s: connecting...", str(self))

                self._conn = await asyncio.wait_for(
                    aiormq.connect(self.url.get_secret_value()),
                    connect_timeout,
                )
                self._refs += 1
                self._supervisor_task = asyncio.create_task(self._supervisor())
                await self._execute_callbacks("on_open")
                self._url_iter.reset()
                break
            except (ConnectionError, asyncio.TimeoutError, TimeoutError) as e:
                try:
                    url = next(self._url_iter)
                    logger.warning("%s: %s", self, e)
                    self.url = url
                except StopIteration:
                    raise e

        logger.info("%s: connected", self)

    async def open(self):
        if self.is_closed:
            raise Exception("Can't reopen closed connection")

        if self.is_open:
            return

        if not self._open_task.done():
            await self._open_task

        if self._conn is None or self._conn.is_closed:
            self._open_task = asyncio.create_task(
                retry(
                    retry_timeouts=self._retry_timeouts,
                    exc_filter=self._exc_filter,
                )(self._connect)()
            )
            await self._open_task

    async def close(self):
        if self.is_closed:
            return

        if not self._open_task.done():
            self._open_task.cancel()

        self._closed.set_result(None)

        self._refs = max(0, self._refs - 1)
        if self._refs == 0:
            if self._conn:
                await self._execute_callbacks("on_close")
                await self._conn.close()
                self._conn = None
                logger.info("%s: closed", self)

        self.remove_callbacks(cancel=True)

        await self._supervisor_task

    async def channel(self) -> aiormq.Channel:
        await self.open()
        return await self._conn.channel()

    @contextlib.asynccontextmanager
    async def channel_ctx(self) -> AsyncGenerator[aiormq.Channel, None]:
        await self.open()
        channel = await self._conn.channel()
        try:
            yield channel
        finally:
            await channel.close()


class Backend(base.Backend):
    def __init__(self, config: BackendConfig):
        super().__init__(config)

        self._task_consumers: Dict[str, Tuple[aiormq.Channel, aiormq.spec.Basic.ConsumeOk]] = {}
        self._message_consumers: Dict[str, Tuple[aiormq.Channel, aiormq.spec.Basic.ConsumeOk]] = {}
        self._events_consumer: Tuple[aiormq.Channel, aiormq.spec.Basic.ConsumeOk] = []

        self.__conn: RMQConnection = RMQConnection(config.url, retry_timeouts=config.conn_retry_timeouts)

        self.__conn.add_callback("on_open", "declare", self._declare)
        self.__conn.add_callback("on_lost", "cleanup", self._task_consumers.clear)
        self.__conn.add_callback("on_lost", "cleanup", self._message_consumers.clear)
        self.__conn.add_callback("on_lost", "cleanup", self._events_consumer.clear)
        self.__conn.add_callback("on_close", "cleanup", self.stop_consume_tasks)
        self.__conn.add_callback("on_close", "cleanup", self.stop_consume_messages)
        self.__conn.add_callback("on_close", "cleanup", self.stop_consume_events)

    def __del__(self):
        if not self.is_closed:
            logger.warning("%s: unclosed", self)

    def __str__(self):
        return f"RMQBackend[{self.__conn}]"

    @property
    def _conn(self) -> RMQConnection:
        if self.is_closed:
            raise Exception(f"{self} is closed")
        return self.__conn

    async def _declare(self):
        config: BackendConfig = self.config
        timeout: TimeoutT = config.timeout

        async def fn():
            async with self._conn.channel_ctx() as channel:
                await channel.exchange_declare(
                    config.tasks_exchange,
                    exchange_type="direct",
                    durable=False,
                    auto_delete=False,
                    timeout=timeout,
                )
                await channel.exchange_declare(
                    config.events_exchange,
                    exchange_type="direct",
                    durable=False,
                    auto_delete=False,
                    timeout=timeout,
                )
                arguments = {}
                if config.events_queue_ttl is not None:
                    arguments["x-message-ttl"] = config.events_queue_ttl * 1000
                arguments["x-queue-type"] = config.events_queue_type.value
                await channel.queue_declare(
                    config.events_queue,
                    durable=config.events_queue_durable,
                    auto_delete=not config.events_queue_durable,
                    arguments=arguments,
                    timeout=timeout,
                )
                await channel.queue_bind(
                    config.events_queue,
                    config.events_exchange,
                    routing_key=config.events_queue,
                    timeout=timeout,
                )

        await self._run_task("declare", fn)

    async def close(self):
        if self.is_closed:
            return
        await self.__conn.close()
        await super().close()

    async def _declare_task_queue(self, queue: str):
        config: BackendConfig = self.config
        timeout: TimeoutT = config.timeout
        arguments = {"x-max-priority": PriorityT.le, "x-queue-type": config.tasks_queue_type.value}
        if config.tasks_queue_ttl is not None:
            arguments["x-message-ttl"] = config.tasks_queue_ttl * 1000

        async def fn():
            async with self._conn.channel_ctx() as channel:
                await channel.queue_declare(
                    queue,
                    durable=config.tasks_queue_durable,
                    auto_delete=not config.tasks_queue_durable,
                    arguments=arguments,
                    timeout=config.timeout,
                )
                await channel.queue_bind(queue, config.tasks_exchange, routing_key=queue, timeout=timeout)

        await self._run_task("declare_task_queue", fn)

    async def send_task(self, task_instance: TaskInstance, result_queue_durable: bool = None, **kwds):
        task_data: TaskData = task_instance.data
        task_data.extra["result_queue_durable"] = result_queue_durable

        @retry(retry_timeouts=self.config.push_retry_timeouts)
        async def fn():
            await self._declare_task_queue(task_data.queue)
            logger.debug("%s: put %s", self, task_instance)
            async with self._conn.channel_ctx() as channel:
                await channel.basic_publish(
                    self.serializer.dumps_task_instance(task_instance),
                    exchange=self.config.tasks_exchange,
                    routing_key=task_data.queue,
                    properties=aiormq.spec.Basic.Properties(
                        delivery_mode=2,
                        message_id=str(task_data.task_id.hex),
                        timestamp=datetime.now(tz=timezone.utc),
                        expiration=str(int(task_data.ttl * 1000)) if task_data.ttl is not None else None,
                        priority=task_data.priority,
                    ),
                    timeout=self.config.timeout,
                )

        await self._run_task("send_task", fn)

    async def consume_tasks(self, queues: List[str], on_task: AsyncCallableT):
        timeout: TimeoutT = self.config.timeout

        @retry()
        async def fn(queue: str):
            await self._declare_task_queue(queue)

            channel = await self._conn.channel()
            await channel.basic_qos(prefetch_count=self.config.tasks_prefetch_count, timeout=timeout)

            async def on_msg(channel: aiormq.Channel, msg):
                try:
                    task_instance = self.serializer.loads_task_instance(msg.body)
                    logger.debug("%s: got %s", self, task_instance)
                    ack_late = task_instance.task.ack_late
                    if not ack_late:
                        await channel.basic_ack(msg.delivery.delivery_tag)
                    await asyncio.shield(on_task(task_instance))
                    if ack_late:
                        await channel.basic_ack(msg.delivery.delivery_tag)
                except Exception as e:
                    logger.exception(e)

            self._task_consumers[queue] = [
                channel,
                await channel.basic_consume(queue, functools.partial(on_msg, channel), timeout=timeout),
            ]
            logger.debug("%s: start consuming tasks queue '%s'", self, queue)

        await asyncio.gather(
            *[
                self._run_task(f"consume_tasks_queue_{queue}", partial(fn, queue))
                for queue in queues
                if queue not in self._task_consumers or self._task_consumers[queue][0].is_closed
            ]
        )

        async def reconsume_tasks():
            await self.consume_tasks(list(self._task_consumers.keys()), on_task)

        self.__conn.add_callback("on_lost", "consume_tasks", reconsume_tasks)

    async def stop_consume_tasks(self, queues: List[str] = None):
        if self._conn.is_closed:
            return

        try:
            for queue in list(self._task_consumers.keys()):
                if queues is None or queue in queues:
                    for aio_task in self._tasks["consume_tasks_queue_{queue}"]:
                        aio_task.cancel()
                    channel, consume_ok = self._task_consumers[queue]
                    if not self.__conn.is_closed and not channel.is_closed:
                        logger.debug("%s: stop consuming queue '%s'", self, queue)
                        await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
                        await channel.close()
                    del self._task_consumers[queue]
        finally:
            if queues is None:
                self._task_consumers.clear()
            if not self._task_consumers:
                self.__conn.remove_callback("on_lost", "consume_tasks")
                self.__conn.remove_callback("on_close", "consume_tasks")

    async def _declare_result_queue(self, task_instance: TaskInstance) -> str:
        config: BackendConfig = self.config
        task_id: UUID = task_instance.data.task_id
        result_ttl = task_instance.data.result_ttl
        queue = routing_key = f"result.{task_id}"
        durable = task_instance.data.extra.get("result_queue_durable")

        async def fn():
            async with self._conn.channel_ctx() as channel:
                await channel.queue_declare(
                    queue,
                    durable=durable,
                    auto_delete=not durable,
                    arguments={"x-expires": result_ttl * 1000} if result_ttl is not None else None,
                    timeout=config.timeout,
                )
                await channel.queue_bind(
                    queue,
                    config.tasks_exchange,
                    routing_key=routing_key,
                    timeout=config.timeout,
                )

        await self._run_task("declare_result_queue", fn)
        return queue

    async def push_task_result(self, task_instance: core.TaskInstance, task_result: TaskResult):
        if not task_instance.task.result_return:
            raise TaskNoResultError(task_instance.data.task_id)

        @retry(retry_timeouts=self.config.push_retry_timeouts)
        async def fn():
            logger.debug("%s: push result for %s", self, task_instance)
            routing_key = await self._declare_result_queue(task_instance)
            async with self._conn.channel_ctx() as channel:
                await channel.basic_publish(
                    self.serializer.dumps_task_result(task_instance, task_result),
                    exchange=self.config.tasks_exchange,
                    routing_key=routing_key,
                    properties=aiormq.spec.Basic.Properties(
                        delivery_mode=2,
                        timestamp=datetime.now(tz=timezone.utc),
                    ),
                    timeout=self.config.timeout,
                )

        await self._run_task("push_task_result", fn)

    async def pop_task_result(self, task_instance: TaskInstance) -> TaskResult:
        task_id: UUID = task_instance.data.task_id
        queue = await self._declare_result_queue(task_instance)

        @retry(retry_timeouts=self.config.pull_retry_timeouts)
        async def fn():

            while True:
                fut = asyncio.Future()

                def on_conn_error():
                    if not fut.done():
                        fut.set_exception(ConnectionError)

                def on_result(msg):
                    try:
                        logger.debug("%s: pop result for %s", self, task_instance)
                        task_result = self.serializer.loads_task_result(msg.body)
                        if not fut.done():
                            fut.set_result(task_result)
                    except Exception as e:
                        if not fut.done():
                            fut.set_exception(e)

                self._conn.add_callback("on_close", task_id, on_conn_error)
                self._conn.add_callback("on_lost", task_id, on_conn_error)
                channel = await self._conn.channel()
                consume_ok = await channel.basic_consume(queue, on_result, timeout=self.config.timeout)
                try:
                    try:
                        await fut
                    except ConnectionError:
                        await channel.close()
                        continue
                    return fut.result()
                finally:
                    self._conn.remove_callback("on_close", task_id)
                    self._conn.remove_callback("on_lost", task_id)
                    if not self._conn.is_closed and not channel.is_closed:
                        await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
                        if not self._conn.is_closed and not channel.is_closed:
                            await channel.queue_delete(queue)
                        if not self._conn.is_closed and not channel.is_closed:
                            await channel.close()

        return await self._run_task("pop_task_result", fn)

    async def send_message(
        self,
        message: Message,
        routing_key: str = None,
        delivery_mode: int = None,
        **kwds,
    ):
        if not routing_key:
            raise ValueError("Invalid routing key")

        @retry(retry_timeouts=self.config.push_retry_timeouts)
        async def fn():
            logger.debug("%s: put %s", self, message)
            async with self._conn.channel_ctx() as channel:
                await channel.basic_publish(
                    self.serializer.dumps(message.data),
                    exchange=message.exchange,
                    routing_key=routing_key,
                    properties=aiormq.spec.Basic.Properties(
                        message_id=str(message.message_id.hex),
                        delivery_mode=delivery_mode or 2,
                        timestamp=datetime.now(tz=timezone.utc),
                        expiration=str(int(message.ttl * 1000)) if message.ttl is not None else None,
                        priority=message.priority,
                    ),
                    timeout=self.config.timeout,
                )

        await self._run_task("send_message", fn)

    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        timeout: TimeoutT = self.config.timeout

        @retry()
        async def fn(queue: str):
            channel = await self._conn.channel()
            await channel.basic_qos(prefetch_count=self.config.messages_prefetch_count, timeout=timeout)

            async def on_msg(channel: aiormq.Channel, msg):
                try:
                    data = {
                        "data": self.serializer.loads(msg.body),
                        "message_id": UUID(msg.header.properties.message_id),
                        "exchange": msg.delivery.exchange,
                        "priority": msg.delivery.routing_key,
                        "ttl": int(msg.header.properties.expiration) // 1000
                        if msg.header.properties.expiration
                        else None,
                    }
                    message = Message(**data)
                    logger.debug("%s: got %s", self, message)
                    ack_late = message.ack_late
                    if not ack_late:
                        await channel.basic_ack(msg.delivery.delivery_tag)
                    await asyncio.shield(on_message(message))
                    if ack_late:
                        await channel.basic_ack(msg.delivery.delivery_tag)
                except Exception as e:
                    logger.exception(e)

            self._message_consumers[queue] = [
                channel,
                await channel.basic_consume(queue, functools.partial(on_msg, channel), timeout=timeout),
            ]
            logger.debug("%s: start consuming messages queue '%s'", self, queue)

        await asyncio.gather(
            *[
                self._run_task(f"consume_messages_queue_{queue}", partial(fn, queue))
                for queue in queues
                if queue not in self._message_consumers or self._message_consumers[queue][0].is_closed
            ]
        )

        async def reconsume_messages():
            await self.consume_messages(list(self._message_consumers.keys()), on_message)

        self._conn.add_callback("on_lost", "consume_messages", reconsume_messages)

    async def stop_consume_messages(self, queues: List[str] = None):
        if self._conn.is_closed:
            return

        try:
            for queue in list(self._message_consumers.keys()):
                if queues is None or queue in queues:
                    for aio_task in self._tasks["consume_messages_queue_{queue}"]:
                        aio_task.cancel()
                    channel, consume_ok = self._message_consumers[queue]
                    if not self.__conn.is_closed and not channel.is_closed:
                        logger.debug("%s: stop consuming messages queue '%s'", self, queue)
                        await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
                        await channel.close()
                    del self._message_consumers[queue]
        finally:
            if queues is None:
                self._message_consumers.clear()
            if not self._message_consumers:
                self.__conn.remove_callback("on_lost", "consume_messages")
                self.__conn.remove_callback("on_close", "consume_messages")

    async def send_event(self, event: Event):
        config: BackendConfig = self.config

        @retry(retry_timeouts=self.config.push_retry_timeouts)
        async def fn():
            async with self._conn.channel_ctx() as channel:
                await channel.basic_publish(
                    self.serializer.dumps_event(event),
                    exchange=config.events_exchange,
                    routing_key=config.events_queue,
                    properties=aiormq.spec.Basic.Properties(
                        delivery_mode=2,
                        timestamp=datetime.now(tz=timezone.utc),
                        expiration=str(int(event.ttl * 1000)) if event.ttl is not None else None,
                    ),
                    timeout=config.timeout,
                )

        await self._run_task("send_event", fn)

    async def consume_events(self, on_event: AsyncCallableT):
        config: BackendConfig = self.config
        timeout: TimeoutT = config.timeout

        @retry()
        async def fn():
            channel = await self._conn.channel()
            await channel.basic_qos(prefetch_count=config.events_prefetch_count, timeout=timeout)

            async def on_msg(channel: aiormq.Channel, msg):
                try:
                    event = self.serializer.loads_event(msg.body)
                    logger.debug("%s: got %s", self, event)
                    # ack_late = event.ack_late
                    ack_late = False
                    if not ack_late:
                        await channel.basic_ack(msg.delivery.delivery_tag)
                    await asyncio.shield(on_event(event))
                    if ack_late:
                        await channel.basic_ack(msg.delivery.delivery_tag)
                except Exception as e:
                    logger.exception(e)

                if self._events_consumer and not self._events_consumer[0].is_closed:
                    return

            self._events_consumer = [
                channel,
                await channel.basic_consume(
                    config.events_queue,
                    functools.partial(on_msg, channel),
                    timeout=timeout,
                ),
            ]
            logger.debug("%s: satrt consuming events queue '%s'", self, config.events_queue)

        await self._run_task("consume_events", fn)

        async def reconsume_events():
            await self.consume_events(on_event)

        self._conn.add_callback("on_lost", "consume_events", reconsume_events)

    async def stop_consume_events(self):
        if self._conn.is_closed:
            return

        self.__conn.remove_callback("on_lost", "consume_events")
        self.__conn.remove_callback("on_close", "consume_events")

        for aio_task in self._tasks["consume_events"]:
            aio_task.cancel()

        if not self._events_consumer:
            return

        channel, consume_ok = self._events_consumer
        if not self.__conn.is_closed and not channel.is_closed:
            logger.debug("%s: stop consuming events queue '%s'", self, self.config.events_queue)
            await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
            await channel.close()

        self._events_consumer = []
