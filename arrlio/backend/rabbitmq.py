import asyncio
import contextlib
import datetime
import functools
import inspect
import logging
from typing import Dict, Iterable, List, Tuple

import aiormq
import yarl
from pydantic import Field

from arrlio import core, utils
from arrlio.backend import base
from arrlio.exc import TaskNoResultError
from arrlio.models import TaskInstance, TaskResult
from arrlio.typing import AsyncCallableT, ExceptionFilterT, PositiveIntT, PriorityT, RabbitMQDsn, SerializerT, TimeoutT


logger = logging.getLogger("arrlio")


BACKEND_NAME: str = "arrlio"
SERIALIZER: str = "arrlio.serializer.json.Json"
URL: str = "amqp://guest:guest@localhost"
TIMEOUT: int = 60
RETRY_TIMEOUTS: Iterable[int] = None
VERIFY_SSL: bool = True
EXCHANGE: str = "arrlio"
QUEUE_TTL: int = None
PREFETCH_COUNT: int = 1


class BackendConfig(base.BackendConfig):
    name: str = Field(default_factory=lambda: BACKEND_NAME)
    serializer: SerializerT = Field(default_factory=lambda: SERIALIZER)
    url: RabbitMQDsn = Field(default_factory=lambda: URL)
    timeout: TimeoutT = Field(default_factory=lambda: TIMEOUT)
    retry_timeouts: List = Field(default_factory=lambda: RETRY_TIMEOUTS)
    verify_ssl: bool = Field(default_factory=lambda: True)
    exchange: str = Field(default_factory=lambda: EXCHANGE)
    queue_ttl: PositiveIntT = Field(default_factory=lambda: QUEUE_TTL)
    prefetch_count: PositiveIntT = Field(default_factory=lambda: PREFETCH_COUNT)

    class Config:
        validate_assignment = True
        env_prefix = "ARRLIO_RMQ_BACKEND_"


class Connection:
    _shared: dict = {}

    def __init__(self, url: RabbitMQDsn):
        if url not in self.__class__._shared:
            self.__class__._shared[url] = {
                "refs": 0,
                "conn": None,
                "conn_lock": asyncio.Lock(),
            }
        self.url = url
        self.on_open = {}
        self.on_lost = {}
        self.on_close = {}
        self._connect_timeout = yarl.URL(url).query.get("connection_timeout")
        if self._connect_timeout is not None:
            self._connect_timeout = int(self._connect_timeout) / 1000
        self._supervisor_task: asyncio.Task = None
        self._on_open_cb_lock = asyncio.Lock()
        self._closing: asyncio.Future = asyncio.Future()
        self._closed: bool = True

    @property
    def _conn(self) -> aiormq.Connection:
        return self.__class__._shared.get(self.url, {}).get("conn")

    @_conn.setter
    def _conn(self, value: aiormq.Connection):
        if self.url in self.__class__._shared:
            self.__class__._shared[self.url]["conn"] = value

    @property
    def _refs(self) -> int:
        return self.__class__._shared.get(self.url, {}).get("refs")

    @_refs.setter
    def _refs(self, value: int):
        if self.url in self.__class__._shared:
            self.__class__._shared[self.url]["refs"] = value

    @property
    def _conn_lock(self) -> asyncio.Lock:
        return self.__class__._shared.get(self.url, {}).get("conn_lock")

    def __str__(self):
        return f"{self.__class__.__name__}[{self.url}]"

    @property
    def is_closed(self) -> bool:
        return self._closed or self._conn is None or (self._conn and self._conn.is_closed)

    async def open(self, retry_timeouts: Iterable[int] = None, exc_filter: ExceptionFilterT = None):
        async def connect():
            logger.info("%s: connecting...", self)
            self._conn = await asyncio.wait_for(aiormq.connect(self.url.get_secret_value()), self._connect_timeout)
            logger.info("%s: connected", self)

        async def supervisor():
            try:
                await asyncio.wait([self._conn.closing, self._closing], return_when=asyncio.FIRST_COMPLETED)
            except Exception as e:
                logger.warning("%s: %s %s", self, e.__class__, e)
            if not self._closed:
                logger.warning("%s: connection lost", self)
                self._refs -= 1
                for _, callback in self.on_lost.items():
                    try:
                        if inspect.iscoroutinefunction(callback):
                            await callback()
                        else:
                            callback()
                    except Exception as e:
                        logger.exception(e)
                self._closed = True

        if not self.is_closed:
            return

        if self._on_open_cb_lock.locked():
            raise ConnectionError()

        async with self._conn_lock:

            if self.is_closed:
                if self._conn is None or self._conn.is_closed:
                    await utils.AsyncRetry(retry_timeouts=retry_timeouts, exc_filter=exc_filter)(connect)

                if self._closed:
                    self._closed = False
                    self._refs += 1

                self._supervisor_task = asyncio.create_task(supervisor())

                async with self._on_open_cb_lock:
                    for x, callback in self.on_open.items():
                        if inspect.iscoroutinefunction(callback):
                            await callback()
                        else:
                            callback()

    async def close(self):
        if self._closed:
            return

        self._refs = max(0, self._refs - 1)
        async with self._conn_lock:
            self._closed = True
            self._closing.set_result(None)
            if self._refs == 0:
                for _, callback in self.on_close.items():
                    try:
                        if inspect.iscoroutinefunction(callback):
                            await callback()
                        else:
                            callback()
                    except Exception as e:
                        logger.exception(e)
                if self._conn:
                    await self._conn.close()
                    self._conn = None
                    del self.__class__._shared[self.url]
                    logger.info("%s: closed", self)
            if self._supervisor_task:
                await self._supervisor_task
                self._supervisor_task = None

    async def channel(self) -> aiormq.Channel:
        await self.open()
        channel = await self._conn.channel()
        return channel

    @contextlib.asynccontextmanager
    async def channel_ctx(self):
        await self.open()
        channel = await self._conn.channel()
        try:
            yield channel
        finally:
            await channel.close()


class Backend(base.Backend):
    def __init__(self, config: BackendConfig):
        super().__init__(config)
        self._conn: Connection = Connection(config.url)
        self._conn.on_open["declare"] = self.declare
        self._consumers: Dict[str, Tuple[aiormq.Channel, aiormq.spec.Basic.ConsumeOk]] = {}
        self._consume_lock: asyncio.Lock = asyncio.Lock()

    def __str__(self):
        return f"[RabbitMQBackend[{self._conn}]]"

    async def close(self):
        await super().close()
        await self._conn.close()

    @base.Backend.task
    async def declare(self):
        async with self._conn.channel_ctx() as channel:
            await channel.exchange_declare(
                self.config.exchange,
                exchange_type="direct",
                durable=False,
                auto_delete=False,
                timeout=self.config.timeout,
            )

    @base.Backend.task
    async def declare_task_queue(self, queue: str):
        async with self._conn.channel_ctx() as channel:
            arguments = {}
            arguments["x-max-priority"] = PriorityT.le
            if self.config.queue_ttl is not None:
                arguments["x-message-ttl"] = self.config.queue_ttl * 1000
            await channel.queue_declare(
                queue,
                durable=False,
                auto_delete=True,
                arguments=arguments,
                timeout=self.config.timeout,
            )
            await channel.queue_bind(queue, self.config.exchange, routing_key=queue, timeout=self.config.timeout)

    @base.Backend.task
    async def send_task(self, task_instance: TaskInstance, encrypt: bool = None, **kwds):
        task_data = task_instance.data
        await self.declare_task_queue(task_data.queue)
        logger.debug("%s: put %s", self, task_instance)
        async with self._conn.channel_ctx() as channel:
            await channel.basic_publish(
                self.serializer.dumps_task_instance(task_instance),
                exchange=self.config.exchange,
                routing_key=task_data.queue,
                properties=aiormq.spec.Basic.Properties(
                    delivery_mode=2,
                    timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                    expiration=str(int(task_data.ttl * 1000)) if task_data.ttl is not None else None,
                    priority=task_data.priority,
                ),
                timeout=self.config.timeout,
            )

    @base.Backend.task
    async def consume_tasks(self, queues: List[str], on_task: AsyncCallableT):
        async with self._consume_lock:
            timeout = self.config.timeout

            async def on_msg(channel: aiormq.Channel, msg):
                task_instance = self.serializer.loads_task_instance(msg.body)
                logger.debug("%s: got %s", self, task_instance)
                ack_late = task_instance.task.ack_late
                if not ack_late:
                    await channel.basic_ack(msg.delivery.delivery_tag)
                await asyncio.shield(on_task(task_instance))
                if ack_late:
                    await channel.basic_ack(msg.delivery.delivery_tag)

            async with self._conn.channel_ctx() as channel:
                await channel.basic_qos(prefetch_count=self.config.prefetch_count, timeout=timeout)

            for queue in queues:
                if queue in self._consumers and not self._consumers[queue][0].is_closed:
                    continue
                await self.declare_task_queue(queue)
                channel = await self._conn.channel()
                self._consumers[queue] = [
                    channel,
                    await channel.basic_consume(queue, functools.partial(on_msg, channel), timeout=timeout),
                ]
                logger.debug("%s: consuming queue '%s'", self, queue)

            async def consume_tasks():
                await self.consume_tasks(list(self._consumers.keys()), on_task)

            self._conn.on_lost["consume_tasks"] = consume_tasks

    async def stop_consume_tasks(self):
        self._conn.on_lost.pop("consume_tasks", None)
        async with self._consume_lock:
            try:
                for queue, (channel, consume_ok) in self._consumers.items():
                    if not self._conn.is_closed and not channel.is_closed:
                        logger.debug("%s: stop consuming queue '%s'", self, queue)
                        await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
                        await channel.close()
            finally:
                self._consumers = {}

    @base.Backend.task
    async def declare_result_queue(self, task_instance: TaskInstance):
        task_id = task_instance.data.task_id
        result_ttl = task_instance.data.result_ttl
        queue = routing_key = f"result.{task_id}"
        async with self._conn.channel_ctx() as channel:
            await channel.queue_declare(
                queue,
                durable=False,
                auto_delete=True,
                arguments={"x-expires": result_ttl * 1000} if result_ttl is not None else None,
                timeout=self.config.timeout,
            )
            await channel.queue_bind(
                queue,
                self.config.exchange,
                routing_key=routing_key,
                timeout=self.config.timeout,
            )
        return queue

    @base.Backend.task
    async def push_task_result(self, task_instance: core.TaskInstance, task_result: TaskResult, encrypt: bool = None):
        if not task_instance.task.result_return:
            raise TaskNoResultError(task_instance.data.task_id)
        routing_key = await self.declare_result_queue(task_instance)
        logger.debug("%s: push result for %s", self, task_instance)
        async with self._conn.channel_ctx() as channel:
            await channel.basic_publish(
                self.serializer.dumps_task_result(task_result),
                exchange=self.config.exchange,
                routing_key=routing_key,
                properties=aiormq.spec.Basic.Properties(
                    delivery_mode=1,
                    timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                ),
                timeout=self.config.timeout,
            )

    @base.Backend.task
    async def pop_task_result(self, task_instance: TaskInstance) -> TaskResult:
        task_id = task_instance.data.task_id
        queue = await self.declare_result_queue(task_instance)

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

            self._conn.on_close[task_id] = on_conn_error
            self._conn.on_lost[task_id] = on_conn_error
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
                self._conn.on_close.pop(task_id, None)
                self._conn.on_lost.pop(task_id, None)
                if not self._conn.is_closed and not channel.is_closed:
                    await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
                    # if not self._conn.is_closed and not channel.is_closed:
                    await channel.queue_delete(queue)
                    # if not self._conn.is_closed and not channel.is_closed:
                    await channel.close()

    async def send_message(message: dict):
        raise NotImplementedError()

    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        raise NotImplementedError()

    async def stop_consume_messages(self):
        return
