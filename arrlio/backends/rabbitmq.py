import asyncio
import itertools
import logging
from asyncio import FIRST_COMPLETED, create_task, gather, get_event_loop, shield, wait
from datetime import datetime, timezone
from enum import Enum
from functools import partial
from inspect import isasyncgenfunction, iscoroutinefunction, isgeneratorfunction
from typing import AsyncGenerator, Awaitable, Callable, Dict, Hashable, Iterable, List, Optional, Tuple, Union
from uuid import UUID

import aiormq
import aiormq.exceptions
import yarl
from pydantic import BaseModel, Field, PositiveInt
from rich.pretty import pretty_repr

from arrlio.backends import base
from arrlio.exc import GraphError, TaskClosedError, TaskResultError
from arrlio.models import Event, Message, TaskData, TaskInstance, TaskResult
from arrlio.settings import ENV_PREFIX
from arrlio.tp import AmqpDsn, AsyncCallableT, ExceptionFilterT, PriorityT, TimeoutT
from arrlio.utils import InfIter, retry, wait_for

logger = logging.getLogger("arrlio.backends.rabbitmq")
is_debug = logger.isEnabledFor(logging.DEBUG)
is_info = logger.isEnabledFor(logging.INFO)

datetime_now = datetime.now
utc = timezone.utc
BasicProperties = aiormq.spec.Basic.Properties

asyncio_Lock = asyncio.Lock  # pylint: disable=invalid-name
asyncio_Task = asyncio.Task  # pylint: disable=invalid-name
asyncio_Event = asyncio.Event  # pylint: disable=invalid-name
asyncio_Future = asyncio.Future  # pylint: disable=invalid-name
asyncio_Semaphore = asyncio.Semaphore  # pylint: disable=invalid-name
asyncio_TimeoutError = asyncio.TimeoutError  # pylint: disable=invalid-name


class QueueType(str, Enum):
    CLASSIC = "classic"
    QUORUM = "quorum"


class ResultQueueMode(str, Enum):
    DIRECT_REPLY_TO = "direct_reply_to"
    SEPARATE = "separate"
    COMMON = "common"


SERIALIZER: str = "arrlio.serializers.json"

URL: str = "amqp://guest:guest@localhost"
VERIFY_SSL: bool = True
TIMEOUT: int = 10

POOL_SIZE: int = 100  # require ack_late=True

CONN_RETRY_TIMEOUTS: Union[List[int], Iterable[int]] = None  # pylint: disable=invalid-name
PUSH_RETRY_TIMEOUTS: Union[List[int], Iterable[int]] = [5, 5, 5, 5]  # pylint: disable=invalid-name
PULL_RETRY_TIMEOUTS: Union[List[int], Iterable[int]] = itertools.repeat(5)  # pylint: disable=invalid-name

TASKS_EXCHANGE: str = "arrlio"
TASKS_EXCHANGE_DURABLE: bool = False
TASKS_QUEUE_TYPE: QueueType = QueueType.CLASSIC
TASKS_QUEUE_DURABLE: bool = False
TASKS_QUEUE_TTL: int = 600
TASKS_PREFETCH_COUNT: int = 1

EVENTS_EXCHANGE: str = "arrlio"
EVENTS_EXCHANGE_DURABLE: bool = False
EVENTS_QUEUE_TYPE: QueueType = QueueType.CLASSIC
EVENTS_QUEUE_DURABLE: bool = False
EVENTS_QUEUE_PREFIX: str = "arrlio."
EVENTS_QUEUE_TTL: int = 600
EVENTS_PREFETCH_COUNT: int = 1

MESSAGES_PREFETCH_COUNT: int = 1

RESULTS_QUEUE_PREFIX: str = "arrlio."
RESULTS_QUEUE_MODE: ResultQueueMode = ResultQueueMode.COMMON
RESULTS_SEPARATE_QUEUE_DURABLE: bool = False
RESULTS_SEPARATE_QUEUE_TYPE: QueueType = QueueType.CLASSIC
RESULTS_COMMON_QUEUE_DURABLE: bool = False
RESULTS_COMMON_QUEUE_TYPE: QueueType = QueueType.CLASSIC
RESULTS_COMMON_QUEUE_TTL: int = 600


class Config(base.Config):
    serializer: base.SerializerConfig = Field(
        default_factory=lambda: base.SerializerConfig(module="arrlio.serializers.json")
    )
    url: Union[AmqpDsn, List[AmqpDsn]] = Field(default_factory=lambda: URL)
    timeout: Optional[TimeoutT] = Field(default_factory=lambda: TIMEOUT)
    verify_ssl: Optional[bool] = Field(default_factory=lambda: True)
    pool_size: PositiveInt = Field(default_factory=lambda: POOL_SIZE)
    conn_retry_timeouts: Optional[Union[List[int], Iterable[int]]] = Field(default_factory=lambda: CONN_RETRY_TIMEOUTS)
    push_retry_timeouts: Optional[Union[List[int], Iterable[int]]] = Field(default_factory=lambda: PUSH_RETRY_TIMEOUTS)
    pull_retry_timeouts: Optional[Union[List[int], Iterable[int]]] = Field(default_factory=lambda: PULL_RETRY_TIMEOUTS)
    tasks_exchange: str = Field(default_factory=lambda: TASKS_EXCHANGE)
    tasks_exchange_durable: bool = Field(default_factory=lambda: TASKS_EXCHANGE_DURABLE)
    tasks_queue_type: QueueType = Field(default_factory=lambda: TASKS_QUEUE_TYPE)
    tasks_queue_durable: bool = Field(default_factory=lambda: TASKS_QUEUE_DURABLE)
    tasks_queue_ttl: Optional[PositiveInt] = Field(default_factory=lambda: TASKS_QUEUE_TTL)
    tasks_prefetch_count: Optional[PositiveInt] = Field(default_factory=lambda: TASKS_PREFETCH_COUNT)
    events_exchange: str = Field(default_factory=lambda: EVENTS_EXCHANGE)
    events_exchange_durable: bool = Field(default_factory=lambda: EVENTS_EXCHANGE_DURABLE)
    events_queue_type: QueueType = Field(default_factory=lambda: EVENTS_QUEUE_TYPE)
    events_queue_durable: bool = Field(default_factory=lambda: EVENTS_QUEUE_DURABLE)
    events_queue_prefix: str = Field(default_factory=lambda: EVENTS_QUEUE_PREFIX)
    events_queue_ttl: Optional[PositiveInt] = Field(default_factory=lambda: EVENTS_QUEUE_TTL)
    events_prefetch_count: Optional[PositiveInt] = Field(default_factory=lambda: EVENTS_PREFETCH_COUNT)
    messages_prefetch_count: Optional[PositiveInt] = Field(default_factory=lambda: MESSAGES_PREFETCH_COUNT)
    results_queue_prefix: str = Field(default_factory=lambda: RESULTS_QUEUE_PREFIX)
    results_queue_mode: ResultQueueMode = Field(default_factory=lambda: RESULTS_QUEUE_MODE)
    results_separate_queue_durable: bool = Field(default_factory=lambda: RESULTS_SEPARATE_QUEUE_DURABLE)
    results_separate_queue_type: QueueType = Field(default_factory=lambda: RESULTS_SEPARATE_QUEUE_TYPE)
    results_common_queue_durable: bool = Field(default_factory=lambda: RESULTS_COMMON_QUEUE_DURABLE)
    results_common_queue_type: QueueType = Field(default_factory=lambda: RESULTS_COMMON_QUEUE_TYPE)
    results_common_queue_ttl: Optional[PositiveInt] = Field(default_factory=lambda: RESULTS_COMMON_QUEUE_TTL)

    class Config:
        env_prefix = f"{ENV_PREFIX}RABBITMQ_"


RETRY_EXCEPTIONS = (
    aiormq.exceptions.ConnectionClosed,
    ConnectionError,
    asyncio_TimeoutError,
    TimeoutError,
)


def _exc_filter(e) -> bool:
    return isinstance(e, RETRY_EXCEPTIONS)


class RMQConnection:
    __shared: dict = {}

    def __init__(
        self,
        url: Union[Union[AmqpDsn, str], List[Union[AmqpDsn, str]]],
        retry_timeouts: Optional[Union[List[int], Iterable[int]]] = None,
        exc_filter: ExceptionFilterT = None,
    ):
        urls: List[AmqpDsn] = self._to_amqpdsn(url)
        self._url_iter = InfIter(urls)
        self.url: AmqpDsn = next(self._url_iter)

        self._retry_timeouts = retry_timeouts
        self._exc_filter = exc_filter or _exc_filter

        self._open_task: Awaitable = asyncio_Future()
        self._open_task.set_result(None)

        self._supervisor_task: asyncio_Task = None

        self._closed: asyncio_Future = asyncio_Future()

        key = (get_event_loop(), tuple(sorted(u.get_secret_value() for u in urls)))
        self.__key = key

        if key not in self.__shared:
            self.__shared[key] = {
                "id": 0,
                "refs": 0,
                "objs": 0,
                "conn": None,
                "connect_lock": asyncio_Lock(),
            }

        shared = self.__shared[key]
        shared["id"] += 1
        shared["objs"] += 1
        shared[self] = {
            "on_open_ordered": {},
            "on_open": {},
            "on_lost_ordered": {},
            "on_lost": {},
            "on_close_ordered": {},
            "on_close": {},
            "callback_tasks": set(),
        }
        self._shared = shared

        self._id = shared["id"]
        self._channel: aiormq.Channel = None

    def _to_amqpdsn(self, url: Union[AmqpDsn, str, List[AmqpDsn], List[str]]) -> List[AmqpDsn]:
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
            shared.pop(self, None)
        if shared["objs"] == 0:
            self.__shared.pop(self.__key, None)

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

    def add_callback(self, tp: str, name: Hashable, cb: Callable, reraise: bool = None):
        if shared := self._shared.get(self):
            if tp not in shared:
                raise ValueError("Invalid callback type")
            shared[tp][name] = (cb, reraise)

    def remove_callback(self, tp: str, name: Hashable):
        if shared := self._shared.get(self):
            if tp not in shared:
                raise ValueError("Invalid callback type")
            if name in shared[tp]:
                del shared[tp][name]

    def remove_callbacks(self, cancel: bool = None):
        if cancel:
            for task in self._shared[self]["callback_tasks"]:
                task.cancel()
        self._shared.pop(self, None)

    def __str__(self):
        if logger.getEffectiveLevel() <= 20:
            return f"{self.__class__.__name__}[{id(self):#x}]#{self._id}[{self.url.host}]"
        return f"{self.__class__.__name__}#{self._id}[{self.url.host}]"

    def __repr__(self):
        return self.__str__()

    @property
    def is_open(self) -> bool:
        return self._supervisor_task is not None and not (self.is_closed or self._conn is None or self._conn.is_closed)

    @property
    def is_closed(self) -> bool:
        return self._closed.done()

    async def _execute_callbacks(self, tp: str):
        async def fn(name, data):
            callback, reraise = data
            try:
                # logger.debug("%s: execute callback '%s' '%s':%s", self, tp, name, callback)
                if iscoroutinefunction(callback):
                    await callback()
                else:
                    callback()
                # logger.debug("%s: callback '%s' '%s':%s done", self, tp, name, callback)
            except Exception as e:
                logger.exception("%s: callback '%s' '%s':%s error: %s %s", self, tp, name, callback, e.__class__, e)
                if reraise:
                    raise e

        async def do():
            items = list(self._shared[self][tp].items())
            if tp.endswith("_ordered"):
                for item in items:
                    await fn(*item)
            else:
                await gather(*[fn(*item) for item in items])

        task: asyncio_Task = create_task(do())
        self._shared[self]["callback_tasks"].add(task)
        try:
            await task
        finally:
            if self in self._shared:
                self._shared[self]["callback_tasks"].discard(task)

    async def _supervisor(self):
        try:
            await wait([self._conn.closing, self._closed], return_when=FIRST_COMPLETED)
        except Exception as e:
            logger.warning("%s: %s %s", self, e.__class__, e)

        self._supervisor_task = None

        if not self._closed.done():
            logger.warning("%s: connection lost", self)
            await self._channel.close()
            self._refs -= 1
            await self._execute_callbacks("on_lost_ordered")
            await self._execute_callbacks("on_lost")

    async def _connect(self):
        connect_timeout = yarl.URL(self.url).query.get("connection_timeout")
        if connect_timeout is not None:
            connect_timeout = int(connect_timeout) / 1000

        while not self.is_closed:
            try:
                logger.info("%s: connecting(timeout=%s)...", self, connect_timeout)

                self._conn = await wait_for(aiormq.connect(self.url.get_secret_value()), connect_timeout)
                self._url_iter.reset()
                break
            except Exception as e:
                if not self._exc_filter(e):
                    raise e
                try:
                    url = next(self._url_iter)
                except StopIteration:
                    raise e
                logger.warning("%s: %s %s", self, e.__class__, e)
                self.url = url

        logger.info("%s: connected", self)

    async def open(self):
        if self.is_open:
            return

        if self.is_closed:
            raise Exception("Can't reopen closed connection")

        async with self._shared["connect_lock"]:
            if self._conn is None or self._conn.is_closed:
                self._open_task = create_task(
                    retry(
                        fn_name=f"{self}:connect",
                        retry_timeouts=self._retry_timeouts,
                        exc_filter=self._exc_filter,
                    )(self._connect)()
                )
                await self._open_task

            if self._supervisor_task is None:
                self._refs += 1
                self._supervisor_task = create_task(self._supervisor())
                await self._execute_callbacks("on_open_ordered")
                await self._execute_callbacks("on_open")

    async def close(self):
        if self.is_closed:
            return

        if not self._open_task.done():
            self._open_task.cancel()

        if self._conn:
            await self._execute_callbacks("on_close_ordered")
            await self._execute_callbacks("on_close")

        self._closed.set_result(None)

        self._refs = max(0, self._refs - 1)
        if self._refs == 0:
            if self._conn:
                await self._conn.close()
                self._conn = None
                logger.info("%s: closed", self)

        self.remove_callbacks(cancel=True)

        if self._supervisor_task:
            await self._supervisor_task

    async def new_channel(self) -> aiormq.Channel:
        await self.open()
        return await self._conn.channel()

    async def channel(self) -> aiormq.Channel:
        if self._channel is None or self._channel.is_closed:
            self._channel = await self.new_channel()
        return self._channel


class Backend(base.Backend):
    def __init__(self, config: Config):
        super().__init__(config)

        self._task_consumers: Dict[str, Tuple[aiormq.Channel, aiormq.spec.Basic.ConsumeOk]] = {}
        self._message_consumers: Dict[str, Tuple[aiormq.Channel, aiormq.spec.Basic.ConsumeOk]] = {}
        self._events_consumer: Tuple[aiormq.Channel, aiormq.spec.Basic.ConsumeOk] = ()
        self._event_callbacks: Dict[str, Tuple[AsyncCallableT, List[str]]] = {}

        self._conn_open_ev: asyncio_Event = asyncio_Event()
        self._tasks_semaphore = asyncio_Semaphore(value=config.pool_size)

        self._declared = set()

        self.__conn: RMQConnection = RMQConnection(config.url, retry_timeouts=config.conn_retry_timeouts)

        self.__conn.add_callback("on_open_ordered", "_on_connection_open", self._on_conneciton_open, reraise=True)
        self.__conn.add_callback("on_open", "on_connection_open", self.on_connection_open)
        self.__conn.add_callback("on_lost_ordered", "_on_connection_lost", self._on_connection_lost)
        self.__conn.add_callback("on_lost", "on_connection_lost", self.on_connection_lost)
        self.__conn.add_callback("on_close_ordered", "_on_connection_close", self._on_connection_close)
        self.__conn.add_callback("on_close", "on_connection_close", self.on_connection_close)

        self._common_results_storage: Dict[UUID, Tuple[asyncio_Event, List[TaskResult]]] = {}
        self._common_results_consumer: Tuple[aiormq.Channel, aiormq.spec.Basic.ConsumeOk] = {}

        self._direct_reply_to_consumer: Tuple[aiormq.Channel, aiormq.spec.Basic.ConsumeOk] = {}
        self._direct_reply_to_channel: aiormq.Channel = None

        @retry(fn_name=f"{self}:send_task", retry_timeouts=config.push_retry_timeouts, exc_filter=_exc_filter)
        async def send_task(task_instance: TaskInstance, **kwds):
            task_data: TaskData = task_instance.data

            task_data.extra.update({"backend:id": config.id})
            results_queue_mode = task_data.extra.setdefault("rabbitmq:results_queue_mode", config.results_queue_mode)

            await self._declare_tasks_queue(task_data.queue)

            if results_queue_mode == ResultQueueMode.DIRECT_REPLY_TO:
                channel = self._direct_reply_to_channel
            else:
                channel = await self._channel()

            if is_debug:
                logger.debug("%s: channel(%s) send\n%s", self, channel, pretty_repr(task_instance.dict()))

            await channel.basic_publish(
                self.serializer.dumps_task_instance(task_instance),
                exchange=config.tasks_exchange,
                routing_key=task_data.queue,
                properties=BasicProperties(
                    delivery_mode=2,
                    message_id=f"{task_data.task_id}",
                    timestamp=datetime_now(tz=utc),
                    expiration=f"{int(task_data.ttl * 1000)}" if task_data.ttl is not None else None,
                    priority=task_data.priority,
                    reply_to=self._reply_to(task_data),
                    correlation_id=f"{task_data.task_id}",
                ),
                timeout=config.timeout,
            )

        @retry(fn_name=f"{self}:push_task_result", retry_timeouts=config.push_retry_timeouts, exc_filter=_exc_filter)
        async def push_task_result(task_instance: TaskInstance, task_result: TaskResult):
            task_data: TaskData = task_instance.data

            exchange, routing_key = await self._ensure_result_queue(task_data)

            channel = await self._channel()

            if is_debug:
                logger.debug(
                    "%s: channel(%s) push result for task %s(%s) into exchange:'%s' routing_key:'%s'\n%s",
                    self,
                    channel,
                    task_data.task_id,
                    task_instance.task.name,
                    exchange,
                    routing_key,
                    pretty_repr(task_result.dict()),
                )

            await channel.basic_publish(
                self.serializer.dumps_task_result(task_instance, task_result),
                exchange=exchange,
                routing_key=routing_key,
                properties=BasicProperties(
                    delivery_mode=2,
                    message_id=f"{task_data.task_id}",
                    timestamp=datetime.now(tz=utc),
                    expiration=f"{int(task_data.result_ttl * 1000)}" if task_data.result_ttl is not None else None,
                    correlation_id=f"{task_data.task_id}",
                ),
                timeout=config.timeout,
            )

        @retry(fn_name=f"{self}:send_mesage", retry_timeouts=config.push_retry_timeouts, exc_filter=_exc_filter)
        async def send_message(message: Message, *, routing_key: str, delivery_mode: int = None, **kwds):
            channel = await self._channel()

            if is_debug:
                logger.debug("%s: channel(%s) send\n%s", self, channel, pretty_repr(message.dict()))

            await channel.basic_publish(
                self.serializer.dumps(message.data),
                exchange=message.exchange,
                routing_key=routing_key,
                properties=BasicProperties(
                    message_id=f"{message.message_id}",
                    delivery_mode=delivery_mode or 2,
                    timestamp=datetime.now(tz=utc),
                    expiration=f"{int(message.ttl * 1000)}" if message.ttl is not None else None,
                    priority=message.priority,
                ),
                timeout=config.timeout,
            )

        @retry(fn_name=f"{self}:send_event", retry_timeouts=config.push_retry_timeouts, exc_filter=_exc_filter)
        async def send_event(event: Event):
            channel = await self._channel()

            if is_debug:
                logger.debug("%s: channel(%s) send\n%s", self, channel, pretty_repr(event.dict()))

            await channel.basic_publish(
                self.serializer.dumps_event(event),
                exchange=config.events_exchange,
                routing_key="events",
                properties=BasicProperties(
                    delivery_mode=2,
                    timestamp=datetime.now(tz=utc),
                    expiration=str(int(event.ttl * 1000)) if event.ttl is not None else None,
                ),
                timeout=config.timeout,
            )

        self._send_task = send_task
        self._push_task_result = push_task_result
        self._send_message = send_message
        self._send_event = send_event

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
        config: Config = self.config

        channel = await self._conn.channel()

        await channel.exchange_declare(
            config.tasks_exchange,
            exchange_type="topic",
            durable=config.tasks_exchange_durable,
            auto_delete=not config.tasks_exchange_durable,
            timeout=config.timeout,
        )

        queue = self._results_common_queue()

        arguments = {"x-queue-type": config.results_common_queue_type.value}
        if config.results_common_queue_ttl is not None:
            arguments["x-expires"] = config.results_common_queue_ttl * 1000

        await channel.queue_declare(
            queue,
            durable=config.results_common_queue_durable,
            auto_delete=not config.results_common_queue_durable,
            arguments=arguments,
            timeout=config.timeout,
        )
        await channel.queue_bind(
            queue,
            config.tasks_exchange,
            routing_key=queue,
            timeout=config.timeout,
        )

    async def _consume_results(self):
        config: Config = self.config

        async def on_msg(channel: aiormq.Channel, msg, no_ack: bool = None):
            try:
                properties = msg.header.properties
                task_id: UUID = UUID(properties.message_id)

                task_result: TaskResult = self.serializer.loads_task_result(msg.body)

                if is_debug:
                    logger.debug(
                        "%s: channel(%s) got task result for task %s\n%s",
                        self,
                        channel,
                        task_id,
                        pretty_repr(task_result.dict()),
                    )

                storage = self._allocate_common_results_storage(task_id)
                storage[1].append(task_result)
                storage[0].set()

                if not no_ack:
                    await channel.basic_ack(msg.delivery.delivery_tag)

                expiration = properties.expiration
                if expiration:
                    get_event_loop().call_later(
                        int(expiration) / 1000,
                        lambda *args: self._cleanup_common_results_storage(task_id),
                    )

            except Exception as e:
                logger.exception(e)

        channel = await self._new_channel()
        await channel.basic_qos(prefetch_count=5, timeout=config.timeout)
        queue: str = self._results_common_queue()
        self._common_results_consumer = (
            channel,
            await channel.basic_consume(
                queue,
                partial(on_msg, channel),
                timeout=config.timeout,
            ),
        )

        if is_debug:
            logger.debug("%s: channel(%s) start consuming results queue '%s'", self, channel, queue)

        channel = self._direct_reply_to_channel = await self._new_channel()
        queue: str = "amq.rabbitmq.reply-to"
        self._direct_reply_to_consumer = (
            channel,
            await channel.basic_consume(
                queue,
                partial(on_msg, channel, no_ack=True),
                no_ack=True,
                timeout=config.timeout,
            ),
        )

        if is_debug:
            logger.debug("%s: channel(%s) start consuming results queue '%s'", self, channel, queue)

    async def _on_conneciton_open(self):
        async def fn():
            await self._declare()
            self._conn_open_ev.set()
            await self._consume_results()

        await self._create_backend_task("_on_connection_open", fn)

    async def _on_connection_lost(self):
        self._conn_open_ev.clear()
        self._declared.clear()

        @retry(exc_filter=_exc_filter)
        async def fn():
            await self._conn.open()

        await self._create_backend_task("_on_connection_lost", fn)

    async def _on_connection_close(self):
        self._conn_open_ev.clear()
        self._declared.clear()
        self._task_consumers.clear()
        self._message_consumers.clear()
        self._events_consumer = ()

    async def on_connection_open(self):
        pass

    async def on_connection_lost(self):
        pass

    async def on_connection_close(self):
        pass

    async def _new_channel(self) -> aiormq.Channel:
        while not self.is_closed:
            channel = await self._conn.new_channel()
            await self._conn_open_ev.wait()
            if not channel.is_closed:
                return channel
        raise Exception(f"{self}: closed")

    async def _channel(self) -> aiormq.Channel:
        while not self.is_closed:
            channel = await self._conn.channel()
            await self._conn_open_ev.wait()
            if not channel.is_closed:
                return channel
        raise Exception(f"{self}: closed")

    def _results_separate_queue(self, task_data: TaskData) -> str:
        return f"{self.config.results_queue_prefix}result.{task_data.task_id}"

    def _results_common_queue(self) -> str:
        return f"{self.config.results_queue_prefix}results.{self.config.id}"

    def _reply_to(self, task_data: TaskData) -> str:
        result_queue_mode = task_data.extra.get("rabbitmq:results_queue_mode") or self.config.results_queue_mode
        if result_queue_mode == ResultQueueMode.SEPARATE:
            return self._results_separate_queue(task_data)
        if result_queue_mode == ResultQueueMode.COMMON:
            return self._results_common_queue()
        return "amq.rabbitmq.reply-to"

    async def _declare_events(self):
        config: Config = self.config

        channel = await self._channel()

        await channel.exchange_declare(
            config.events_exchange,
            exchange_type="topic",
            durable=config.events_exchange_durable,
            auto_delete=not config.events_exchange_durable,
            timeout=config.timeout,
        )

        arguments = {"x-queue-type": config.events_queue_type.value}
        if config.events_queue_ttl is not None:
            arguments["x-expires"] = config.events_queue_ttl * 1000

        await channel.queue_declare(
            f"{config.events_queue_prefix}events.{config.id}",
            durable=config.events_queue_durable,
            auto_delete=not config.events_queue_durable,
            arguments=arguments,
            timeout=config.timeout,
        )
        await channel.queue_bind(
            f"{config.events_queue_prefix}events.{config.id}",
            config.events_exchange,
            routing_key="events",
            timeout=config.timeout,
        )

    async def _declare_tasks_queue(self, queue: str):
        if queue in self._declared:
            return

        config: Config = self.config

        arguments = {
            "x-max-priority": PriorityT.le,
            "x-queue-type": config.tasks_queue_type.value,
        }
        if config.tasks_queue_ttl is not None:
            arguments["x-expires"] = config.tasks_queue_ttl * 1000

        channel = await self._channel()

        await channel.queue_declare(
            queue,
            durable=config.tasks_queue_durable,
            auto_delete=not config.tasks_queue_durable,
            arguments=arguments,
            timeout=config.timeout,
        )
        await channel.queue_bind(
            queue,
            config.tasks_exchange,
            routing_key=queue,
            timeout=config.timeout,
        )

        self._declared.add(queue)

    def _allocate_common_results_storage(self, task_id: UUID) -> str:
        if task_id not in self._common_results_storage:
            self._common_results_storage[task_id] = (asyncio_Event(), [])
        return self._common_results_storage[task_id]

    def _cleanup_common_results_storage(self, task_id: UUID) -> str:
        self._common_results_storage.pop(task_id, None)

    async def _ensure_result_queue(self, task_data: TaskData) -> Tuple[str, str]:
        config: Config = self.config

        exchange = config.tasks_exchange
        routing_key = task_data.extra.get("rabbitmq:reply_to") or self._reply_to(task_data)

        result_queue_mode = task_data.extra.get("rabbitmq:results_queue_mode") or self.config.results_queue_mode
        if result_queue_mode == ResultQueueMode.SEPARATE:
            await self._declare_results_separate_queue(task_data)
        elif result_queue_mode == ResultQueueMode.DIRECT_REPLY_TO:
            exchange = ""

        return exchange, routing_key

    async def _declare_results_separate_queue(self, task_data: TaskData) -> str:
        config: Config = self.config

        queue = routing_key = self._results_separate_queue(task_data)
        durable: bool = task_data.extra.get("rabbitmq:result_queue_durable", config.results_separate_queue_durable)

        channel = await self._channel()

        await channel.queue_declare(
            queue,
            durable=durable,
            auto_delete=not durable,
            arguments={"x-expires": task_data.result_ttl * 1000} if task_data.result_ttl is not None else None,
            timeout=config.timeout,
        )
        await channel.queue_bind(
            queue,
            config.tasks_exchange,
            routing_key=routing_key,
            timeout=config.timeout,
        )

        return queue

    async def stop_consume_results(self):
        if self.__conn.is_closed:
            return

        for aio_task in self._backend_tasks["consume_results"]:
            aio_task.cancel()

        if not self._common_results_consumer:
            return

        channel, consume_ok = self._common_results_consumer
        if not self.__conn.is_closed and not channel.is_closed:
            if is_debug:
                logger.debug("%s: channel(%s) stop consuming results queue", self, channel)
            await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
            await channel.close()

        channel, consume_ok = self._direct_reply_to_consumer
        if not self.__conn.is_closed and not channel.is_closed:
            if is_debug:
                logger.debug("%s: channel(%s) stop consuming results queue", self, channel)
            await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
            await channel.close()

    async def close(self):
        if self.is_closed:
            return
        await self.stop_consume_results()
        await super().close()
        await self.__conn.close()

    async def send_task(self, task_instance: TaskInstance, **kwds):
        if (
            "graph:id" in task_instance.data.extra
            and (task_instance.data.extra.get("rabbitmq:results_queue_mode") or self.config.results_queue_mode)
            == ResultQueueMode.DIRECT_REPLY_TO
        ):
            raise GraphError(f"Unsupported {ResultQueueMode.DIRECT_REPLY_TO}")
        await self._create_backend_task("send_task", lambda: self._send_task(task_instance, **kwds))

    async def consume_tasks(self, queues: List[str], on_task: AsyncCallableT):
        @retry(fn_name=f"{self}:consume_tasks", exc_filter=_exc_filter)
        async def fn(queue: str):
            await self._declare_tasks_queue(queue)

            config: Config = self.config

            async def on_msg(channel: aiormq.Channel, msg: aiormq.abc.DeliveredMessage):
                try:
                    async with self._tasks_semaphore:
                        task_instance: TaskInstance = self.serializer.loads_task_instance(msg.body)
                        task_instance.data.extra["rabbitmq:reply_to"] = msg.header.properties.reply_to
                        ack_late = task_instance.data.ack_late
                        if not ack_late:
                            await channel.basic_ack(msg.delivery.delivery_tag)
                        if is_debug:
                            logger.debug("%s: got\n%s", self, pretty_repr(task_instance.dict()))
                        await shield(on_task(task_instance))
                        if ack_late:
                            await channel.basic_ack(msg.delivery.delivery_tag)
                except Exception as e:
                    logger.exception(e)

            channel = await self._new_channel()
            await channel.basic_qos(prefetch_count=config.tasks_prefetch_count, timeout=config.timeout)

            self._task_consumers[queue] = [
                channel,
                await channel.basic_consume(queue, partial(on_msg, channel), timeout=config.timeout),
            ]
            if is_debug:
                logger.debug("%s: channel(%s) start consuming tasks queue '%s'", self, channel, queue)

        coros = [
            self._create_backend_task(f"consume_tasks_queue_{queue}", partial(fn, queue))
            for queue in queues
            if queue not in self._task_consumers or self._task_consumers[queue][0].is_closed
        ]
        if coros:
            await gather(*coros)

        async def reconsume():
            await self.consume_tasks(list(self._task_consumers.keys()), on_task)

        self.__conn.add_callback("on_lost", "consume_tasks", reconsume)

    async def stop_consume_tasks(self, queues: List[str] = None):
        if self.__conn.is_closed:
            return

        try:
            for queue in list(self._task_consumers.keys()):
                if queues is None or queue in queues:
                    for aio_task in self._backend_tasks["consume_tasks_queue_{queue}"]:
                        aio_task.cancel()
                    channel, consume_ok = self._task_consumers[queue]
                    if not self.__conn.is_closed and not channel.is_closed:
                        if is_debug:
                            logger.debug("%s: channel(%s) stop consuming queue '%s'", self, channel, queue)
                        await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
                        await channel.close()
                    self._task_consumers.pop(queue, None)
                    self._declared.discard(queue)
        finally:
            if queues is None:
                self._task_consumers.clear()
            if not self._task_consumers:
                self.__conn.remove_callback("on_lost", "consume_tasks")

    async def push_task_result(self, task_instance: TaskInstance, task_result: TaskResult):
        await self._create_backend_task("push_task_result", lambda: self._push_task_result(task_instance, task_result))

    async def pop_task_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        task_data: TaskData = task_instance.data

        if not task_data.result_return:
            raise TaskResultError(f"{task_data.task_id}")

        result_queue_mode = task_data.extra.get("rabbitmq:results_queue_mode") or self.config.results_queue_mode

        if result_queue_mode in (ResultQueueMode.COMMON, ResultQueueMode.DIRECT_REPLY_TO):
            pop_fn = self._pop_task_results_from_common_queue
        else:
            pop_fn = self._pop_task_results_from_separate_queue

        async for task_result in pop_fn(task_instance):
            yield task_result

    async def _pop_task_results_from_common_queue(self, task_instance: TaskInstance):
        task_data: TaskData = task_instance.data
        task_id: UUID = task_data.task_id

        async def fn():
            func = task_instance.task.func

            if task_data.extra.get("graph:graph") or isasyncgenfunction(func) or isgeneratorfunction(func):

                while not self.is_closed:

                    if task_id not in self._common_results_storage:
                        raise TaskResultError(f"Result for {task_id}({task_instance.task.name}) expired")

                    ev, results = self._common_results_storage[task_id]
                    await ev.wait()
                    ev.clear()

                    while results:
                        task_result: TaskResult = results.pop(0)

                        if isinstance(task_result.exc, TaskClosedError):
                            return

                        if is_debug:
                            logger.debug("%s: pop result for %s(%s)", self, task_id, task_instance.task.name)

                        yield task_result

            else:

                ev, results = self._common_results_storage[task_id]
                await ev.wait()
                ev.clear()

                if is_debug:
                    logger.debug("%s: pop result for %s(%s)", self, task_id, task_instance.task.name)

                yield results.pop(0)

        __anext__ = fn().__anext__

        self._allocate_common_results_storage(task_id)

        idx_data: [str, int] = {}

        try:
            while not self.is_closed:
                task_result: TaskResult = await self._create_backend_task("pop_task_result", __anext__)
                idx = task_result.idx
                if idx:
                    idx_0, idx_1 = idx
                    if idx_0 not in idx_data:
                        idx_data[idx_0] = idx_1
                    else:
                        idx_data[idx_0] += 1
                    if idx_data[idx_0] != idx_1:
                        raise TaskResultError(
                            f"Unexpected result index for task {task_id}. Expect {idx_data[idx_0]}, got {idx_1}"
                        )
                yield task_result
        except StopAsyncIteration:
            return
        finally:
            self._cleanup_common_results_storage(task_id)

    async def _pop_task_results_from_separate_queue(self, task_instance: TaskInstance) -> TaskResult:
        task_data: TaskData = task_instance.data
        task_id: UUID = task_data.task_id

        @retry(
            fn_name=f"{self}:pop_task_results_from_separate_queue",
            retry_timeouts=self.config.pull_retry_timeouts,
            exc_filter=_exc_filter,
        )
        async def fn():
            queue = await self._declare_results_separate_queue(task_data)

            fut: asyncio_Future = None
            results: List[TaskResult] = []

            def on_conn_error():
                if not fut.done():
                    fut.set_exception(ConnectionError)

            async def on_result(channel: aiormq.Channel, msg: aiormq.abc.DeliveredMessage):
                try:
                    if is_debug:
                        logger.debug("%s: pop result for %s(%s)", self, task_id, task_instance.task.name)
                    results.append(self.serializer.loads_task_result(msg.body))
                    if not fut.done():
                        fut.set_result(None)
                    await channel.basic_ack(msg.delivery.delivery_tag)
                except Exception as e:
                    if not fut.done():
                        fut.set_exception(e)

            self._conn.add_callback("on_lost", task_id, on_conn_error)
            self._conn.add_callback("on_close", task_id, on_conn_error)

            try:
                func = task_instance.task.func

                while not self.is_closed:
                    fut = asyncio_Future()

                    channel = await self._new_channel()
                    consume_ok = await channel.basic_consume(
                        queue, partial(on_result, channel), timeout=self.config.timeout
                    )

                    if is_debug:
                        logger.debug("%s: channel(%s) start consuming result queue '%s'", self, channel, queue)

                    try:

                        if task_data.extra.get("graph:graph") or isasyncgenfunction(func) or isgeneratorfunction(func):

                            while not self.is_closed:

                                await fut
                                fut = asyncio_Future()
                                while results:
                                    task_result: TaskResult = results.pop(0)
                                    if isinstance(task_result.exc, TaskClosedError):
                                        return
                                    yield task_result

                        else:

                            await fut
                            yield results.pop(0)

                        return

                    except ConnectionError:
                        await channel.close()
                        continue

                    except Exception as e:
                        logger.exception(e)
                        raise

                    finally:

                        if not self.__conn.is_closed and not channel.is_closed:
                            await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
                            await channel.close()

            finally:
                self.__conn.remove_callback("on_lost", task_id)
                self.__conn.remove_callback("on_close", task_id)

        gen = fn()

        idx_data: [str, int] = {}

        try:
            while not self.is_closed:
                task_result: TaskResult = await self._create_backend_task(f"pop_task_result.{task_id}", gen.__anext__)
                idx = task_result.idx
                if idx:
                    idx_0, idx_1 = idx
                    if idx_0 not in idx_data:
                        idx_data[idx_0] = idx_1
                    else:
                        idx_data[idx_0] += 1
                    if idx_data[idx_0] != idx_1:
                        raise TaskResultError(
                            f"Unexpected result index for task {task_id}. Expect {idx_data[idx_0]}, got {idx_1}"
                        )
                yield task_result

        except StopAsyncIteration:
            return
        finally:
            try:
                queue = self._results_separate_queue(task_data)
                channel = await self._channel()
                await channel.queue_delete(queue)
            except Exception as e:
                logger.exception(e)

    async def close_task(self, task_instance: TaskInstance, idx: Tuple[str, int] = None):
        task_data: TaskData = task_instance.data

        if is_debug:
            logger.debug("%s: close task %s(%s)", self, task_data.task_id, task_instance.task.name)

        result_queue_mode = task_data.extra.get("rabbitmq:results_queue_mode") or self.config.results_queue_mode
        if result_queue_mode == ResultQueueMode.SEPARATE:
            await self._declare_results_separate_queue(task_data)

        await self.push_task_result(task_instance, TaskResult(exc=TaskClosedError(), idx=idx))

    async def send_message(
        self,
        message: Message,
        *,
        routing_key: str,
        delivery_mode: int = None,
        **kwds,
    ):  # pylint: disable=(arguments-differ
        await self._create_backend_task(
            "send_message",
            lambda: self._send_message(
                message,
                routing_key=routing_key,
                delivery_mode=delivery_mode,
            ),
        )

    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        @retry(fn_name=f"{self}:consume_messages", exc_filter=_exc_filter)
        async def fn(queue: str):
            config: Config = self.config

            async def on_msg(channel: aiormq.Channel, msg):
                try:
                    data = {
                        "data": self.serializer.loads(msg.body),
                        "message_id": msg.header.properties.message_id,
                        "exchange": msg.delivery.exchange,
                        "priority": msg.header.properties.priority,
                        "ttl": f"{int(msg.header.properties.expiration) // 1000}"
                        if msg.header.properties.expiration
                        else None,
                    }
                    message = Message(**data)
                    if is_debug:
                        logger.debug("%s: got\n%s", self, pretty_repr(message.dict()))
                    ack_late = message.ack_late
                    if not ack_late:
                        await channel.basic_ack(msg.delivery.delivery_tag)
                    await shield(on_message(message))
                    if ack_late:
                        await channel.basic_ack(msg.delivery.delivery_tag)
                except Exception as e:
                    logger.exception(e)

            channel = await self._new_channel()
            await channel.basic_qos(prefetch_count=config.messages_prefetch_count, timeout=config.timeout)

            self._message_consumers[queue] = [
                channel,
                await channel.basic_consume(queue, partial(on_msg, channel), timeout=config.timeout),
            ]
            if is_debug:
                logger.debug("%s: channel(%s) start consuming messages queue '%s'", self, channel, queue)

        coros = [
            self._create_backend_task(f"consume_messages_queue_{queue}", partial(fn, queue))
            for queue in queues
            if queue not in self._message_consumers or self._message_consumers[queue][0].is_closed
        ]
        if coros:
            await gather(*coros)

        async def reconsume_messages():
            await self.consume_messages(list(self._message_consumers.keys()), on_message)

        self._conn.add_callback("on_lost", "consume_messages", reconsume_messages)

    async def stop_consume_messages(self, queues: List[str] = None):
        if self.__conn.is_closed:
            return

        try:
            for queue in list(self._message_consumers.keys()):
                if queues is None or queue in queues:
                    for aio_task in self._backend_tasks["consume_messages_queue_{queue}"]:
                        aio_task.cancel()
                    channel, consume_ok = self._message_consumers[queue]
                    if not self.__conn.is_closed and not channel.is_closed:
                        if is_debug:
                            logger.debug("%s: channel(%s) stop consuming messages queue '%s'", self, channel, queue)
                        await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
                        await channel.close()
                    self._message_consumers.pop(queue, None)
        finally:
            if queues is None:
                self._message_consumers.clear()
            if not self._message_consumers:
                self.__conn.remove_callback("on_lost", "consume_messages")

    async def send_event(self, event: Event):
        await self._create_backend_task("send_event", lambda: self._send_event(event))

    async def consume_events(self, cb_id: str, cb: Union[Callable, AsyncCallableT], event_types: List[str] = None):
        self._event_callbacks[cb_id] = (cb, event_types)

        if self._events_consumer:
            return

        async def cb_task(event: Event):
            try:
                await cb(event)
            except Exception as e:
                logger.exception(e)

        config: Config = self.config

        loads_event = self.serializer.loads_event
        event_callbacks = self._event_callbacks
        create_backend_task = self._create_backend_task

        @retry(fn_name=f"{self}:consume_events", exc_filter=_exc_filter)
        async def fn():
            if not self.__conn.is_open:
                await self.__conn.open()

            await self._declare_events()

            async def on_msg(channel: aiormq.Channel, msg):
                try:
                    event: Event = loads_event(msg.body)

                    if is_debug:
                        logger.debug("%s: got event\n%s", self, pretty_repr(event.dict()))

                    ack_late = False
                    if not ack_late:
                        await channel.basic_ack(msg.delivery.delivery_tag)

                    for cb, event_types in event_callbacks.values():
                        if event_types is not None and event.type not in event_types:
                            continue
                        if iscoroutinefunction(cb):
                            create_backend_task("event_cb", partial(cb_task, event))
                        else:
                            cb(event)

                    if ack_late:
                        await channel.basic_ack(msg.delivery.delivery_tag)
                except Exception as e:
                    logger.exception(e)

            channel = await self._new_channel()
            await channel.basic_qos(prefetch_count=config.events_prefetch_count, timeout=config.timeout)

            self._events_consumer = (
                channel,
                await channel.basic_consume(
                    f"{config.events_queue_prefix}events.{config.id}",
                    partial(on_msg, channel),
                    timeout=config.timeout,
                ),
            )
            if is_debug:
                logger.debug(
                    "%s: channel(%s) satrt consuming events queue '%s'",
                    self,
                    channel,
                    f"{config.events_queue_prefix}events.{config.id}",
                )

        await self._create_backend_task("consume_events", fn)

        async def reconsume_events():
            for cb_id, (cb, event_types) in self._event_callbacks.items():
                await self.consume_events(cb_id, cb, event_types=event_types)

        self._conn.add_callback("on_lost", "consume_events", reconsume_events)

    async def stop_consume_events(self, cb_id: str = None):
        if cb_id:
            self._event_callbacks.pop(cb_id, None)
            if self._event_callbacks:
                return

        self._event_callbacks = {}

        if self.__conn.is_closed:
            return

        self.__conn.remove_callback("on_lost", "consume_events")

        for aio_task in self._backend_tasks["consume_events"]:
            aio_task.cancel()

        if not self._events_consumer:
            return

        channel, consume_ok = self._events_consumer
        if not self.__conn.is_closed and not channel.is_closed:
            if is_debug:
                logger.debug(
                    "%s: channel(%s) stop consuming events queue '%s'",
                    self,
                    channel,
                    f"{self.config.events_queue_prefix}events.{self.config.id}",
                )
            await channel.basic_cancel(consume_ok.consumer_tag, timeout=self.config.timeout)
            await channel.close()

        self._events_consumer = ()
