import asyncio
import dataclasses
import itertools
import logging
from asyncio import Semaphore, create_task
from functools import partial
from typing import Iterable, List, Optional

import siderpy  # pylint: disable=import-error
from pydantic import Field, PositiveInt

from arrlio import core
from arrlio.backends import base
from arrlio.exc import TaskNoResultError
from arrlio.models import Event, Message, TaskData, TaskInstance, TaskResult
from arrlio.settings import ENV_PREFIX
from arrlio.tp import AsyncCallableT, RedisDsn, SerializerT, TimeoutT
from arrlio.utils import retry

logger = logging.getLogger("arrlio.backends.redis")

BACKEND_NAME: str = "arrlio"
SERIALIZER: str = "arrlio.serializers.json"
URL: str = "redis://localhost?db=0"
TIMEOUT: int = 60
CONNECT_TIMEOUT: int = 30
CONN_POOL_SIZE: int = 10
VERIFY_SSL: bool = True
PUSH_RETRY_TIMEOUTS: Iterable[int] = [5, 5, 5, 5]
PULL_RETRY_TIMEOUTS: Iterable[int] = itertools.repeat(5)
POOL_SIZE: int = 100


class BackendConfig(base.BackendConfig):
    name: Optional[str] = Field(default_factory=lambda: BACKEND_NAME)
    serializer: SerializerT = Field(default_factory=lambda: SERIALIZER)
    url: RedisDsn = Field(default_factory=lambda: URL)
    timeout: Optional[TimeoutT] = Field(default_factory=lambda: TIMEOUT)
    connect_timeout: Optional[TimeoutT] = Field(default_factory=lambda: CONNECT_TIMEOUT)
    conn_pool_size: Optional[PositiveInt] = Field(default_factory=lambda: CONN_POOL_SIZE)
    push_retry_timeouts: Optional[Iterable] = Field(default_factory=lambda: PUSH_RETRY_TIMEOUTS)
    pull_retry_timeouts: Optional[Iterable] = Field(default_factory=lambda: PULL_RETRY_TIMEOUTS)
    verify_ssl: Optional[bool] = Field(default_factory=lambda: True)
    pool_size: Optional[PositiveInt] = Field(default_factory=lambda: POOL_SIZE)

    class Config:
        env_prefix = f"{ENV_PREFIX}REDIS_BACKEND_"


class Backend(base.Backend):
    def __init__(self, config: BackendConfig):
        super().__init__(config)
        self.redis_pool = siderpy.RedisPool(
            config.url.get_secret_value(),
            connect_timeout=config.connect_timeout,
            timeout=config.timeout,
            size=config.conn_pool_size,
        )
        self._consumed_task_queues = set()
        self._consumed_message_queues = set()
        self._semaphore = Semaphore(value=config.pool_size)

    def __del__(self):
        if not self.is_closed:
            logger.warning("%s: unclosed", self)

    def __str__(self):
        return f"RedisBackend[{self.redis_pool}]"

    async def close(self):
        if self.is_closed:
            return
        await super().close()
        await self.redis_pool.close()

    def _make_task_queue_key(self, queue: str) -> str:
        return f"q.t.{queue}"

    def _make_result_key(self, task_id: str) -> str:
        return f"r.t.{task_id}"

    def _make_message_queue_key(self, queue: str) -> str:
        return f"q.m.{queue}"

    async def send_task(self, task_instance: TaskInstance, **kwds):
        task_data: TaskData = task_instance.data
        queue = task_data.queue
        queue_key = self._make_task_queue_key(queue)
        data = self.serializer.dumps_task_instance(task_instance)

        @retry(retry_timeouts=self.config.push_retry_timeouts)
        async def fn():
            logger.debug("%s: put %s", self, task_instance)
            async with self.redis_pool.get_redis() as redis:
                with redis.pipeline():
                    await redis.multi()
                    await redis.setex(f"{task_data.task_id}", task_data.ttl, data)
                    await redis.rpush(queue_key, f"{task_data.priority}|{task_data.task_id}")
                    if task_data.priority:
                        await redis.sort(queue, "BY", "*", "ASC", "STORE", queue)
                    await redis.execute()
                    await redis.pipeline_execute()

        await self._create_backend_task("send_task", fn)

    async def consume_tasks(self, queues: List[str], on_task: AsyncCallableT):
        @retry()
        async def fn(queue: str):
            logger.info("%s: start consuming tasks queue '%s'", self, queue)
            queue_key = self._make_task_queue_key(queue)

            semaphore_acquire = self._semaphore.acquire
            semaphore_release = self._semaphore.release
            redis_pool = self.redis_pool
            loads_task_instance = self.serializer.loads_task_instance

            try:
                while True:
                    try:
                        await semaphore_acquire()
                        try:
                            _, queue_value = await redis_pool.blpop(queue_key, 0)
                            _, task_id = queue_value.decode().split("|")
                            serialized_data = await redis_pool.get(task_id)
                            if serialized_data is None:
                                continue
                            task_instance: TaskInstance = loads_task_instance(serialized_data)
                            logger.debug("%s: got %s", self, task_instance)
                            tsk: asyncio.Task = create_task(on_task(task_instance))
                            tsk.add_done_callback(lambda *args: semaphore_release())
                        except (BaseException, Exception) as e:
                            semaphore_release()
                            raise e
                    except asyncio.CancelledError:
                        logger.info("%s: stop consuming tasks queue '%s'", self, queue)
                        raise
                    except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                        raise e
                    except Exception as e:
                        logger.exception(e)
            finally:
                self._consumed_task_queues.discard(queue)

        for queue in queues:
            if queue not in self._consumed_task_queues:
                self._create_backend_task(f"consume_tasks_queue_{queue}", partial(fn, queue))
                self._consumed_task_queues.add(queue)

    async def stop_consume_tasks(self, queues: List[str] = None):
        for queue in self._consumed_task_queues:
            if queues is None or queue in queues:
                self._cancel_backend_tasks(f"consume_tasks_queue_{queue}")

    async def push_task_result(self, task_instance: core.TaskInstance, task_result: TaskResult):
        task_data: TaskData = task_instance.data

        if not task_data.result_return:
            return

        result_key = self._make_result_key(task_data.task_id)

        @retry(retry_timeouts=self.config.push_retry_timeouts)
        async def fn():
            async with self.redis_pool.get_redis() as redis:
                with redis.pipeline():
                    await redis.multi()
                    await redis.rpush(
                        result_key,
                        self.serializer.dumps_task_result(task_instance, task_result),
                    )
                    await redis.expire(result_key, task_data.result_ttl)
                    await redis.execute()
                    await redis.pipeline_execute()

        await self._create_backend_task("push_task_result", fn)

    async def pop_task_result(self, task_instance: TaskInstance) -> TaskResult:
        task_data: TaskData = task_instance.data

        if not task_data.result_return:
            raise TaskNoResultError(f"{task_data.task_id}")

        result_key = self._make_result_key(task_data.task_id)

        @retry(retry_timeouts=self.config.pull_retry_timeouts)
        async def fn():
            raw_data = await self.redis_pool.blpop(result_key, 0)
            return self.serializer.loads_task_result(raw_data[1])

        return await self._create_backend_task("pop_task_result", fn)

    async def send_message(self, message: Message, **kwds):
        queue = message.exchange
        queue_key = self._make_message_queue_key(queue)
        data = self.serializer.dumps(dataclasses.asdict(message))

        @retry(retry_timeouts=self.config.push_retry_timeouts)
        async def fn():
            logger.debug("%s: put %s", self, message)
            async with self.redis_pool.get_redis() as redis:
                with redis.pipeline():
                    await redis.multi()
                    await redis.setex(f"{message.message_id}", message.ttl, data)
                    await redis.rpush(queue_key, f"{message.priority}|{message.message_id}")
                    if message.priority:
                        await redis.sort(queue, "BY", "*", "ASC", "STORE", queue)
                    await redis.execute()
                    await redis.pipeline_execute()

        await self._create_backend_task("send_message", fn)

    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        @retry()
        async def fn(queue):
            logger.info("%s: start consuming messages queue '%s'", self, queue)
            queue_key = self._make_message_queue_key(queue)
            self._consumed_message_queues.add(queue)

            semaphore_acquire = self._semaphore.acquire
            semaphore_release = self._semaphore.release
            loads = self.serializer.loads
            try:
                while True:
                    try:
                        await semaphore_acquire()
                        try:
                            _, queue_value = await self.redis_pool.blpop(queue_key, 0)
                            _, message_id = queue_value.decode().split("|")
                            serialized_data = await self.redis_pool.get(message_id)
                            if serialized_data is None:
                                continue
                            data = loads(serialized_data)
                            message = Message(**data)
                            logger.debug("%s: got %s", self, message)
                            tsk: asyncio.Task = create_task(on_message(message))
                        except (BaseException, Exception) as e:
                            semaphore_release()
                            raise e
                        tsk.add_done_callback(lambda *args: semaphore_release())
                    except asyncio.CancelledError:
                        logger.info("%s: stop consuming messages queue '%s'", self, queue)
                        break
                    except (ConnectionError, TimeoutError) as e:
                        raise e
                    except Exception as e:
                        logger.exception(e)
            finally:
                self._consumed_message_queues.discard(queue)

        for queue in queues:
            if queue not in self._consumed_message_queues:
                self._create_backend_task(f"consume_messages_queue_{queue}", partial(fn, queue))

    async def stop_consume_messages(self, queues: List[str] = None):
        for queue in list(self._consumed_message_queues):
            if queues is None or queue in queues:
                self._cancel_backend_tasks(f"consume_messages_queue_{queue}")

    async def send_event(self, event: Event):
        queue_key = "arrlio.events"
        data = self.serializer.dumps_event(event)

        @retry(retry_timeouts=self.config.push_retry_timeouts)
        async def fn():
            async with self.redis_pool.get_redis() as redis:
                with redis.pipeline():
                    await redis.multi()
                    await redis.setex(f"{event.event_id}", event.ttl, data)
                    await redis.rpush(queue_key, f"{event.event_id}")
                    await redis.execute()
                    await redis.pipeline_execute()

        await self._create_backend_task("push_event", fn)

    async def consume_events(self, on_event: AsyncCallableT):
        if "consume_events" in self._backend_tasks:
            raise Exception("Already consuming")

        @retry(retry_timeouts=self.config.pull_retry_timeouts)
        async def fn():
            logger.info("%s: start consuming events", self)
            queue_key = "arrlio.events"

            loads_event = self.serializer.loads_event
            while True:
                try:
                    _, queue_value = await self.redis_pool.blpop(queue_key, 0)
                    event_id = queue_value.decode()
                    serialized_data = await self.redis_pool.get(event_id)
                    if serialized_data is None:
                        continue
                    event = loads_event(serialized_data)
                    logger.debug("%s: got %s", self, event)
                    create_task(on_event(event))
                except asyncio.CancelledError:
                    logger.info("%s: stop consuming events")
                    break
                except (ConnectionError, asyncio.TimeoutError, TimeoutError) as e:
                    raise e
                except Exception as e:
                    logger.exception(e)

        self._create_backend_task("consume_events", fn)

    async def stop_consume_events(self):
        self._cancel_backend_tasks("consume_events")
