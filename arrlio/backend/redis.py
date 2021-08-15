import asyncio
import itertools
import logging
from typing import Iterable, List

import siderpy
from pydantic import Field

from arrlio import core
from arrlio.backend import base
from arrlio.exc import TaskNoResultError
from arrlio.models import TaskInstance, TaskResult
from arrlio.typing import AsyncCallableT, PositiveIntT, RedisDsn, SerializerT, TimeoutT


logger = logging.getLogger("arrlio")


SERIALIZER: str = "arrlio.serializer.json.Json"
URL: str = "redis://localhost?db=0"
TIMEOUT: int = 60
CONNECT_TIMEOUT: int = 30
POOL_SIZE: int = 10
RETRY_TIMEOUTS: Iterable[int] = None
VERIFY_SSL: bool = True


class BackendConfig(base.BackendConfig):
    serializer: SerializerT = Field(default_factory=lambda: SERIALIZER)
    url: RedisDsn = Field(default_factory=lambda: URL)
    timeout: TimeoutT = Field(default_factory=lambda: TIMEOUT)
    connect_timeout: TimeoutT = Field(default_factory=lambda: CONNECT_TIMEOUT)
    retry_timeouts: List = Field(default_factory=lambda: RETRY_TIMEOUTS)
    pool_size: PositiveIntT = Field(default_factory=lambda: POOL_SIZE)
    verify_ssl: bool = Field(default_factory=lambda: True)

    class Config:
        validate_assignment = True
        env_prefix = "ARRLIO_REDIS_BACKEND_"


class Backend(base.Backend):
    def __init__(self, config: BackendConfig):
        super().__init__(config)
        self.redis_pool = siderpy.RedisPool(
            config.url.get_secret_value(),
            connect_timeout=config.connect_timeout,
            timeout=config.timeout,
            size=config.pool_size,
        )
        self._consumers = {}

    def __str__(self):
        return f"[RedisBackend[{self.redis_pool}]]"

    async def close(self):
        await super().close()
        await self.redis_pool.close()

    def _make_queue_key(self, queue: str) -> str:
        return f"q.{queue}"

    def _make_result_key(self, task_id: str) -> str:
        return f"r.{task_id}"

    @base.Backend.task
    async def send_task(self, task_instance: TaskInstance, encrypt: bool = None, **kwds):
        queue = task_instance.data.queue
        queue_key = self._make_queue_key(queue)
        data = self.serializer.dumps_task_instance(task_instance)

        async with self.redis_pool.get_redis() as redis:
            with redis.pipeline():
                await redis.multi()
                await redis.setex(f"{task_instance.data.task_id}", task_instance.data.ttl, data)
                await redis.rpush(queue_key, f"{task_instance.data.priority}|{task_instance.data.task_id}")
                if task_instance.data.priority:
                    await redis.sort(queue, "BY", "*", "ASC", "STORE", queue)
                await redis.execute()
                await redis.pipeline_execute()

    @base.Backend.task
    async def consume_tasks(self, queues: List[str], on_task: AsyncCallableT):
        async def consume_queue(queue):
            logger.debug("%s: consuming queue '%s'", self, queue)
            queue_key = self._make_queue_key(queue)
            while True:
                try:
                    _, queue_value = await self.redis_pool.blpop(queue_key, 0)
                    priority, task_id = queue_value.decode().split("|")
                    serialized_data = await self.redis_pool.get(task_id)
                    if serialized_data is None:
                        logger.warning("Task %s expired", task_id)
                        continue
                    task_instance = self.serializer.loads_task_instance(serialized_data)
                    await asyncio.shield(on_task(task_instance))
                except asyncio.CancelledError:
                    break
                except (ConnectionError, TimeoutError) as e:
                    logger.error("%s: %s %s", self, e.__class__, e)
                    retry_timeouts = (
                        iter(self.config.retry_timeouts) if self.config.retry_timeouts else itertools.repeat(1)
                    )
                    seconds = next(retry_timeouts, None)
                    if seconds is None:
                        raise e
                    await asyncio.sleep(seconds)
                except Exception:
                    logger.exception("Internal error")
                    await asyncio.sleep(5)

        for queue in queues:
            self._consumers[queue] = asyncio.create_task(consume_queue(queue))

    async def stop_consume_tasks(self):
        for queue, task in self._consumers.items():
            logger.debug("%s: stop consuming queue '%s'", self, queue)
            task.cancel()
        self._consumers = {}

    @base.Backend.task
    async def push_task_result(self, task_instance: core.TaskInstance, task_result: TaskResult, encrypt: bool = None):
        if not task_instance.task.result_return:
            raise TaskNoResultError(task_instance.data.task_id)
        result_key = self._make_result_key(task_instance.data.task_id)

        async with self.redis_pool.get_redis() as redis:
            with redis.pipeline():
                await redis.multi()
                await redis.rpush(result_key, self.serializer.dumps_task_result(task_result))
                await redis.expire(result_key, task_instance.data.result_ttl)
                await redis.execute()
                await redis.pipeline_execute()

    @base.Backend.task
    async def pop_task_result(self, task_instance: TaskInstance) -> TaskResult:
        result_key = self._make_result_key(task_instance.data.task_id)
        raw_data = await self.redis_pool.blpop(result_key, 0)
        return self.serializer.loads_task_result(raw_data[1])

    async def send_message(message: dict):
        raise NotImplementedError()

    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        raise NotImplementedError()

    async def stop_consume_messages(self):
        return
