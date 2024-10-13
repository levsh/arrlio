import asyncio
import logging

from asyncio import Semaphore, create_task
from collections import defaultdict
from functools import partial
from time import monotonic
from typing import Callable, Coroutine
from uuid import uuid4

from pydantic import Field, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio import gettext, settings
from arrlio.abc import AbstractBroker
from arrlio.models import TaskInstance
from arrlio.settings import ENV_PREFIX
from arrlio.types import TASK_MAX_PRIORITY, TASK_MIN_PRIORITY
from arrlio.utils import AioTasksRunner, Closable, is_debug_level


_ = gettext.gettext


logger = logging.getLogger("arrlio.backends.brokers.local")


POOL_SIZE = 100


class Config(BaseSettings):
    """
    Local `Broker` config.

    Atributes:
        id: Broker Id.
        pool_size: Tasks pool size.
            .. caution:: Maybe removed in the future.
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}LOCAL_BROKER_")

    id: str = Field(default_factory=lambda: f"{uuid4().hex[-4:]}")
    pool_size: PositiveInt = Field(default_factory=lambda: POOL_SIZE)


class Broker(Closable, AbstractBroker):
    """
    Local `Broker`.

    Args:
        config: `Broker` config.
    """

    def __init__(self, config: Config):
        super().__init__()

        self.config = config

        self._internal_tasks_runner = AioTasksRunner()

        self._queues = defaultdict(asyncio.PriorityQueue)

        self._consumed_queues = set()

        self._semaphore = Semaphore(value=config.pool_size)

    def __str__(self):
        return f"Broker[local#{self.config.id}]"

    def __repr__(self):
        return self.__str__()

    async def init(self):
        return

    async def close(self):
        await self._internal_tasks_runner.close()
        await super().close()

    async def send_task(self, task_instance: TaskInstance, **kwds):
        if is_debug_level():
            logger.debug(
                _("%s send task\n%s"),
                self,
                task_instance.pretty_repr(sanitize=settings.LOG_SANITIZE),
            )

        self._queues[task_instance.queue].put_nowait(
            (
                (
                    (TASK_MAX_PRIORITY - min(TASK_MAX_PRIORITY, task_instance.priority))
                    if task_instance.priority
                    else TASK_MIN_PRIORITY
                ),
                monotonic(),
                task_instance.ttl,
                task_instance,
            )
        )

    async def consume_tasks(self, queues: list[str], callback: Callable[[TaskInstance], Coroutine]):

        async def fn(queue: str):
            logger.info(_("%s start consuming tasks queue '%s'"), self, queue)

            semaphore_acquire = self._semaphore.acquire
            semaphore_release = self._semaphore.release
            queue_get = self._queues[queue].get

            self._consumed_queues.add(queue)

            try:
                while not self.is_closed:
                    try:
                        await semaphore_acquire()

                        try:
                            priority, ts, ttl, task_instance = await queue_get()

                            if ttl is not None and monotonic() >= ts + ttl:
                                continue

                            if is_debug_level():
                                logger.debug(
                                    _("%s got task\n%s"),
                                    self,
                                    task_instance.pretty_repr(sanitize=settings.LOG_SANITIZE),
                                )

                            aio_task = create_task(callback(task_instance))

                        except (BaseException, Exception) as e:
                            semaphore_release()
                            raise e

                        aio_task.add_done_callback(lambda *args: semaphore_release())

                    except asyncio.CancelledError:
                        logger.info(_("%s stop consuming tasks queue '%s'"), self, queue)
                        return
                    except Exception as e:
                        logger.exception(e)
            finally:
                self._consumed_queues.discard(queue)

        for queue in queues:
            if queue not in self._consumed_queues:
                self._internal_tasks_runner.create_task(f"consume_queue_[{queue}]", partial(fn, queue))

    async def stop_consume_tasks(self, queues: list[str] | None = None):
        for queue in self._consumed_queues:
            if queues is None or queue in queues:
                self._internal_tasks_runner.cancel_tasks(f"consume_queue_[{queue}]")
