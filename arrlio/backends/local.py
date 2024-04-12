import asyncio
import logging
from asyncio import Event as asyncio_Event
from asyncio import Semaphore, create_task, get_event_loop
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from functools import partial
from inspect import isasyncgenfunction, iscoroutinefunction, isgeneratorfunction
from time import monotonic
from typing import Any, AsyncGenerator, Callable, Coroutine
from uuid import UUID

from pydantic import Field, PositiveInt
from pydantic_settings import SettingsConfigDict

from arrlio import settings
from arrlio.backends import base
from arrlio.exceptions import TaskClosedError, TaskResultError
from arrlio.models import Event, TaskInstance, TaskResult
from arrlio.settings import ENV_PREFIX
from arrlio.types import TASK_MAX_PRIORITY, TASK_MIN_PRIORITY, AsyncCallable
from arrlio.utils import is_debug_level, is_info_level

logger = logging.getLogger("arrlio.backends.local")


BACKEND_ID = "arrlio"
SERIALIZER = "arrlio.serializers.nop"
POOL_SIZE = 100


class Config(base.Config):
    """Local backend config."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}LOCAL_")

    id: str = Field(default_factory=lambda: BACKEND_ID)
    pool_size: PositiveInt = Field(default_factory=lambda: POOL_SIZE)
    """.. caution:: Maybe removed in the future."""


class Backend(base.Backend):
    """Local backend."""

    __shared: dict = {}

    def __init__(self, config: Config):
        """
        Args:
            config: backend config.
        """

        super().__init__(config)

        shared: dict = self.__shared
        if config.id not in shared:
            shared[config.id] = {
                "refs": 0,
                "task_queues": defaultdict(asyncio.PriorityQueue),
                "message_queues": defaultdict(asyncio.Queue),
                "results": {},
                "events": {},
                "event_cond": asyncio.Condition(),
            }
        shared = shared[config.id]
        shared["refs"] += 1
        self._task_queues = shared["task_queues"]
        self._results = shared["results"]
        self._message_queues = shared["message_queues"]
        self._events = shared["events"]
        self._event_cond = shared["event_cond"]
        self._consumed_task_queues = set()
        self._consumed_message_queues = set()
        self._pool_semaphore = Semaphore(value=config.pool_size)
        self._event_callbacks: dict[str, tuple[AsyncCallable, list[str] | None]] = {}

    def __del__(self):
        if self.config.id in self.__shared:
            self._refs = max(0, self._refs - 1)
            if self._refs == 0:
                del self.__shared[self.config.id]

    def __str__(self):
        return f"Backend[{self.config.id}]"

    @property
    def _shared(self) -> dict:
        return self.__shared[self.config.id]

    @property
    def _refs(self) -> int:
        return self._shared["refs"]

    @_refs.setter
    def _refs(self, value: int):
        self._shared["refs"] = value

    async def send_task(self, task_instance: TaskInstance, **kwds):
        if task_instance.result_return and task_instance.task_id not in self._results:
            self._results[task_instance.task_id] = [asyncio_Event(), [], None]

        self._task_queues[task_instance.queue].put_nowait(
            (
                (TASK_MAX_PRIORITY - task_instance.priority) if task_instance.priority else TASK_MIN_PRIORITY,
                monotonic(),
                task_instance.ttl,
                self.serializer.dumps_task_instance(task_instance),
            )
        )

    async def consume_tasks(self, queues: list[str], callback: Callable[[TaskInstance], Coroutine]):
        async def fn(queue: str):
            if is_info_level():
                logger.info("%s start consuming tasks queue '%s'", self, queue)

            semaphore = self._pool_semaphore
            semaphore_acquire = semaphore.acquire
            semaphore_release = semaphore.release
            task_queue_get = self._task_queues[queue].get

            self._consumed_task_queues.add(queue)
            try:
                while not self.is_closed:
                    try:
                        await semaphore_acquire()

                        try:
                            _, ts, ttl, data = await task_queue_get()
                            if ttl is not None and monotonic() >= ts + ttl:
                                continue

                            task_instance: TaskInstance = self.serializer.loads_task_instance(data)

                            if is_debug_level():
                                logger.debug(
                                    "%s got task\n%s",
                                    self,
                                    task_instance.pretty_repr(sanitize=settings.LOG_SANITIZE),
                                )

                            aio_task: asyncio.Task = create_task(callback(task_instance))

                        except (BaseException, Exception) as e:
                            semaphore_release()
                            raise e

                        aio_task.add_done_callback(lambda *args: semaphore_release())

                    except asyncio.CancelledError:
                        if is_info_level():
                            logger.info("%s stop consuming tasks queue '%s'", self, queue)
                        return
                    except Exception as e:
                        logger.exception(e)
            finally:
                self._consumed_task_queues.discard(queue)

        for queue in queues:
            if queue not in self._consumed_task_queues:
                self._create_internal_task(f"consume_tasks_queue_{queue}", partial(fn, queue))

    async def stop_consume_tasks(self, queues: list[str] | None = None):
        for queue in self._consumed_task_queues:
            if queues is None or queue in queues:
                self._cancel_internal_tasks(f"consume_tasks_queue_{queue}")

    async def push_task_result(self, task_result: TaskResult, task_instance: TaskInstance):
        if not task_instance.result_return:
            return

        task_id: UUID = task_instance.task_id

        if is_debug_level():
            logger.debug(
                "%s push result for %s[%s]\n%s",
                self,
                task_instance.name,
                task_id,
                task_result.pretty_repr(sanitize=settings.LOG_SANITIZE),
            )

        results = self._results

        if task_id not in results:
            results[task_id] = [asyncio_Event(), [], None]

        result = results[task_id]

        result[1].append(self.serializer.dumps_task_result(task_result, task_instance))
        result[0].set()

        if task_instance.result_ttl is not None:
            if result[2] is None:
                result[2] = datetime.now(tz=timezone.utc)
            result[2] += timedelta(seconds=task_instance.result_ttl)
            get_event_loop().call_later(
                task_instance.result_ttl,
                lambda: results.pop(task_id, None)
                if task_id in results
                and results[task_id][2] is not None
                and results[task_id][2] <= datetime.now(tz=timezone.utc)
                else None,
            )

    async def pop_task_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        task_id: UUID = task_instance.task_id

        if not task_instance.result_return:
            raise TaskResultError("try to pop result for task with result_return=False")

        async def fn():
            func = task_instance.func

            if task_instance.extra.get("arrlio:closable") or isasyncgenfunction(func) or isgeneratorfunction(func):
                while not self.is_closed:
                    if task_id not in self._results:
                        self._results[task_id] = [asyncio_Event(), [], None]

                    ev, results, _ = self._results[task_id]
                    await ev.wait()
                    ev.clear()

                    while results:
                        task_result: TaskResult = self.serializer.loads_task_result(results.pop(0))

                        if is_debug_level():
                            logger.debug(
                                "%s pop result for task %s[%s]\n%s",
                                self,
                                task_instance.name,
                                task_id,
                                task_result.pretty_repr(sanitize=settings.LOG_SANITIZE),
                            )

                        if isinstance(task_result.exc, TaskClosedError):
                            return
                        yield task_result

            else:
                if task_id not in self._results:
                    self._results[task_id] = [asyncio_Event(), [], None]

                ev, results, _ = self._results[task_id]
                await ev.wait()
                ev.clear()

                task_result: TaskResult = self.serializer.loads_task_result(results.pop(0))

                if is_debug_level():
                    logger.debug(
                        "%s: pop result for task %s[%s]\n%s",
                        self,
                        task_instance.name,
                        task_id,
                        task_result.pretty_repr(sanitize=settings.LOG_SANITIZE),
                    )

                yield task_result

        __anext__ = fn().__anext__

        try:
            while not self.is_closed:
                yield await self._create_internal_task("pop_task_result", __anext__)
        except StopAsyncIteration:
            return
        finally:
            self._results.pop(task_id, None)

    async def close_task(self, task_instance: TaskInstance, idx: tuple[str, int] | None = None):
        # TODO idx pylint: disable=fixme

        if is_debug_level():
            logger.debug(
                "%s close task %s[%s]",
                self,
                task_instance.name,
                task_instance.task_id,
            )

        await self.push_task_result(
            TaskResult(exc=TaskClosedError(task_instance.task_id), idx=idx),
            task_instance,
        )

    async def send_event(self, event: Event):
        if is_debug_level():
            logger.debug("%s put event\n%s", self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

        self._events[event.event_id] = self.serializer.dumps_event(event)

        async with self._event_cond:
            self._event_cond.notify()

        if event.ttl is not None:
            get_event_loop().call_later(event.ttl, lambda: self._events.pop(event.event_id, None))

    async def consume_events(
        self,
        callback_id: str,
        callback: Callable[[Event], Any],
        event_types: list[str] | None = None,
    ):
        self._event_callbacks[callback_id] = (callback, event_types)

        if "consume_events" in self._internal_tasks:
            return

        async def cb_task(event: Event):
            try:
                await callback(event)
            except Exception as e:
                logger.exception(e)

        async def fn():
            if is_info_level():
                logger.info("%s start consuming events", self)

            event_cond = self._event_cond
            event_cond_wait = event_cond.wait
            events = self._events
            events_pop = events.pop
            events_keys = events.keys
            event_callbacks = self._event_callbacks
            create_internal_task = self._create_internal_task

            while not self.is_closed:
                try:
                    if not events:
                        async with event_cond:
                            await event_cond_wait()

                    event: Event = self.serializer.loads_event(events_pop(next(iter(events_keys()))))

                    if is_debug_level():
                        logger.debug("%s got event\n%s", self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

                    for callback, event_types in event_callbacks.values():
                        if event_types is not None and event.type not in event_types:
                            continue
                        if iscoroutinefunction(callback):
                            create_internal_task("event_cb", partial(cb_task, event))
                        else:
                            callback(event)

                except asyncio.CancelledError:
                    if is_info_level():
                        logger.info("%s stop consuming events", self)
                    return
                except Exception as e:
                    logger.exception(e)

        self._create_internal_task("consume_events", fn)

    async def stop_consume_events(self, callback_id: str | None = None):
        self._event_callbacks.pop(callback_id, None)
        if not self._event_callbacks:
            self._cancel_internal_tasks("consume_events")
