import asyncio
import dataclasses
import logging
from asyncio import Event as asyncio_Event
from asyncio import Semaphore, create_task, get_event_loop
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from functools import partial
from inspect import isasyncgenfunction, iscoroutinefunction, isgeneratorfunction
from time import monotonic
from typing import AsyncGenerator, Callable, Dict, List, Tuple, Union
from uuid import UUID

from pydantic import Field, PositiveInt

from arrlio.backends import base
from arrlio.exc import TaskClosedError, TaskResultError
from arrlio.models import Event, Message, TaskData, TaskInstance, TaskResult
from arrlio.settings import ENV_PREFIX
from arrlio.tp import AsyncCallableT, PriorityT

logger = logging.getLogger("arrlio.backends.local")

BACKEND_ID: str = "arrlio"
SERIALIZER: str = "arrlio.serializers.nop"
POOL_SIZE: int = 100


class Config(base.Config):
    id: str = Field(default_factory=lambda: BACKEND_ID)
    pool_size: PositiveInt = Field(default_factory=lambda: POOL_SIZE)

    class Config:
        env_prefix = f"{ENV_PREFIX}LOCAL_"


class Backend(base.Backend):
    __shared: dict = {}

    def __init__(self, config: Config):
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
        self._semaphore = Semaphore(value=config.pool_size)
        self._event_callbacks: Dict[str, Tuple[AsyncCallableT, List[str]]] = {}

    def __del__(self):
        if self.config.id in self.__shared:
            self._refs = max(0, self._refs - 1)
            if self._refs == 0:
                del self.__shared[self.config.id]

    def __str__(self):
        return f"LocalBackend[{self.config.id}]"

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
        task_data: TaskData = task_instance.data

        if task_data.result_return and task_data.task_id not in self._results:
            self._results[task_data.task_id] = [asyncio_Event(), [], None]

        logger.debug("%s: put %s", self, task_instance)

        self._task_queues[task_data.queue].put_nowait(
            (
                (PriorityT.le - task_data.priority) if task_data.priority else PriorityT.ge,
                monotonic(),
                task_data.ttl,
                self.serializer.dumps_task_instance(task_instance),
            )
        )

    async def consume_tasks(self, queues: List[str], on_task: AsyncCallableT):
        async def fn(queue: str):
            logger.info("%s: start consuming tasks queue '%s'", self, queue)

            semaphore = self._semaphore
            semaphore_acquire = semaphore.acquire
            semaphore_release = semaphore.release
            task_queue_get = self._task_queues[queue].get
            loads_task_instance = self.serializer.loads_task_instance

            self._consumed_task_queues.add(queue)
            try:
                while not self.is_closed:
                    try:
                        await semaphore_acquire()
                        try:
                            _, ts, ttl, data = await task_queue_get()
                            if ttl is not None and monotonic() >= ts + ttl:
                                continue
                            task_instance: TaskInstance = loads_task_instance(data)
                            logger.debug("%s: got %s", self, task_instance)
                            tsk: asyncio.Task = create_task(on_task(task_instance))
                        except (BaseException, Exception) as e:
                            semaphore_release()
                            raise e
                        tsk.add_done_callback(lambda *args: semaphore_release())
                    except asyncio.CancelledError:
                        logger.info("%s: stop consuming tasks queue '%s'", self, queue)
                        return
                    except Exception as e:
                        logger.exception(e)
            finally:
                self._consumed_task_queues.discard(queue)

        for queue in queues:
            if queue not in self._consumed_task_queues:
                self._create_backend_task(f"consume_tasks_queue_{queue}", partial(fn, queue))

    async def stop_consume_tasks(self, queues: List[str] = None):
        for queue in self._consumed_task_queues:
            if queues is None or queue in queues:
                self._cancel_backend_tasks(f"consume_tasks_queue_{queue}")

    async def push_task_result(self, task_instance: TaskInstance, task_result: TaskResult):
        task_data: TaskData = task_instance.data

        if not task_data.result_return:
            return

        task_id: UUID = task_data.task_id

        logger.debug("%s: push result for %s(%s)", self, task_id, task_instance.task.name)

        results = self._results

        if task_id not in results:
            results[task_id] = [asyncio_Event(), [], None]

        result = results[task_id]

        result[1].append(self.serializer.dumps_task_result(task_instance, task_result))
        result[0].set()

        if task_data.result_ttl is not None:
            if result[2] is None:
                result[2] = datetime.now(tz=timezone.utc)
            result[2] += timedelta(seconds=task_data.result_ttl)
            get_event_loop().call_later(
                task_data.result_ttl,
                lambda: results.pop(task_id, None)
                if task_id in results
                and results[task_id][2] is not None
                and results[task_id][2] <= datetime.now(tz=timezone.utc)
                else None,
            )

    async def pop_task_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        task_data: TaskData = task_instance.data
        task_id: UUID = task_data.task_id

        if not task_data.result_return:
            raise TaskResultError(f"{task_id}({task_instance.task.name})")

        async def fn():
            serializer_loads_task_result = self.serializer.loads_task_result
            func = task_instance.task.func

            if task_data.extra.get("graph") or isasyncgenfunction(func) or isgeneratorfunction(func):

                while not self.is_closed:

                    if task_id not in self._results:
                        self._results[task_id] = [asyncio_Event(), [], None]

                    ev, results, _ = self._results[task_id]
                    await ev.wait()
                    ev.clear()

                    while results:
                        task_result: TaskResult = serializer_loads_task_result(results.pop(0))

                        logger.debug("%s: pop result for %s(%s)", self, task_id, task_instance.task.name)

                        if isinstance(task_result.exc, TaskClosedError):
                            return
                        yield task_result

            else:

                if task_id not in self._results:
                    self._results[task_id] = [asyncio_Event(), [], None]

                ev, results, _ = self._results[task_id]
                await ev.wait()
                ev.clear()

                logger.debug("%s: pop result for %s(%s)", self, task_id, task_instance.task.name)

                yield serializer_loads_task_result(results.pop(0))

        gen = fn()

        try:
            while not self.is_closed:
                yield await self._create_backend_task("pop_task_result", gen.__anext__)
        except StopAsyncIteration:
            return
        finally:
            self._results.pop(task_id, None)

    async def close_task(self, task_instance: TaskInstance):
        logger.debug("%s: close task %s(%s)", self, task_instance.data.task_id, task_instance.task.name)

        await self.push_task_result(task_instance, TaskResult(exc=TaskClosedError()))

    async def send_message(self, message: Message, **kwds):
        data: dict = dataclasses.asdict(message)
        data["data"] = self.serializer.dumps(message.data)

        logger.debug("%s: put %s", self, message)

        self._message_queues[message.exchange].put_nowait(
            (
                (PriorityT.le - message.priority) if message.priority else PriorityT.ge,
                monotonic(),
                message.ttl,
                data,
            )
        )

    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        async def fn(queue: str):
            logger.info("%s: start consuming messages queue '%s'", self, queue)

            self._consumed_message_queues.add(queue)

            semaphore = self._semaphore
            semaphore_acquire = semaphore.acquire
            semaphore_release = semaphore.release
            message_queue_get = self._message_queues[queue].get
            loads = self.serializer.loads

            try:
                while not self.is_closed:
                    try:
                        await semaphore_acquire()
                        try:
                            _, ts, ttl, data = await message_queue_get()
                            if ttl is not None and monotonic() >= ts + ttl:
                                continue
                            data["data"] = loads(data["data"])
                            message = Message(**data)
                            logger.debug("%s: got %s", self, message)
                            tsk: asyncio.Task = create_task(on_message(message))
                        except (BaseException, Exception) as e:
                            semaphore_release()
                            raise e
                        tsk.add_done_callback(lambda *args: semaphore_release())
                    except asyncio.CancelledError:
                        logger.info("%s: stop consuming messages queue '%s'", self, queue)
                        return
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
        logger.debug("%s: put %s", self, event)

        self._events[event.event_id] = self.serializer.dumps_event(event)

        async with self._event_cond:
            self._event_cond.notify()

        if event.ttl is not None:
            get_event_loop().call_later(event.ttl, lambda: self._events.pop(event.event_id, None))

    async def consume_events(self, cb_id: str, cb: Union[Callable, AsyncCallableT], event_types: List[str] = None):
        self._event_callbacks[cb_id] = (cb, event_types)

        if "consume_events" in self._backend_tasks:
            return

        async def cb_task(event: Event):
            try:
                await cb(event)
            except Exception as e:
                logger.exception(e)

        async def fn():
            logger.info("%s: start consuming events", self)

            event_cond = self._event_cond
            event_cond_wait = event_cond.wait
            events = self._events
            events_pop = events.pop
            events_keys = events.keys
            loads_event = self.serializer.loads_event
            event_callbacks = self._event_callbacks
            create_backend_task = self._create_backend_task

            while not self.is_closed:
                try:
                    if not events:
                        async with event_cond:
                            await event_cond_wait()

                    event: Event = loads_event(events_pop(next(iter(events_keys()))))

                    logger.debug("%s: got %s", self, event)

                    for cb, event_types in event_callbacks.values():
                        if event_types is not None and event.type not in event_types:
                            continue
                        if iscoroutinefunction(cb):
                            create_backend_task("event_cb", partial(cb_task, event))
                        else:
                            cb(event)
                except asyncio.CancelledError:
                    logger.info("%s: stop consuming events", self)
                    return
                except Exception as e:
                    logger.exception(e)

        self._create_backend_task("consume_events", fn)

    async def stop_consume_events(self, cb_id: str = None):
        self._event_callbacks.pop(cb_id, None)
        if not self._event_callbacks:
            self._cancel_backend_tasks("consume_events")
