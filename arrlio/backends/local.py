import asyncio
import dataclasses
import logging
import time
from collections import defaultdict
from functools import partial
from typing import List, Optional
from uuid import UUID

from pydantic import Field

from arrlio.backends import base
from arrlio.core import TaskNoResultError
from arrlio.models import Event, Message, TaskData, TaskInstance, TaskResult
from arrlio.settings import ENV_PREFIX
from arrlio.tp import AsyncCallableT, PriorityT, SerializerT

logger = logging.getLogger("arrlio.backends.local")


BACKEND_NAME: str = "arrlio"
SERIALIZER: str = "arrlio.serializers.nop"


class BackendConfig(base.BackendConfig):
    name: Optional[str] = Field(default_factory=lambda: BACKEND_NAME)
    serializer: SerializerT = Field(default_factory=lambda: SERIALIZER)

    class Config:
        env_prefix = f"{ENV_PREFIX}LOCAL_BACKEND_"


class Backend(base.Backend):
    __shared: dict = {}

    def __init__(self, config: BackendConfig):
        super().__init__(config)
        name: str = self.config.name
        shared: dict = self.__shared
        if name not in shared:
            shared[name] = {
                "refs": 0,
                "task_queues": defaultdict(asyncio.PriorityQueue),
                "message_queues": defaultdict(asyncio.Queue),
                "results": {},
                "events": {},
                "event_cond": asyncio.Condition(),
            }
        shared = shared[name]
        shared["refs"] += 1
        self._task_queues = shared["task_queues"]
        self._message_queues = shared["message_queues"]
        self._results = shared["results"]
        self._events = shared["events"]
        self._event_cond = shared["event_cond"]
        self._consumed_task_queues = set()
        self._consumed_message_queues = set()

    def __del__(self):
        if self.config.name in self.__shared:
            self._refs = max(0, self._refs - 1)
            if self._refs == 0:
                del self.__shared[self.config.name]

    def __str__(self):
        return f"LocalBackend[{self.config.name}]"

    @property
    def _shared(self) -> dict:
        return self.__shared[self.config.name]

    @property
    def _refs(self) -> int:
        return self._shared["refs"]

    @_refs.setter
    def _refs(self, value: int):
        self._shared["refs"] = value

    async def send_task(self, task_instance: TaskInstance, **kwds):
        task_data: TaskData = task_instance.data

        if task_data.result_return and task_data.task_id not in self._results:
            self._results[task_data.task_id] = [asyncio.Event(), None]

        async def fn():
            logger.debug("%s: put %s", self, task_instance)
            await self._task_queues[task_data.queue].put(
                (
                    (PriorityT.le - task_data.priority) if task_data.priority else PriorityT.ge,
                    time.monotonic(),
                    task_data.ttl,
                    self.serializer.dumps_task_instance(task_instance),
                )
            )

        await self._run_task("send_task", fn)

    async def consume_tasks(self, queues: List[str], on_task: AsyncCallableT):
        async def fn(queue: str):
            logger.info("%s: start consuming tasks queue '%s'", self, queue)
            self._consumed_task_queues.add(queue)
            try:
                while True:
                    try:
                        _, ts, ttl, data = await self._task_queues[queue].get()
                        if ttl is not None and time.monotonic() >= ts + ttl:
                            continue
                        task_instance: TaskInstance = self.serializer.loads_task_instance(data)
                        logger.debug("%s: got %s", self, task_instance)
                        await asyncio.shield(on_task(task_instance))
                    except asyncio.CancelledError:
                        logger.info("%s: stop consuming tasks queue '%s'", self, queue)
                        return
                    except Exception as e:
                        logger.exception(e)
            finally:
                self._consumed_task_queues.discard(queue)

        for queue in queues:
            if queue not in self._consumed_task_queues:
                self._run_task(f"consume_tasks_queue_{queue}", partial(fn, queue))

    async def stop_consume_tasks(self, queues: List[str] = None):
        for queue in list(self._consumed_task_queues):
            if queues is None or queue in queues:
                self._cancel_tasks(f"consume_tasks_queue_{queue}")

    async def push_task_result(self, task_instance: TaskInstance, task_result: TaskResult):
        task_data: TaskData = task_instance.data

        if not task_data.result_return:
            return

        task_id: UUID = task_data.task_id

        self._results[task_id][1] = self.serializer.dumps_task_result(task_instance, task_result)
        self._results[task_id][0].set()

        if task_data.result_ttl is not None:
            loop = asyncio.get_event_loop()
            loop.call_later(task_data.result_ttl, lambda: self._results.pop(task_id, None))

    async def pop_task_result(self, task_instance: TaskInstance) -> TaskResult:
        task_id: UUID = task_instance.data.task_id

        if not task_instance.data.result_return:
            raise TaskNoResultError(f"{task_id}")

        if task_id not in self._results:
            self._results[task_id] = [asyncio.Event(), None]

        async def fn():
            await self._results[task_id][0].wait()
            try:
                return self.serializer.loads_task_result(self._results[task_id][1])
            finally:
                del self._results[task_id]

        return await self._run_task("pop_task_result", fn)

    async def send_message(self, message: Message, **kwds):
        data: dict = dataclasses.asdict(message)
        data["data"] = self.serializer.dumps(message.data)

        async def fn():
            logger.debug("%s: put %s", self, message)
            await self._message_queues[message.exchange].put(
                (
                    (PriorityT.le - message.priority) if message.priority else PriorityT.ge,
                    time.monotonic(),
                    message.ttl,
                    data,
                )
            )

        await self._run_task("send_message", fn)

    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        async def fn(queue: str):
            logger.info("%s: start consuming messages queue '%s'", self, queue)
            self._consumed_message_queues.add(queue)
            try:
                while True:
                    try:
                        _, ts, ttl, data = await self._message_queues[queue].get()
                        if ttl is not None and time.monotonic() >= ts + ttl:
                            continue
                        data["data"] = self.serializer.loads(data["data"])
                        message = Message(**data)
                        logger.debug("%s: got %s", self, message)
                        await asyncio.shield(on_message(message))
                    except asyncio.CancelledError:
                        logger.info("%s: stop consuming messages queue '%s'", self, queue)
                        break
                    except Exception as e:
                        logger.exception(e)
            finally:
                self._consumed_message_queues.discard(queue)

        for queue in queues:
            if queue not in self._consumed_message_queues:
                self._run_task(f"consume_messages_queue_{queue}", partial(fn, queue))

    async def stop_consume_messages(self, queues: List[str] = None):
        for queue in list(self._consumed_message_queues):
            if queues is None or queue in queues:
                self._cancel_tasks(f"consume_messages_queue_{queue}")

    async def send_event(self, event: Event):
        self._events[event.event_id] = self.serializer.dumps_event(event)
        async with self._event_cond:
            self._event_cond.notify()
        if event.ttl is not None:
            loop = asyncio.get_event_loop()
            loop.call_later(event.ttl, lambda: self._events.pop(event.event_id, None))

    async def consume_events(self, on_event: AsyncCallableT):
        if "consume_events" in self._tasks:
            raise Exception("Already consuming")

        async def fn():
            logger.info("%s: start consuming events", self)
            while True:
                try:
                    if not self._events:
                        async with self._event_cond:
                            await self._event_cond.wait()
                    data = self._events.pop(next(iter(self._events.keys())))
                    event: Event = self.serializer.loads_event(data)
                    logger.debug("%s: got %s", self, event)
                    await asyncio.shield(on_event(event))
                except asyncio.CancelledError:
                    logger.info("%s: stop consuming events", self)
                    break
                except Exception as e:
                    logger.exception(e)

        self._run_task("consume_events", fn)

    async def stop_consume_events(self):
        self._cancel_tasks("consume_events")
