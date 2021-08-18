import asyncio
import collections
import logging
import time
from typing import List
from uuid import UUID

from pydantic import Field

from arrlio.backend import base
from arrlio.core import TaskNoResultError
from arrlio.models import TaskInstance, TaskResult
from arrlio.tp import AsyncCallableT, PriorityT, SerializerT


logger = logging.getLogger("arrlio")


BACKEND_NAME: str = "arrlio"
SERIALIZER: str = "arrlio.serializer.nop.Nop"


class BackendConfig(base.BackendConfig):
    name: str = Field(default_factory=lambda: BACKEND_NAME)
    serializer: SerializerT = Field(default_factory=lambda: SERIALIZER)

    class Config:
        validate_assignment = True
        env_prefix = "ARRLIO_LOCAL_BACKEND_"


class Backend(base.Backend):
    _shared: dict = {}

    def __init__(self, config: BackendConfig):
        super().__init__(config)
        name = self.config.name
        shared = self.__class__._shared
        if name not in shared:
            shared[name] = {
                "refs": 0,
                "task_queues": collections.defaultdict(asyncio.PriorityQueue),
                "message_queues": collections.defaultdict(asyncio.Queue),
                "results": {},
            }
        shared = shared[name]
        shared["refs"] += 1
        self._task_queues = shared["task_queues"]
        self._message_queues = shared["message_queues"]
        self._results = shared["results"]
        self._consumers = {}

    def __str__(self):
        return f"[LocalBackend({self.config.name})]"

    @property
    def _refs(self) -> int:
        return self.__class__._shared.get(self.config.name, {}).get("refs")

    @_refs.setter
    def _refs(self, value: int):
        if self.config.name in self.__class__._shared:
            self.__class__._shared[self.config.name]["refs"] = value

    async def close(self):
        await super().close()
        self._refs = max(0, self._refs - 1)
        if self._refs == 0:
            del self._shared[self.config.name]

    @base.Backend.task
    async def send_task(self, task_instance: TaskInstance, encrypt: bool = None, **kwds):
        task_data = task_instance.data
        if task_instance.task.result_return:
            self._results[task_data.task_id] = [asyncio.Event(), None]
        await self._task_queues[task_data.queue].put(
            (
                (PriorityT.le - task_data.priority) if task_data.priority else PriorityT.ge,
                (
                    time.monotonic(),
                    self.serializer.dumps_task_instance(task_instance, encrypt=encrypt),
                ),
            )
        )

    @base.Backend.task
    async def consume_tasks(self, queues: List[str], on_task: AsyncCallableT):
        async def consume_queue(queue: str):
            try:
                logger.info("%s: consuming queue '%s'", self, queue)
                while True:
                    _, (ts, data) = await self._task_queues[queue].get()
                    task_instance = self.serializer.loads_task_instance(data)
                    if task_instance.data.ttl is not None and time.monotonic() >= ts + task_instance.data.ttl:
                        continue
                    await asyncio.shield(on_task(task_instance))
            except asyncio.CancelledError:
                logger.info("%s: stop consume queue '%s'", self, queue)
            except Exception as e:
                logger.exception(e)

        for queue in queues:
            self._consumers[queue] = asyncio.create_task(consume_queue(queue))

    async def stop_consume_tasks(self):
        for _, task in self._consumers.items():
            task.cancel()
        self._consumers = {}

    @base.Backend.task
    async def push_task_result(self, task_instance: TaskInstance, task_result: TaskResult, encrypt: bool = None):
        if not task_instance.task.result_return:
            return
        task_id: UUID = task_instance.data.task_id
        self._results[task_id][1] = self.serializer.dumps_task_result(task_result, encrypt=encrypt)
        self._results[task_id][0].set()
        if task_instance.data.result_ttl is not None:
            loop = asyncio.get_event_loop()
            loop.call_later(task_instance.data.result_ttl, lambda: self._results.pop(task_id, None))

    @base.Backend.task
    async def pop_task_result(self, task_instance: TaskInstance) -> TaskResult:
        task_id: UUID = task_instance.data.task_id
        try:
            await self._results[task_id][0].wait()
            try:
                return self.serializer.loads_task_result(self._results[task_id][1])
            finally:
                del self._results[task_id]
        except KeyError:
            raise TaskNoResultError()

    async def send_message(message: dict):
        raise NotImplementedError()

    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        raise NotImplementedError()

    async def stop_consume_messages(self):
        return
