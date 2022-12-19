import abc
import asyncio
import logging
from asyncio import create_task
from collections import defaultdict
from typing import Callable, Dict, List, Set
from uuid import uuid4

from pydantic import BaseSettings, Field

from arrlio.models import Event, Message, TaskInstance, TaskResult
from arrlio.serializers.base import Serializer
from arrlio.tp import AsyncCallableT, SerializerT

logger = logging.getLogger("arrlio.backends.base")


class BackendConfig(BaseSettings):
    id: str = Field(default_factory=lambda: f"{uuid4()}")
    serializer: SerializerT

    class Config:
        validate_assignment = True


class Backend(abc.ABC):
    __slots__ = ("config", "serializer", "_closed", "_backend_tasks")

    def __init__(self, config: BackendConfig):
        self.config: BackendConfig = config
        self.serializer: Serializer = config.serializer()
        self._closed: asyncio.Future = asyncio.Future()
        self._backend_tasks: Dict[str, Set[asyncio.Task]] = defaultdict(set)

    def __repr__(self):
        return self.__str__()

    def _cancel_all_backend_tasks(self):
        for tasks in self._backend_tasks.values():
            for task in tasks:
                task.cancel()

    def _cancel_backend_tasks(self, key: str):
        for task in self._backend_tasks[key]:
            task.cancel()

    def _create_backend_task(self, key: str, coro_factory: Callable):
        if self._closed.done():
            raise Exception(f"Closed {self}")

        task: asyncio.Task = create_task(coro_factory())
        self._backend_tasks[key].add(task)
        task.add_done_callback(lambda *args: self._backend_tasks[key].discard(task))

        return task

    @property
    def is_closed(self) -> bool:
        return self._closed.done()

    async def close(self):
        if self.is_closed:
            return
        try:
            await asyncio.gather(
                self.stop_consume_tasks(),
                self.stop_consume_messages(),
                self.stop_consume_events(),
            )
        finally:
            self._cancel_all_backend_tasks()
            self._closed.set_result(None)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @abc.abstractmethod
    async def send_task(self, task_instance: TaskInstance, **kwds):
        pass

    @abc.abstractmethod
    async def consume_tasks(self, queues: List[str], on_task: AsyncCallableT):
        pass

    @abc.abstractmethod
    async def stop_consume_tasks(self, queues: List[str] = None):
        pass

    @abc.abstractmethod
    async def push_task_result(self, task_instance: TaskInstance, task_result: TaskResult):
        pass

    @abc.abstractmethod
    async def pop_task_result(self, task_instance: TaskInstance) -> TaskResult:
        pass

    @abc.abstractmethod
    async def send_message(self, message: Message, **kwds):
        pass

    @abc.abstractmethod
    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        pass

    @abc.abstractmethod
    async def stop_consume_messages(self, queues: List[str] = None):
        pass

    @abc.abstractmethod
    async def send_event(self, event: Event):
        pass

    @abc.abstractmethod
    async def consume_events(self, on_event: AsyncCallableT):
        pass

    @abc.abstractmethod
    async def stop_consume_events(self):
        pass
