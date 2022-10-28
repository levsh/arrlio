import abc
import asyncio
import logging
from types import MethodType
from typing import List, Optional, Set

from pydantic import BaseSettings

from arrlio.models import Event, Message, TaskInstance, TaskResult
from arrlio.serializers.base import Serializer
from arrlio.tp import AsyncCallableT, SerializerT

logger = logging.getLogger("arrlio.backends.base")


class BackendConfig(BaseSettings):
    name: Optional[str]
    serializer: SerializerT


class Backend(abc.ABC):
    def __init__(self, config: BackendConfig):
        self._closed: asyncio.Future = asyncio.Future()
        self._tasks: Set[asyncio.Task] = set()
        self.config: BackendConfig = config
        self.serializer: Serializer = config.serializer()

    def __repr__(self):
        return self.__str__()

    def _cancel_tasks(self):
        for task in self._tasks:
            logger.debug("%s: cancel task %s", self, task)
            task.cancel()

    def task(method: MethodType):
        async def wrap(self, *args, **kwds):
            if self._closed.done():
                raise Exception(f"Call {method} on closed {self}")
            task: asyncio.Task = asyncio.create_task(method(self, *args, **kwds))
            self._tasks.add(task)
            try:
                return await task
            finally:
                self._tasks.discard(task)
                del task

        return wrap

    @property
    def is_closed(self) -> bool:
        return self._closed.done()

    async def close(self):
        if self.is_closed:
            return
        self._closed.set_result(None)
        self._cancel_tasks()
        await asyncio.gather(
            self.stop_consume_tasks(),
            self.stop_consume_messages(),
            self.stop_consume_events(),
        )

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
    async def push_event(self, event: Event):
        pass

    @abc.abstractmethod
    async def consume_events(self, on_event: AsyncCallableT):
        pass

    @abc.abstractmethod
    async def stop_consume_events(self):
        pass

    @abc.abstractmethod
    async def send_message(self, message: Message, **kwds):
        pass

    @abc.abstractmethod
    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        pass

    @abc.abstractmethod
    async def stop_consume_messages(self):
        pass
