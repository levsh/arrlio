import abc
import asyncio
import logging
from types import MethodType
from typing import List

from pydantic import BaseSettings

from arrlio.models import Message, TaskInstance, TaskResult
from arrlio.serializer.base import Serializer
from arrlio.tp import AsyncCallableT, SerializerT, TimeoutT


logger = logging.getLogger("arrlio")


class BackendConfig(BaseSettings):
    name: str = None
    serializer: SerializerT
    timeout: TimeoutT = None


class Backend(abc.ABC):
    def __init__(self, config: BackendConfig):
        self.config: BackendConfig = config
        self.serializer: Serializer = config.serializer()
        self._closed: asyncio.Future = asyncio.Future()
        self._tasks: set = set()

    def __repr__(self):
        return self.__str__()

    def _cancel_tasks(self):
        for task in self._tasks:
            task.cancel()

    def task(method: MethodType):
        async def wrap(self, *args, **kwds):
            if self._closed.done():
                raise Exception(f"Call {method} on closed {self}")
            task = asyncio.create_task(method(self, *args, **kwds))
            self._tasks.add(task)
            try:
                return await task
            finally:
                self._tasks.discard(task)

        return wrap

    @property
    def is_closed(self):
        return self._closed.done()

    async def close(self):
        if self.is_closed:
            return
        self._closed.set_result(None)
        self._cancel_tasks()
        await self.stop_consume_tasks()
        await self.stop_consume_messages()

    @abc.abstractmethod
    async def send_task(self, task_instance: TaskInstance, encrypt: bool = None, **kwds):
        pass

    @abc.abstractmethod
    async def consume_tasks(self, queues: List[str], on_task: AsyncCallableT):
        pass

    @abc.abstractmethod
    async def stop_consume_tasks(self):
        pass

    @abc.abstractmethod
    async def push_task_result(self, task_instance: TaskInstance, task_result: TaskResult, encrypt: bool = None):
        pass

    @abc.abstractmethod
    async def pop_task_result(self, task_instance: TaskInstance) -> TaskResult:
        pass

    @abc.abstractmethod
    async def send_message(self, message: Message, encrypt: bool = None, **kwds):
        pass

    @abc.abstractmethod
    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        pass

    @abc.abstractmethod
    async def stop_consume_messages(self):
        pass
