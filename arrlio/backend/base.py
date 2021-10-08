import abc
import asyncio
import logging
from types import MethodType
from typing import List

from pydantic import BaseSettings

from arrlio.models import TaskInstance, TaskResult
from arrlio.tp import AsyncCallableT, SerializerT, TimeoutT


logger = logging.getLogger("arrlio")


class BackendConfig(BaseSettings):
    name: str = None
    serializer: SerializerT
    timeout: TimeoutT = None


class Backend(abc.ABC):
    def __init__(self, config: BackendConfig):
        self.config = config
        self.serializer = config.serializer()
        self._closing: bool = False
        self._closed: bool = False
        self._tasks: set = set()

    def __repr__(self):
        return self.__str__()

    def _cancel(self):
        for task in self._tasks:
            task.cancel()

    def task(method: MethodType):
        async def wrap(self, *args, **kwds):
            # if self._closing:
            #     return
            if self._closed:
                raise Exception(f"Trying to call {method} but backend {self} has been closed")
            task = asyncio.create_task(method(self, *args, **kwds))
            self._tasks.add(task)
            try:
                return await task
            finally:
                self._tasks.discard(task)

        return wrap

    async def close(self):
        self._closing = True
        self._cancel()
        await self.stop_consume_tasks()
        await self.stop_consume_messages()
        self._closed = True

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
    async def send_message(self, message: dict, **kwds):
        pass

    @abc.abstractmethod
    async def consume_messages(self, queues: List[str], on_message: AsyncCallableT):
        pass

    @abc.abstractmethod
    async def stop_consume_messages(self):
        pass
