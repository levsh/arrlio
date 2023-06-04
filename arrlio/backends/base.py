import abc
import asyncio
import logging
from asyncio import create_task, current_task
from collections import defaultdict
from typing import Any, Callable, Dict, List, Set, Type, Union
from uuid import uuid4

from pydantic import Field

from arrlio.models import Event, TaskInstance, TaskResult
from arrlio.serializers.base import Serializer
from arrlio.settings import ENV_PREFIX, BaseConfig, ConfigValidatorMixIn
from arrlio.types import AsyncFunction, SerializerModule

logger = logging.getLogger("arrlio.backends.base")


class SerializerConfig(ConfigValidatorMixIn, BaseConfig):
    module: SerializerModule
    config: Any = Field(default_factory=dict)

    class Config:
        env_prefix = [f"{ENV_PREFIX}SERIALIZER_"]


class Config(BaseConfig):
    """Config for backend."""

    id: str = Field(default_factory=lambda: f"{uuid4()}")
    serializer: SerializerConfig = Field(default_factory=lambda: SerializerConfig(module="arrlio.serializers.nop"))


class Backend(abc.ABC):
    __slots__ = ("config", "_serializer", "_closed", "_internal_tasks")

    def __init__(self, config: Type[Config]):
        """
        Args:
            config: Backend config.
        """
        self.config: Type[Config] = config
        self._serializer: Type[Serializer] = config.serializer.module.Serializer(config.serializer.config)
        self._closed: asyncio.Future = asyncio.Future()
        self._internal_tasks: Dict[str, Set[asyncio.Task]] = defaultdict(set)

    def __repr__(self):
        return self.__str__()

    def _cancel_all_internal_tasks(self):
        for tasks in self._internal_tasks.values():
            for task in tasks:
                task.cancel()

    def _cancel_internal_tasks(self, key: str):
        for task in self._internal_tasks[key]:
            task.cancel()

    def _create_internal_task(self, key: str, coro_factory: Callable) -> asyncio.Task:
        if self._closed.done():
            raise Exception(f"{self} closed")

        async def fn():
            task: asyncio.Task = current_task()
            internal_tasks = self._internal_tasks[key]
            internal_tasks.add(task)
            try:
                return await coro_factory()
            except Exception as e:
                if not isinstance(e, (StopIteration, StopAsyncIteration)):
                    logger.exception(e.__class__.__name__)
                raise e
            finally:
                internal_tasks.discard(task)
                if not internal_tasks:
                    del self._internal_tasks[key]

        return create_task(fn())

    @property
    def is_closed(self) -> bool:
        return self._closed.done()

    async def close(self):
        """Close backend. Stop consuming tasks and events. Cancel all internal tasks."""

        if self.is_closed:
            return
        try:
            await asyncio.gather(self.stop_consume_tasks(), self.stop_consume_events())
        finally:
            self._cancel_all_internal_tasks()
            self._closed.set_result(None)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @abc.abstractmethod
    async def send_task(self, task_instance: TaskInstance, **kwds):
        """Send task to backend."""
        return

    @abc.abstractmethod
    async def close_task(self, task_instance: TaskInstance):
        return

    @abc.abstractmethod
    async def consume_tasks(self, queues: List[str], callback: AsyncFunction):
        """Consume tasks from the queues and invoke `callback` on `arrlio.models.TaskInstance` received."""
        return

    @abc.abstractmethod
    async def stop_consume_tasks(self, queues: List[str] = None):
        """Stop consuming tasks."""
        return

    @abc.abstractmethod
    async def push_task_result(self, task_instance: TaskInstance, task_result: TaskResult):
        """Push task result to backend."""
        return

    @abc.abstractmethod
    async def pop_task_result(self, task_instance: TaskInstance) -> TaskResult:
        """Pop task result for `arrlio.models.TaskInstance` from backend."""
        return

    @abc.abstractmethod
    async def send_event(self, event: Event):
        """Send event to backend."""
        return

    @abc.abstractmethod
    async def consume_events(
        self,
        callback_id: str,
        callback: Union[Callable, AsyncFunction],
        event_types: List[str] = None,
    ):
        """Consume event from the queues."""
        return

    @abc.abstractmethod
    async def stop_consume_events(self, callback_id: str = None):
        """Stop consuming events."""
        return
