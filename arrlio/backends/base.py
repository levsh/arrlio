import abc
import asyncio
import logging
from asyncio import create_task, current_task
from collections import defaultdict
from typing import Any, Callable, Coroutine, cast
from uuid import uuid4

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio.configs import ModuleConfigValidatorMixIn
from arrlio.models import Event, TaskInstance, TaskResult
from arrlio.serializers.base import Serializer
from arrlio.settings import ENV_PREFIX
from arrlio.types import ModuleConfig, SerializerModule

logger = logging.getLogger("arrlio.backends.base")


SERIALIZER = "arrlio.serializers.nop"


class SerializerConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """Config for backend serializer."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}SERIALIZER_")

    module: SerializerModule = SERIALIZER
    config: ModuleConfig = Field(default_factory=BaseSettings)


class Config(BaseSettings):
    """Config for backend."""

    model_config = SettingsConfigDict()

    id: str = Field(default_factory=lambda: f"{uuid4()}")
    serializer: SerializerConfig = Field(default_factory=SerializerConfig)


class Backend(abc.ABC):
    __slots__ = ("config", "serializer", "_internal_tasks", "_closed")

    def __init__(self, config: Config):
        """
        Args:
            config: Backend config.
        """

        self.config: Config = config
        self.serializer: Serializer = config.serializer.module.Serializer(config.serializer.config)
        self._internal_tasks: dict[str, set[asyncio.Task]] = defaultdict(set)
        self._closed: asyncio.Future = asyncio.Future()

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
            task = cast(asyncio.Task, current_task())
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
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.stop_consume_tasks())
                tg.create_task(self.stop_consume_events())
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
    async def consume_tasks(self, queues: list[str], callback: Callable[[TaskInstance], Coroutine]):
        """Consume tasks from the queues and invoke `callback` on `arrlio.models.TaskInstance` received."""

        return

    @abc.abstractmethod
    async def stop_consume_tasks(self, queues: list[str] | None = None):
        """Stop consuming tasks."""

        return

    @abc.abstractmethod
    async def push_task_result(self, task_result: TaskResult, task_instance: TaskInstance):
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
        callback: Callable[[Event], Any],
        event_types: list[str] | None = None,
    ):
        """Consume event from the queues."""

        return

    @abc.abstractmethod
    async def stop_consume_events(self, callback_id: str | None = None):
        """Stop consuming events."""

        return
