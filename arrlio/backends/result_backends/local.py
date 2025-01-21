import asyncio
import logging

from asyncio import get_event_loop
from datetime import datetime, timedelta, timezone
from inspect import isasyncgenfunction, isgeneratorfunction
from typing import AsyncGenerator, Optional, cast
from uuid import UUID, uuid4

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio import settings
from arrlio.abc import AbstractResultBackend
from arrlio.exceptions import TaskClosedError, TaskResultError
from arrlio.models import Shared, TaskInstance, TaskResult
from arrlio.settings import ENV_PREFIX
from arrlio.utils import AioTasksRunner, Closable, is_debug_level


logger = logging.getLogger("arrlio.backends.results.local")


class Config(BaseSettings):
    """
    Local `ResultBackend` config.

    Args:
        config: `ResultBackend` config.
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}LOCAL_RESULT_BACKEND_")

    id: str = Field(default_factory=lambda: f"{uuid4().hex[-4:]}")


class ResultBackend(Closable, AbstractResultBackend):
    """
    Local `ResultBackend`.

    Args:
        config: `ResultBackend` config.
    """

    def __init__(self, config: Config):
        super().__init__()

        self.config = config
        self._internal_tasks_runner = AioTasksRunner()

        self._results: dict[UUID, tuple[asyncio.Event, list, Optional[datetime]]] = {}

    def __str__(self):
        return f"ResultBackend[local#{self.config.id}]"

    def __repr__(self):
        return self.__str__()

    async def init(self):
        return

    async def close(self):
        await self._internal_tasks_runner.close()
        await super().close()

    def make_headers(self, task_instance: TaskInstance) -> dict:
        return {}

    def make_shared(self, task_instance: TaskInstance) -> Shared:
        return Shared()

    async def allocate_storage(self, task_instance: TaskInstance):
        task_id = task_instance.task_id

        if task_id in self._results or not task_instance.result_return:
            return

        if task_instance.result_ttl is not None:
            expire_at = datetime.now(tz=timezone.utc) + timedelta(seconds=task_instance.result_ttl)
            get_event_loop().call_later(
                task_instance.result_ttl,
                lambda: (
                    self._results.pop(task_id, None)
                    if task_id in self._results
                    and cast(datetime, self._results[task_id][2]) <= datetime.now(tz=timezone.utc)
                    else None
                ),
            )
        else:
            expire_at = None

        self._results[task_id] = (asyncio.Event(), [], expire_at)

    async def push_task_result(self, task_result: TaskResult, task_instance: TaskInstance):
        if not task_instance.result_return:
            return

        task_id = task_instance.task_id

        if is_debug_level():
            logger.debug(
                "%s push result for %s[%s]\n%s",
                self,
                task_instance.name,
                task_id,
                task_result.pretty_repr(sanitize=settings.LOG_SANITIZE),
            )

        await self.allocate_storage(task_instance)

        ev, results, expire_at = self._results[task_id]

        results.append(task_result)
        ev.set()

    async def pop_task_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        task_id = task_instance.task_id

        if not task_instance.result_return:
            raise TaskResultError("Try to pop result for task with result_return=False")

        async def fn():
            func = task_instance.func

            if task_instance.headers.get("arrlio:closable") or isasyncgenfunction(func) or isgeneratorfunction(func):
                while not self.is_closed:
                    await self.allocate_storage(task_instance)

                    ev, results, expire_at = self._results[task_id]
                    await ev.wait()
                    ev.clear()

                    while results:
                        task_result: TaskResult = results.pop(0)

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
                await self.allocate_storage(task_instance)

                ev, results, expire_at = self._results[task_id]
                await ev.wait()
                ev.clear()

                task_result: TaskResult = results.pop(0)

                if is_debug_level():
                    logger.debug(
                        "%s pop result for task %s[%s]\n%s",
                        self,
                        task_instance.name,
                        task_id,
                        task_result.pretty_repr(sanitize=settings.LOG_SANITIZE),
                    )

                yield task_result

        __anext__ = fn().__anext__

        try:
            while not self.is_closed:
                yield await self._internal_tasks_runner.create_task("pop_task_result", __anext__)  # type: ignore
        except StopAsyncIteration:
            return
        finally:
            self._results.pop(task_id, None)

    async def close_task(self, task_instance: TaskInstance, idx: tuple[str, int] | None = None):
        # TODO

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
