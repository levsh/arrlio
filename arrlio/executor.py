import asyncio
import logging
import sys
import threading
from asyncio import get_running_loop, new_event_loop, set_event_loop, wait_for
from inspect import isasyncgenfunction, iscoroutinefunction, isgeneratorfunction
from threading import Thread, current_thread
from time import monotonic
from typing import AsyncGenerator

from pydantic import BaseSettings

from arrlio.exc import NotFoundError, TaskTimeoutError
from arrlio.models import TaskInstance, TaskResult
from arrlio.utils import is_info_level

logger = logging.getLogger("arrlio.executor")

asyncio_Event = asyncio.Event  # pylint: disable=invalid-name
threading_Event = threading.Event  # pylint: disable=invalid-name


class Config(BaseSettings):
    """`arrlio.executor.Executor` config class."""

    class Config:
        validate_assignment = True


class Executor:
    """Executor class."""

    def __init__(self, config: Config):
        """
        Args:
            config: Executor config.
        """

        self.config = config

    def __str__(self):
        return self.__class__.__name__

    def __repr__(self):
        return self.__str__()

    async def __call__(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        """Execute `arrlio.models.TaskInstance`. Blocking until the task result available.

        Args:
            task_instance: `arrlio.models.TaskInstance` object.
        Yields:
            `arrlio.models.TaskResult` object."""

        if task_instance.thread:
            execute = self.execute_in_thread
        else:
            execute = self.execute
        async for task_result in execute(task_instance):
            yield task_result

    async def execute(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        """Execute `arrlio.models.TaskInstance` in the same thread. Blocking until the task result available.

        Args:
            task_instance: `arrlio.models.TaskInstance` object.
        Yields:
            `arrlio.models.TaskResult` object.
        """

        res, exc, trb = None, None, None
        t0 = monotonic()

        try:
            if (func := task_instance.func) is None:
                raise NotFoundError(f"Task '{task_instance.name}' not found")

            # task_instance.validate()

            meta: bool = (kwdefaults := func.__kwdefaults__) is not None and "meta" in kwdefaults

            if is_info_level():
                logger.info(
                    "%s[%s]: execute task %s(%s)",
                    self,
                    current_thread().name,
                    task_instance.task_id,
                    task_instance.name,
                )

            try:
                if iscoroutinefunction(func):
                    res = await wait_for(task_instance(meta=meta), task_instance.timeout)
                    if isinstance(res, TaskResult):
                        yield res
                    else:
                        yield TaskResult(res=res, exc=exc, trb=trb)

                elif isgeneratorfunction(func):
                    for res in task_instance(meta=meta):
                        if isinstance(res, TaskResult):
                            yield res
                        else:
                            yield TaskResult(res=res, exc=exc, trb=trb)

                elif isasyncgenfunction(func):
                    agen = task_instance(meta=meta)

                    timeout_time = (monotonic() + timeout) if (timeout := task_instance.timeout) is not None else None

                    while True:
                        timeout = (timeout_time - monotonic()) if timeout_time is not None else None

                        try:
                            res = await wait_for(agen.__anext__(), timeout)
                            if isinstance(res, TaskResult):
                                yield res
                            else:
                                yield TaskResult(res=res, exc=exc, trb=trb)

                        except StopAsyncIteration:
                            break
                        except asyncio.TimeoutError:
                            raise TaskTimeoutError(task_instance.timeout)

                else:
                    res = task_instance(meta=meta)
                    if isinstance(res, TaskResult):
                        yield res
                    else:
                        yield TaskResult(res=res, exc=exc, trb=trb)

            except asyncio.TimeoutError:
                raise TaskTimeoutError(task_instance.timeout)

        except Exception as e:
            exc_info = sys.exc_info()
            exc, trb = exc_info[1], exc_info[2]
            if isinstance(e, TaskTimeoutError):
                logger.error(
                    "%s[%s]: task %s(%s) timeout",
                    self,
                    current_thread().name,
                    task_instance.task_id,
                    task_instance.name,
                )
            else:
                logger.exception(
                    "%s[%s]: task %s(%s)",
                    self,
                    current_thread().name,
                    task_instance.task_id,
                    task_instance.name,
                )
            yield TaskResult(res=res, exc=exc, trb=trb)

        if is_info_level():
            logger.info(
                "%s[%s]: task %s(%s) done(%s) in %.2f second(s)",
                self,
                current_thread().name,
                task_instance.task_id,
                task_instance.name,
                "success" if exc is None else "error",
                monotonic() - t0,
            )

    async def execute_in_thread(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        """Execute `arrlio.models.TaskInstance` in the separate thread. Blocking until the task result available.

        Args:
            task_instance: `arrlio.models.TaskInstance` object.
        Yields:
            `arrlio.models.TaskResult` object.
        """

        root_loop = get_running_loop()
        done_ev = asyncio_Event()
        sync_ev = threading_Event()
        res_ev = asyncio_Event()
        task_result: TaskResult = None

        def thread(root_loop, res_ev, sync_ev, done_ev):
            nonlocal task_result
            loop = new_event_loop()
            run_until_complete = loop.run_until_complete
            call_soon_threadsafe = root_loop.call_soon_threadsafe
            try:
                set_event_loop(loop)
                __anext__ = self.execute(task_instance).__anext__
                while True:
                    try:
                        sync_ev.clear()
                        task_result = run_until_complete(__anext__())
                        call_soon_threadsafe(res_ev.set)
                        sync_ev.wait()
                    except StopAsyncIteration:
                        break
                    except Exception as e:
                        logger.exception(e)
            finally:
                run_until_complete(loop.shutdown_asyncgens())
                loop.close()
                if not root_loop.is_closed():
                    call_soon_threadsafe(done_ev.set)
                    call_soon_threadsafe(res_ev.set)

        Thread(
            target=thread,
            args=(root_loop, res_ev, sync_ev, done_ev),
            # name=f"Task[{task_instance.task_id}]",
        ).start()

        while True:
            await res_ev.wait()
            if done_ev.is_set():
                break
            res_ev.clear()
            sync_ev.set()
            yield task_result
