import asyncio
import logging
import sys
import threading
from asyncio import get_running_loop, new_event_loop, set_event_loop, wait_for
from inspect import isasyncgenfunction, iscoroutinefunction, isgeneratorfunction
from threading import Thread, current_thread
from time import monotonic

from pydantic import BaseSettings

from arrlio.exc import NotFoundError, TaskError, TaskTimeoutError
from arrlio.models import Task, TaskData, TaskInstance, TaskResult

logger = logging.getLogger("arrlio.executor")

asyncio_Event = asyncio.Event  # pylint: disable=invalid-name
threading_Event = threading.Event  # pylint: disable=invalid-name


class Config(BaseSettings):
    class Config:
        validate_assignment = True


class Executor:
    def __init__(self, config: Config):
        self.config = config

    def __str__(self):
        return self.__class__.__name__

    def __repr__(self):
        return self.__str__()

    async def __call__(self, task_instance: TaskInstance) -> TaskResult:
        if task_instance.data.thread:
            execute = self.execute_in_thread
        else:
            execute = self.execute
        async for res in execute(task_instance):
            yield res

    async def execute(self, task_instance: TaskInstance) -> TaskResult:
        task: Task = task_instance.task
        task_data: TaskData = task_instance.data

        res, exc, trb = None, None, None
        t0 = monotonic()

        try:

            if (func := task.func) is None:
                raise NotFoundError(f"Task '{task.name}' not found")

            kwdefaults = func.__kwdefaults__
            meta: bool = kwdefaults is not None and "meta" in kwdefaults

            logger.info("%s[%s]: execute task %s(%s)", self, current_thread().name, task_data.task_id, task.name)

            try:

                if iscoroutinefunction(func):

                    res = await wait_for(task_instance(meta=meta), task_data.timeout)
                    if isinstance(res, TaskResult):
                        yield res
                    else:
                        yield TaskResult(res=res, exc=exc, trb=trb)

                elif isgeneratorfunction(func):

                    if task_data.extra.get("graph"):
                        raise TaskError("generator not supported")

                    for res in task_instance(meta=meta):
                        if isinstance(res, TaskResult):
                            yield res
                        else:
                            yield TaskResult(res=res, exc=exc, trb=trb)

                elif isasyncgenfunction(func):

                    agen = task_instance(meta=meta)

                    timeout_time = (monotonic() + task_data.timeout) if task_data.timeout else None

                    while True:
                        timeout = (timeout_time - monotonic()) if timeout_time else None

                        try:
                            res = await wait_for(agen.__anext__(), timeout)
                            if isinstance(res, TaskResult):
                                yield res
                            else:
                                yield TaskResult(res=res, exc=exc, trb=trb)

                        except StopAsyncIteration:
                            break
                        except asyncio.TimeoutError:
                            raise TaskTimeoutError(task_data.timeout)

                else:

                    res = task_instance(meta=meta)
                    if isinstance(res, TaskResult):
                        yield res
                    else:
                        yield TaskResult(res=res, exc=exc, trb=trb)

            except asyncio.TimeoutError:
                raise TaskTimeoutError(task_data.timeout)

        except Exception as e:
            exc_info = sys.exc_info()
            exc, trb = exc_info[1], exc_info[2]
            if isinstance(e, TaskTimeoutError):
                logger.error("%s: task %s(%s) timeout", self, task.name, task_data.task_id)
            else:
                logger.exception(task_instance.dict(exclude=["data.args", "data.kwds"]))
            yield TaskResult(res=res, exc=exc, trb=trb)

        logger.info(
            "%s: task %s(%s) done in %.2f second(s)",
            self,
            task_data.task_id,
            task.name,
            monotonic() - t0,
        )

    async def execute_in_thread(self, task_instance: TaskInstance) -> TaskResult:
        root_loop = get_running_loop()
        done_ev: asyncio_Event = asyncio_Event()
        res_ev: asyncio_Event = asyncio_Event()
        sync_ev: threading_Event = threading_Event()
        task_result: TaskResult = None

        def thread(root_loop, res_ev, sync_ev, done_ev):
            nonlocal task_result
            loop = new_event_loop()
            try:
                set_event_loop(loop)
                agen = self.execute(task_instance)
                while True:
                    try:
                        sync_ev.clear()
                        task_result = loop.run_until_complete(agen.__anext__())
                        root_loop.call_soon_threadsafe(res_ev.set)
                        sync_ev.wait()
                    except StopAsyncIteration:
                        break
                    except Exception as e:
                        logger.exception(e)
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
                if not root_loop.is_closed():
                    root_loop.call_soon_threadsafe(done_ev.set)
                    root_loop.call_soon_threadsafe(res_ev.set)

        Thread(target=thread, args=(root_loop, res_ev, sync_ev, done_ev)).start()

        while True:
            await res_ev.wait()
            if done_ev.is_set():
                break
            res_ev.clear()
            sync_ev.set()
            yield task_result
