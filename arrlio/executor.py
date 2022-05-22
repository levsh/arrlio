import asyncio
import inspect
import logging
import sys
import threading
import time

from arrlio.exc import NotFoundError, TaskTimeoutError
from arrlio.models import Task, TaskData, TaskInstance, TaskResult

logger = logging.getLogger("arrlio.executor")


class Executor:
    def __str__(self):
        return self.__class__.__name__

    def __repr__(self):
        return self.__str__()

    async def execute(self, task_instance: TaskInstance) -> TaskResult:
        task: Task = task_instance.task
        task_data: TaskData = task_instance.data

        res, exc, trb = None, None, None
        t0 = time.monotonic()

        logger.info("%s: execute task %s(%s)", self, task.name, task_data.task_id)

        try:

            if task.func is None:
                raise NotFoundError(f"Task '{task.name}' not found")

            kwdefaults = task.func.__kwdefaults__
            meta: bool = kwdefaults is not None and "meta" in kwdefaults

            try:
                if inspect.iscoroutinefunction(task.func):
                    res = await asyncio.wait_for(task_instance(meta=meta), task_data.timeout)
                else:
                    res = task_instance(meta=meta)
            except asyncio.TimeoutError:
                raise TaskTimeoutError(task_data.timeout)

        except Exception as e:
            exc_info = sys.exc_info()
            exc, trb = exc_info[1], exc_info[2]
            if isinstance(e, TaskTimeoutError):
                logger.error("%s: task timeout for %s", self, task_instance)
            else:
                logger.exception(task_instance)

        logger.info(
            "%s: task %s(%s) done in %.2f second(s)",
            self,
            task.name,
            task_data.task_id,
            time.monotonic() - t0,
        )

        if isinstance(res, TaskResult):
            return res

        return TaskResult(res=res, exc=exc, trb=trb)

    async def execute_in_thread(self, task_instance: TaskInstance) -> TaskResult:
        root_loop = asyncio.get_running_loop()
        done_ev: asyncio.Event = asyncio.Event()
        task_result: TaskResult = None

        def thread():
            nonlocal done_ev
            nonlocal task_result
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                task_result = loop.run_until_complete(self.execute(task_instance))
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
                root_loop.call_soon_threadsafe(lambda: done_ev.set())

        th = threading.Thread(target=thread)
        th.start()

        await done_ev.wait()

        return task_result

    async def __call__(self, task_instance: TaskInstance) -> TaskResult:
        if task_instance.data.thread:
            return await self.execute_in_thread(task_instance)
        return await self.execute(task_instance)
