import asyncio
import logging
import sys
import time
from types import FunctionType, MethodType
from typing import Callable, Union
from uuid import UUID

from arrlio import __tasks__, settings
from arrlio.exc import TaskError, TaskNoResultError, TaskTimeoutError
from arrlio.models import Task, TaskData, TaskInstance, TaskResult
from arrlio.settings import ClientConfig, WorkerConfig


logger = logging.getLogger("arrlio")


def task(
    func: FunctionType = None,
    name: str = None,
    bind: bool = None,
    base: Task = None,
    queue: str = None,
    priority: int = None,
    timeout: int = None,
    ttl: int = None,
    ack_late: bool = None,
    result_ttl: int = None,
    result_return: bool = None,
    result_encrypt: bool = None,
    loads: Callable = None,
    dumps: Callable = None,
) -> Task:

    if bind is None:
        bind = settings.TASK_BIND
    if base is None:
        base = Task
    if queue is None:
        queue = settings.TASK_QUEUE
    if priority is None:
        priority = settings.TASK_PRIORITY
    if timeout is None:
        timeout = settings.TASK_TIMEOUT
    if ttl is None:
        ttl = settings.TASK_TTL
    if ack_late is None:
        ack_late = settings.TASK_ACK_LATE
    if result_ttl is None:
        result_ttl = settings.RESULT_TTL
    if result_return is None:
        result_return = settings.RESULT_RETURN
    if result_encrypt is None:
        result_encrypt = settings.RESULT_ENCRYPT

    if func is not None:
        if not isinstance(func, (FunctionType, MethodType)):
            raise TypeError("Argument 'func' does not a function or method")
        if name is None:
            name = f"{func.__module__}.{func.__name__}"
        if name in __tasks__:
            raise ValueError(f"Task '{name}' already registered")
        t = base(
            func=func,
            name=name,
            bind=bind,
            queue=queue,
            priority=priority,
            timeout=timeout,
            ttl=ttl,
            ack_late=ack_late,
            result_ttl=result_ttl,
            result_return=result_return,
            result_encrypt=result_encrypt,
        )
        __tasks__[name] = t
        logger.info("Register %s", t)
        return t
    else:

        def wrapper(func):
            return task(
                func=func,
                name=name,
                bind=bind,
                base=base,
                queue=queue,
                priority=priority,
                timeout=timeout,
                ttl=ttl,
                ack_late=ack_late,
                result_ttl=result_ttl,
                result_return=result_return,
                result_encrypt=result_encrypt,
            )

        return wrapper


class Base:
    def __init__(self, config: Union[ClientConfig, WorkerConfig], backend_config_kwds: dict = None):
        self.config = config
        backend_config_kwds = backend_config_kwds or {}
        self.backend = self.config.backend.Backend(self.config.backend.BackendConfig(**(backend_config_kwds)))

    def __str__(self):
        return f"[{self.__class__.__name__}{self.backend}]"

    def __repr__(self):
        return self.__str__()


class Client(Base):
    def __init__(self, config: ClientConfig, backend_config_kwds: dict = None):
        super().__init__(config, backend_config_kwds=backend_config_kwds)

    async def close(self):
        await self.backend.close()

    async def call(
        self,
        task_or_name: Union[Task, str],
        args: tuple = None,
        kwds: dict = None,
        queue: str = None,
        priority: int = None,
        timeout: int = None,
        ttl: int = None,
        encrypt: bool = None,
        result_ttl: int = None,
        result_return: bool = None,
        result_encrypt: bool = None,
    ) -> "AsyncResult":

        name = task_or_name
        if isinstance(task_or_name, Task):
            name = task_or_name.name

        if args is None:
            args = ()
        if kwds is None:
            kwds = {}

        task_data = TaskData(
            args=args,
            kwds=kwds,
            queue=queue,
            priority=priority,
            timeout=timeout,
            ttl=ttl,
            result_ttl=result_ttl,
            result_return=result_return,
            result_encrypt=result_encrypt,
        )

        if name in __tasks__:
            task_instance = __tasks__[name].instatiate(data=task_data)
        else:
            task_instance = Task(None, name).instatiate(data=task_data)

        logger.info("%s: send %s", self, task_instance)

        await self.backend.send_task(task_instance, encrypt=encrypt)

        return AsyncResult(self, task_instance)

    async def pop_result(self, task_instance: TaskInstance):
        task_result = await self.backend.pop_task_result(task_instance)
        if task_result.exc:
            if isinstance(task_result.exc, TaskError):
                raise task_result.exc
            else:
                raise TaskError(task_result.exc, task_result.trb)
        return task_result.res


class Worker(Base):
    def __init__(self, config: WorkerConfig, backend_config_kwds: dict = None):
        super().__init__(config, backend_config_kwds=backend_config_kwds)
        self._running_tasks: dict = {}
        self._lock: asyncio.Lock = asyncio.Lock()
        self._stopped: bool = False

    async def run(self):
        logger.info("%s: consuming task queues %s", self, self.config.task_queues)
        self._stopped = False
        await self.backend.consume_tasks(self.config.task_queues, self._on_task)

    async def stop(self):
        self._stopped = True
        await self.backend.close()
        for task_id, aio_task in self._running_tasks.items():
            logger.debug("Canceling task '%s'", task_id)
            aio_task.cancel()

    async def _on_task(self, task_instance: TaskInstance):
        try:
            task_id: UUID = task_instance.data.task_id

            async with self._lock:
                if len(self._running_tasks) + 1 >= self.config.pool_size:
                    await self.backend.stop_consume_tasks()
                    try:
                        aio_task = asyncio.create_task(self._execute_task(task_instance))
                        aio_task.add_done_callback(lambda *args: self._running_tasks.pop(task_id, None))
                        self._running_tasks[task_id] = aio_task
                        await aio_task
                    finally:
                        if not self._stopped:
                            await self.backend.consume_tasks(self.config.task_queues, self._on_task)
                    return

            aio_task = asyncio.create_task(self._execute_task(task_instance))
            aio_task.add_done_callback(lambda *args: self._running_tasks.pop(task_id, None))
            self._running_tasks[task_id] = aio_task

        except Exception as e:
            logger.exception(e)

    async def _execute_task(self, task_instance: TaskInstance):
        try:
            if self._stopped:
                return

            task_data = task_instance.data
            task = task_instance.task

            res, exc, trb = None, None, None
            t0 = time.monotonic()

            logger.info("%s: call task %s(%s)", self, task.name, task_data.task_id)

            try:
                try:
                    res = await asyncio.wait_for(task_instance(), task_data.timeout)
                except asyncio.TimeoutError:
                    raise TaskTimeoutError(task_data.timeout)
            except Exception as e:
                exc_info = sys.exc_info()
                exc = exc_info[1]
                trb = exc_info[2]
                if isinstance(e, TaskTimeoutError):
                    logger.error("Task timeout for %s", task_instance)
                else:
                    logger.exception("%s: %s", self, task_instance)

            logger.info(
                "%s: task %s(%s) done in %.2f second(s)",
                self,
                task.name,
                task_data.task_id,
                time.monotonic() - t0,
            )

            if task_instance.task.result_return:
                try:
                    await self.backend.push_task_result(
                        task_instance,
                        TaskResult(res=res, exc=exc, trb=trb),
                        encrypt=task_instance.task.result_encrypt,
                    )
                except Exception as e:
                    logger.exception(e)

        except asyncio.CancelledError:
            pass


class AsyncResult:
    def __init__(self, client: Client, task_instance: TaskInstance):
        self._client = client
        self._task_instance = task_instance
        self._result = None
        self._exception: Exception = None
        self._ready: bool = False

    @property
    def task_instance(self) -> TaskInstance:
        return self._task_instance

    @property
    def result(self):
        return self._result

    @property
    def exception(self) -> Exception:
        return self._exception

    @property
    def ready(self) -> bool:
        return self._ready

    async def get(self):
        if not self._task_instance.task.result_return:
            raise TaskNoResultError(self._task_instance.data.task_id)
        if not self._ready:
            try:
                self._result = await self._client.pop_result(self._task_instance)
                self._ready = True
            except TaskError as e:
                self._exception = e
                self._ready = True
        if self._exception:
            raise self._exception
        return self._result
