import asyncio
import logging
from asyncio import current_task, gather
from contextlib import AsyncExitStack
from contextvars import ContextVar
from inspect import isasyncgenfunction, isgeneratorfunction
from types import FunctionType, MethodType
from typing import Any, AsyncGenerator, Callable, Dict, List, Type, Union
from uuid import UUID

from roview import rodict

from arrlio.exc import GraphError, TaskClosedError, TaskError
from arrlio.models import Event, Message, Task, TaskData, TaskInstance, TaskResult
from arrlio.plugins.base import Plugin
from arrlio.settings import Config
from arrlio.tp import AsyncCallableT

logger = logging.getLogger("arrlio.core")


__tasks__ = {}


registered_tasks = rodict(__tasks__, nested=True)


def task(func: Union[FunctionType, MethodType, Type] = None, name: str = None, base: Type[Task] = None, **kwds):
    """
    Args:
        func (Union[FunctionType, MethodType, Type], optional): Task function.
        name (str, optional): ~arrlio.models.Task name.
        base (~arrlio.models.Task, optional): ~arrlio.models.Task base class.
        kwds (dict, optional): ~arrlio.models.TaskData arguments.
    """

    if base is None:
        base = Task
    if func is not None:
        if not isinstance(func, (FunctionType, MethodType)):
            raise TypeError("Argument 'func' does not a function or method")
        if name is None:
            name = f"{func.__module__}.{func.__name__}"
        if name in __tasks__:
            raise ValueError(f"Task '{name}' already registered")
        t = base(func=func, name=name, **kwds)
        __tasks__[name] = t
        logger.debug("Register task '%s'", t.name)
        return t

    def wrapper(func):
        return task(base=base, func=func, name=name, **kwds)

    return wrapper


class App:
    """
    Args:
        config (~arrlio.settings.Config): Arrlio application config.
    """

    def __init__(self, config: Config):
        self.config = config
        self._backend = config.backend.module.Backend(config.backend.config)
        self._closed: asyncio.Future = asyncio.Future()
        self._running_tasks: Dict[UUID, asyncio.Task] = {}
        self._running_messages: Dict[UUID, asyncio.Task] = {}
        self._executor = config.executor.module.Executor(config.executor.config)
        self._context = ContextVar("context", default={})

        self._hooks = {
            "on_init": [],
            "on_close": [],
            "on_task_send": [],
            "on_task_received": [],
            "on_task_result": [],
            "on_task_done": [],
            "task_context": [],
        }
        self._plugins = {}
        for plugin_config in config.plugins:
            plugin = plugin_config.module.Plugin(self, plugin_config.config)
            self._plugins[plugin.name] = plugin
            for k, hooks in self._hooks.items():
                if getattr(plugin, k).__func__ != getattr(Plugin, k):
                    hooks.append(getattr(plugin, k))

        self._task_settings = {
            k: v
            for k, v in self.config.task.dict(exclude_unset=True).items()
            if k in TaskData.__dataclass_fields__  # pylint: disable=no-member
        }

    def __str__(self):
        return f"{self.__class__.__name__}[{self._backend}]"

    def __repr__(self):
        return self.__str__()

    async def __aenter__(self):
        await self.init()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @property
    def hooks(self):
        return rodict(self._hooks, nested=True)

    @property
    def plugins(self):
        return rodict(self._plugins, nested=True)

    @property
    def backend(self):
        return self._backend

    @property
    def executor(self):
        return self._executor

    @property
    def context(self):
        return self._context

    @property
    def is_closed(self) -> bool:
        return self._closed.done()

    @property
    def task_settings(self) -> dict:
        return self._task_settings

    async def init(self):
        if self.is_closed:
            return

        logger.info("%s: initialization...", self)

        await self._execute_hooks("on_init")

        logger.info("%s: initialization done", self)

    async def close(self):
        if self.is_closed:
            return

        try:
            await self._execute_hooks("on_close")
            for hooks in self._hooks.values():
                hooks.clear()

            await gather(
                self.stop_consume_tasks(),
                self.stop_consume_messages(),
                self.stop_consume_events(),
                return_exceptions=True,
            )

            for task_id, aio_task in self._running_tasks.items():
                logger.debug("%s: cancel processing task '%s'", self, task_id)
                aio_task.cancel()
            self._running_tasks = {}

            for message_id, aio_task in self._running_messages.items():
                logger.debug("%s: cancel processing message '%s'", self, message_id)
                aio_task.cancel()

            await self._backend.close()
        finally:
            self._closed.set_result(None)

    async def _execute_hook(self, hook_fn, *args, **kwds):
        try:
            logger.debug("%s: execute hook %s", self, hook_fn)
            await hook_fn(*args, **kwds)
        except Exception:
            logger.exception("%s: hook %s error", self, hook_fn)

    async def _execute_hooks(self, hook: str, *args, **kwds):
        await gather(*(self._execute_hook(hook_fn, *args, **kwds) for hook_fn in self._hooks[hook]))

    async def send_task(
        self,
        task: Union[Task, str],  # pylint: disable=redefined-outer-name
        args: tuple = None,
        kwds: dict = None,
        extra: dict = None,
        **kwargs,
    ) -> "AsyncResult":
        """
        Args:
            task (Union[~arrlio.models.Task, str]): ~arrlio.models.Task of task name.
            args (tuple, optional): Task args.
            kwds (dict, optional): Task kwds.
            extra (dict, optional): ~arrlio.models.TaskData extra.
            kwargs (dict, optional): Other ~arrlio.models.TaskData arguments.

        Returns:
            AsyncResult: Task ~arrlio.core.AsyncResult.
        """

        name = task
        if isinstance(task, Task):
            name = task.name

        if args is None:
            args = ()
        if kwds is None:
            kwds = {}
        if extra is None:
            extra = {}

        extra["app_id"] = self.config.app_id

        if name in __tasks__:
            task_instance = __tasks__[name].instantiate(
                args=args,
                kwds=kwds,
                extra=extra,
                **{**self._task_settings, **kwargs},
            )
        else:
            task_instance = Task(None, name).instantiate(
                args=args,
                kwds=kwds,
                extra=extra,
                **{**self._task_settings, **kwargs},
            )

        logger.info("%s: send %s", self, task_instance.dict(exclude=["data.args", "data.kwds"]))

        await self._execute_hooks("on_task_send", task_instance)

        await self._backend.send_task(task_instance)

        return AsyncResult(self, task_instance)

    async def send_message(self, data: Any, routing_key: str = None, **kwds):
        """
        Args:
            data (Any): Message data.
            routing_key (str, optional): Message routing key.
            kwds (dict, optional): ~arrlio.models.Message arguments.
        """

        message_settings = self.config.message.dict(exclude_unset=True)
        message = Message(data=data, **{**message_settings, **kwds})

        logger.info("%s: send %s", self, message)

        await self._backend.send_message(message, routing_key=routing_key)

    async def send_event(self, event: Event):
        logger.info("%s: send %s", self, event)
        await self._backend.send_event(event)

    async def pop_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        # if not task_instance.data.result_return:
        #     raise TaskNoResultError(task_instance.data.task_id)

        async for task_result in self._backend.pop_task_result(task_instance):

            if task_result.exc:
                if isinstance(task_result.exc, TaskError):
                    raise task_result.exc
                raise TaskError(task_result.exc, task_result.trb)

            yield task_result.res

    async def consume_tasks(self, queues: List[str] = None):
        queues = queues or self.config.task_queues
        if not queues:
            return

        async def cb(task_instance: TaskInstance):
            task_data: TaskData = task_instance.data
            task_id: UUID = task_data.task_id

            try:
                self._running_tasks[task_id] = current_task()

                async with AsyncExitStack() as stack:

                    for context in self._hooks["task_context"]:
                        await stack.enter_async_context(context(task_instance))

                    await self._execute_hooks("on_task_received", task_instance)

                    task_result: TaskResult = TaskResult()

                    async for task_result in self.execute_task(task_instance):

                        if task_data.result_return:
                            await self._backend.push_task_result(task_instance, task_result)

                        await self._execute_hooks("on_task_result", task_instance, task_result)

                    if task_data.result_return and not task_data.extra.get("graph"):
                        func = task_instance.task.func
                        if isasyncgenfunction(func) or isgeneratorfunction(func):
                            await self._backend.close_task(task_instance)

                    await self._execute_hooks(
                        "on_task_done",
                        task_instance,
                        {"exc": task_result.exc, "trb": task_result.trb},
                    )

            except asyncio.CancelledError:
                logger.error("%s: task %s(%s) cancelled", self, task_id, task_instance.task.name)
            except Exception as e:
                logger.exception(e)
            finally:
                self._running_tasks.pop(task_id, None)

        await self._backend.consume_tasks(queues, cb)
        logger.info("%s: consuming task queues %s", self, queues)

    async def stop_consume_tasks(self, queues: List[str] = None):
        await self._backend.stop_consume_tasks(queues=queues)
        if queues is not None:
            logger.info("%s: stop consuming task queues %s", self, queues)
        else:
            logger.info("%s: stop consuming task queues", self)

    async def execute_task(self, task_instance: TaskInstance) -> TaskResult:
        async for task_result in self._executor(task_instance):
            yield task_result

    async def consume_messages(self, on_message: AsyncCallableT):
        queues = self.config.message_queues
        if not queues:
            return

        async def cb(message: Message):
            message_id: UUID = message.message_id

            try:
                self._running_messages[message_id] = current_task()
                await on_message(message.data)
            except Exception as e:
                logger.exception(e)
            finally:
                self._running_messages.pop(message_id, None)

        await self._backend.consume_messages(queues, cb)
        logger.info("%s: consuming message queues %s", self, queues)

    async def stop_consume_messages(self):
        await self._backend.stop_consume_messages()
        logger.info("%s: stop consuming messages", self)
        self._running_messages = {}

    async def consume_events(self, cb_id: str, cb: Union[Callable, AsyncCallableT], event_types: List[str] = None):
        await self._backend.consume_events(cb_id, cb, event_types=event_types)

    async def stop_consume_events(self, cb_id=None):
        await self._backend.stop_consume_events(cb_id=cb_id)

    def send_graph(self, *args, **kwds):
        if "arrlio.graphs" not in self.plugins:
            raise GraphError("Plugin required: allrio.graphs")
        return self.plugins["arrlio.graphs"].send_graph(*args, **kwds)


class AsyncResult:
    __slots__ = ("_app", "_task_instance", "_gen", "_result", "_exception", "_ready")

    def __init__(self, app: App, task_instance: TaskInstance):
        self._app: App = app
        self._task_instance: TaskInstance = task_instance
        self._gen = self._app.pop_result(self._task_instance)
        self._result = None
        self._exception: Exception = None
        self._ready: bool = False

    @property
    def task_instance(self) -> TaskInstance:
        return self._task_instance

    @property
    def task_id(self):
        return self._task_instance.data.task_id

    @property
    def result(self):
        return self._result

    @property
    def exception(self) -> Exception:
        return self._exception

    @property
    def ready(self) -> bool:
        return self._ready

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._ready:
            try:
                self._result = await self._gen.__anext__()
                return self._result
            except TaskError as e:
                self._ready = True
                self._exception = e
            except StopAsyncIteration as e:
                self._ready = True
                raise e

        if self._exception:
            if isinstance(self._exception.args[0], Exception):
                raise self._exception from self._exception.args[0]
            raise self._exception

        raise StopAsyncIteration

    async def get(self):
        noresult = not self._ready
        async for _ in self:
            noresult = False
        if noresult:
            raise TaskClosedError(self.task_id)
        return self._result
