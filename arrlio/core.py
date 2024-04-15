import asyncio
import dataclasses
import logging
from asyncio import current_task, gather
from contextlib import AsyncExitStack
from contextvars import ContextVar
from inspect import isasyncgenfunction, isgeneratorfunction
from types import FunctionType, MethodType
from typing import Any, AsyncGenerator, Callable, Type
from uuid import UUID, uuid4

from rich.pretty import pretty_repr
from roview import rodict

from arrlio import settings
from arrlio.backends.base import Backend
from arrlio.configs import Config
from arrlio.exceptions import GraphError, TaskClosedError, TaskError
from arrlio.executor import Executor
from arrlio.models import Event, Task, TaskInstance, TaskResult
from arrlio.plugins.base import Plugin
from arrlio.types import Args, Kwds
from arrlio.utils import is_debug_level, is_info_level

logger = logging.getLogger("arrlio.core")


registered_tasks = rodict({}, nested=True)


def task(
    func: FunctionType | MethodType | Type | None = None,
    name: str | None = None,
    base: Type[Task] | None = None,
    **kwds: dict,
):
    """Task decorator.

    Args:
        func: Task function.
        name: Task name.
        base: Task base class.
        kwds: `arrlio.models.Task` arguments.
    """

    if base is None:
        base = Task
    if func is not None:
        if not isinstance(func, (FunctionType, MethodType)):
            raise TypeError("Argument 'func' does not a function or method")
        if name is None:
            name = f"{func.__module__}.{func.__name__}"
        if name in registered_tasks:
            raise ValueError(f"Task '{name}' already registered")
        t = base(func=func, name=name, **kwds)
        registered_tasks.__original__[name] = t
        logger.debug("Register task '%s'", t.name)
        return t

    def wrapper(func):
        return task(base=base, func=func, name=name, **kwds)

    return wrapper


class App:
    """Arrlio application."""

    def __init__(self, config: Config):
        """
        Args:
            config: Arrlio application `arrlio.settings.Config`.
        """

        self.config = config

        self._backend = config.backend.module.Backend(config.backend.config)
        self._closed: asyncio.Future = asyncio.Future()
        self._running_tasks: dict[UUID, asyncio.Task] = {}
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
            k: v for k, v in config.task.model_dump(exclude_unset=True).items() if k in dataclasses.fields(Task)
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
    def plugins(self) -> dict[str, Plugin]:
        """Application plugins."""

        return rodict(self._plugins, nested=True)

    @property
    def backend(self) -> Backend:
        """Application backend."""

        return self._backend

    @property
    def executor(self) -> Executor:
        """Application executor."""

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
        """Init application and plugins."""

        if self.is_closed:
            return

        logger.info("%s: initializing with config\n%s", self, pretty_repr(self.config.model_dump()))

        await self._execute_hooks("on_init")

        logger.info("%s: initialization done", self)

    async def close(self):
        """Close application."""

        if self.is_closed:
            return

        try:
            await self._execute_hooks("on_close")
            for hooks in self._hooks.values():
                hooks.clear()

            await gather(
                self.stop_consume_tasks(),
                self.stop_consume_events(),
                return_exceptions=True,
            )

            await self._backend.close()

            for task_id, aio_task in tuple(self._running_tasks.items()):
                logger.warning("%s: cancel processing task '%s'", self, task_id)
                aio_task.cancel()
                try:
                    await aio_task
                except asyncio.CancelledError:
                    pass
            self._running_tasks = {}

        finally:
            self._closed.set_result(None)

    async def _execute_hook(self, hook_fn, *args, **kwds):
        try:
            if is_debug_level():
                logger.debug("%s: execute hook %s", self, hook_fn)
            await hook_fn(*args, **kwds)
        except Exception:
            logger.exception("%s: hook %s error", self, hook_fn)

    async def _execute_hooks(self, hook: str, *args, **kwds):
        await gather(*(self._execute_hook(hook_fn, *args, **kwds) for hook_fn in self._hooks[hook]))

    async def send_task(
        self,
        task: Task | str,  # pylint: disable=redefined-outer-name
        args: Args | None = None,
        kwds: Kwds | None = None,
        extra: dict | None = None,
        **kwargs: dict,
    ) -> "AsyncResult":
        """Send task.

        Args:
            task: `arrlio.models.Task` or task name.
            args: Task args.
            kwds: Task kwds.
            extra: `arrlio.models.Task` extra argument.
            kwargs: Other `arrlio.models.Task` other arguments.

        Returns:
            Task `arrlio.core.AsyncResult`.
        """

        name = task
        if isinstance(task, Task):
            name = task.name

        if extra is None:
            extra = {}

        extra["app_id"] = self.config.app_id

        if name in registered_tasks:
            task_instance = registered_tasks[name].instantiate(
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

        if is_info_level():
            logger.info(
                "%s: send task instance\n%s",
                self,
                task_instance.pretty_repr(sanitize=settings.LOG_SANITIZE),
            )

        await self._execute_hooks("on_task_send", task_instance)

        await self._backend.send_task(task_instance)

        return AsyncResult(self, task_instance)

    async def send_event(self, event: Event):
        if is_info_level():
            logger.info("%s: send event\n%s", self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

        await self._backend.send_event(event)

    async def pop_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        async for task_result in self._backend.pop_task_result(task_instance):
            if is_info_level():
                logger.info(
                    "%s got result[idx=%s, exc=%s] for task %s[%s]",
                    self,
                    task_result.idx,
                    task_result.exc is not None,
                    task_instance.name,
                    task_instance.task_id,
                )
            if task_result.exc:
                if isinstance(task_result.exc, TaskError):
                    raise task_result.exc
                raise TaskError(task_instance.task_id, task_result.exc, task_result.trb)

            yield task_result.res

    async def consume_tasks(self, queues: list[str] | None = None):
        """Consume tasks from the queues."""

        queues = queues or self.config.task_queues
        if not queues:
            return

        async def cb(task_instance: TaskInstance):
            task_id: UUID = task_instance.task_id

            self._running_tasks[task_id] = current_task()
            try:
                async with AsyncExitStack() as stack:
                    for context in self._hooks["task_context"]:
                        await stack.enter_async_context(context(task_instance))

                    await self._execute_hooks("on_task_received", task_instance)

                    task_result: TaskResult = TaskResult()

                    idx_0 = uuid4().hex
                    idx_1 = 0

                    async for task_result in self.execute_task(task_instance):
                        idx_1 += 1
                        task_result.set_idx([idx_0, idx_1])

                        if task_instance.result_return:
                            await self._backend.push_task_result(task_result, task_instance)

                        await self._execute_hooks("on_task_result", task_instance, task_result)

                    if task_instance.result_return and not task_instance.extra.get("graph:graph"):
                        func = task_instance.func
                        if isasyncgenfunction(func) or isgeneratorfunction(func):
                            idx_1 += 1
                            await self._backend.close_task(task_instance, idx=(idx_0, idx_1))

                    await self._execute_hooks("on_task_done", task_instance, task_result)

            except asyncio.CancelledError:
                logger.error("%s: task %s(%s) cancelled", self, task_id, task_instance.name)
                raise
            except Exception as e:
                logger.exception(e)
            finally:
                self._running_tasks.pop(task_id, None)

        await self._backend.consume_tasks(queues, cb)
        logger.info("%s: consuming task queues %s", self, queues)

    async def stop_consume_tasks(self, queues: list[str] | None = None):
        """Stop consuming tasks."""

        await self._backend.stop_consume_tasks(queues=queues)
        if queues is not None:
            logger.info("%s: stop consuming task queues %s", self, queues)
        else:
            logger.info("%s: stop consuming task queues", self)

    async def execute_task(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        """Execute the task instance locally by the application executor."""

        async for task_result in self._executor(task_instance):
            yield task_result

    async def consume_events(
        self,
        callback_id: str,
        callback: Callable[[Event], Any],
        event_types: list[str] | None = None,
    ):
        """Consume events."""

        await self._backend.consume_events(callback_id, callback, event_types=event_types)

    async def stop_consume_events(self, callback_id: str | None = None):
        """Stop consuming events."""

        await self._backend.stop_consume_events(callback_id=callback_id)

    def send_graph(self, *args, **kwds):
        if "arrlio.graphs" not in self.plugins:
            raise GraphError("Plugin required: allrio.graphs")
        return self.plugins["arrlio.graphs"].send_graph(*args, **kwds)


class AsyncResult:
    __slots__ = ("_app", "_task_instance", "_gen", "_result", "_exception", "_ready")

    def __init__(self, app: App, task_instance: TaskInstance):
        self._app = app
        self._task_instance = task_instance
        self._gen = app.pop_result(task_instance)
        self._result = None
        self._exception = None
        self._ready = False

    @property
    def task_instance(self) -> TaskInstance:
        """Task instance."""
        return self._task_instance

    @property
    def task_id(self):
        """Task Id."""
        return self._task_instance.task_id

    @property
    def result(self):
        """Task last result."""
        return self._result

    @property
    def exception(self) -> Exception:
        """Task exception."""
        return self._exception

    @property
    def ready(self) -> bool:
        """Task ready status."""
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

        if exception := self._exception:
            if isinstance(exception.args[0], Exception):
                raise exception from exception.args[0]
            raise exception

        raise StopAsyncIteration

    async def get(self) -> Any:
        """
        Get task result. Blocking until the task result available.
        For generator or asyncgenerator return the last available result.
        """

        noresult = not self._ready
        async for _ in self:
            noresult = False
        if noresult:
            raise TaskClosedError(self.task_id)
        return self._result
