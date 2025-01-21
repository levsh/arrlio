import asyncio
import dataclasses
import logging

from asyncio import TaskGroup, current_task, gather
from contextlib import AsyncExitStack
from contextvars import ContextVar
from inspect import isasyncgenfunction, isgeneratorfunction
from types import FunctionType, MethodType
from typing import Any, AsyncGenerator, Callable, Protocol, Type
from uuid import UUID, uuid4

from rich.pretty import pretty_repr
from roview import rodict

from arrlio import gettext, settings
from arrlio.abc import AbstractBroker, AbstractEventBackend, AbstractResultBackend
from arrlio.configs import Config
from arrlio.exceptions import (
    GraphError,
    HooksError,
    InternalError,
    TaskCancelledError,
    TaskClosedError,
    TaskError,
    TaskNotFoundError,
)
from arrlio.executor import Executor
from arrlio.models import Event, Task, TaskInstance, TaskResult
from arrlio.plugins.base import Plugin
from arrlio.types import Args, Kwds
from arrlio.utils import is_debug_level, is_info_level


_ = gettext.gettext


logger = logging.getLogger("arrlio.core")


registered_tasks = rodict({}, nested=True)


_curr_app = ContextVar("curr_app", default=None)


class AsyncFunc(Protocol):
    async def __call__(self, *args, **kwds) -> Any: ...


def task(
    func: FunctionType | MethodType | Type | AsyncFunc | None = None,
    name: str | None = None,
    base: Type[Task] | None = None,
    **kwds,
):
    """Task decorator.

    Args:
        func: Task function.
        name: Task name.
        base: Task base class.
        kwds: `Task` arguments.
    """

    if base is None:
        base = Task
    if func is not None:
        if not isinstance(func, (FunctionType, MethodType)):
            raise TypeError(_("Argument 'func' does not a function or method"))
        if name is None:
            name = f"{func.__module__}.{func.__name__}"
        if name in registered_tasks:
            raise ValueError(_("Task '{}' already registered").format(name))
        t = base(func=func, name=name, **kwds)
        registered_tasks.__original__[name] = t
        logger.debug(_("Register task '%s'"), t.name)
        return t

    def wrapper(func):
        return task(base=base, func=func, name=name, **kwds)

    return wrapper


class App:
    """
    Arrlio application.

    Args:
        config: Arrlio application `Config`.
    """

    def __init__(self, config: Config):
        self.config = config

        self._broker = config.broker.module.Broker(config.broker.config)
        self._result_backend = config.result_backend.module.ResultBackend(config.result_backend.config)
        self._event_backend = config.event_backend.module.EventBackend(config.event_backend.config)

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
        return f"{self.__class__.__name__}[{self.config.app_id}]"

    def __repr__(self):
        return self.__str__()

    async def __aenter__(self):
        await self.init()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @property
    def hooks(self):
        """Application hooks."""

        return rodict(self._hooks, nested=True)

    @property
    def plugins(self) -> dict[str, Plugin]:
        """Application plugins."""

        return rodict(self._plugins, nested=True)

    @property
    def broker(self) -> AbstractBroker:
        """Application broker."""

        return self._broker

    @property
    def result_backend(self) -> AbstractResultBackend:
        """Application result storage."""

        return self._result_backend

    @property
    def event_backend(self) -> AbstractEventBackend | None:
        """Application event backend."""

        return self._event_backend

    @property
    def executor(self) -> Executor:
        """Application executor."""

        return self._executor

    @property
    def context(self) -> dict:
        """Application current context."""

        return self._context.get()

    @property
    def is_closed(self) -> bool:
        """Application close satatus."""

        return self._closed.done()

    @property
    def task_settings(self) -> dict:
        return self._task_settings

    async def init(self):
        """Init application and plugins."""

        if self.is_closed:
            return

        logger.info(_("%s initializing with config\n%s"), self, pretty_repr(self.config.model_dump()))

        await self._broker.init()
        await self._result_backend.init()
        await self._event_backend.init()

        await self._execute_hooks("on_init")

        logger.info(_("%s initialization done"), self)

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

            await self._broker.close()
            await self._result_backend.close()
            await self._event_backend.close()

            for task_id, aio_task in tuple(self._running_tasks.items()):
                logger.warning(_("%s cancel processing task '%s'"), self, task_id)
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
                logger.debug(_("%s execute hook %s"), self, hook_fn)
            await hook_fn(*args, **kwds)
        except Exception as e:
            logger.exception(_("%s hook %s error"), self, hook_fn)
            raise e

    async def _execute_hooks(self, hook: str, *args, **kwds):
        try:
            async with TaskGroup() as tg:
                for hook_fn in self._hooks[hook]:
                    tg.create_task(self._execute_hook(hook_fn, *args, **kwds))
        except ExceptionGroup as eg:
            raise HooksError(exceptions=eg.exceptions)

    async def send_task(
        self,
        task: Task | str,
        args: Args | None = None,
        kwds: Kwds | None = None,
        headers: dict | None = None,
        **kwargs: dict,
    ) -> "AsyncResult":
        """
        Send task.

        Args:
            task: `Task` or task name.
            args: Task positional arguments.
            kwds: Task keyword arguments.
            headers: `Task` headers argument.
            kwargs: Other `Task` arguments.

        Returns:
            Task `AsyncResult`.
        """

        name = task
        if isinstance(task, Task):
            name = task.name

        if headers is None:
            headers = {}

        headers["app_id"] = self.config.app_id

        if name in registered_tasks:
            task_instance = registered_tasks[name].instantiate(
                args=args,
                kwds=kwds,
                headers=headers,
                **{**self._task_settings, **kwargs},
            )
        else:
            task_instance = Task(None, name).instantiate(
                args=args,
                kwds=kwds,
                headers=headers,
                **{**self._task_settings, **kwargs},
            )

        task_instance.headers.update(self._result_backend.make_headers(task_instance))
        task_instance.shared.update(self._result_backend.make_shared(task_instance))

        if is_info_level():
            logger.info(
                _("%s send task\n%s"),
                self,
                task_instance.pretty_repr(sanitize=settings.LOG_SANITIZE),
            )

        await self._execute_hooks("on_task_send", task_instance)

        await self._broker.send_task(task_instance)

        await self._result_backend.allocate_storage(task_instance)

        return AsyncResult(self, task_instance)

    async def send_event(self, event: Event):
        """
        Send event.

        Args:
            event: Event to send.
        """
        if is_info_level():
            logger.info(_("%s send event\n%s"), self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

        await self._event_backend.send_event(event)

    async def pop_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        """
        Pop result for provided `TaskInstance`.

        Args:
            task_instance: Task instance.

        Yields:
            Task result.
        """
        async for task_result in self._result_backend.pop_task_result(task_instance):
            if is_info_level():
                logger.info(
                    _("%s got result[idx=%s, exc=%s] for task %s[%s]"),
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

    def cancel_local_task(self, task_id: UUID | str):
        if isinstance(task_id, str):
            task_id = UUID(f"{task_id}")
        if task_id not in self._running_tasks:
            raise TaskNotFoundError(task_id)
        self._running_tasks[task_id].cancel()

    async def consume_tasks(self, queues: list[str] | None = None):
        """
        Consume tasks from the queues.

        Args:
            queues: List of queue names to consume.
        """

        queues = queues or self.config.task_queues
        if not queues:
            return

        async def cb(task_instance: TaskInstance):
            task_id: UUID = task_instance.task_id
            self._running_tasks[task_id] = current_task()

            idx_0 = uuid4().hex
            idx_1 = 0

            self._context.set({})
            context = self.context

            try:
                task_result: TaskResult = TaskResult()

                async with AsyncExitStack() as stack:
                    try:
                        context["task_instance"] = task_instance

                        for context_hook in self._hooks["task_context"]:
                            await stack.enter_async_context(context_hook(task_instance))

                        await self._execute_hooks("on_task_received", task_instance)

                        async for task_result in self.execute_task(task_instance):
                            task_result.set_idx((idx_0, idx_1 + 1))

                            if task_instance.result_return:
                                await self._result_backend.push_task_result(task_result, task_instance)

                            await self._execute_hooks("on_task_result", task_instance, task_result)
                            idx_1 += 1

                    except (asyncio.CancelledError, Exception) as e:
                        if isinstance(e, asyncio.CancelledError):
                            logger.error(_("%s task %s[%s] cancelled"), self, task_instance.name, task_id)
                            task_result = TaskResult(exc=TaskCancelledError(task_id))
                            raise e
                        if isinstance(e, HooksError):
                            if len(e.exceptions) == 1:
                                e = e.exceptions[0]
                            else:
                                e = TaskError(exceptions=e.exceptions)
                            logger.error(_("%s task %s[%s] %s: %s"), self, task_instance.name, task_id, e.__class__, e)
                            task_result = TaskResult(exc=e)
                        else:
                            logger.exception(e)
                            task_result = TaskResult(exc=InternalError())
                        task_result.set_idx((idx_0, idx_1 + 1))
                        if task_instance.result_return:
                            await self._result_backend.push_task_result(task_result, task_instance)
                        idx_1 += 1
                    finally:
                        try:
                            if task_instance.result_return and not task_instance.headers.get("graph:graph"):
                                func = task_instance.func
                                if isasyncgenfunction(func) or isgeneratorfunction(func):
                                    await self._result_backend.close_task(task_instance, idx=(idx_0, idx_1 + 1))
                                    idx_1 += 1
                        finally:
                            await self._execute_hooks("on_task_done", task_instance, task_result)

            except Exception as e:
                logger.exception(e)
            finally:
                self._running_tasks.pop(task_id, None)

        await self._broker.consume_tasks(queues, cb)
        logger.info(_("%s consuming task queues %s"), self, queues)

    async def stop_consume_tasks(self, queues: list[str] | None = None):
        """
        Stop consuming tasks.

        Args:
            queues: List of queue names to stop consume.
        """

        await self._broker.stop_consume_tasks(queues=queues)
        if queues is not None:
            logger.info(_("%s stop consuming task queues %s"), self, queues)
        else:
            logger.info(_("%s stop consuming task queues"), self)

    async def execute_task(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        """
        Execute the `TaskInstance` locally by the executor.

        Args:
            task_instance: Task instance to execute.

        Yields:
            Task result.
        """

        token = _curr_app.set(self)
        try:
            async for task_result in self._executor(task_instance):
                yield task_result
        finally:
            _curr_app.reset(token)

    async def consume_events(
        self,
        callback_id: str,
        callback: Callable[[Event], Any],
        event_types: list[str] | None = None,
    ):
        """
        Consume events and invoke `callback` on `Event` received.

        Args:
            callback_id: Callback Id. Needed for later use when stop consuming.
            callback: Callback to invoke then event received by backend.
            event_types: List of event types to consume.
        """

        await self._event_backend.consume_events(callback_id, callback, event_types=event_types)

    async def stop_consume_events(self, callback_id: str | None = None):
        """
        Stop consuming events.

        Args:
            callback_id: Callback Id for wich to stop consuming events.
        """

        await self._event_backend.stop_consume_events(callback_id=callback_id)

    def send_graph(self, *args, **kwds):
        """
        Send graph.
        """

        if "arrlio.graphs" not in self.plugins:
            raise GraphError(_("Plugin required: allrio.graphs"))

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

        Returns:
            Task result. For generator or asyncgenerator return the last available result.
        """

        noresult = not self._ready
        async for _ in self:
            noresult = False
        if noresult:
            raise TaskClosedError(self.task_id)
        return self._result


def get_app() -> App | None:
    return _curr_app.get()
