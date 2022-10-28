import asyncio
import copy
import logging
from contextlib import AsyncExitStack
from contextvars import ContextVar
from types import FunctionType, MethodType, ModuleType
from typing import Any, Dict, List, Type, Union
from uuid import UUID

from roview import rodict, rolist

from arrlio import __tasks__
from arrlio.exc import TaskError, TaskNoResultError
from arrlio.models import Event, Graph, Message, Task, TaskData, TaskInstance, TaskResult
from arrlio.plugins.base import Plugin
from arrlio.settings import Config
from arrlio.tp import AsyncCallableT

logger = logging.getLogger("arrlio.core")


def task(func: Union[FunctionType, MethodType] = None, name: str = None, base: Type[Task] = None, **kwds):
    """
    Args:
        func (FunctionType, optional): Task function.
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
        logger.info("Register task '%s'", t.name)
        return t
    else:

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
        if isinstance(config.backend, ModuleType):
            self._backend = self.config.backend.Backend(self.config.backend.BackendConfig())
        else:
            self._backend = self.config.backend()
        self._closed: asyncio.Future = asyncio.Future()
        self._running_tasks: Dict[UUID, asyncio.Task] = {}
        self._running_messages: Dict[UUID, asyncio.Task] = {}
        self._lock: asyncio.Lock = asyncio.Lock()
        self._executor = config.executor()
        self._context = ContextVar("context", default={})

        self._hooks = {
            "on_init": [],
            "on_close": [],
            "on_task_send": [],
            "on_task_received": [],
            "on_task_done": [],
            "task_context": [],
        }
        self._plugins = {}
        for plugin_cls in config.plugins:
            plugin = plugin_cls(self)
            self._plugins[plugin.name] = plugin
            for k in self._hooks:
                if getattr(plugin, k).__func__ != getattr(Plugin, k):
                    self._hooks[k].append(getattr(plugin, k))

        self._plugins_tasks = []
        for hook in self._hooks["on_init"]:
            # task = asyncio.create_task(hook())
            task = asyncio.create_task(self._execute_hook(hook))
            self._plugins_tasks.append(task)
            task.add_done_callback(lambda fut: self._plugins_tasks.remove(task))

    def __str__(self):
        return f"{self.__class__.__name__}[{self._backend}]"

    def __repr__(self):
        return self.__str__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @property
    def hooks(self):
        return rolist(list(self._hooks.keys()))

    @property
    def plugins(self):
        return rodict(self._plugins)

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

    async def wait_init(self):
        for task in self._plugins_tasks:
            await task

    async def close(self):
        try:
            for task in self._plugins_tasks:
                task.cancel()
                logger.warning("Cancel %s", task)

            await self._execute_hooks("on_close")
            for k in self._hooks:
                self._hooks[k].clear()

            await self.stop_consume_tasks()
            await self.stop_consume_messages()
            await self.stop_consume_events()

            for task_id, aio_task in self._running_tasks.items():
                logger.debug("%s: cancel processing task '%s'", str(self), task_id)
                aio_task.cancel()
            self._running_tasks = {}

            for message_id, aio_task in self._running_messages.items():
                logger.debug("%s: cancel processing message '%s'", str(self), message_id)
                aio_task.cancel()

            await self._backend.close()
        finally:
            self._closed.set_result(None)

    async def _execute_hook(self, hook_fn, *args, **kwds):
        try:
            await hook_fn(*args, **kwds)
        except Exception:
            logger.exception("%s: plugin %s error", str(self), hook_fn)

    async def _execute_hooks(self, hook: str, *args, **kwds):
        for hook_fn in self._hooks[hook]:
            await self._execute_hook(hook_fn, *args, **kwds)

    async def send_task(
        self,
        task: Union[Task, str],
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

        task_settings = self.config.task.dict(exclude_unset=True)

        if name in __tasks__:
            task_instance = __tasks__[name].instantiate(
                args=args, kwds=kwds, extra=extra, **{**task_settings, **kwargs}
            )
        else:
            task_instance = Task(None, name).instantiate(
                args=args, kwds=kwds, extra=extra, **{**task_settings, **kwargs}
            )

        logger.info("%s: send %s", str(self), task_instance)

        await self._execute_hooks("on_task_send", task_instance)

        await self._backend.send_task(task_instance)

        return AsyncResult(self, task_instance)

    async def send_graph(
        self,
        graph: Graph,
        args: tuple = None,
        kwds: dict = None,
        meta: dict = None,
    ) -> Dict[str, "AsyncResult"]:
        """
        Args:
            graph (Graph): ~arrlio.models.Graph.
            args (tuple, optional): ~arrlio.models.Graph root nodes args.
            kwds (dict, optional): ~arrlio.models.Graph root nodes kwds.
            meta (dict, optional): ~arrlio.models.Graph root nodes meta.

        Returns:
            Dict[str, ~arrlio.core.AsyncResult]: Dictionary with AsyncResult objects.
        """

        nodes = copy.deepcopy(graph.nodes)
        edges = graph.edges
        roots = graph.roots

        if not nodes or not roots:
            raise ValueError("Empty graph or missing roots")

        logger.info("%s: send %s with args: %s and kwds: %s", str(self), graph, args, kwds)

        task_instances = {}
        for node_name, (task, node_kwds) in nodes.items():
            if node_name not in roots and node_kwds.get("task_id"):
                continue
            if task in __tasks__:
                task_instance = __tasks__[task].instantiate(**node_kwds)
            else:
                task_instance = Task(None, task).instantiate(**node_kwds)
            node_kwds["task_id"] = task_instance.data.task_id
            task_instances[node_name] = task_instance

        for root in roots:
            data = task_instances[root].data
            data.args += tuple(args or ())
            data.kwds.update(kwds or {})
            data.meta.update(meta or {})
            data.graph = Graph(graph.id, nodes=nodes, edges=edges, roots={root})

            logger.info("%s: send %s", str(self), task_instances[root])

            await self._backend.send_task(task_instances[root])

        return {k: AsyncResult(self, task_instance) for k, task_instance in task_instances.items()}

    async def send_message(self, data: Any, routing_key: str = None, **kwds):
        """
        Args:
            data (Any): Message data.
            routing_key (str, optional): Message routing key.
            kwds (dict, optional): ~arrlio.models.Message arguments.
        """

        message_settings = self.config.message.dict(exclude_unset=True)
        message = Message(data=data, **{**message_settings, **kwds})

        logger.info("%s: send %s", str(self), message)

        await self._backend.send_message(message, routing_key=routing_key)

    async def send_event(self, event: Event):
        logger.info("%s: send %s", str(self), event)
        await self._backend.push_event(event)

    async def pop_result(self, task_instance: TaskInstance):
        task_result: TaskResult = await self._backend.pop_task_result(task_instance)
        if task_result.exc:
            if isinstance(task_result.exc, TaskError):
                raise task_result.exc
            else:
                raise TaskError(task_result.exc, task_result.trb)
        return task_result.res

    async def consume_tasks(self, queues: List[str] = None):
        queues = queues or self.config.task_queues
        if queues:
            await self._backend.consume_tasks(queues, self._on_task)
            logger.info("%s: consuming task queues %s", str(self), queues)

    async def stop_consume_tasks(self, queues: List[str] = None):
        async with self._lock:
            await self._backend.stop_consume_tasks(queues=queues)
            logger.info("%s: stop consuming task queues %s", str(self), self.config.task_queues)

    async def _on_task(self, task_instance: TaskInstance):
        try:
            await self._execute_hooks("on_task_received", task_instance)

            task_id: UUID = task_instance.data.task_id

            def fn():
                aio_task = asyncio.create_task(self.execute_task(task_instance))
                aio_task.add_done_callback(lambda *args: self._running_tasks.pop(task_id, None))
                self._running_tasks[task_id] = aio_task
                return aio_task

            async with self._lock:
                if self.is_closed:
                    return

                if len(self._running_tasks) + len(self._running_messages) + 1 >= self.config.pool_size:
                    await self._backend.stop_consume_tasks()
                    try:
                        await fn()
                    finally:
                        if not self.is_closed:
                            await self._backend.consume_tasks(self.config.task_queues, self._on_task)
                    return

            fn()

        except Exception as e:
            logger.exception(e)

    async def execute_task(self, task_instance: TaskInstance):
        async with AsyncExitStack() as stack:
            try:
                for context in self._hooks["task_context"]:
                    await stack.enter_async_context(context(task_instance))

                task_data: TaskData = task_instance.data

                task_result: TaskResult = await self._executor(task_instance)

                graph: Graph = task_data.graph
                if graph is not None and task_result.exc is None:
                    routes = task_result.routes
                    args = (task_result.res,) or ()
                    if isinstance(routes, str):
                        routes = [routes]

                    root: str = next(iter(graph.roots))
                    if root in graph.edges:
                        for node_id, node_id_routes in graph.edges[root]:
                            if not ((routes is None and node_id_routes is None) or set(routes) & set(node_id_routes)):
                                continue
                            await self.send_graph(
                                Graph(
                                    id=graph.id,
                                    nodes=graph.nodes,
                                    edges=graph.edges,
                                    roots={node_id},
                                ),
                                args=args,
                                meta={"source_node": root},
                            )

                if task_instance.data.result_return:
                    await self._backend.push_task_result(task_instance, task_result)

                await self._execute_hooks("on_task_done", task_instance, task_result)

            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.exception(e)

    async def consume_messages(self, on_message: AsyncCallableT):
        if self.config.message_queues:

            async def handle(message: Message):
                try:
                    message_id: UUID = message.message_id

                    async with self._lock:
                        if len(self._running_tasks) + len(self._running_messages) + 1 >= self.config.pool_size:
                            await self._backend.stop_consume_messages()
                            try:
                                aio_task = asyncio.create_task(on_message(message.data))
                                aio_task.add_done_callback(lambda *args: self._running_messages.pop(message_id, None))
                                self._running_messages[message_id] = aio_task
                                await aio_task
                            finally:
                                if not self.is_closed:
                                    await self._backend.consume_messages(self.config.message_queues, handle)
                            return

                    aio_task = asyncio.create_task(on_message(message.data))
                    aio_task.add_done_callback(lambda *args: self._running_messages.pop(message_id, None))
                    self._running_messages[message_id] = aio_task

                except Exception as e:
                    logger.exception(e)

            logger.info("%s: consuming message queues %s", str(self), self.config.message_queues)
            await self._backend.consume_messages(self.config.message_queues, handle)

    async def stop_consume_messages(self):
        async with self._lock:
            await self._backend.stop_consume_messages()
            self._running_messages = {}

    async def consume_events(self, on_event: AsyncCallableT):
        logger.info("%s: consuming events", str(self))
        await self._backend.consume_events(on_event)

    async def stop_consume_events(self):
        await self._backend.stop_consume_events()


class AsyncResult:
    def __init__(self, app: App, task_instance: TaskInstance):
        self._app = app
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
        if not self._task_instance.data.result_return:
            raise TaskNoResultError(self._task_instance.data.task_id)
        if not self._ready:
            try:
                self._result = await self._app.pop_result(self._task_instance)
                self._ready = True
            except TaskError as e:
                self._exception = e
                self._ready = True
        if self._exception:
            if isinstance(self._exception.args[0], Exception):
                raise self._exception from self._exception.args[0]
            raise self._exception
        return self._result
