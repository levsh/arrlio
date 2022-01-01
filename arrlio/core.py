import asyncio
import inspect
import logging
import sys
import threading
import time
from types import FunctionType, MethodType
from typing import Any, Dict, List, Tuple, Union
from uuid import UUID

from arrlio import __tasks__, settings
from arrlio.exc import NotFoundError, TaskError, TaskNoResultError, TaskTimeoutError
from arrlio.models import Graph, Message, Task, TaskData, TaskInstance, TaskResult
from arrlio.settings import ConsumerConfig, ProducerConfig


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
    thread: bool = None,
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
            thread=thread,
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
                thread=thread,
            )

        return wrapper


class Base:
    def __init__(self, config: Union[ProducerConfig, ConsumerConfig], backend_config_kwds: dict = None):
        self.config = config
        backend_config_kwds = backend_config_kwds or {}
        self.backend = self.config.backend.Backend(self.config.backend.BackendConfig(**(backend_config_kwds)))
        self._closed: asyncio.Future = asyncio.Future()

    def __str__(self):
        return f"[{self.__class__.__name__}{self.backend}]"

    def __repr__(self):
        return self.__str__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @property
    def is_closed(self):
        return self._closed.done()

    async def close(self):
        await self.backend.close()
        self._closed.set_result(None)


class Producer(Base):
    def __init__(self, config: ProducerConfig, backend_config_kwds: dict = None):
        super().__init__(config, backend_config_kwds=backend_config_kwds)

    async def send_task(
        self,
        task: Union[Task, str],
        args: tuple = None,
        kwds: dict = None,
        queue: str = None,
        priority: int = None,
        timeout: int = None,
        ttl: int = None,
        encrypt: bool = None,
        ack_late: bool = None,
        result_ttl: int = None,
        result_return: bool = None,
        result_encrypt: bool = None,
        thread: bool = None,
        extra: dict = None,
        **kwargs,
    ) -> "AsyncResult":
        name = task
        if isinstance(task, Task):
            name = task.name

        if args is None:
            args = ()
        if kwds is None:
            kwds = {}
        if extra is None:
            extra = {}

        task_data = TaskData(
            args=args,
            kwds=kwds,
            queue=queue,
            priority=priority,
            timeout=timeout,
            ttl=ttl,
            encrypt=encrypt,
            ack_late=ack_late,
            result_ttl=result_ttl,
            result_return=result_return,
            result_encrypt=result_encrypt,
            thread=thread,
            extra=extra,
        )

        if name in __tasks__:
            task_instance = __tasks__[name].instantiate(data=task_data)
        else:
            task_instance = Task(None, name).instantiate(data=task_data)

        logger.info("%s: send %s", self, task_instance)

        await self.backend.send_task(task_instance, **kwargs)

        return AsyncResult(self, task_instance)

    async def send_graph(self, graph: Graph, args: Union[Tuple, List] = None, kwds: dict = None):
        logger.info("%s: send %s with args: %s and kwds: %s", self, graph, args, kwds)

        for root in graph.roots:
            name, root_kwds = graph.nodes[root]

            task_data = TaskData(**root_kwds)
            task_data.args += tuple(args or ())
            task_data.kwds.update(kwds or {})
            task_data.graph = Graph(graph.id, nodes=graph.nodes, edges=graph.edges, roots={root})

            if name in __tasks__:
                task_instance = __tasks__[name].instantiate(data=task_data)
            else:
                task_instance = Task(None, name).instantiate(data=task_data)

            logger.info("%s: send %s", self, task_instance)

            await self.backend.send_task(task_instance)

    async def send_message(
        self,
        message: Any,
        exchange: str = None,
        routing_key: str = None,
        priority: int = None,
        ttl: int = None,
        encrypt: bool = None,
    ):
        message = Message(exchange=exchange, data=message, priority=priority, ttl=ttl)
        logger.info("%s: send %s", self, message)
        await self.backend.send_message(message, routing_key=routing_key, encrypt=encrypt)

    async def pop_result(self, task_instance: TaskInstance):
        task_result = await self.backend.pop_task_result(task_instance)
        if task_result.exc:
            if isinstance(task_result.exc, TaskError):
                raise task_result.exc
            else:
                raise TaskError(task_result.exc, task_result.trb)
        return task_result.res


class Executor:
    def __str__(self):
        return f"[{self.__class__.__name__}]"

    def __repr__(self):
        return self.__str__()

    async def __call__(self, task_instance: TaskInstance) -> TaskResult:
        task_data: TaskData = task_instance.data
        task: Task = task_instance.task

        res, exc, trb = None, None, None
        t0 = time.monotonic()

        logger.info("%s: execute task %s(%s)", self, task.name, task_data.task_id)

        try:
            if task_instance.task.func is None:
                raise NotFoundError(f"Task '{task_instance.task.name}' not found")
            try:
                if inspect.iscoroutinefunction(task_instance.task.func):
                    res = await asyncio.wait_for(task_instance(), task_data.timeout)
                else:
                    res = task_instance()
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

        return TaskResult(res=res, exc=exc, trb=trb)


class ThreadExecutor(Executor):
    async def __call__(self, task_instance: TaskInstance) -> TaskResult:
        root_loop = asyncio.get_running_loop()
        done_ev: asyncio.Event = asyncio.Event()
        task_result: TaskResult = None

        def thread():
            nonlocal done_ev
            nonlocal task_result
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                task_result = loop.run_until_complete(super(ThreadExecutor, self).__call__(task_instance))
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
                root_loop.call_soon_threadsafe(lambda: done_ev.set())

        th = threading.Thread(target=thread)
        th.start()

        await done_ev.wait()

        return task_result


class Consumer(Base):
    def __init__(self, config: ConsumerConfig, backend_config_kwds: dict = None, on_message=None):
        super().__init__(config, backend_config_kwds=backend_config_kwds)
        self.producer = Producer(
            ProducerConfig(**{k: v for k, v in config.dict().items() if k in ProducerConfig.__fields__}),
            backend_config_kwds=backend_config_kwds,
        )

        if on_message:
            self.on_message = on_message

        self._running_tasks: dict = {}
        self._running_messages: dict = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    async def close(self):
        await self.stop_consume_tasks()
        await self.stop_consume_messages()
        await super().close()

    async def consume_tasks(self):
        if self.config.task_queues:
            logger.info("%s: consuming task queues %s", self, self.config.task_queues)
            await self.backend.consume_tasks(self.config.task_queues, self._on_task)

    async def stop_consume_tasks(self):
        async with self._lock:
            for task_id, aio_task in self._running_tasks.items():
                logger.debug("Cancel processing task '%s'", task_id)
                aio_task.cancel()
            self._running_tasks = {}

    async def _on_task(self, task_instance: TaskInstance):
        try:
            task_id: UUID = task_instance.data.task_id

            async with self._lock:
                if self.is_closed:
                    return

                if len(self._running_tasks) + len(self._running_messages) + 1 >= self.config.pool_size:
                    await self.backend.stop_consume_tasks()
                    try:
                        aio_task = asyncio.create_task(self._execute_task(task_instance))
                        aio_task.add_done_callback(lambda *args: self._running_tasks.pop(task_id, None))
                        self._running_tasks[task_id] = aio_task
                        await aio_task
                    finally:
                        if not self.is_closed:
                            await self.backend.consume_tasks(self.config.task_queues, self._on_task)
                    return

            aio_task = asyncio.create_task(self._execute_task(task_instance))
            aio_task.add_done_callback(lambda *args: self._running_tasks.pop(task_id, None))
            self._running_tasks[task_id] = aio_task

        except Exception as e:
            logger.exception(e)

    async def _execute_task(self, task_instance: TaskInstance):
        try:
            task_data: TaskData = task_instance.data

            if task_data.thread is True:
                executor = ThreadExecutor()
            else:
                executor = Executor()

            task_result: TaskResult = await executor(task_instance)

            graph: Graph = task_data.graph
            if graph is not None:
                if task_result.exc is None:
                    root: str = next(iter(graph.roots))
                    if root in graph.edges:
                        for child in graph.edges[root]:
                            graph: Graph = Graph(
                                id=graph.id,
                                nodes=graph.nodes,
                                edges=graph.edges,
                                roots={child},
                            )
                            await self.producer.send_graph(
                                graph,
                                args=(task_result.res,),
                                kwds={"graph_source_node": root},
                            )

            if task_instance.data.result_return:
                try:
                    await self.backend.push_task_result(task_instance, task_result)
                except Exception as e:
                    logger.exception(e)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(e)

    async def consume_messages(self):
        if self.config.message_queues:
            logger.info("%s: consuming message queues %s", self, self.config.message_queues)
            await self.backend.consume_messages(self.config.message_queues, self._on_message)

    async def stop_consume_messages(self):
        async with self._lock:
            for message_id, aio_task in self._running_messages.items():
                logger.debug("Cancel processing message '%s'", message_id)
                aio_task.cancel()
            self._running_messages = {}

    async def on_message(self, message: Any):
        pass

    async def _on_message(self, message: Message):
        try:
            message_id: UUID = message.message_id

            async with self._lock:
                if len(self._running_tasks) + len(self._running_messages) + 1 >= self.config.pool_size:
                    await self.backend.stop_consume_messages()
                    try:
                        aio_task = asyncio.create_task(self.on_message(message.data))
                        aio_task.add_done_callback(lambda *args: self._running_tasks.pop(message_id, None))
                        self._running_tasks[message_id] = aio_task
                        await aio_task
                    finally:
                        if not self.is_closed:
                            await self.backend.consume_messages(self.config.message_queues, self._on_message)
                    return

            aio_task = asyncio.create_task(self.on_message(message.data))
            aio_task.add_done_callback(lambda *args: self._running_messages.pop(message_id, None))
            self._running_messages[message_id] = aio_task

        except Exception as e:
            logger.exception(e)


class AsyncResult:
    def __init__(self, producer: Producer, task_instance: TaskInstance):
        self._producer = producer
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
                self._result = await self._producer.pop_result(self._task_instance)
                self._ready = True
            except TaskError as e:
                self._exception = e
                self._ready = True
        if self._exception:
            if isinstance(self._exception.args[0], Exception):
                raise self._exception from self._exception.args[0]
            raise self._exception
        return self._result
