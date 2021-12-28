import inspect
from dataclasses import dataclass, field
from types import FunctionType, TracebackType
from typing import Any, Tuple, Union
from uuid import UUID, uuid4

from arrlio.settings import (
    MESSAGE_ACK_LATE,
    MESSAGE_EXCHANGE,
    MESSAGE_PRIORITY,
    MESSAGE_TTL,
    RESULT_ENCRYPT,
    RESULT_RETURN,
    RESULT_TTL,
    TASK_ACK_LATE,
    TASK_BIND,
    TASK_PRIORITY,
    TASK_QUEUE,
    TASK_TIMEOUT,
    TASK_TTL,
)


@dataclass
class TaskData:
    task_id: UUID = field(default_factory=uuid4)
    args: tuple = field(default_factory=tuple)
    kwds: dict = field(default_factory=dict)
    queue: str = None
    priority: int = None
    timeout: int = None
    ttl: int = None
    ack_late: bool = None
    result_ttl: int = None
    result_return: bool = None
    result_encrypt: bool = None
    thread: bool = None
    extra: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Task:
    func: FunctionType
    name: str
    bind: bool = None
    queue: str = None
    priority: int = None
    timeout: int = None
    ttl: int = None
    ack_late: bool = None
    result_ttl: int = None
    result_return: bool = None
    result_encrypt: bool = None
    thread: bool = None

    def __post_init__(self):
        if self.bind is None:
            object.__setattr__(self, "bind", TASK_BIND)
        if self.queue is None:
            object.__setattr__(self, "queue", TASK_QUEUE)
        if self.priority is None:
            object.__setattr__(self, "priority", TASK_PRIORITY)
        if self.timeout is None:
            object.__setattr__(self, "timeout", TASK_TIMEOUT)
        if self.ttl is None:
            object.__setattr__(self, "ttl", TASK_TTL)
        if self.ack_late is None:
            object.__setattr__(self, "ack_late", TASK_ACK_LATE)
        if self.result_ttl is None:
            object.__setattr__(self, "result_ttl", RESULT_TTL)
        if self.result_return is None:
            object.__setattr__(self, "result_return", RESULT_RETURN)
        if self.result_encrypt is None:
            object.__setattr__(self, "result_encrypt", RESULT_ENCRYPT)

    def instatiate(self, data: TaskData = None) -> "TaskInstance":
        if data is None:
            data = TaskData()
        if isinstance(data.task_id, str):
            data.task_id = UUID(data.task_id)
        if isinstance(data.args, list):
            data.args = tuple(data.args)
        if data.queue is None:
            data.queue = self.queue
        if data.priority is None:
            data.priority = self.priority
        if data.timeout is None:
            data.timeout = self.timeout
        if data.ttl is None:
            data.ttl = self.ttl
        if data.ack_late is None:
            data.ack_late = self.ack_late
        if data.result_ttl is None:
            data.result_ttl = self.result_ttl
        if data.result_return is None:
            data.result_return = self.result_return
        if data.result_encrypt is None:
            data.result_encrypt = self.result_encrypt
        if data.thread is None:
            data.thread = self.thread
        return TaskInstance(task=self, data=data)

    async def __call__(self, *args, **kwds) -> Any:
        return await self.instatiate(TaskData(args=args, kwds=kwds))()


@dataclass(frozen=True)
class TaskInstance:
    task: Task
    data: TaskData

    async def __call__(self):
        args = self.data.args
        kwds = self.data.kwds
        if self.task.bind:
            args = (self,) + args
        if inspect.iscoroutinefunction(self.task.func):
            return await self.task.func(*args, **kwds)
        return self.task.func(*args, **kwds)


@dataclass(frozen=True)
class TaskResult:
    res: Any = None
    exc: Union[Exception, Tuple[str, str, str]] = None
    trb: Union[TracebackType, str] = None


@dataclass(frozen=True)
class Message:
    data: Any
    message_id: UUID = field(default_factory=uuid4)
    exchange: str = None
    priority: int = None
    ttl: int = None
    ack_late: bool = None

    def __post_init__(self):
        if self.exchange is None:
            object.__setattr__(self, "exchange", MESSAGE_EXCHANGE)
        if self.priority is None:
            object.__setattr__(self, "priority", MESSAGE_PRIORITY)
        if self.ttl is None:
            object.__setattr__(self, "ttl", MESSAGE_TTL)
        if self.ack_late is None:
            object.__setattr__(self, "ack_late", MESSAGE_ACK_LATE)
