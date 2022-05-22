import datetime
from dataclasses import dataclass, field
from types import FunctionType, TracebackType
from typing import Any, Dict, List, Set, Tuple, Union
from uuid import UUID, uuid4

from roview import rodict, roset

from arrlio.settings import (
    EVENT_TTL,
    MESSAGE_ACK_LATE,
    MESSAGE_EXCHANGE,
    MESSAGE_PRIORITY,
    MESSAGE_TTL,
    TASK_ACK_LATE,
    TASK_BIND,
    TASK_EVENT_TTL,
    TASK_EVENTS,
    TASK_PRIORITY,
    TASK_QUEUE,
    TASK_RESULT_RETURN,
    TASK_RESULT_TTL,
    TASK_TIMEOUT,
    TASK_TTL,
)


@dataclass
class TaskData:
    task_id: UUID = field(default_factory=uuid4)
    args: tuple = field(default_factory=tuple)
    kwds: dict = field(default_factory=dict)
    meta: dict = field(default_factory=dict)
    graph: "Graph" = None

    queue: str = TASK_QUEUE
    priority: int = TASK_PRIORITY
    timeout: int = TASK_TIMEOUT
    ttl: int = TASK_TTL
    ack_late: bool = TASK_ACK_LATE
    result_ttl: int = TASK_RESULT_TTL
    result_return: bool = TASK_RESULT_RETURN
    thread: bool = None
    events: Union[bool, Set[str]] = TASK_EVENTS
    event_ttl: int = EVENT_TTL

    extra: dict = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.task_id, str):
            object.__setattr__(self, "task_id", UUID(self.task_id))
        if isinstance(self.args, list):
            object.__setattr__(self, "args", tuple(self.args))


@dataclass(frozen=True)
class Task:
    func: FunctionType
    name: str
    bind: bool = TASK_BIND

    queue: str = TASK_QUEUE
    priority: int = TASK_PRIORITY
    timeout: int = TASK_TIMEOUT
    ttl: int = TASK_TTL
    ack_late: bool = TASK_ACK_LATE
    result_ttl: int = TASK_RESULT_TTL
    result_return: bool = TASK_RESULT_RETURN
    thread: bool = None
    events: Union[bool, Set[str]] = TASK_EVENTS
    event_ttl: int = TASK_EVENT_TTL

    extra: dict = field(default_factory=dict)

    def instantiate(self, extra: dict = None, **kwds) -> "TaskInstance":
        data: TaskData = TaskData(
            **{
                **{
                    "queue": self.queue,
                    "priority": self.priority,
                    "timeout": self.timeout,
                    "ttl": self.ttl,
                    "ack_late": self.ack_late,
                    "result_ttl": self.result_ttl,
                    "result_return": self.result_return,
                    "thread": self.thread,
                    "events": self.events,
                    "event_ttl": self.event_ttl,
                    "extra": {**self.extra, **(extra or {})},
                },
                **kwds,
            }
        )
        return TaskInstance(task=self, data=data)

    def __call__(self, *args, **kwds) -> Any:
        return self.instantiate(args=args, kwds=kwds)()


@dataclass(frozen=True)
class TaskInstance:
    task: Task
    data: TaskData

    def __call__(self, meta: bool = False):
        args = self.data.args
        kwds = self.data.kwds
        if meta is True:
            kwds["meta"] = self.data.meta
        if self.task.bind:
            args = (self,) + args
        return self.task.func(*args, **kwds)


@dataclass(frozen=True)
class TaskResult:
    res: Any = None
    exc: Union[Exception, Tuple[str, str, str]] = None
    trb: Union[TracebackType, str] = None
    routes: Union[str, List[str]] = None


@dataclass(frozen=True)
class Message:
    data: Any
    message_id: UUID = field(default_factory=uuid4)
    exchange: str = MESSAGE_EXCHANGE
    priority: int = MESSAGE_PRIORITY
    ttl: int = MESSAGE_TTL
    ack_late: bool = MESSAGE_ACK_LATE
    extra: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Event:
    type: str
    data: dict
    event_id: UUID = field(default_factory=uuid4)
    dt: datetime.datetime = None
    ttl: int = EVENT_TTL

    def __post_init__(self):
        if not isinstance(self.event_id, UUID):
            object.__setattr__(self, "event_id", UUID(self.event_id))
        if self.dt is None:
            object.__setattr__(self, "dt", datetime.datetime.now(tz=datetime.timezone.utc))
        elif isinstance(self.dt, str):
            object.__setattr__(self, "dt", datetime.datetime.fromisoformat(self.dt))


class Graph:
    def __init__(
        self,
        id: str,
        nodes: dict = None,
        edges: dict = None,
        roots: set = None,
    ):
        self.id = id
        self.nodes: Dict[str, List[str]] = rodict({}, nested=True)
        self.edges: Dict[str, List[str]] = rodict({}, nested=True)
        self.roots: Set[str] = roset(set())
        nodes = nodes or {}
        edges = edges or {}
        roots = roots or set()
        for node_id, (task, kwds) in nodes.items():
            self.add_node(node_id, task, root=node_id in roots, **kwds)
        for node_id_from, nodes_to in edges.items():
            for node_id_to, routes in nodes_to:
                self.add_edge(node_id_from, node_id_to, routes=routes)

    def __str__(self):
        return f"{self.__class__.__name__}(id={self.id} nodes={self.nodes} edges={self.edges} roots={self.roots}"

    def __repr__(self):
        return self.__str__()

    def add_node(self, node_id: str, task: Union[Task, str], root: bool = None, **kwds):
        if node_id in self.nodes:
            raise Exception(f"Node '{node_id}' already in graph")
        if isinstance(task, Task):
            task = task.name
        self.nodes.__original__[node_id] = [task, kwds]
        if root:
            self.roots.__original__.add(node_id)

    def add_edge(self, node_id_from: str, node_id_to: str, routes: Union[str, List[str]] = None):
        if node_id_from not in self.nodes:
            raise Exception(f"Node '{node_id_from}' not found in graph")
        if node_id_to not in self.nodes:
            raise Exception(f"Node '{node_id_to}' not found in graph")
        if isinstance(routes, str):
            routes = [routes]
        self.edges.__original__.setdefault(node_id_from, []).append([node_id_to, routes])

    def dict(self):
        return {
            "id": self.id,
            "nodes": self.nodes,
            "edges": self.edges,
            "roots": self.roots,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            id=data["id"],
            nodes=data["nodes"],
            edges=data["edges"],
            roots=data["roots"],
        )
