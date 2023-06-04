import datetime
from dataclasses import asdict, dataclass, field
from inspect import Signature, _empty, signature
from types import TracebackType
from typing import Any, Callable, Dict, List, Set, Tuple, Union
from uuid import UUID, uuid4

from pydantic import Field, create_model
from roview import rodict, roset

from arrlio.exc import GraphError
from arrlio.settings import (
    EVENT_TTL,
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
from arrlio.types import Args, AsyncFunction, Kwds, Priority, TaskId, Timeout


@dataclass(frozen=True)
class Task:
    """Task `dataclass`.

    Attributes:
        func: Task function.
        name: Task name.
        bind: Bind the Task object as the first argument.
        queue: Task queue.
        priority: Task priority. Min 1, max 10.
        timeout: Task timeout, seconds.
        ttl: Task time to live, seconds.
        ack_late: Ack late behaviour.
        result_ttl: Task result time to live, seconds.
        result_return: Whether the worker should return or not the result of the task.
        thread: Should `arrlio.executor.Executor` execute task in the separate thread.
        events: Enable or disable events for the task.
        event_ttl: Event time to live, seconds.
        extra: Task extra data.
        loads: Function to load task arguments.
        dumps: Function to dump task result
    """

    func: Union[Callable, AsyncFunction]

    name: str
    bind: bool = TASK_BIND
    queue: str = TASK_QUEUE
    priority: Priority = TASK_PRIORITY
    timeout: Timeout = TASK_TIMEOUT
    ttl: Timeout = TASK_TTL
    ack_late: bool = TASK_ACK_LATE
    result_ttl: Timeout = TASK_RESULT_TTL
    result_return: bool = TASK_RESULT_RETURN
    thread: bool = None
    events: Union[bool, Set[str]] = TASK_EVENTS
    event_ttl: Timeout = TASK_EVENT_TTL
    extra: dict = field(default_factory=dict)

    loads: Callable = None
    dumps: Callable = None

    def __call__(self, *args, **kwds) -> Any:
        """Call task function with args and kwds."""

        return self.func(*args, **kwds)

    def dict(self, exclude: List[str] = None):
        """Convert to dict.

        Args:
            exclude: fields to exclude.
        Returns:
            `arrlio.models.Task` as `dict`.
        """

        # exclude = (exclude or []) + ["loads", "dumps"]
        exclude = exclude or []
        return {k: v for k, v in asdict(self).items() if k not in exclude}

    def instantiate(
        self,
        task_id: TaskId = None,
        args: Args = None,
        kwds: Kwds = None,
        meta: dict = None,
        extra: dict = None,
        **kwargs,
    ) -> "TaskInstance":
        """Instantiate new `arrlio.models.TaskInstance` object with provided arguments.

        Returns:
            `arrlio.models.TaskInstance` object.
        """

        extra = {**self.extra, **(extra or {})}
        return TaskInstance(
            **{
                **self.dict(),
                "task_id": task_id,
                "args": args or (),
                "kwds": kwds or {},
                "meta": meta or {},
                "extra": extra or {},
                **kwargs,
            }
        )


@dataclass(frozen=True)
class TaskInstance(Task):
    """Task instance `dataclass`.

    Attributes:
        task_id: Task Id.
        args: Task function positional arguments.
        kwds: Task function keyword arguments.
        meta: Task function additional meta keyword argument.
    """

    task_id: UUID = field(default_factory=uuid4)
    args: tuple = field(default_factory=tuple)
    kwds: dict = field(default_factory=dict)
    meta: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.task_id is None:
            object.__setattr__(self, "task_id", uuid4())
        elif isinstance(self.task_id, str):
            object.__setattr__(self, "task_id", UUID(self.task_id))
        if isinstance(self.args, list):
            object.__setattr__(self, "args", tuple(self.args))

    def __call__(self, meta: bool = None):  # pylint: disable=arguments-differ
        """Call `arrlio.models.TaskInstance`.

        Args:
            meta: Add additional keyword argument `meta` to the task function call.
        """

        args = self.args
        kwds = self.kwds
        if self.bind:
            args = (self, *args)
        if meta is True:
            kwds = {"meta": self.meta, **kwds}
        if isinstance(self.func, type):
            func = self.func()
        else:
            func = self.func
        return func(*args, **kwds)

    def instantiate(self, *args, **kwds):
        raise NotImplementedError

    def validate(self):
        """Validate `args` and `kwds` and converts values according to the function signature(type hints)."""

        sig: Signature = signature(self.func)

        __dict__ = {}
        for k, v in sig.parameters.items():
            if v.default != _empty:
                _field = Field(default=v.default)
            else:
                _field = Field()
            __dict__[k] = (v.annotation if v.annotation != _empty else Any, _field)

        Model = create_model(f"{self.func}", **__dict__)  # pylint: disable=invalid-name

        binded = sig.bind(*self.args, **self.kwds)
        binded.apply_defaults()

        model = Model(**binded.arguments)

        args = []
        kwds = {}
        for k, v in sig.parameters.items():
            if v.kind == v.VAR_POSITIONAL:
                args.extend(getattr(model, k))
            elif v.kind == v.VAR_KEYWORD:
                kwds.update(getattr(model, k))
            elif v.default == _empty:
                args.append(getattr(model, k))
            else:
                kwds[k] = getattr(model, k)

        object.__setattr__(self, "args", tuple(args))
        object.__setattr__(self, "kwds", kwds)


@dataclass(frozen=True)
class TaskResult:
    """Task result `dataclass`."""

    res: Any = None
    exc: Union[Exception, Tuple[str, str, str]] = None
    trb: Union[TracebackType, str] = None
    idx: Tuple[str, int] = None
    routes: Union[str, List[str]] = None

    def set_idx(self, idx: Tuple[str, int]):
        object.__setattr__(self, "idx", idx)

    def dict(self):
        """Convert to dict.

        Returns:
            `arrlio.models.TaskResult` as `dict`.
        """

        return {
            "res": self.res,
            "exc": self.exc,
            "trb": self.trb,
            "idx": self.idx,
            "routes": self.routes,
        }


@dataclass(frozen=True)
class Event:
    """Event `dataclass`.

    Attributes:
        type: Event type.
        event_id: Event Id.
        dt: Event datetime.
        ttl: Event time to live, seconds.
    """

    type: str
    data: dict
    event_id: UUID = field(default_factory=uuid4)
    dt: datetime.datetime = None
    ttl: Timeout = EVENT_TTL

    def __post_init__(self):
        if not isinstance(self.event_id, UUID):
            object.__setattr__(self, "event_id", UUID(self.event_id))
        if self.dt is None:
            object.__setattr__(self, "dt", datetime.datetime.now(tz=datetime.timezone.utc))
        elif isinstance(self.dt, str):
            object.__setattr__(self, "dt", datetime.datetime.fromisoformat(self.dt))

    def dict(self):
        """Convert to dict.

        Returns:
            `arrlio.models.Event` as `dict`.
        """

        return asdict(self)


class Graph:
    """Graph class."""

    def __init__(
        self,
        name: str,
        nodes: Dict = None,
        edges: Dict = None,
        roots: Set = None,
    ):
        """
        Args:
            name: Graph name.
            node: List of the graph nodes.
            edges: List of the graph edges.
            roots: List of the graph roots.
        """

        self.name = name
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
        return f"{self.__class__.__name__}(name={self.name} nodes={self.nodes} edges={self.edges} roots={self.roots}"

    def __repr__(self):
        return self.__str__()

    def add_node(self, node_id: str, task: Union[Task, str], root: bool = None, **kwds):
        """Add node to the graph.

        Args:
            node_id: Node Id.
            task: `arrlio.models.Task` or task name.
            root: Is node the root of the graph.
        """

        if node_id in self.nodes:
            raise GraphError(f"Node '{node_id}' already in graph")
        if isinstance(task, Task):
            task = task.name
        self.nodes.__original__[node_id] = [task, kwds]
        if root:
            self.roots.__original__.add(node_id)

    def add_edge(self, node_id_from: str, node_id_to: str, routes: Union[str, List[str]] = None):
        """Add edge to the graph.
        If routes are specified then only results with a matching route will be passed to the incoming node.

        Args:
            node_id_from: Outgoing node.
            node_id_to: Incomming node.
            routes: Edge route.
        """

        if node_id_from not in self.nodes:
            raise GraphError(f"Node '{node_id_from}' not found in graph")
        if node_id_to not in self.nodes:
            raise GraphError(f"Node '{node_id_to}' not found in graph")
        if isinstance(routes, str):
            routes = [routes]
        self.edges.__original__.setdefault(node_id_from, []).append([node_id_to, routes])

    def dict(self):
        """Convert to the dict.

        Returns:
            `arrlio.models.Graph` as `dict`.
        """

        return {
            "name": self.name,
            "nodes": self.nodes,
            "edges": self.edges,
            "roots": self.roots,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "Graph":
        """Create `arrlio.models.Graph` from `dict`.

        Args:
            data: Data as dictionary object.
        Returns:
            `arrlio.models.Graph` object.
        """

        return cls(
            name=data["name"],
            nodes=data["nodes"],
            edges=data["edges"],
            roots=data["roots"],
        )
