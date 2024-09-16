import datetime

from collections.abc import MutableMapping
from dataclasses import asdict, dataclass, field
from types import TracebackType
from typing import Any, Callable, ClassVar, Optional
from uuid import UUID, uuid4

from rich.pretty import pretty_repr
from roview import rodict, roset

from arrlio.exceptions import GraphError
from arrlio.settings import (
    EVENT_TTL,
    TASK_ACK_LATE,
    TASK_EVENTS,
    TASK_PRIORITY,
    TASK_QUEUE,
    TASK_RESULT_RETURN,
    TASK_RESULT_TTL,
    TASK_TIMEOUT,
    TASK_TTL,
)
from arrlio.types import Args, AsyncCallable, Kwds, TaskId, TaskPriority, Timeout


class FuncProxy:
    """Proxy class for function object."""

    def __init__(self, func):
        self._original = func

    def __getattribute__(self, name: str):
        if name not in ("_original", "__deepcopy__"):
            return getattr(object.__getattribute__(self, "_original"), name)
        return object.__getattribute__(self, name)

    def __str__(self):
        return self._original.__str__()

    def __repr__(self):
        return self._original.__repr__()

    def __call__(self, *args, **kwds):
        return self._original(*args, **kwds)

    def __deepcopy__(self, memo):
        return self


class Shared(MutableMapping):
    """Object to share settings between broker/result_backend/event_backend."""

    def __init__(self):
        self._data = {}

    def __getitem__(self, *args, **kwds):
        return self._data.__getitem__(*args, **kwds)

    def __setitem__(self, *args, **kwds):
        return self._data.__setitem__(*args, **kwds)

    def __delitem__(self, *args, **kwds):
        return self._data.__delitem__(*args, **kwds)

    def __contains__(self, *args, **kwds):
        return self._data.__contains__(*args, **kwds)

    def __len__(self, *args, **kwds):
        return self._data.__len__(*args, **kwds)

    def __iter__(self, *args, **kwds):
        return self._data.__iter__(*args, **kwds)

    def __deepcopy__(self, memo):
        return self

    def get(self, *args, **kwds):
        return self._data.get(*args, **kwds)

    def update(self, *args, **kwds):  # pylint: disable=arguments-differ
        return self._data.update(*args, **kwds)


@dataclass(slots=True, frozen=True)
class Task:
    """Task `dataclass`.

    Attributes:
        func: Task function.
        name: Task name.
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
        headers: Task headers.
        loads: Function to load task arguments.
        dumps: Function to dump task result
    """

    func: Callable | AsyncCallable

    name: str
    queue: str = TASK_QUEUE
    priority: TaskPriority = TASK_PRIORITY
    timeout: Timeout = TASK_TIMEOUT
    ttl: Timeout = TASK_TTL
    ack_late: bool = TASK_ACK_LATE
    result_ttl: Timeout = TASK_RESULT_TTL
    result_return: bool = TASK_RESULT_RETURN
    thread: Optional[bool] = None
    events: bool | set[str] = TASK_EVENTS
    event_ttl: Timeout = EVENT_TTL
    headers: dict = field(default_factory=dict)  # pylint: disable=used-before-assignment

    # NOTE
    # args_kwds_loads
    # result_dumps
    # result_loads
    loads: Optional[Callable] = None
    dumps: Optional[Callable] = None

    def __post_init__(self):
        if self.func:
            object.__setattr__(self, "func", FuncProxy(self.func))
        if self.loads:
            object.__setattr__(self, "loads", FuncProxy(self.loads))
        if self.dumps:
            object.__setattr__(self, "dumps", FuncProxy(self.dumps))

    def __call__(self, *args, **kwds) -> Any:
        """Call task function with args and kwds."""

        return self.func(*args, **kwds)

    def asdict(self, exclude: list[str] | None = None, sanitize: bool | None = None):  # pylint: disable=unused-argument
        """Convert to dict.

        Args:
            exclude: fields to exclude.
            sanitize: flag to sanitize sensitive data.
        Returns:
            `arrlio.models.Task` as `dict`.
        """

        # exclude = (exclude or []) + ["loads", "dumps"]
        exclude = exclude or []
        return {k: v for k, v in asdict(self).items() if k not in exclude}

    def pretty_repr(self, exclude: list[str] | None = None, sanitize: bool | None = None):
        return pretty_repr(self.asdict(exclude=exclude, sanitize=sanitize))

    def instantiate(
        self,
        task_id: TaskId | None = None,
        args: Args | None = None,
        kwds: Kwds | None = None,
        meta: dict | None = None,
        headers: dict | None = None,
        **kwargs,
    ) -> "TaskInstance":
        """Instantiate new `arrlio.models.TaskInstance` object with provided arguments.

        Returns:
            `arrlio.models.TaskInstance` object.
        """

        headers = {**self.headers, **(headers or {})}
        return TaskInstance(
            **{
                **self.asdict(),
                "task_id": task_id,
                "args": args or (),
                "kwds": kwds or {},
                "meta": meta or {},
                "headers": headers or {},
                **kwargs,
            }
        )


@dataclass(slots=True, frozen=True)
class TaskInstance(Task):
    """Task instance `dataclass`.

    Attributes:
        task_id: Task Id.
        args: Task function positional arguments.
        kwds: Task function keyword arguments.
        meta: Task function additional meta keyword argument.
    """

    task_id: UUID = field(default_factory=uuid4)
    args: Args = field(default_factory=tuple)
    kwds: Kwds = field(default_factory=dict)  # pylint: disable=used-before-assignment
    meta: dict = field(default_factory=dict)  # pylint: disable=used-before-assignment

    shared: Shared = field(default_factory=Shared, init=False)

    sanitizer: ClassVar[Optional[Callable]] = None

    def __post_init__(self):
        if self.task_id is None:
            object.__setattr__(self, "task_id", uuid4())
        elif isinstance(self.task_id, str):
            object.__setattr__(self, "task_id", UUID(self.task_id))
        if not isinstance(self.args, tuple):
            object.__setattr__(self, "args", tuple(self.args))

    def asdict(self, exclude: list[str] | None = None, sanitize: bool | None = None):
        """Convert to dict.

        Args:
            exclude: fields to exclude.
            sanitize: flag to sanitize sensitive data.
        Returns:
            `arrlio.models.TaskInstance` as `dict`.
        """

        exclude = exclude or []
        # pylint: disable=super-with-arguments
        data = super(TaskInstance, self).asdict(exclude=exclude, sanitize=sanitize)
        if sanitize:
            if self.sanitizer:
                data = self.sanitizer(data)  # pylint: disable=not-callable
            else:
                if data["args"]:
                    data["args"] = "<hidden>"
                if data["kwds"]:
                    data["kwds"] = "<hidden>"
        return data

    def pretty_repr(self, exclude: list[str] | None = None, sanitize: bool | None = None):
        exclude = (exclude or []) + ["shared"]
        return pretty_repr(self.asdict(exclude=exclude, sanitize=sanitize))

    def __call__(self, meta: bool | None = None):  # pylint: disable=arguments-differ
        """Call `arrlio.models.TaskInstance`.

        Args:
            meta: Add additional keyword argument `meta` to the task function call.
        """

        args = self.args
        kwds = self.kwds
        if meta is True:
            kwds = {"meta": self.meta, **kwds}
        if isinstance(self.func, type):
            func = self.func()
        else:
            func = self.func
        return func(*args, **kwds)

    def instantiate(self, *args, **kwds):
        raise NotImplementedError


@dataclass(slots=True, frozen=True)
class TaskResult:
    """Task result `dataclass`."""

    res: Any = None
    exc: Optional[Exception | tuple[str, str, str]] = None
    trb: Optional[TracebackType | str] = None
    idx: Optional[tuple[str, int]] = None
    routes: Optional[str | list[str]] = None

    def set_idx(self, idx: tuple[str, int]):
        object.__setattr__(self, "idx", idx)

    def asdict(self, sanitize: bool | None = None):
        """Convert to dict.

        Args:
            sanitize: flag to sanitize sensitive data.
        Returns:
            `arrlio.models.TaskResult` as `dict`.
        """

        return {
            "res": self.res if self.res is None or not sanitize else "<hidden>",
            "exc": self.exc,
            "trb": self.trb,
            "idx": self.idx,
            "routes": self.routes,
        }

    def pretty_repr(self, sanitize: bool | None = None):
        return pretty_repr(self.asdict(sanitize=sanitize))


@dataclass(slots=True, frozen=True)
class Event:
    """Event `dataclass`.

    Attributes:
        type: Event type.
        event_id: Event Id.
        dt: Event datetime.
        ttl: Event time to live, seconds.
    """

    type: str
    data: dict  # pylint: disable=used-before-assignment
    event_id: UUID = field(default_factory=uuid4)
    dt: Optional[datetime.datetime] = None
    ttl: Timeout = EVENT_TTL

    def __post_init__(self):
        if not isinstance(self.event_id, UUID):
            object.__setattr__(self, "event_id", UUID(self.event_id))
        if self.dt is None:
            object.__setattr__(self, "dt", datetime.datetime.now(tz=datetime.timezone.utc))
        elif isinstance(self.dt, str):
            object.__setattr__(self, "dt", datetime.datetime.fromisoformat(self.dt))

    def asdict(self, sanitize: bool | None = None):  # pylint: disable=unused-argument
        """Convert to dict.

        Args:
            sanitize: flag to sanitize sensitive data.
        Returns:
            `arrlio.models.Event` as `dict`.
        """

        data = asdict(self)
        if hasattr(data["data"], "sanitize"):
            data["data"] = data["data"].sanitize()
        return data

    def pretty_repr(self, sanitize: bool | None = None):
        return pretty_repr(self.asdict(sanitize=sanitize))


class Graph:
    """Graph class."""

    def __init__(
        self,
        name: str,
        nodes: dict[str, list] | None = None,
        edges: dict[str, list] | None = None,
        roots: set | None = None,
    ):
        """
        Args:
            name: graph name.
            node: graph nodes.
            edges: graph edges.
            roots: graph roots.
        """

        self.name = name
        self.nodes: dict[str, list] = rodict({}, nested=True)
        self.edges: dict[str, list] = rodict({}, nested=True)
        self.roots: set[str] = roset(set())
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

    def add_node(self, node_id: str, task: Task | str, root: bool | None = None, **kwds):
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
        self.nodes.__original__[node_id] = (task, kwds)
        if root:
            self.roots.__original__.add(node_id)

    def add_edge(self, node_id_from: str, node_id_to: str, routes: str | list[str] | None = None):
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
        self.edges.__original__.setdefault(node_id_from, []).append((node_id_to, routes))

    def asdict(self, sanitize: bool | None = None):  # pylint: disable=unused-argument
        """Convert to the dict.

        Args:
            sanitize: flag to sanitize sensitive data.
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
    def from_dict(cls, data: dict) -> "Graph":
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

    def pretty_repr(self, sanitize: bool | None = None):
        return pretty_repr(self.asdict(sanitize=sanitize))
