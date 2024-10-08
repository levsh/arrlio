import importlib

from traceback import format_tb
from types import TracebackType
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio import registered_tasks
from arrlio.abc import AbstractSerializer
from arrlio.exceptions import TaskError
from arrlio.models import Event, Graph, Task, TaskInstance, TaskResult


class Config(BaseSettings):
    model_config = SettingsConfigDict()


class Serializer(AbstractSerializer):
    """Serializer class.

    Args:
        config: Serializer config.
    """

    __slots__ = ("config",)

    def __init__(self, config: Config):
        self.config = config

    @property
    def content_type(self) -> str | None:
        return

    def dumps(self, data: Any, **kwds) -> bytes | Any:
        """
        Dump data.

        Args:
            data: data to dump.
        """

    def loads(self, data: bytes | Any) -> Any:
        """
        Load data.

        Args:
            data: data to load.
        """

    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> bytes:
        """
        Dump `arrlio.models.TaskInstance` object as json encoded string.

        Args:
            task_instance: Task instance to dump.
        """

        data = task_instance.asdict(exclude=["func", "shared", "dumps", "loads"])
        headers = data["headers"]
        if graph := headers.get("graph:graph"):
            headers["graph:graph"] = graph.asdict()
        return self.dumps({k: v for k, v in data.items() if v is not None})

    def loads_task_instance(self, data: bytes, **kwds) -> TaskInstance:
        """
        Load `arrlio.models.TaskInstance` object from json encoded string.

        Args:
            data: data to load from.
        """

        data: dict = self.loads(data)
        if data["headers"].get("graph:graph"):
            data["headers"]["graph:graph"] = Graph.from_dict(data["headers"]["graph:graph"])
        name = data["name"]
        task_instance: TaskInstance
        if name in registered_tasks:
            task_instance = registered_tasks[name].instantiate(**data)
        else:
            task_instance = Task(None, name).instantiate(**data)

        if task_instance.loads:
            args, kwds = task_instance.loads(*task_instance.args, **task_instance.kwds)
            if not isinstance(args, tuple) or not isinstance(kwds, dict):
                raise TypeError(f"task '{task_instance.name}' loads function should return tuple[tuple, dict]")
            object.__setattr__(task_instance, "args", args)
            object.__setattr__(task_instance, "kwds", kwds)

        return task_instance

    def dumps_exc(self, exc: Exception) -> tuple:
        """
        Dump exception as json encoded string.

        Args:
            exc: Exception to dump.
        """

        return (getattr(exc, "__module__", "builtins"), exc.__class__.__name__, f"{exc}")

    def loads_exc(self, exc: tuple[str, str, str]) -> Exception:
        """
        Load exception from json encodes string.

        Args:
            exc: Data to load from.
        """

        try:
            module = importlib.import_module(exc[0])
            return getattr(module, exc[1])(exc[2])
        except Exception:
            return TaskError(exc[1], exc[2])

    def dumps_trb(self, trb: TracebackType) -> str | None:
        """
        Dump traceback object as json encoded string.

        Args:
            trb: Traceback to dump.
        """

        return "".join(format_tb(trb, 5)) if trb else None

    def loads_trb(self, trb: str) -> str:
        """
        Load traceback string.

        Args:
            trb: Data to load from.
        """

        return trb

    def dumps_task_result(self, task_result: TaskResult, task_instance: TaskInstance | None = None, **kwds) -> bytes:
        """
        Dump `arrlio.models.TaskResult` as json encoded string.

        Args:
            task_result: Task result to dump.
            task_instance: Task instance which task result belongs to.
        """

        data = task_result.asdict()
        if data["exc"]:
            data["exc"] = self.dumps_exc(data["exc"])
            data["trb"] = self.dumps_trb(data["trb"])
        elif task_instance and task_instance.dumps:
            data["res"] = task_instance.dumps(data["res"])
        return self.dumps(data)

    def loads_task_result(self, data: bytes, **kwds) -> TaskResult:
        """
        Load `arrlio.models.TaskResult` from json encoded string.

        Args:
            data: Data to load from.
        """

        data = self.loads(data)
        if data["exc"]:
            data["exc"] = self.loads_exc(data["exc"])
            data["trb"] = self.loads_trb(data["trb"])
        return TaskResult(**data)

    def dumps_event(self, event: Event, **kwds) -> bytes:
        """
        Dump `arrlio.models.Event` as json encoded string.

        Args:
            event: Event to dump.
        """

        data = event.asdict()
        if event.type == "task.result":
            result = data["data"]["result"]
            if result["exc"]:
                result["exc"] = self.dumps_exc(result["exc"])
                result["trb"] = self.dumps_trb(result["trb"])
        elif event.type == "task.done":
            result = data["data"]["result"]
            result["res"] = None
            if result["exc"]:
                result["exc"] = self.dumps_exc(result["exc"])
                result["trb"] = self.dumps_trb(result["trb"])
        return self.dumps(data)

    def loads_event(self, data: bytes, **kwds) -> Event:
        """
        Load `arrlio.models.Event` from json encoded string.

        Args:
            data: Data to load from.
        """

        event: Event = Event(**self.loads(data))
        if event.type in {"task.result", "task.done"}:
            result = event.data["result"]
            if result["exc"]:
                result["exc"] = self.loads_exc(result["exc"])
                result["trb"] = self.loads_trb(result["trb"])
            event.data["result"] = TaskResult(**result)
        return event
