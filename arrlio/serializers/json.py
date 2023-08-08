import importlib
import json
import logging
import traceback
from types import TracebackType
from typing import Any, Tuple, Type

from pydantic import Field

from arrlio import registered_tasks
from arrlio.models import Event, Graph, Task, TaskInstance, TaskResult
from arrlio.serializers import base
from arrlio.utils import ExtendedJSONEncoder

logger = logging.getLogger("arrlio.serializers.json")

json_dumps = json.dumps
json_loads = json.loads


class Config(base.Config):
    encoder: Type[json.JSONEncoder] = Field(default=ExtendedJSONEncoder)


class Serializer(base.Serializer):
    """Json serializer class."""

    def dumps(self, data: Any, **kwds) -> bytes:
        """Dumps data as json encoded string.

        Args:
            data: Data to dumps.
        """

        return json_dumps(data, cls=self.config.encoder).encode()

    def loads(self, data: bytes) -> Any:
        """Loads json encoded data to Python object.

        Args:
            data: Data to loads.
        """

        return json_loads(data)

    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> bytes:
        """Dumps `arrlio.models.TaskInstance` object as json encoded string."""

        data = task_instance.dict(exclude=["func", "dumps", "loads"])
        extra = data["extra"]
        if graph := extra.get("graph:graph"):
            extra["graph:graph"] = graph.dict()
        return self.dumps({k: v for k, v in data.items() if v is not None})

    def loads_task_instance(self, data: bytes) -> TaskInstance:
        """Loads `arrlio.models.TaskInstance` object from json encoded string."""

        data: dict = self.loads(data)
        if data["extra"].get("graph:graph"):
            data["extra"]["graph:graph"] = Graph.from_dict(data["extra"]["graph:graph"])
        name = data["name"]
        task_instance: TaskInstance
        if name in registered_tasks:
            task_instance = registered_tasks[name].instantiate(**data)
        else:
            task_instance = Task(None, name).instantiate(**data)

        if task_instance.loads:
            args, kwds = task_instance.loads(*task_instance.args, **task_instance.kwds)
            if not isinstance(args, tuple) or not isinstance(kwds, dict):
                raise TypeError(f"Task '{task_instance.name}' loads function should return Tuple[Tuple, Dict]")
            object.__setattr__(task_instance, "args", args)
            object.__setattr__(task_instance, "kwds", kwds)

        return task_instance

    def dumps_exc(self, exc: Exception) -> tuple:
        """Dumps exception as json encoded string."""

        return (getattr(exc, "__module__", "builtins"), exc.__class__.__name__, f"{exc}")

    def loads_exc(self, exc: Tuple[str, str, str]) -> Exception:
        """Loads exception from json encodes string."""

        try:
            module = importlib.import_module(exc[0])
            return getattr(module, exc[1])(exc[2])
        except Exception:
            raise Exception(exc[1], exc[2])

    def dumps_trb(self, trb: TracebackType) -> str:
        """Dumps traceback object as json encoded string."""

        return "".join(traceback.format_tb(trb, 5)) if trb else None

    def loads_trb(self, trb: str) -> str:
        """Loads traceback string."""

        return trb

    def dumps_task_result(self, task_instance: TaskInstance, task_result: TaskResult, **kwds) -> bytes:
        """Dumps `arrlio.models.TaskResult` as json encoded string."""

        data = task_result.dict()
        if data["exc"]:
            data["exc"] = self.dumps_exc(data["exc"])
            data["trb"] = self.dumps_trb(data["trb"])
        elif task_instance.dumps:
            data["res"] = task_instance.dumps(data["res"])
        return self.dumps(data)

    def loads_task_result(self, data: bytes) -> TaskResult:
        """Loads `arrlio.models.TaskResult` from json encoded string."""

        data = self.loads(data)
        if data["exc"]:
            data["exc"] = self.loads_exc(data["exc"])
            data["trb"] = self.loads_trb(data["trb"])
        return TaskResult(**data)

    def dumps_event(self, event: Event, **kwds) -> bytes:
        """Dumps `arrlio.models.Event` as json encoded string."""

        data = event.dict()
        if event.type == "task:result":
            result = data["data"]["result"]
            if result["exc"]:
                result["exc"] = self.dumps_exc(result["exc"])
                result["trb"] = self.dumps_trb(result["trb"])
        elif event.type == "task:done":
            status = data["data"]["status"]
            if status.get("exc"):
                status["exc"] = self.dumps_exc(status["exc"])
                status["trb"] = self.dumps_trb(status["trb"])
        return self.dumps(data)

    def loads_event(self, data: bytes) -> Event:
        """Loads `arrlio.models.Event` from json encoded string."""

        event: Event = Event(**self.loads(data))
        if event.type == "task:result":
            result = event.data["result"]
            if result["exc"]:
                result["exc"] = self.loads_exc(result["exc"])
                result["trb"] = self.loads_trb(result["trb"])
            event.data["result"] = TaskResult(**result)
        elif event.type == "task:done":
            status = event.data["status"]
            if status.get("exc"):
                status["exc"] = self.loads_exc(status["exc"])
                status["trb"] = self.loads_trb(status["trb"])
        return event
