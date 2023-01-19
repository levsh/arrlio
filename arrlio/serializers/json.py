import importlib
import json
import logging
import traceback
from types import TracebackType
from typing import Any, Tuple, Type

from pydantic import Field

from arrlio.core import __tasks__
from arrlio.models import Event, Graph, Task, TaskData, TaskInstance, TaskResult
from arrlio.serializers import base
from arrlio.utils import ExtendedJSONEncoder

logger = logging.getLogger("arrlio.serializers.json")


class Config(base.Config):
    encoder: Type[json.JSONEncoder] = Field(default=ExtendedJSONEncoder)


class Serializer(base.Serializer):
    def dumps(self, data: Any, **kwds) -> bytes:
        return json.dumps(data, cls=self.config.encoder).encode()

    def loads(self, data: bytes) -> Any:
        return json.loads(data)

    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> bytes:
        dct = task_instance.dict()
        extra = dct["data"]["extra"]
        if graph := extra.get("graph"):
            extra["graph"] = graph.dict()
        return self.dumps(
            {
                "name": dct["task"]["name"],
                **{k: v for k, v in dct["data"].items() if v is not None},
            },
            cls=self.config.encoder,
        )

    def loads_task_instance(self, data: bytes) -> TaskInstance:
        data = self.loads(data)
        if data["extra"].get("graph"):
            data["extra"]["graph"] = Graph.from_dict(data["extra"]["graph"])
        name = data.pop("name")
        if name in __tasks__:
            task_instance = __tasks__[name].instantiate(**data)
        else:
            task_instance = Task(None, name).instantiate(**data)

        task: Task = task_instance.task
        task_data: TaskData = task_instance.data

        if task.loads:
            args, kwds = task.loads(*task_data.args, **task_data.kwds)
            if not isinstance(args, tuple) or not isinstance(kwds, dict):
                raise TypeError(f"Task '{task.name}' loads function should return Tuple[Tuple, Dict]")
            task_data.args = args
            task_data.kwds = kwds

        return task_instance

    def dumps_exc(self, exc: Exception) -> tuple:
        return (getattr(exc, "__module__", "builtins"), exc.__class__.__name__, f"{exc}")

    def loads_exc(self, exc: Tuple[str, str, str]) -> Exception:
        try:
            module = importlib.import_module(exc[0])
            return getattr(module, exc[1])(exc[2])
        except Exception:
            raise Exception(exc[1], exc[2])

    def dumps_trb(self, trb: TracebackType) -> str:
        return "".join(traceback.format_tb(trb, 5)) if trb else None

    def loads_trb(self, trb: str) -> str:
        return trb

    def dumps_task_result(self, task_instance: TaskInstance, task_result: TaskResult, **kwds) -> bytes:
        data = task_result.dict()
        if data["exc"]:
            data["exc"] = self.dumps_exc(data["exc"])
            data["trb"] = self.dumps_trb(data["trb"])
        elif task_instance.task.dumps:
            data["res"] = task_instance.task.dumps(data["res"])
        return self.dumps(data)

    def loads_task_result(self, data: bytes) -> TaskResult:
        data = self.loads(data)
        if data["exc"]:
            data["exc"] = self.loads_exc(data["exc"])
            data["trb"] = self.loads_trb(data["trb"])
        return TaskResult(**data)

    def dumps_event(self, event: Event, **kwds) -> bytes:
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
