import importlib
import json
import logging
import traceback
from typing import Any

from arrlio.core import __tasks__
from arrlio.models import Event, Graph, Task, TaskData, TaskInstance, TaskResult
from arrlio.serializers.base import Serializer
from arrlio.utils import ExtendedJSONEncoder

logger = logging.getLogger("arrlio.serializers.json")

json_dumps = json.dumps
json_loads = json.loads


class Serializer(Serializer):  # pylint: disable=function-redefined
    def __init__(self, encoder=None):
        self.encoder = encoder or ExtendedJSONEncoder

    def __str__(self):
        return "json.Serializer"

    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> bytes:
        dct = task_instance.dict()
        data = dct["data"]
        if graph := data["graph"]:
            data["graph"] = graph.dict()
        return json_dumps(
            {
                "name": dct["task"]["name"],
                **{k: v for k, v in data.items() if v is not None},
            },
            cls=self.encoder,
        ).encode()

    def loads_task_instance(self, data: bytes) -> TaskInstance:
        data = json_loads(data)
        if data.get("graph"):
            data["graph"] = Graph.from_dict(data["graph"])
        name = data.pop("name")
        if name in __tasks__:
            task_instance = __tasks__[name].instantiate(**data)
        else:
            task_instance = Task(None, name).instantiate(**data)

        task: Task = task_instance.task
        task_data: TaskData = task_instance.data

        if task.loads:
            args, kwds = task.loads(*task_data.args, **task_data.kwds)
            if not isinstance(args, tuple):
                raise TypeError("Task loads function should return [tuple, dict]")
            if not isinstance(kwds, dict):
                raise TypeError("Task loads function should return [tuple, dict]")
            task_data.args = args
            task_data.kwds = kwds

        return task_instance

    def dumps_task_result(self, task_instance: TaskInstance, task_result: TaskResult, **kwds) -> bytes:
        if task_result.exc:
            data = (
                None,
                (
                    getattr(task_result.exc, "__module__", "builtins"),
                    task_result.exc.__class__.__name__,
                    str(task_result.exc),
                ),
                "".join(traceback.format_tb(task_result.trb, 3)) if task_result.trb else None,
            )
        else:
            if task_instance.task.dumps:
                data = (task_instance.task.dumps(task_result.res), None, None)
            else:
                data = (task_result.res, None, None)
        return json.dumps(data, cls=self.encoder).encode()

    def loads_task_result(self, data: bytes) -> TaskResult:
        result_data = json.loads(data)
        if exc := result_data[1]:
            try:
                module = importlib.import_module(exc[0])
                result_data[1] = getattr(module, exc[1])(exc[2])
            except Exception:
                pass
        return TaskResult(*result_data)

    def dumps_event(self, event: Event, **kwds) -> bytes:
        data = event.dict()
        return json.dumps(data, cls=self.encoder).encode()

    def loads_event(self, data: bytes) -> Event:
        return Event(**json.loads(data))

    def dumps(self, data: Any, **kwds) -> bytes:
        return json.dumps(data, cls=self.encoder).encode()

    def loads(self, data: bytes) -> Any:
        return json.loads(data)
