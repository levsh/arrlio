import json
import logging
import traceback
from dataclasses import asdict
from typing import Any

from arrlio import __tasks__
from arrlio.models import Event, Graph, Task, TaskData, TaskInstance, TaskResult
from arrlio.serializers.base import Serializer
from arrlio.utils import ExtendedJSONEncoder


logger = logging.getLogger("arrlio")


class Serializer(Serializer):
    def __init__(self, encoder=None):
        self.encoder = encoder or ExtendedJSONEncoder

    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> bytes:
        dct = asdict(task_instance)
        data = dct["data"]
        if graph := data["graph"]:
            data["graph"] = graph.dict()
        return json.dumps(
            {
                "name": dct["task"]["name"],
                **{k: v for k, v in data.items() if v is not None},
            },
            cls=self.encoder,
        ).encode()

    def loads_task_instance(self, data: bytes) -> TaskInstance:
        data = json.loads(data)
        if data.get("graph"):
            data["graph"] = Graph.from_dict(data["graph"])
        name = data.pop("name")
        if name in __tasks__:
            task_instance = __tasks__[name].instantiate(data=TaskData(**data))
        else:
            task_instance = Task(None, name).instantiate(data=TaskData(**data))
        return task_instance

    def dumps_task_result(self, task_result: TaskResult, **kwds) -> bytes:
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
            data = (task_result.res, None, None)
        return json.dumps(data, cls=self.encoder).encode()

    def loads_task_result(self, data: bytes) -> TaskResult:
        return TaskResult(*json.loads(data))

    def dumps_event(self, event: Event, **kwds) -> bytes:
        data = asdict(event)
        return json.dumps(data, cls=self.encoder).encode()

    def loads_event(self, data: bytes) -> Event:
        return Event(**json.loads(data))

    def dumps(self, data: Any, **kwds) -> bytes:
        return json.dumps(data, cls=self.encoder).encode()

    def loads(self, data: bytes) -> Any:
        return json.loads(data)
