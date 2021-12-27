import dataclasses
import json
import logging
import traceback
from typing import Any, Callable

from arrlio import __tasks__
from arrlio.utils import ExtendedJSONEncoder
from arrlio.models import Task, TaskData, TaskInstance, TaskResult
from arrlio.serializer import base


logger = logging.getLogger("arrlio")


class Json(base.Serializer):
    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> bytes:
        data = dataclasses.asdict(task_instance.data)
        data["name"] = task_instance.task.name
        return json.dumps(data, cls=ExtendedJSONEncoder).encode()

    def loads_task_instance(self, data: bytes) -> TaskInstance:
        data = json.loads(data)
        name = data.pop("name")
        if name in __tasks__:
            return __tasks__[name].instatiate(data=TaskData(**data))
        return Task(None, name).instatiate(data=TaskData(**data))

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
        return json.dumps(data, cls=ExtendedJSONEncoder).encode()

    def loads_task_result(self, data: bytes) -> TaskResult:
        return TaskResult(*json.loads(data))

    def dumps(self, data: Any, **kwds) -> bytes:
        return json.dumps(data, cls=ExtendedJSONEncoder).encode()

    def loads(self, data: bytes) -> Any:
        return json.loads(data)


class CryptoJson(Json):
    def __init__(self, encryptor: Callable = lambda x: x, decryptor: Callable = lambda x: x):
        self.encryptor = encryptor
        self.decryptor = decryptor

    def dumps_task_instance(self, task_instance: TaskInstance, encrypt: bool = None, **kwds) -> bytes:
        data: bytes = super().dumps_task_instance(task_instance, **kwds)
        if encrypt:
            data: bytes = b"1" + self.encryptor(data)
        else:
            data: bytes = b"0" + data
        return data

    def loads_task_instance(self, data: bytes) -> TaskInstance:
        header, data = data[0:1], data[1:]
        if header == b"1":
            data: bytes = self.decryptor(data)
        return super().loads_task_instance(data)

    def dumps_task_result(self, task_result: TaskResult, encrypt: bool = None, **kwds) -> bytes:
        data: bytes = super().dumps_task_result(task_result, **kwds)
        if encrypt:
            data: bytes = b"1" + self.encryptor(data)
        else:
            data: bytes = b"0" + data
        return data

    def loads_task_result(self, data: bytes) -> TaskResult:
        header, data = data[0:1], data[1:]
        if header == b"1":
            data: bytes = self.decryptor(data)
        return super().loads_task_result(data)

    def dumps(self, data: Any, encrypt: bool = None, **kwds) -> bytes:
        data: bytes = super().dumps(data, **kwds)
        if encrypt:
            data: bytes = b"1" + self.encryptor(data)
        else:
            data: bytes = b"0" + data
        return data

    def loads(self, data: bytes) -> Any:
        header, data = data[0:1], data[1:]
        if header == b"1":
            data: bytes = self.decryptor(data)
        return super().loads(data)
