import logging
from typing import Any, Callable

from arrlio.models import TaskInstance, TaskResult
from arrlio.serializers.json import Serializer


logger = logging.getLogger("arrlio")


class Serializer(Serializer):
    def __init__(self, encoder=None, encryptor: Callable = lambda x: x, decryptor: Callable = lambda x: x):
        super().__init__(encoder=encoder)
        self.encryptor = encryptor
        self.decryptor = decryptor

    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> bytes:
        data: bytes = super().dumps_task_instance(task_instance, **kwds)
        if task_instance.data.encrypt:
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
