import importlib
from types import ModuleType
from typing import Callable, Coroutine, Union

from pydantic import PyObject, conint


AsyncCallableT = Callable[[], Coroutine]
ExceptionFilterT = Callable[[Exception], bool]
PriorityT = conint(ge=1, le=10)
SerializerT = PyObject
TimeoutT = Union[int, float]


class BackendT:
    validate_always = True

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if isinstance(v, str):
            v = importlib.import_module(v)
        if not isinstance(v, ModuleType):
            raise ValueError
        return v
