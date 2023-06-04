import importlib
import re
from types import ModuleType
from typing import Any, Callable, Coroutine, Dict, Iterable, List, Optional, Sequence, Tuple, Union, no_type_check
from uuid import UUID

from pydantic import AnyUrl, ConstrainedInt, PositiveFloat, PositiveInt

AsyncFunction = Callable[..., Coroutine]
ExceptionFilter = Callable[[Exception], bool]


class Priority(ConstrainedInt):
    ge = 1
    le = 10


PositiveNumber = Union[PositiveInt, PositiveFloat]

Timeout = PositiveNumber
RetryTimeout = Union[Sequence[Timeout], Iterable[Timeout]]


TaskId = Union[str, UUID]
Args = Union[List, Tuple]
Kwds = Dict


class SecretAnyUrl(AnyUrl):
    __slots__ = ("_url",)

    @no_type_check
    def __new__(cls, url: Optional[str], **kwds) -> object:
        if url is None:
            _url = cls.build(**kwds)
        else:
            _url = url
            if re.match(r".*://([^:.]*):?([^:.]*)@.*", url):
                url = re.sub(r"://([^:.]*):?([^.]*)@", "://***:***@", url)
        if kwds.get("user") is not None:
            kwds["user"] = "***"
        if kwds.get("password") is not None:
            kwds["password"] = "***"
        obj = super().__new__(cls, url, **kwds)
        obj._url = _url
        return obj

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self}')"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, SecretAnyUrl) and self.get_secret_value() == other.get_secret_value()

    def __hash__(self):
        return self._url.__hash__()

    def get_secret_value(self) -> str:
        return self._url


class SecretAmqpDsn(SecretAnyUrl):
    allowed_schemes = {"amqp", "amqps"}
    user_required = True


class SecretRedisDsn(SecretAnyUrl):
    allowed_schemes = {"redis", "rediss"}
    user_required = False

    @classmethod
    def validate_parts(cls, parts: Dict[str, str], **kwds) -> Dict[str, str]:  # pylint: disable=arguments-differ
        defaults = {
            "domain": "localhost" if not (parts["ipv4"] or parts["ipv6"]) else "",
            "port": "6379",
            "path": "/0",
        }
        for key, value in defaults.items():
            if not parts[key]:
                parts[key] = value
        return super().validate_parts(parts, **kwds)


class _Module:
    name: str = None
    validate_always = True

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if isinstance(v, str):
            v = importlib.import_module(v)
            if not hasattr(v, "Config"):
                raise TypeError("Module doesn't provide Config class")
            if not hasattr(v, cls.name):
                raise TypeError(f"Module doesn't provide {cls.name} class")
        if not isinstance(v, ModuleType):
            raise ValueError("Expect Module")
        return v


class BackendModule(_Module):
    name = "Backend"


class SerializerModule(_Module):
    name = "Serializer"


class PluginModule(_Module):
    name = "Plugin"


class ExecutorModule(_Module):
    name = "Executor"
