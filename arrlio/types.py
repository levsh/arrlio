import logging

from dataclasses import dataclass
from importlib import import_module
from types import ModuleType
from typing import Any, Callable, Coroutine, NewType, Optional, TypeVar, Union
from uuid import UUID

from annotated_types import Ge, Le
from pydantic import AfterValidator, AnyUrl, BaseModel, GetPydanticSchema, SecretStr
from pydantic_core import core_schema
from pydantic_settings import BaseSettings
from typing_extensions import Annotated

from arrlio.settings import TASK_MAX_PRIORITY, TASK_MIN_PRIORITY


logger = logging.getLogger("arrlio.types")


T = TypeVar("T")


AsyncCallable = Callable[..., Coroutine]
ExceptionFilter = Callable[[Exception], bool]

Timeout = Annotated[int, Ge(0)]

TTL = Annotated[int, Ge(1)]

TaskPriority = Annotated[int, Ge(TASK_MIN_PRIORITY), Le(TASK_MAX_PRIORITY)]

TaskId = Union[str, UUID]
Args = Union[list, tuple]
Kwds = dict

UniqueList = Annotated[list[T], AfterValidator(lambda v: [*dict.fromkeys(v)])]


Meta = NewType("Meta", Union[dict])


@dataclass
class ModuleConstraints:
    has_attrs: Optional[list[str]]

    def __hash__(self):
        return hash(";".join(self.has_attrs or []))

    def __call__(self, v):
        if self.has_attrs:
            for name in self.has_attrs:
                if not hasattr(v, name):
                    raise ValueError(f"module doesn't provide required attribute '{name}'")
        return v


class Module(ModuleType):
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        def validate_from_str(v):
            if isinstance(v, str):
                try:
                    v = import_module(v)
                except ModuleNotFoundError as e:
                    logger.exception(e)
                    raise ValueError("unable to load module") from e
            return v

        from_str_schema = core_schema.chain_schema(
            [
                core_schema.str_schema(),
                core_schema.no_info_plain_validator_function(validate_from_str),
            ]
        )

        return core_schema.union_schema(
            [
                core_schema.is_instance_schema(ModuleType),
                from_str_schema,
            ],
            serialization=core_schema.plain_serializer_function_ser_schema(
                function=lambda x: x.__spec__ and x.__spec__.name or f"{x}",
                when_used="json",
            ),
        )

    # @classmethod
    # def __get_pydantic_json_schema__(cls, core_schema, handler):
    #     return handler(core_schema.str_schema())


BrokerModule = Annotated[Module, AfterValidator(ModuleConstraints(has_attrs=["Broker", "Config"]))]
ResultBackendModule = Annotated[Module, AfterValidator(ModuleConstraints(has_attrs=["ResultBackend", "Config"]))]
EventBackendModule = Annotated[Module, AfterValidator(ModuleConstraints(has_attrs=["EventBackend", "Config"]))]
SerializerModule = Annotated[Module, AfterValidator(ModuleConstraints(has_attrs=["Serializer", "Config"]))]
ExecutorModule = Annotated[Module, AfterValidator(ModuleConstraints(has_attrs=["Executor", "Config"]))]
PluginModule = Annotated[Module, AfterValidator(ModuleConstraints(has_attrs=["Plugin", "Config"]))]


ModuleConfig = Annotated[
    Union[BaseModel, BaseSettings],
    GetPydanticSchema(
        lambda source_type, handler: core_schema.no_info_after_validator_function(
            lambda v: v,
            core_schema.is_instance_schema((dict, source_type)),
        )
    ),
    GetPydanticSchema(
        lambda source_type, handler: core_schema.no_info_before_validator_function(
            lambda v: v,
            core_schema.chain_schema([core_schema.is_instance_schema((dict, source_type)), handler(source_type)]),
        )
    ),
]


class SecretAnyUrl(AnyUrl):
    @property
    def username(self) -> SecretStr:
        return SecretStr(self._url.username) if self._url.username is not None else None

    @property
    def password(self) -> SecretStr:
        return SecretStr(self._url.password) if self._url.password is not None else None

    def __str__(self) -> str:
        url = self._url
        return str(
            url.build(
                scheme=url.scheme,
                host=url.host,
                username="***" if url.username is not None else None,
                password="***" if url.password is not None else None,
                port=url.port,
                path=(url.path or "").lstrip("/"),
                query=url.query,
                fragment=url.fragment,
            )
        )

    def __repr__(self) -> str:
        return f"SecretAnyUrl('{self}')"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, SecretAnyUrl) and self.get_secret_value() == other.get_secret_value()

    def get_secret_value(self):
        return self._url


@dataclass
class UrlConstraints:
    allowed_schemes: Optional[list[str]]

    def __hash__(self):
        return hash(";".join(self.allowed_schemes or []))

    def __call__(self, v):
        if self.allowed_schemes and v.scheme not in self.allowed_schemes:
            raise ValueError(f"URL scheme should be one of {self.allowed_schemes}")
        return v


SecretHttpUrl = Annotated[
    SecretAnyUrl,
    AfterValidator(UrlConstraints(allowed_schemes=["http", "https"])),
]


SecretAmqpDsn = Annotated[
    SecretAnyUrl,
    AfterValidator(UrlConstraints(allowed_schemes=["amqp", "amqps"])),
]
