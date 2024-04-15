import abc
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio.models import Event, TaskInstance, TaskResult


class Config(BaseSettings):
    model_config = SettingsConfigDict()


class Serializer(abc.ABC):
    __slots__ = ("config",)

    def __init__(self, config: Config):
        self.config = config

    @abc.abstractmethod
    def dumps(self, data: Any, **kwds) -> bytes | Any:
        pass

    @abc.abstractmethod
    def loads(self, data: bytes | Any) -> Any:
        pass

    @abc.abstractmethod
    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> bytes | TaskInstance:
        pass

    @abc.abstractmethod
    def loads_task_instance(self, data: bytes | TaskInstance) -> TaskInstance:
        pass

    @abc.abstractmethod
    def dumps_task_result(
        self,
        task_result: TaskResult,
        task_instance: TaskInstance | None = None,
        **kwds,
    ) -> bytes | TaskResult:
        pass

    @abc.abstractmethod
    def loads_task_result(self, data: bytes | TaskResult) -> TaskResult:
        pass

    @abc.abstractmethod
    def dumps_event(self, event: Event, **kwds) -> bytes | Event:
        pass

    @abc.abstractmethod
    def loads_event(self, data: bytes | Event) -> Event:
        pass
