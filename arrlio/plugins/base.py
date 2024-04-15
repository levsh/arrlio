import abc
import contextlib
import logging

from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio.models import TaskInstance, TaskResult

logger = logging.getLogger("arrlio.plugins.base")


class Config(BaseSettings):
    model_config = SettingsConfigDict()


class Plugin(abc.ABC):
    # pylint: disable=unused-argument

    def __init__(self, app, config: Config):
        self.app = app
        self.config = config

    def __str__(self):
        return f"Plugin[{self.name}]"

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    async def on_init(self):
        pass

    async def on_close(self):
        pass

    @contextlib.asynccontextmanager
    def task_context(self, task_instance: TaskInstance):
        yield

    async def on_task_send(self, task_instance: TaskInstance) -> None:
        pass

    async def on_task_received(self, task_instance: TaskInstance) -> None:
        pass

    async def on_task_result(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        pass

    async def on_task_done(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        pass
