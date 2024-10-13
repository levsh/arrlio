import contextlib
import logging

from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio.abc import AbstractPlugin
from arrlio.models import TaskInstance, TaskResult


logger = logging.getLogger("arrlio.plugins.base")


class Config(BaseSettings):
    model_config = SettingsConfigDict()


class Plugin(AbstractPlugin):
    """
    Args:
        app: `App` instance.
        config: Plugin config.
    """

    def __init__(self, app, config: Config):
        self.app = app
        self.config = config

    def __str__(self):
        return f"Plugin[{self.name}]"

    async def on_init(self):
        """
        This method will be called then the Arrlio app is initialized.
        """

    async def on_close(self):
        """
        This method will be called then the Arrlio app is closed.
        """

    @contextlib.asynccontextmanager
    def task_context(self, task_instance: TaskInstance):
        """
        This method will be called then the task instance is received by Arrlio but before `on_task_received`.
        """
        yield

    async def on_task_send(self, task_instance: TaskInstance) -> None:
        """
        This method will be called then the task instance is send.
        """

    async def on_task_received(self, task_instance: TaskInstance) -> None:
        """
        This method will be called then the task instance is received.
        """

    async def on_task_result(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        """
        This method will be called then the task result is ready.
        """

    async def on_task_done(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        """
        This method will be called then the task done.
        """
