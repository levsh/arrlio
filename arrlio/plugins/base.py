import abc
import contextlib
import logging

from arrlio.models import TaskInstance, TaskResult

logger = logging.getLogger("arrlio.plugins.base")


class Plugin(abc.ABC):
    def __init__(self, app):
        self.app = app

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass

    async def on_init(self):
        logger.info("Plugin[%s] initialization done", self.name)

    async def on_close(self):
        pass

    @contextlib.asynccontextmanager
    def task_context(self, task_instance: TaskInstance):
        # pylint: disable=unused-argument
        yield

    async def on_task_send(self, task_instance: TaskInstance) -> None:
        pass

    async def on_task_received(self, task_instance: TaskInstance) -> None:
        pass

    async def on_task_done(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        pass
