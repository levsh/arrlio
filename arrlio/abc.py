from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Callable, Coroutine

from arrlio.models import Event, Shared, TaskInstance, TaskResult


class AbstractClosable(ABC):
    @property
    @abstractmethod
    def is_closed(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    async def close(self):
        raise NotImplementedError

    async def __aenter__(self):
        await self.init()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


class AbstractBroker(AbstractClosable, ABC):
    @abstractmethod
    async def send_task(self, task_instance: TaskInstance, **kwds):
        """Send task to backend."""
        raise NotImplementedError

    @abstractmethod
    async def consume_tasks(self, queues: list[str], callback: Callable[[TaskInstance], Coroutine]):
        """Consume tasks from the queues and invoke `callback` on `arrlio.models.TaskInstance` received."""
        raise NotImplementedError

    @abstractmethod
    async def stop_consume_tasks(self, queues: list[str] | None = None):
        """Stop consuming tasks."""
        raise NotImplementedError


class AbstractResultBackend(AbstractClosable, ABC):
    @abstractmethod
    def make_headers(self, task_instance: TaskInstance) -> dict:
        """Make result backend headers for `arrlio.models.TaskInstance`."""
        raise NotImplementedError

    @abstractmethod
    def make_shared(self, task_instance: TaskInstance) -> Shared:
        """Make result backend shared for `arrio.models.TaskInstance`."""
        raise NotImplementedError

    @abstractmethod
    async def allocate_storage(self, task_instance: TaskInstance):
        """Allocate storage for future task result if needed."""
        raise NotImplementedError

    @abstractmethod
    async def push_task_result(self, task_result: TaskResult, task_instance: TaskInstance):
        """Push `arrlio.models.TaskResult` for `arrlio.models.TaskInstance`."""
        raise NotImplementedError

    @abstractmethod
    async def pop_task_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        """Pop `arrlio.models.TaskResult` for `arrlio.models.TaskInstance`."""
        raise NotImplementedError

    @abstractmethod
    async def close_task(self, task_instance: TaskInstance, idx: tuple[str, int] | None = None):
        """Close task."""
        raise NotImplementedError


class AbstractEventBackend(AbstractClosable, ABC):
    @abstractmethod
    async def send_event(self, event: Event):
        """Send `arrlio.models.Event`."""
        raise NotImplementedError

    @abstractmethod
    async def consume_events(
        self,
        callback_id: str,
        callback: Callable[[Event], Any],
        event_types: list[str] | None = None,
    ):
        """Consume events and invoke `callback` on `arrlio.models.Event` received."""
        raise NotImplementedError

    @abstractmethod
    async def stop_consume_events(self, callback_id: str | None = None):
        """Stop consuming events."""
        raise NotImplementedError
