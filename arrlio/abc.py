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

    async def __aenter__(self) -> "AbstractClosable":
        await self.init()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


class AbstractBroker(AbstractClosable, ABC):
    @abstractmethod
    async def send_task(self, task_instance: TaskInstance, **kwds):
        """
        Send task.

        Args:
            task_instance: Task instance to send.
        """

        raise NotImplementedError

    @abstractmethod
    async def consume_tasks(self, queues: list[str], callback: Callable[[TaskInstance], Coroutine]):
        """
        Consume tasks from the queues and invoke `callback` on `TaskInstance` received.

        Args:
            queues: List of queue names to consume.
            callback: Async callback to invoke on `TaskInstance` received.
        """

        raise NotImplementedError

    @abstractmethod
    async def stop_consume_tasks(self, queues: list[str] | None = None):
        """
        Stop consuming tasks.

        Args:
            queues: List of queue names to stop consume. Stop consuming from all queues if `None`.
        """

        raise NotImplementedError


class AbstractResultBackend(AbstractClosable, ABC):
    @abstractmethod
    def make_headers(self, task_instance: TaskInstance) -> dict:
        """
        Make result backend headers for `TaskInstance`.

        Args:
            task_instance: Task instance.
        """

        raise NotImplementedError

    @abstractmethod
    def make_shared(self, task_instance: TaskInstance) -> Shared:
        """
        Make result backend `Shared` instance for `TaskInstance`.

        Args:
            task_instance: Task instance.
        """

        raise NotImplementedError

    @abstractmethod
    async def allocate_storage(self, task_instance: TaskInstance):
        """Allocate storage for future task result if needed."""

        raise NotImplementedError

    @abstractmethod
    async def push_task_result(self, task_result: TaskResult, task_instance: TaskInstance):
        """
        Push `TaskResult` for `TaskInstance`.

        Args:
            task_result: Task result to push.
            task_instance: Task instance.
        """

        raise NotImplementedError

    @abstractmethod
    async def pop_task_result(self, task_instance: TaskInstance) -> AsyncGenerator[TaskResult, None]:
        """
        Pop `TaskResult` for `TaskInstance` from `ResultBackend`.

        Args:
            task_instance: Task instance.
        """

        raise NotImplementedError

    @abstractmethod
    async def close_task(self, task_instance: TaskInstance, idx: tuple[str, int] | None = None):
        """Close task."""

        raise NotImplementedError


class AbstractEventBackend(AbstractClosable, ABC):
    @abstractmethod
    async def send_event(self, event: Event):
        """
        Send `Event`.

        Args:
            event: Event to send.
        """

        raise NotImplementedError

    @abstractmethod
    async def consume_events(
        self,
        callback_id: str,
        callback: Callable[[Event], Any],
        event_types: list[str] | None = None,
    ):
        """
        Consume events and invoke `callback` on `Event` received.

        Args:
            callback_id: Callback Id. Needed for later use when stop consuming.
            callback: Callback to invoke then event received by backend.
            event_types: List of event types to consume.
        """

        raise NotImplementedError

    @abstractmethod
    async def stop_consume_events(self, callback_id: str | None = None):
        """
        Stop consuming events.

        Args:
            callback_id: Callback Id for wich to stop consuming events.
        """

        raise NotImplementedError


class AbstractSerializer(ABC):
    @abstractmethod
    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> tuple[bytes | TaskInstance, dict]:
        """
        Dump `TaskInstance`.

        Args:
            task_instance: Task instance.
        """

    @abstractmethod
    def loads_task_instance(self, data: bytes | TaskInstance, headers: dict, **kwds) -> TaskInstance:
        """
        Load `data` into `TaskInstance`.

        Args:
            data: data to load from.
        """

    @abstractmethod
    def dumps_task_result(
        self,
        task_result: TaskResult,
        *,
        task_instance: TaskInstance | None = None,
        **kwds,
    ) -> tuple[bytes | TaskResult, dict]:
        """
        Dump `TaskResult`.

        Args:
            task_result: Task result to dump.
            task_instance: Task instance which task result belongs to.
        """

    @abstractmethod
    def loads_task_result(self, data: bytes | TaskResult, headers: dict, **kwds) -> TaskResult:
        """
        Load data into `TaskResult`.

        Args:
            data: data to load from.
        """

    @abstractmethod
    def dumps_event(self, event: Event, **kwds) -> tuple[bytes | Event, dict]:
        """
        Dump `arrlio.models.Event`.

        Args:
            event: Event to dump.
        """

    @abstractmethod
    def loads_event(self, data: bytes | Event, headers: dict, **kwds) -> Event:
        """
        Load `data` into `Event`.

        Args:
            data: data to load from.
        """


class AbstractPlugin(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """Plugin name."""
