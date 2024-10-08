import asyncio
import logging

from asyncio import create_task, sleep
from datetime import datetime, timezone
from time import monotonic
from typing import Annotated, Callable
from uuid import UUID

from pydantic import Field, PlainSerializer

from arrlio import gettext
from arrlio.models import Event, TaskInstance, TaskResult
from arrlio.plugins import base


_ = gettext.gettext


logger = logging.getLogger("arrlio.plugins.events")


EventDataExtender = Annotated[
    Callable[[TaskInstance], dict],
    PlainSerializer(lambda x: f"{x}", return_type=str, when_used="json"),
    # PlainSerializer(lambda x: "<EventDataExtender>", return_type=str, when_used="json"),
]


class Config(base.Config):
    """
    Events Plugin config.

    Attributes:
        event_data_extenders: Mapping with callable to extend event data.
    """

    event_data_extenders: dict[str, EventDataExtender] = Field(default_factory=dict)


class Plugin(base.Plugin):
    """
    Events Plugin.

    Args:
        app: `arrlio.core.App` instance.
        config: Events Plugin config.
    """

    def __init__(self, app, config: Config):
        super().__init__(app, config)
        self._ping_tasks: dict[UUID, asyncio.Task] = {}

    @property
    def name(self) -> str:
        """Plugin name."""

        return "arrlio.events"

    @property
    def event_types(self) -> tuple[str, ...]:
        """Supported event types."""

        return (
            "task.send",
            "task.ping",
            "task.reveived",
            "task.result",
            "task.done",
        )

    async def _ping(self, task_instance: TaskInstance):
        timeout_time = monotonic() + 60
        while True:
            try:
                timeout = max(timeout_time - monotonic(), 0)
                await sleep(timeout)
                timeout_time = monotonic() + 60
                event: Event = Event(
                    type="task.ping",
                    dt=datetime.now(tz=timezone.utc),
                    ttl=task_instance.event_ttl,
                    data={
                        **self.config.event_data_extenders.get("task.ping", lambda *args: {})(task_instance),
                        **{"task:id": task_instance.task_id},
                    },
                )
                await self.app.send_event(event)
            except Exception as e:
                logger.exception(e)

    async def on_init(self):
        logger.info(_("%s initializing..."), self)
        logger.info(_("%s initialization done"), self)

    async def on_close(self):
        for task in self._ping_tasks.values():
            task.cancel()

    async def on_task_send(self, task_instance: TaskInstance) -> None:
        events = task_instance.events
        if events is True or isinstance(events, (list, set, tuple)) and "task.send" in events:
            event: Event = Event(
                type="task.send",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_instance.event_ttl,
                data={
                    **self.config.event_data_extenders.get("task.send", lambda *args: {})(task_instance),
                    **{"task:id": task_instance.task_id},
                },
            )
            await self.app.send_event(event)

    async def on_task_received(self, task_instance: TaskInstance) -> None:
        events = task_instance.events
        if events is True or isinstance(events, (list, set, tuple)) and "task.received" in events:
            event: Event = Event(
                type="task.received",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_instance.event_ttl,
                data={
                    **self.config.event_data_extenders.get("task.received", lambda *args: {})(task_instance),
                    **{"task:id": task_instance.task_id},
                },
            )
            await self.app.send_event(event)
        if events is True or isinstance(events, (list, set, tuple)) and "task.ping" in events:
            self._ping_tasks[task_instance.task_id] = create_task(self._ping(task_instance))

    async def on_task_result(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        events = task_instance.events
        if events is True or isinstance(events, (list, set, tuple)) and "task.result" in events:
            event: Event = Event(
                type="task.result",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_instance.event_ttl,
                data={
                    **self.config.event_data_extenders.get("task.result", lambda *args: {})(task_instance),
                    **{"task:id": task_instance.task_id, "result": task_result},
                },
            )
            await self.app.send_event(event)

    async def on_task_done(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        events = task_instance.events
        if events is True or isinstance(events, (list, set, tuple)) and "task.ping" in events:
            ping_task = self._ping_tasks.pop(task_instance.task_id, None)
            if ping_task:
                ping_task.cancel()
        if events is True or isinstance(events, (list, set, tuple)) and "task.done" in events:
            event: Event = Event(
                type="task.done",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_instance.event_ttl,
                data={
                    **self.config.event_data_extenders.get("task.done", lambda *args: {})(task_instance),
                    **{"task:id": task_instance.task_id, "result": task_result},
                },
            )
            await self.app.send_event(event)
