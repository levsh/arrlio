import logging
from datetime import datetime, timezone

from arrlio.models import Event, TaskInstance, TaskResult
from arrlio.plugins import base

logger = logging.getLogger("arrlio.plugins.events")


class Config(base.Config):
    pass


class Plugin(base.Plugin):
    @property
    def name(self) -> str:
        return "arrlio.events"

    async def on_init(self):
        logger.info("%s initializing...", self)
        logger.info("%s initialization done", self)

    async def on_task_send(self, task_instance: TaskInstance) -> None:
        events = task_instance.events
        if events is True or isinstance(events, (list, set, tuple)) and "task:send" in events:
            event: Event = Event(
                type="task:send",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_instance.event_ttl,
                data={"task:id": task_instance.task_id},
            )
            await self.app.send_event(event)

    async def on_task_received(self, task_instance: TaskInstance) -> None:
        events = task_instance.events
        if events is True or isinstance(events, (list, set, tuple)) and "task:received" in events:
            event: Event = Event(
                type="task:received",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_instance.event_ttl,
                data={"task:id": task_instance.task_id},
            )
            await self.app.send_event(event)

    async def on_task_result(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        events = task_instance.events
        if events is True or isinstance(events, (list, set, tuple)) and "task:result" in events:
            event: Event = Event(
                type="task:result",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_instance.event_ttl,
                data={"task:id": task_instance.task_id, "result": task_result},
            )
            await self.app.send_event(event)

    async def on_task_done(self, task_instance: TaskInstance, status: dict) -> None:
        events = task_instance.events
        if events is True or isinstance(events, (list, set, tuple)) and "task:done" in events:
            event: Event = Event(
                type="task:done",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_instance.event_ttl,
                data={"task:id": task_instance.task_id, "status": status},
            )
            await self.app.send_event(event)
