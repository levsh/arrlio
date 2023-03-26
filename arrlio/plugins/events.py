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
        logger.info("%s initializing ...", self)
        logger.info("%s initialization done", self)

    async def on_task_send(self, task_instance: TaskInstance) -> None:
        task_data = task_instance.data
        events = task_data.events
        if events is True or isinstance(events, (list, set, tuple)) and "task:send" in events:
            event: Event = Event(
                type="task:send",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_data.event_ttl,
                data={"task_id": task_data.task_id},
            )
            await self.app.send_event(event)

    async def on_task_received(self, task_instance: TaskInstance) -> None:
        task_data = task_instance.data
        events = task_data.events
        if events is True or isinstance(events, (list, set, tuple)) and "task:received" in events:
            event: Event = Event(
                type="task:received",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_data.event_ttl,
                data={"task_id": task_data.task_id},
            )
            await self.app.send_event(event)

    async def on_task_result(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        task_data = task_instance.data
        events = task_data.events
        if events is True or isinstance(events, (list, set, tuple)) and "task:result" in events:
            event: Event = Event(
                type="task:result",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_data.event_ttl,
                data={"task_id": task_data.task_id, "result": task_result},
            )
            await self.app.send_event(event)

    async def on_task_done(self, task_instance: TaskInstance, status: dict) -> None:
        task_data = task_instance.data
        events = task_data.events
        if events is True or isinstance(events, (list, set, tuple)) and "task:done" in events:
            event: Event = Event(
                type="task:done",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_data.event_ttl,
                data={"task_id": task_data.task_id, "status": status},
            )
            await self.app.send_event(event)
