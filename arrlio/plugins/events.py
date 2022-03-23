import logging
from datetime import datetime, timezone

from arrlio.models import Event, TaskInstance, TaskResult
from arrlio.plugins.base import Plugin


logger = logging.getLogger("arrlio.plugins.events")


class Plugin(Plugin):
    async def on_task_received(self, task_instance: TaskInstance) -> None:
        task_type = "task received"
        task_data = task_instance.data
        if (
            task_data.events is True
            or isinstance(task_data.events, (list, set, tuple))
            and task_type in task_data.events
        ):
            event: Event = Event(
                type=task_type,
                dt=datetime.now(tz=timezone.utc),
                ttl=task_data.event_ttl,
                data={"task_id": task_data.task_id},
            )
            await self.app.send_event(event)

    async def on_task_done(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        task_type = "task done"
        task_data = task_instance.data
        if (
            task_data.events is True
            or isinstance(task_data.events, (list, set, tuple))
            and task_type in task_data.events
        ):
            event: Event = Event(
                type=task_type,
                dt=datetime.now(tz=timezone.utc),
                ttl=task_data.event_ttl,
                data={"task_id": task_data.task_id, "status": task_result.exc is None},
            )
            await self.app.send_event(event)
