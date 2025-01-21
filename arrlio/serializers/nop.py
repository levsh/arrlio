from arrlio.abc import AbstractSerializer
from arrlio.models import Event, TaskInstance, TaskResult
from arrlio.serializers import base


class Config(base.Config):
    pass


class Serializer(AbstractSerializer):
    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> tuple[TaskInstance, dict]:
        return task_instance, {}

    def loads_task_instance(self, data: TaskInstance, headers: dict, **kwds) -> TaskInstance:
        return data

    def dumps_task_result(
        self,
        task_result: TaskResult,
        task_instance: TaskInstance | None = None,
        **kwds,
    ) -> tuple[TaskResult, dict]:
        return task_result, {}

    def loads_task_result(self, data: TaskResult, headers: dict, **kwds) -> TaskResult:
        return data

    def dumps_event(self, event: Event, **kwds) -> tuple[Event, dict]:
        return event, {}

    def loads_event(self, data: Event, headers: dict, **kwds) -> Event:
        return data
