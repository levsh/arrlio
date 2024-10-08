from arrlio.abc import AbstractSerializer
from arrlio.models import Event, TaskInstance, TaskResult
from arrlio.serializers import base


class Config(base.Config):
    pass


class Serializer(AbstractSerializer):
    def dumps_task_instance(self, task_instance: TaskInstance, **kwds) -> TaskInstance:
        return task_instance

    def loads_task_instance(self, data: TaskInstance, **kwds) -> TaskInstance:
        return data

    def dumps_task_result(
        self,
        task_result: TaskResult,
        task_instance: TaskInstance | None = None,
        **kwds,
    ) -> TaskResult:
        return task_result

    def loads_task_result(self, data: TaskResult, **kwds) -> TaskResult:
        return data

    def dumps_event(self, event: Event, **kwds) -> Event:
        return event

    def loads_event(self, data: Event, **kwds) -> Event:
        return data
