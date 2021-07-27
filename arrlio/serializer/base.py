import abc

from arrlio.models import TaskInstance, TaskResult


class Serializer(abc.ABC):
    @abc.abstractmethod
    def dumps_task_instance(self, task_instance: TaskInstance, **kwds):
        pass

    @abc.abstractmethod
    def loads_task_instance(self, data) -> TaskInstance:
        pass

    @abc.abstractmethod
    def dumps_task_result(self, result: TaskResult, **kwds):
        pass

    @abc.abstractmethod
    def loads_task_result(self, data) -> TaskResult:
        pass
