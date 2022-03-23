from arrlio.models import TaskInstance, TaskResult


class Plugin:
    def __init__(self, app):
        self.app = app

    async def close(self):
        pass

    async def on_task_send(self, task_instance: TaskInstance) -> None:
        pass

    async def on_task_received(self, task_instance: TaskInstance) -> None:
        pass

    async def on_task_done(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        pass
