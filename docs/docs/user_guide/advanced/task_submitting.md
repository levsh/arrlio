Task can be invoked by task object or name and can have the following parameters:

- `queue`
- `priority` from 1 to 5.
- `timeout`
- `ttl`
- `result_return`. If `False` no result will be returned from the task.
- `result_ttl`
- `thread`. If `True` task will be executed in a separate thread.
- `events`. If `True` task events will be dispatched.
- `headers`

!!! info
    These parameters passed in `send_task` method override task and application settings.

```python
from arrlio import App, Config, task

@task(name="Task foo", priority=1, thread=False, events=False)
def foo():
    return

app = App(Config())
async with app:
    await app.send_task(
        foo,  # or "Task foo"
        queue="tasks.foo",
        priority=5,
        timeout=10,
        ttl=60,
        result_return=False,
        thread=True,
        events=True,
        headers={"key": "Additional headers"}
    )
```
!!! info 
    For `rabbitmq` broker/backends **Arrlio** creates all needed exchanges and queues.
