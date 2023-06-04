
**Arrlio** supports generators:

```python
from arrlio import App, Config, task

@task
def xrange(x: int):
    for i in range(x):
        yield i

app = App(Config())
async with app:
    await app.consume_tasks()
    ar = await app.send_task(xrange, args=(3,))
    async for i in ar:
        print(i)
```

!!! note
    For RabbitMQ backend this feature works only with `ResutsQueueMode.COMMON` mode.
