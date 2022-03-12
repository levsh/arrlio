

```python
from arrlio import App, Config, task

@task
def add_one(x: int):
    return x + 1

app = App(Config())
async with app:
    await app.consume_tasks()
    ar = await app.run_task(add_one, args=(1,))
    result = await ar.get()
    print(f"1 + 1 = {result}")
```
