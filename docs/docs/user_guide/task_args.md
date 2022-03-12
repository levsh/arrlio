Positional arguments:

```python
from arrlio import App, Config, task

@task
def hello_world(name: str):
    print(f"Hello {name}")

app = App(Config())
async with app:
    await app.consume_tasks()
    await app.run_task(hello_world, args=("World",))
```

Named arguments:

```python
from arrlio import App, Config, task

@task
def hello_world(name: str = None):
    name = name or "World"
    print(f"Hello {name}")

app = App(Config())
async with app:
    await app.consume_tasks()
    await app.run_task(hello_world, kwds={"name": "User"})
```
