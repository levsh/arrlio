Create your task:

```python
from arrlio import task

@task
def hello_world():
    print("Hello from Arrlio")
```

Create Arrlio application(by default `local` backend will be used):

```python
from arrlio import App, Config

app = App(Config())

```

Start consuming tasks and run `hello_world` task:

```python
async with app:
    await app.consume_tasks()
    await app.send_task(hello_world)
```

Summary:

```python
from arrlio import App, Config, task

@task
def hello_world():
    print("Hello from Arrlio")


app = App(Config())
async with app:
    await app.consume_tasks()
    await app.send_task(hello_world)
```
