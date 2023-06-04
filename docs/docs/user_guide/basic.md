Define your tasks:

``` python
from arrlio import task

@task
def hello_world():
    print("Hello from Arrlio")
```

Create application:

``` python
from arrlio import App, Config

app = App(Config())
```
!!! info
    By default `local` backend will be used

Start consuming tasks and run `hello_world` task:

``` python
async with app:
    await app.consume_tasks()
    await app.send_task(hello_world)
```

Summary:

``` python title="main.py" linenums="1"
import asyncio
from arrlio import App, Config, task

@task
def hello_world():
    print("Hello from Arrlio")


async def main():
    app = App(Config())
    async with app:
        await app.consume_tasks()
        await app.send_task(hello_world)


if __name__ == "__main__":
    asyncio.run(main())
```

More examples can be found in the `examples` directory.

