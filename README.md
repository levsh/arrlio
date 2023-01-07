# Arrlio [Unstable, WIP]

[Documentation](https://levsh.github.io/arrlio)

Simplest asyncio distributed task/workflow system

![tests](https://github.com/levsh/arrlio/workflows/tests/badge.svg)
![coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/levsh/727ed723ccaee0d5825513af6472e3a5/raw/coverage.json)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

```bash
pip install arrlio
```

```python
# tasks.py

import io

import invoke

import arrlio


@arrlio.task
async def hello_world():
    return "Hello World!"


@arrlio.task(name="foo")
async def foo():
    arrlio.logger.info("Hello from task 'foo'!")


@arrlio.task(bind=True)
async def bind(self):
    arrlio.logger.info(self.data.task_id)
    arrlio.logger.info(self)


@arrlio.task
async def exception():
    raise ZeroDivisionError


@arrlio.task
def xrange(count):
    for x in range(count):
        yield x


@arrlio.task
async def add_one(value: str):
    return int(value) + 1


@arrlio.task
async def bash(cmd, stdin: str = None):
    in_stream = io.StringIO(stdin)
    out_stream = io.StringIO()
    result = invoke.run(cmd, in_stream=in_stream, out_stream=out_stream)
    return result.stdout
```

```python
import asyncio
import logging

import arrlio
import tasks

logger = logging.getLogger("arrlio")
logger.setLevel("INFO")

BACKEND = "arrlio.backends.local"
# BACKEND = "arrlio.backends.rabbitmq"
# BACKEND = "arrlio.backends.redis"


async def main():
    app = arrlio.App(arrlio.Config(backend={"module": BACKEND}))

    async with app:
        await app.consume_tasks()

        # call by task
        ar = await app.send_task(tasks.hello_world)
        logger.info(await ar.get())

        # call by task name
        ar = await app.send_task("foo")
        logger.info(await ar.get())

        # task args example
        ar = await app.send_task(tasks.add_one, args=(1,))
        logger.info(await ar.get())

        # exception
        try:
            ar = await app.send_task(tasks.exception)
            logger.info(await ar.get())
        except Exception as e:
            print(f"\nThis is example exception for {app.backend}:\n")
            logger.exception(e)
            print()

        # generator
        results = []
        ar = await app.send_task(tasks.xrange, args=(3,))
        async for result in ar:
            results.append(result)
        logger.info(results)


if __name__ == "__main__":
    asyncio.run(main())
```

```python
import asyncio
import logging

import arrlio
import tasks

logger = logging.getLogger("arrlio")
logger.setLevel("INFO")

BACKEND = "arrlio.backends.local"
# BACKEND = "arrlio.backends.rabbitmq"
# BACKEND = "arrlio.backends.redis"


async def main():
    graph = arrlio.Graph("My Graph")
    graph.add_node("A", tasks.add_one, root=True)
    graph.add_node("B", tasks.add_one)
    graph.add_node("C", tasks.add_one)
    graph.add_edge("A", "B")
    graph.add_edge("B", "C")

    app = arrlio.App(
        arrlio.Config(
            backend={"module": BACKEND},
            plugins=[
                {"module": "arrlio.plugins.events"},
                {"module": "arrlio.plugins.graphs"},
            ],
        )
    )

    async with app:
        await app.consume_tasks()

        ars = await app.send_graph(graph, args=(0,))
        logger.info("A: %i", await ars["A"].get())
        logger.info("B: %i", await ars["B"].get())
        logger.info("C: %i", await ars["C"].get())


if __name__ == "__main__":
    asyncio.run(main())
```

```python
import asyncio
import logging

import arrlio
import tasks

logger = logging.getLogger("arrlio")
logger.setLevel("INFO")

BACKEND = "arrlio.backends.local"
# BACKEND = "arrlio.backends.rabbitmq"
# BACKEND = "arrlio.backends.redis"


async def main():
    graph = arrlio.Graph("My Graph")
    graph.add_node("A", tasks.bash, root=True)
    graph.add_node("B", tasks.bash, args=("wc -w",))
    graph.add_edge("A", "B")

    app = arrlio.App(
        arrlio.Config(
            backend={"module": BACKEND},
            plugins=[
                {"module": "arrlio.plugins.events"},
                {"module": "arrlio.plugins.graphs"},
            ],
        )
    )

    async with app:
        await app.consume_tasks()

        ars = await app.send_graph(graph, args=('echo "Number of words in this sentence:"',))
        logger.info(await asyncio.wait_for(ars["B"].get(), timeout=2))


if __name__ == "__main__":
    asyncio.run(main())
```

```bash
pipenv install
pipenv run python examples/main.py
```
