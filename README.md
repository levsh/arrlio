# Arrlio [WIP]

[Documentation](https://levsh.github.io/arrlio) (WIP)

Asyncio distributed task/workflow system with supports generators and graphs

![tests](https://github.com/levsh/arrlio/workflows/tests/badge.svg)
![coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/levsh/727ed723ccaee0d5825513af6472e3a5/raw/coverage.json)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

### Installation
```bash
pip install arrlio
```
Or to use latest develop version
```bash
pip install git+https://github.com/levsh/arrlio
```

### Usage

#### Create tasks file
```python
# tasks.py

import io

import arrlio
import invoke

@arrlio.task
async def hello_world():
    return "Hello World!"

# task custom name
@arrlio.task(name="foo")
async def foo():
    arrlio.logger.info("Hello from task 'foo'!")

# access to task object as self argument
@arrlio.task(bind=True)
async def bind(self):
    arrlio.logger.info(self.data.task_id)
    arrlio.logger.info(self)

# exception example
@arrlio.task
async def exception():
    raise ZeroDivisionError

# Arrlio supports generators and async generators
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
    result = invoke.run(
        cmd,
        in_stream=in_stream,
        out_stream=out_stream
    )
    return result.stdout
```

#### Create main file and run it

```python
import asyncio
import logging

import arrlio
import tasks

logger = logging.getLogger("arrlio")
logger.setLevel("INFO")

BACKEND = "arrlio.backends.local"
# BACKEND = "arrlio.backends.rabbitmq"

async def main():
    app = arrlio.App(arrlio.Config(backend={"module": BACKEND}))

    async with app:
        await app.consume_tasks()

        # call by task object
        ar = await app.send_task(tasks.hello_world)
        logger.info(await ar.get())

        # call by task name
        ar = await app.send_task("foo")
        logger.info(await ar.get())

        # task args example
        ar = await app.send_task(tasks.add_one, args=(1,))
        logger.info(await ar.get())

        # exception example
        try:
            ar = await app.send_task(tasks.exception)
            logger.info(await ar.get())
        except Exception as e:
            print(f"\nThis is example exception for {app.backend}:\n")
            logger.exception(e)
            print()

        # generator example
        results = []
        ar = await app.send_task(tasks.xrange, args=(3,))
        async for result in ar:
            results.append(result)
        logger.info(results)  # -> [0, 1, 2]


if __name__ == "__main__":
    asyncio.run(main())
```

#### Arrlio supports graph execution
```python
import asyncio
import logging

import arrlio
import tasks

logger = logging.getLogger("arrlio")
logger.setLevel("INFO")

BACKEND = "arrlio.backends.local"
# BACKEND = "arrlio.backends.rabbitmq"


async def main():
    graph = arrlio.Graph("My Graph")
    graph.add_node("A", tasks.add_one, root=True)
    graph.add_node("B", tasks.add_one)
    graph.add_node("C", tasks.add_one)
    graph.add_edge("A", "B")
    graph.add_edge("B", "C")

	# arrlio.plugins.events and arrlio.plugins.graphs
	# plugins are required
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

		# execute graph with argument 0
        ars = await app.send_graph(graph, args=(0,))
        logger.info("A: %i", await ars["A"].get())  # -> A: 1
        logger.info("B: %i", await ars["B"].get())  # -> B: 2
        logger.info("C: %i", await ars["C"].get())  # -> C: 3


if __name__ == "__main__":
    asyncio.run(main())
```
#### Another graph example
```python
import asyncio
import logging

import arrlio
import tasks

logger = logging.getLogger("arrlio")
logger.setLevel("INFO")

BACKEND = "arrlio.backends.local"
# BACKEND = "arrlio.backends.rabbitmq"


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

        ars = await app.send_graph(
            graph,
            args=('echo "Number of words in this sentence:"',)
        )
        logger.info(await asyncio.wait_for(ars["B"].get(), timeout=2))  # -> 6


if __name__ == "__main__":
    asyncio.run(main())
```

#### And more examples
```bash
poetry install
poetry run python examples/main.py
```
