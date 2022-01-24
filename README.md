# Arrlio
Simplest asyncio distributed task/workflow system

![tests](https://github.com/levsh/arrlio/workflows/tests/badge.svg)

```bash
pip install arrlio
```

```python
import asyncio
import os

import arrlio


@arrlio.task(name="sync hello_world")
def sync_hello_world():
    return "Hello World!"

@arrlio.task(name="async hello_world")
async def async_hello_world():
    return "Hello World!"

BACKEND = "arrlio.backend.local"
# BACKEND = "arrlio.backend.rabbitmq"
# BACKEND = "arrlio.backend.redis"

async def main():
    producer = arrlio.Producer(arrlio.ProducerConfig(backend=BACKEND))
    consumer = arrlio.Consumer(arrlio.ConsumerConfig(backend=BACKEND))

    async with producer, consumer:
        await consumer.consume_tasks()

        ar = await producer.send_task("sync hello_world")
        print(await ar.get())

        ar = await producer.send_task("async hello_world")
        print(await ar.get())


asyncio.run(main())
```

```python
import asyncio
import os

import arrlio


@arrlio.task
async def add_one(value: str, **kwds):
    return int(value) + 1

graph = arrlio.Graph("My Graph")
graph.add_node("A", add_one, root=True)
graph.add_node("B", add_one)
graph.add_node("C", add_one)
graph.add_edge("A", "B")
graph.add_edge("B", "C")

BACKEND = "arrlio.backend.local"

async def main():
    producer = arrlio.Producer(arrlio.ProducerConfig(backend=BACKEND))
    consumer = arrlio.Consumer(arrlio.ConsumerConfig(backend=BACKEND))

    async with producer, consumer:
        await consumer.consume_tasks()

        ars = await producer.send_graph(graph, args=(0,))
        print(await ars["C"].get())


asyncio.run(main())
```

```python
import asyncio
import os

import arrlio
import invoke


@arrlio.task(thread=True)
async def bash(cmd):
    return invoke.run(cmd).stdout

graph = arrlio.Graph("My Graph")
graph.add_node("A", bash, root=True)
graph.add_node("B", bash, args=("wc -w",))

BACKEND = "arrlio.backend.local"

async def main():
    producer = arrlio.Producer(arrlio.ProducerConfig(backend=BACKEND))
    consumer = arrlio.Consumer(arrlio.ConsumerConfig(backend=BACKEND))

    async with producer, consumer:
        await consumer.consume_tasks()

        ars = await producer.send_graph(graph, args=("This sentence contains 5 words",))
        print(await ars["B"].get())


asyncio.run(main())
```

```bash
pipenv install
pipenv run python examples/main.py
```
