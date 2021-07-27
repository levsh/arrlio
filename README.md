# Arrlio
Simplest asyncio task system

![tests](https://github.com/levsh/arrlio/workflows/tests/badge.svg)

```python
import arrlio


@arrlio.task(name="hello_word")
async def hello_world():
    return "Hello World!"


worker = arrlio.Worker(arrlio.WorkerConfig(backend="arrlio.backend.local"))
client = arrlio.Client(arrlio.ClientConfig(backend="arrlio.backend.local"))

await worker.run()

ar = await client.call("hello_world")
await ar.get()
```

```bash
pipenv install
pipenv run python examples/main.py
```
