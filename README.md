# Arrlio
Simplest asyncio task system

![tests](https://github.com/levsh/arrlio/workflows/tests/badge.svg)

```python
import asyncio
import os

import arrlio
from arrlio import crypto
from arrlio.serializer.json import CryptoJson


@arrlio.task(name="hello_world")
async def hello_world():
    return "Hello World!"


BACKEND = "arrlio.backend.local"
# BACKEND = "arrlio.backend.rabbitmq"
# BACKEND = "arrlio.backend.redis"


def serializer():
    key = os.urandom(32)
    return CryptoJson(
        encryptor=lambda x: crypto.s_encrypt(x, key),
        decryptor=lambda x: crypto.s_decrypt(x, key),
    )

async def main():
    backend_config_kwds = {"serializer": serializer}
    producer = arrlio.TaskProducer(
        arrlio.TaskProducerConfig(backend=BACKEND),
        backend_config_kwds=backend_config_kwds
    )
    consumer = arrlio.TaskConsumer(
        arrlio.TaskConsumerConfig(backend=BACKEND),
        backend_config_kwds=backend_config_kwds
    )

    async with producer, consumer:
        await consumer.consume()
        ar = await producer.send("hello_world")
        await ar.get()


asyncio.run(main())
```

```bash
pipenv install
pipenv run python examples/main.py
```
