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

key = os.urandom(32)

def serializer():
    return CryptoJson(
        encryptor=lambda x: crypto.s_encrypt(x, key),
        decryptor=lambda x: crypto.s_decrypt(x, key),
    )

backend_config_kwds = {"serializer": serializer}
client = arrlio.Client(
    arrlio.ClientConfig(backend=BACKEND),
    backend_config_kwds=backend_config_kwds
)
executor = arrlio.Executor(
    arrlio.ExecutorConfig(backend=BACKEND),
    backend_config_kwds=backend_config_kwds
)


async def main():
    try:
        await executor.run()
        ar = await client.call("hello_world")
        await ar.get()
    finally:
        await executor.stop()
        await client.close()


asyncio.run(main())
```

```bash
pipenv install
pipenv run python examples/main.py
```
