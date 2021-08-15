import asyncio
import logging

import arrlio
import tasks

from arrlio import crypto
from arrlio.serializer.json import CryptoJson


logger = logging.getLogger("arrlio")
logger.setLevel("INFO")

BACKEND = "arrlio.backend.local"


async def main():
    async def example_1():
        client = arrlio.Client(arrlio.ClientConfig(backend=BACKEND))
        executor = arrlio.Executor(arrlio.ExecutorConfig(backend=BACKEND))

        try:
            await executor.run()

            # call by task
            ar = await client.call(tasks.hello_world)
            logger.info(await ar.get())

            # call by task name
            ar = await client.call("foo")
            logger.info(await ar.get())

            # task bind example
            ar = await client.call(tasks.bind)
            logger.info(await ar.get())

            # exception
            try:
                ar = await client.call(tasks.exception)
                logger.info(await ar.get())
            except Exception as e:
                print(f"\nThis is example exception for {client.backend}:\n")
                logger.exception(e)
                print()

        finally:
            await executor.stop()
            await client.close()

    async def example_2():
        pri_key = crypto.generate_private_key()
        pub_key = pri_key.public_key()

        def serializer():
            return CryptoJson(
                encryptor=lambda x: crypto.a_encrypt(x, pub_key),
                decryptor=lambda x: crypto.a_decrypt(x, pri_key),
            )

        backend_config_kwds = {"serializer": serializer}
        executor = arrlio.Executor(
            arrlio.ExecutorConfig(backend=BACKEND),
            backend_config_kwds=backend_config_kwds,
        )
        client = arrlio.Client(
            arrlio.ClientConfig(backend=BACKEND),
            backend_config_kwds=backend_config_kwds,
        )

        try:
            await executor.run()

            ar = await client.call(tasks.hello_world, encrypt=True, result_encrypt=True)
            logger.info(await ar.get())

        finally:
            await executor.stop()
            await client.close()

    await example_1()
    await example_2()


if __name__ == "__main__":
    asyncio.run(main())
