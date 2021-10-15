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
        producer = arrlio.TaskProducer(arrlio.TaskProducerConfig(backend=BACKEND))
        consumer = arrlio.TaskConsumer(arrlio.TaskConsumerConfig(backend=BACKEND))

        async with producer, consumer:

            await consumer.consume()

            # call by task
            ar = await producer.send(tasks.hello_world)
            logger.info(await ar.get())

            # call by task name
            ar = await producer.send("foo")
            logger.info(await ar.get())

            # task bind example
            ar = await producer.send(tasks.bind)
            logger.info(await ar.get())

            # exception
            try:
                ar = await producer.send(tasks.exception)
                logger.info(await ar.get())
            except Exception as e:
                print(f"\nThis is example exception for {producer.backend}:\n")
                logger.exception(e)
                print()

    async def example_2():
        pri_key = crypto.generate_private_key()
        pub_key = pri_key.public_key()

        def serializer():
            return CryptoJson(
                encryptor=lambda x: crypto.a_encrypt(x, pub_key),
                decryptor=lambda x: crypto.a_decrypt(x, pri_key),
            )

        backend_config_kwds = {"serializer": serializer}
        consumer = arrlio.TaskConsumer(
            arrlio.TaskConsumerConfig(backend=BACKEND),
            backend_config_kwds=backend_config_kwds,
        )
        producer = arrlio.TaskProducer(
            arrlio.TaskProducerConfig(backend=BACKEND),
            backend_config_kwds=backend_config_kwds,
        )

        async with producer, consumer:

            await consumer.consume()

            ar = await producer.send(tasks.hello_world, encrypt=True, result_encrypt=True)
            logger.info(await ar.get())

    await example_1()
    await example_2()


if __name__ == "__main__":
    asyncio.run(main())
