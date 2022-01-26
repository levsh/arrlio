import asyncio
import logging

import arrlio
import tasks

from arrlio import crypto
from arrlio.serializer.json import CryptoJson


logger = logging.getLogger("arrlio")
logger.setLevel("INFO")

BACKEND = "arrlio.backends.local"


async def main():
    async def example_1():
        app = arrlio.App(arrlio.Config(backend=BACKEND))

        async with app:
            await app.consume_tasks()

            # call by task
            ar = await app.run_task(tasks.hello_world)
            logger.info(await ar.get())

            # call by task name
            ar = await app.run_task("foo")
            logger.info(await ar.get())

            # task bind example
            ar = await app.run_task(tasks.bind)
            logger.info(await ar.get())

            # exception
            try:
                ar = await app.run_task(tasks.exception)
                logger.info(await ar.get())
            except Exception as e:
                print(f"\nThis is example exception for {app.backend}:\n")
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
        app = arrlio.App(arrlio.Config(backend=BACKEND), backend_config_kwds=backend_config_kwds)

        async with app:
            await app.consume_tasks()

            ar = await app.run_task(tasks.hello_world, encrypt=True, result_encrypt=True)
            logger.info(await ar.get())

    async def example_3():
        graph = arrlio.Graph("My Graph")
        graph.add_node("A", tasks.add_one, root=True)
        graph.add_node("B", tasks.add_one)
        graph.add_node("C", tasks.add_one)
        graph.add_edge("A", "B")
        graph.add_edge("B", "C")

        app = arrlio.App(arrlio.Config(backend=BACKEND))

        async with app:
            await app.consume_tasks()

            ars = await app.run_graph(graph, args=(0,))
            logger.info("A: %i", await ars["A"].get())
            logger.info("B: %i", await ars["B"].get())
            logger.info("C: %i", await ars["C"].get())

    async def example_4():
        graph = arrlio.Graph("My Graph")
        graph.add_node("A", tasks.bash, root=True)
        graph.add_node("B", tasks.bash, args=("wc -w",))
        graph.add_edge("A", "B")

        app = arrlio.App(arrlio.Config(backend=BACKEND))

        async with app:
            await app.consume_tasks()

            ars = await app.run_graph(graph, args=('echo "Number of words in this sentence:"',))
            logger.info(await asyncio.wait_for(ars["B"].get(), timeout=2))

    await example_1()
    await example_2()
    await example_3()
    await example_4()


if __name__ == "__main__":
    asyncio.run(main())
