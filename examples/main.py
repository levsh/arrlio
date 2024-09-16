import asyncio
import logging

import arrlio
import tasks


logger = logging.getLogger("arrlio")
logger.setLevel("INFO")


async def main():
    async def example_1():
        app = arrlio.App(arrlio.Config())

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
                print(f"\nThis is example exception for {app.result_backend}:\n")
                logger.exception(e)
                print()

            # generator
            results = []
            ar = await app.send_task(tasks.xrange, args=(3,))
            async for result in ar:
                results.append(result)
            logger.info(results)

    async def example_2():
        graph = arrlio.Graph("My Graph")
        graph.add_node("A", tasks.add_one, root=True)
        graph.add_node("B", tasks.add_one)
        graph.add_node("C", tasks.add_one)
        graph.add_edge("A", "B")
        graph.add_edge("B", "C")

        app = arrlio.App(
            arrlio.Config(
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

    async def example_3():
        graph = arrlio.Graph("My Graph")
        graph.add_node("A", tasks.bash, root=True)
        graph.add_node("B", tasks.bash, args=("wc -w",))
        graph.add_edge("A", "B")

        app = arrlio.App(
            arrlio.Config(
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

    await example_1()
    await example_2()
    await example_3()


if __name__ == "__main__":
    asyncio.run(main())
