import asyncio
import logging

import arrlio
import tasks


logger = logging.getLogger("arrlio")
# logger.setLevel(logging.DEBUG)


async def main():
    app = arrlio.App(
        arrlio.Config(
            broker={"module": "arrlio.backends.brokers.rabbitmq"},
            result_backend={"module": "arrlio.backends.result_backends.rabbitmq"},
            event_backend={"module": "arrlio.backends.event_backends.rabbitmq"},
        ),
    )

    async with app:
        await app.consume_tasks()
        await asyncio.sleep(1000)


if __name__ == "__main__":
    asyncio.run(main())
