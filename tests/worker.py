import asyncio

import arrlio
import tasks

BACKEND = "arrlio.backends.rabbitmq"


async def main():
    app = arrlio.App(arrlio.Config(backend=BACKEND))

    async with app:
        await app.consume_tasks()
        await asyncio.sleep(1000)


if __name__ == "__main__":
    asyncio.run(main())
