import asyncio
import threading

import arrlio


@arrlio.task(name="hello_world")
async def hello_world():
    return "Hello World!"


@arrlio.task
async def echo(*args, **kwds):
    return args, kwds


@arrlio.task
async def sleep(timeout):
    await asyncio.sleep(timeout)


@arrlio.task(result_return=False)
async def noresult():
    pass


@arrlio.task(ack_late=True)
async def ack_late():
    if not hasattr(ack_late.func, "counter"):
        ack_late.func.counter = 0
    ack_late.func.counter += 1
    if ack_late.func.counter == 1:
        await asyncio.sleep(5)


@arrlio.task(name="thread_name", thread=True)
async def thread_name():
    return threading.current_thread().name
