import asyncio
import logging
import threading

import arrlio


logger = logging.getLogger("arrlio.tests")


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


@arrlio.task(name="sync_task")
def sync_task():
    return "Hello from sync_task!"


@arrlio.task
def add_one(x: str):
    return int(x) + 1


@arrlio.task
def logger_info(data, *, meta: dict = None):
    assert meta["source_node"]
    logger.info(data)


@arrlio.task
def compare(a, b) -> bool:
    res = a == b
    return arrlio.TaskResult(res=res, routes={True: "true", False: "false"}[res])
