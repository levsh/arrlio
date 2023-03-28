import asyncio
import logging
import threading
from dataclasses import asdict, dataclass

import arrlio
from arrlio import TaskInstance

logger = logging.getLogger("arrlio.tests")


@arrlio.task(name="hello_world")
async def hello_world():
    return "Hello World!"


@arrlio.task(name="bind_true", bind=True)
async def bind_true(task_instance: TaskInstance):
    assert isinstance(task_instance, TaskInstance)


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
def zero_division():
    return 1 / 0


@arrlio.task
def add_one(x: str):
    return int(x) + 1


@arrlio.task
def logger_info(data, *, meta: dict = None):
    assert meta["graph:source_node"]
    assert meta["graph:app_id"]
    assert meta["graph:id"]
    assert meta["graph:name"]
    logger.info(data)


@arrlio.task
def compare(a, b) -> bool:
    res = a == b
    return arrlio.TaskResult(res=res, routes={True: "true", False: "false"}[res])


@dataclass
class LoadsDumps:
    x: int = None


@arrlio.task(loads=lambda x: ((LoadsDumps(**x),), {}), dumps=lambda x: asdict(x))
def loads_dumps(x: LoadsDumps):
    assert isinstance(x, LoadsDumps)
    return x


@arrlio.task
async def xrange(n, sleep=None):
    sleep = sleep or 0
    for x in range(n):
        yield x
        await asyncio.sleep(sleep)


@arrlio.task
def grange(n):
    for x in range(n):
        yield x
