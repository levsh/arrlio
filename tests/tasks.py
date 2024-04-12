import asyncio
import logging
import threading
import time
from dataclasses import asdict, dataclass
from typing import List

import arrlio
from arrlio import TaskInstance

logger = logging.getLogger("arrlio.tests")


@arrlio.task(name="hello_world")
async def hello_world():
    return "Hello World!"


@arrlio.task(name="meta_true")
async def meta_true(*, meta: dict = None):
    assert meta
    assert isinstance(meta, dict)


@arrlio.task
async def echo(*args, **kwds):
    return args, kwds


@arrlio.task
async def sleep(timeout: float):
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
async def async_xrange(n: int, sleep=None):
    sleep = sleep or 0
    for x in range(n):
        yield x
        await asyncio.sleep(sleep)


@arrlio.task
def xrange(n: int, sleep=None):
    sleep = sleep or 0
    for x in range(n):
        yield x
        time.sleep(sleep)


@arrlio.task
def test_args_kwds_validation(i: int, f: float, s: str, lst: List[int] = None):
    assert isinstance(i, int)
    assert isinstance(f, float)
    assert isinstance(s, str)
    if lst is not None:
        for item in lst:
            assert isinstance(item, int)
