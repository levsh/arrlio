import asyncio
import itertools
import json
import logging
from asyncio import wait
from datetime import datetime
from functools import wraps
from typing import Awaitable, Iterable
from uuid import UUID

from pydantic import SecretBytes, SecretStr

from arrlio.models import Task
from arrlio.tp import ExceptionFilterT

logger = logging.getLogger("arrlio.utils")


async def wait_for(aw: Awaitable, timeout):
    done, pending = await wait([aw], timeout=timeout)
    if pending:
        for pending_coro in pending:
            pending_coro.cancel()
        raise asyncio.TimeoutError
    return next(iter(done)).result()


class ExtendedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, (UUID, SecretStr, SecretBytes)):
            return str(o)
        if isinstance(o, set):
            return list(o)
        if isinstance(o, Task):
            o = o.dict()
            o["func"] = f"{o['func'].__module__}.{o['func'].__name__}"
            return o
        return super().default(o)


def retry(retry_timeouts: Iterable[int] = None, exc_filter: ExceptionFilterT = None):
    if retry_timeouts is None:
        retry_timeouts = itertools.repeat(5)

    if exc_filter is None:

        def exc_filter(exc):
            return isinstance(exc, (ConnectionError, TimeoutError, asyncio.TimeoutError))

    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwds):
            timeouts = iter(retry_timeouts)
            attempt = 0
            while True:
                try:
                    return await fn(*args, **kwds)
                except Exception as e:
                    if not exc_filter(e):
                        raise e
                    try:
                        t = next(timeouts)
                        attempt += 1
                        logger.error("%s %s %s - retry(%s) in %s second(s)", fn, e.__class__, e, attempt, t)
                        await asyncio.sleep(t)
                    except StopIteration:
                        raise e

        return wrapper

    return decorator


class InfIter:
    def __init__(self, data: list):
        self._data = data
        self._i = -1
        self._j = 0
        self._iter = iter(data)

    def __next__(self):
        if self._j == len(self._data):
            self._j = 0
            raise StopIteration
        self._i = (self._i + 1) % len(self._data)
        self._j += 1
        return self._data[self._i]

    def reset(self):
        self._j = 1
