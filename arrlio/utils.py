import asyncio
import contextlib
import itertools
import json
import logging
from datetime import datetime
from types import MethodType
from typing import Iterable
from uuid import UUID

import pydantic

from arrlio.tp import AsyncCallableT, ExceptionFilterT


logger = logging.getLogger("arrlio")


class ExtendedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, (UUID, pydantic.SecretStr, pydantic.SecretBytes)):
            return str(o)
        return super().default(o)


class AsyncRetry:
    def __init__(self, retry_timeouts: Iterable[int] = None, exc_filter: ExceptionFilterT = None):
        self.retry_timeouts = retry_timeouts and iter(retry_timeouts) or itertools.repeat(1)
        if exc_filter is not None:
            self.exc_filter = exc_filter

    def exc_filter(self, e: Exception) -> bool:
        return isinstance(e, (ConnectionError, TimeoutError, asyncio.TimeoutError))

    async def __call__(self, fn: AsyncCallableT, *args, **kwds):
        while True:
            try:
                return await fn(*args, **kwds)
            except Exception as e:
                if not self.exc_filter(e):
                    raise e
                try:
                    retry_timeout = next(self.retry_timeouts)
                    logger.error("%s %s %s, retry in %i seconds", fn, e.__class__.__name__, e, retry_timeout)
                    await asyncio.sleep(retry_timeout)
                except StopIteration:
                    raise e


class TasksMixIn:
    def __init__(self):
        self.__tasks = set()
        self.__closed: bool = False

    def close(self):
        self.__closed = True

    def task(self, method: MethodType):
        async def wrapper(self, *args, **kwds):
            if self.__closed:
                raise Exception(f"Trying to call {method} on closed object {self}")
            task = asyncio.create_task(method(self, *args, **kwds))
            self.__tasks.add(task)
            try:
                return task
            finally:
                self.__tasks.discard(task)

        return wrapper

    async def cancel_all_tasks(self):
        for task in self.__tasks:
            task.cancel()
        await asyncio.gather(*self.__tasks, return_exceptions=True)


class Lock:
    def __init__(self):
        self._cnt = 0
        self._lock = asyncio.Lock()
        self._unlock_ev = asyncio.Event()
        self._unlock_ev.set()
        self._zero_cnt_ev = asyncio.Event()
        self._zero_cnt_ev.set()

    async def aquire(self, full: bool = False):
        if full:
            await self._lock.acquire()
            self._unlock_ev.clear()
            await self._zero_cnt_ev.wait()
        else:
            await self._unlock_ev.wait()
        self._cnt += 1
        self._zero_cnt_ev.clear()

    def release(self, full: bool = False):
        self._cnt = max(0, self._cnt - 1)
        if full:
            self._lock.release()
            self._unlock_ev.set()
        if self._cnt == 0:
            self._zero_cnt_ev.set()

    async def __aenter__(self):
        await self.aquire()

    async def __aexit__(self, exc_type, exc, tb):
        self.release()

    @contextlib.asynccontextmanager
    async def full_aquire(self):
        try:
            await self.aquire(full=True)
            yield
        finally:
            self.release(full=True)
