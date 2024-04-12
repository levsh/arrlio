import asyncio
import json
import logging
from asyncio import create_task, sleep, wait
from datetime import datetime
from functools import wraps
from inspect import isasyncgenfunction
from itertools import repeat
from typing import Coroutine, Iterable
from uuid import UUID

from pydantic import SecretBytes, SecretStr

from arrlio.models import Task
from arrlio.types import ExceptionFilter, Timeout

logger = logging.getLogger("arrlio.utils")


isEnabledFor = logger.isEnabledFor
DEBUG = logging.DEBUG
INFO = logging.INFO


def is_debug_level():
    return isEnabledFor(DEBUG)


def is_info_level():
    return isEnabledFor(INFO)


async def wait_for(coro: Coroutine, timeout: Timeout):
    """Wait for coroutine to complete.

    Args:
        coro: Coroutine for wait.
        timeout: wait timeout.

    Raises:
        asyncio.TimeoutError: On timeout occurs
    """

    done, pending = await wait({create_task(coro)}, timeout=timeout)
    if pending:
        for pending_coro in pending:
            pending_coro.cancel()
        raise asyncio.TimeoutError
    return next(iter(done)).result()


class ExtendedJSONEncoder(json.JSONEncoder):
    """Extended JSONEncoder class."""

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, (UUID, SecretStr, SecretBytes)):
            return str(o)
        if isinstance(o, set):
            return list(o)
        if isinstance(o, Task):
            o = o.dict(exclude=["loads", "dumps"])
            o["func"] = f"{o['func'].__module__}.{o['func'].__name__}"
            return o
        return super().default(o)


def retry(
    msg: str = None,
    retry_timeouts: Iterable[Timeout] = None,
    exc_filter: ExceptionFilter = None,
    on_error=None,
    reraise: bool = True,
):
    """Retry decorator.

    Args:
        msg: Message to log on retry.
        retry_timeouts: Retry timeout as iterable, for example: `[1, 2, 3]` or `itertools.repeat(5)`.
        exc_filter: callable to determine whether or not to repeat.
        reraise: Reraise exception or not.
    """

    if retry_timeouts is None:
        retry_timeouts = repeat(5)

    if exc_filter is None:

        def exc_filter(exc):
            return isinstance(
                exc,
                (
                    ConnectionError,
                    TimeoutError,
                    asyncio.TimeoutError,
                ),
            )

    def decorator(fn):
        if isasyncgenfunction(fn):

            @wraps(fn)
            async def wrapper(*args, **kwds):
                timeouts = iter(retry_timeouts)
                attempt = 0
                while True:
                    try:
                        async for res in fn(*args, **kwds):
                            yield res
                        return
                    except Exception as e:
                        if not exc_filter(e):
                            if reraise:
                                raise e
                            logger.exception(e)
                            return
                        try:
                            t = next(timeouts)
                            attempt += 1
                            if is_debug_level():
                                logger.exception(
                                    "%s (%s %s) retry(%s) in %s second(s)",
                                    msg or fn,
                                    e.__class__,
                                    e,
                                    attempt,
                                    t,
                                )
                            else:
                                logger.warning(
                                    "%s (%s %s) retry(%s) in %s second(s)",
                                    msg or fn,
                                    e.__class__,
                                    e,
                                    attempt,
                                    t,
                                )
                            if on_error:
                                await on_error(e)
                            await sleep(t)
                        except StopIteration:
                            raise e

        else:

            @wraps(fn)
            async def wrapper(*args, **kwds):
                timeouts = iter(retry_timeouts)
                attempt = 0
                while True:
                    try:
                        return await fn(*args, **kwds)
                    except Exception as e:
                        if not exc_filter(e):
                            if reraise:
                                raise e
                            logger.exception(e)
                            return
                        try:
                            t = next(timeouts)
                            attempt += 1
                            if is_debug_level():
                                logger.exception(
                                    "%s (%s %s) retry(%s) in %s second(s)",
                                    msg or fn,
                                    e.__class__,
                                    e,
                                    attempt,
                                    t,
                                )
                            else:
                                logger.warning(
                                    "%s (%s %s) retry(%s) in %s second(s)",
                                    msg or fn,
                                    e.__class__,
                                    e,
                                    attempt,
                                    t,
                                )
                            if on_error:
                                await on_error(e)
                            await sleep(t)
                        except StopIteration:
                            raise e

        return wrapper

    return decorator


class LoopIter:
    """Infinity iterator class."""

    __slots__ = ("_data", "_i", "_j", "_iter")

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
