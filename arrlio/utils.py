import asyncio
import json
import logging

from asyncio import Future, create_task, current_task, sleep, wait
from collections import defaultdict
from datetime import datetime
from functools import wraps
from inspect import isasyncgenfunction
from itertools import repeat
from typing import Callable, Coroutine, Iterable, cast
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


class ExtendedJSONEncoder(json.JSONEncoder):
    """Extended JSONEncoder class."""

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, (UUID, SecretStr, SecretBytes)):
            return f"{o}"
        if isinstance(o, set):
            return list(o)
        if isinstance(o, Task):
            o = o.asdict(exclude=["loads", "dumps"])
            o["func"] = f"{o['func'].__module__}.{o['func'].__name__}"
            return o
        return super().default(o)


def retry(
    msg: str | None = None,
    retry_timeouts: Iterable[Timeout] | None = None,
    exc_filter: ExceptionFilter | None = None,
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
                            if is_debug_level():
                                logger.exception(e)
                            else:
                                logger.error(e)
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
                            if is_debug_level():
                                logger.exception(e)
                            else:
                                logger.error(e)
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


def event_type_to_regex(routing_key):
    return routing_key.replace(".", "\\.").replace("*", "([^.]+)").replace("#", "([^.]+(\\.[^.]+)*)")


class Closable:
    "Base class for closable class." ""

    __slots__ = ("_closed",)

    def __init__(self):
        self._closed = Future()

    @property
    def is_closed(self) -> bool:
        return self._closed.done()

    async def close(self):
        if self.is_closed:
            return
        self._closed.set_result(None)


class AioTasksRunner(Closable):
    __slots__ = ("_tasks",)

    def __init__(self):
        super().__init__()
        self._tasks: dict[str, set[asyncio.Task]] = defaultdict(set)

    @property
    def task_keys(self):
        return self._tasks.keys()

    def cancel_all_tasks(self):
        for tasks in self._tasks.values():
            for task in tasks:
                task.cancel()

    def cancel_tasks(self, key: str):
        for task in self._tasks[key]:
            task.cancel()

    def create_task(self, key: str, coro_factory: Callable[[], Coroutine]) -> asyncio.Task:
        if self._closed.done():
            raise Exception(f"{self} closed")

        async def fn():
            task = cast(asyncio.Task, current_task())
            tasks = self._tasks[key]
            tasks.add(task)
            try:
                return await coro_factory()
            except Exception as e:
                if not isinstance(e, (StopIteration, StopAsyncIteration)):
                    logger.exception(e)
                raise e
            finally:
                tasks.discard(task)
                if not tasks:
                    del self._tasks[key]

        return create_task(fn())

    async def close(self):
        await super().close()
        self.cancel_all_tasks()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


def doc(docstring):
    def wrapper(fn):
        fn.__doc__ = docstring
        return fn

    return wrapper
