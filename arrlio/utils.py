import asyncio
import itertools
import json
import logging
from dataclasses import asdict
from datetime import datetime
from typing import Iterable
from uuid import UUID

import pydantic

from arrlio.models import Task
from arrlio.tp import ExceptionFilterT


logger = logging.getLogger("arrlio")


class ExtendedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, (UUID, pydantic.SecretStr, pydantic.SecretBytes)):
            return str(o)
        if isinstance(o, set):
            return list(o)
        if isinstance(o, Task):
            o = asdict(o)
            o["func"] = f"{o['func'].__module__}.{o['func'].__name__}"
            return o
        return super().default(o)


def retry(retry_timeouts: Iterable[int] = None, exc_filter: ExceptionFilterT = None):
    retry_timeouts = iter(retry_timeouts) if retry_timeouts else itertools.repeat(5)
    if exc_filter is None:
        exc_filter = lambda e: isinstance(e, (ConnectionError, TimeoutError, asyncio.TimeoutError))  # noqa

    def decorator(fn):
        async def wrapper(*args, **kwds):
            while True:
                try:
                    return await fn(*args, **kwds)
                except Exception as e:
                    if not exc_filter(e):
                        logger.error(e)
                        raise e
                    try:
                        t = next(retry_timeouts)
                        logger.error("%s %s - retry in %s second(s)", e.__class__, e, t)
                        await asyncio.sleep(t)
                    except StopIteration:
                        raise e

        return wrapper

    return decorator
