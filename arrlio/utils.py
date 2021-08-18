import asyncio
import itertools
import json
import logging
from datetime import datetime
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
