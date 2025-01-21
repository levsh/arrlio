import asyncio
import logging
import re

from asyncio import get_event_loop
from functools import partial
from inspect import iscoroutinefunction
from typing import Any, Callable
from uuid import UUID, uuid4

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio import settings
from arrlio.abc import AbstractEventBackend
from arrlio.models import Event
from arrlio.settings import ENV_PREFIX
from arrlio.types import AsyncCallable
from arrlio.utils import AioTasksRunner, Closable, event_type_to_regex, is_debug_level


logger = logging.getLogger("arrlio.backends.events.local")


class Config(BaseSettings):
    """
    Local `EventBackend` config.

    Attributes:
        id: `EventBackend` Id.
    """

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}LOCAL_EVENT_BACKEND_")

    id: str = Field(default_factory=lambda: f"{uuid4().hex[-4:]}")


class EventBackend(Closable, AbstractEventBackend):
    """
    Local `EventBackend`.

    Args:
        config: `EventBackend` config.
    """

    def __init__(self, config: Config):
        super().__init__()

        self.config = config

        self._internal_tasks_runner = AioTasksRunner()

        self._events: dict[UUID, Event] = {}
        self._event_cond = asyncio.Condition()
        self._event_callbacks: dict[str, tuple[AsyncCallable, list[str] | None, list]] = {}

    def __str__(self):
        return f"EventBackend[local#{self.config.id}]"

    def __repr__(self):
        return self.__str__()

    async def init(self):
        return

    async def close(self):
        await self._internal_tasks_runner.close()
        await super().close()

    async def send_event(self, event: Event):
        if is_debug_level():
            logger.debug("%s send event\n%s", self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

        self._events[event.event_id] = event

        async with self._event_cond:
            self._event_cond.notify()

        if event.ttl is not None:
            get_event_loop().call_later(event.ttl, lambda: self._events.pop(event.event_id, None))

    async def consume_events(
        self,
        callback_id: str,
        callback: Callable[[Event], Any],
        event_types: list[str] | None = None,
    ):
        self._event_callbacks[callback_id] = (
            callback,
            event_types,
            [re.compile(event_type_to_regex(event_type)) for event_type in event_types or []],
        )

        logger.info(
            "%s start consuming events[callback_id=%s, event_types=%s]",
            self,
            callback_id,
            event_types,
        )

        if "consume_events" in self._internal_tasks_runner.task_keys:
            return

        async def callback_task(event: Event):
            try:
                await callback(event)
            except Exception as e:
                logger.exception(e)

        async def fn():
            logger.debug("%s start consuming events", self)

            event_cond = self._event_cond
            event_cond_wait = event_cond.wait
            events = self._events
            events_pop = events.pop
            events_keys = events.keys
            event_callbacks = self._event_callbacks
            create_internal_task = self._internal_tasks_runner.create_task

            while not self.is_closed:
                try:
                    if not events:
                        async with event_cond:
                            await event_cond_wait()

                    event: Event = events_pop(next(iter(events_keys())))

                    if is_debug_level():
                        logger.debug("%s got event\n%s", self, event.pretty_repr(sanitize=settings.LOG_SANITIZE))

                    for callback, cb_event_types, patterns in event_callbacks.values():
                        if cb_event_types is not None and not any(pattern.match(event.type) for pattern in patterns):
                            continue
                        if iscoroutinefunction(callback):
                            create_internal_task("event_cb", partial(callback_task, event))
                        else:
                            callback(event)  # type: ignore

                except asyncio.CancelledError:
                    logger.debug("%s stop consuming events", self)
                    return
                except Exception as e:
                    logger.exception(e)

        self._internal_tasks_runner.create_task("consume_events", fn)

    async def stop_consume_events(self, callback_id: str | None = None):
        logger.info("%s stop consuming events[callback_id=%s]", self, callback_id)

        if callback_id is not None:
            self._event_callbacks.pop(callback_id, None)
        else:
            self._event_callbacks = {}

        if not self._event_callbacks:
            self._internal_tasks_runner.cancel_tasks("consume_events")
