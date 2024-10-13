import asyncio

from enum import StrEnum

import aiormq
import rmqaio

from rmqaio import Connection

from arrlio import settings
from arrlio.types import SecretAmqpDsn


URL = "amqp://guest:guest@localhost"
"""RabbitMQ URL."""

TIMEOUT = 15
"""RabbitMQ network operation timeout in seconds."""

PUSH_RETRY_TIMEOUTS = [5, 5, 5, 5]
"""Push operation retry timeouts(sequence) in seconds."""

PULL_RETRY_TIMEOUT = 5
"""Pull operation retry timeout in seconds."""


rmqaio.LOG_SANITIZE = settings.LOG_SANITIZE


class ReplyToMode(StrEnum):
    """RabbitMQ reply_to mode."""

    DIRECT_REPLY_TO = "direct_reply_to"
    """
    See [spec](https://www.rabbitmq.com/docs/direct-reply-to).
    """

    COMMON_QUEUE = "common_queue"
    """Common(single) results queue per `ResultBackend` Id used for all task results."""


def exc_filter(e) -> bool:
    return isinstance(
        e,
        (
            aiormq.AMQPConnectionError,
            ConnectionError,
            asyncio.TimeoutError,
            TimeoutError,
        ),
    )


def connection_factory(url: SecretAmqpDsn | list[SecretAmqpDsn]) -> Connection:
    """Connection factory.

    Args:
        url: RabbitMQ URL or list of URLs.

    Returns:
        `Connection` instance.
    """

    if not isinstance(url, list):
        url = [url]
    return Connection([u.get_secret_value() for u in url])
