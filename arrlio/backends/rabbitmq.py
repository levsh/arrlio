import asyncio

from enum import StrEnum

import aiormq
import rmqaio

from rmqaio import Connection

from arrlio import settings
from arrlio.types import SecretAmqpDsn


URL = "amqp://guest:guest@localhost"
TIMEOUT = 15

PUSH_RETRY_TIMEOUTS = [5, 5, 5, 5]  # pylint: disable=invalid-name
PULL_RETRY_TIMEOUT = 5  # pylint: disable=invalid-name

rmqaio.LOG_SANITIZE = settings.LOG_SANITIZE


class ReplyToMode(StrEnum):
    """RabbitMQ reply_to mode."""

    DIRECT_REPLY_TO = "direct_reply_to"
    """
    Allow to avoid declaring a response queue per request. See [spec](https://www.rabbitmq.com/docs/direct-reply-to).
    """

    COMMON_QUEUE = "common_queue"
    """Common(single) results queue per backend id used for all task results."""

    DISABLE = "disable"


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
    """Connection factory."""

    if not isinstance(url, list):
        url = [url]
    return Connection([u.get_secret_value() for u in url])
