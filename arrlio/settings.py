import os
from typing import List, Optional

from pydantic import BaseSettings, Field

from arrlio.tp import BackendT, PositiveIntT, PriorityT, TimeoutT


ENV_PREFIX = os.environ.get("ARRLIO_ENV_PREFIX", "ARRLIO_")


BACKEND = "arrlio.backends.local"

TASK_BIND = False
TASK_QUEUE = "arrlio.tasks"
TASK_PRIORITY = 1
TASK_TIMEOUT = 300
TASK_TTL = 300
TASK_ACK_LATE = False

TASK_RESULT_TTL = 300
TASK_RESULT_RETURN = True

EVENTS = False
EVENT_TTL = 300

MESSAGE_EXCHANGE = "arrlio.messages"
MESSAGE_PRIORITY = 1
MESSAGE_TTL = 300
MESSAGE_ACK_LATE = False

TASK_QUEUES = [TASK_QUEUE]
MESSAGE_QUEUES = [MESSAGE_EXCHANGE]
POOL_SIZE = 100


class TaskConfig(BaseSettings):
    queue: str = Field(default_factory=lambda: TASK_QUEUE)
    priority: PriorityT = Field(default_factory=lambda: TASK_PRIORITY)
    timeout: Optional[TimeoutT] = Field(default_factory=lambda: TASK_TIMEOUT)
    ttl: Optional[PositiveIntT] = Field(default_factory=lambda: TASK_TTL)
    ack_late: Optional[bool] = Field(default_factory=lambda: TASK_ACK_LATE)
    result_return: Optional[bool] = Field(default_factory=lambda: TASK_RESULT_RETURN)
    result_ttl: Optional[PositiveIntT] = Field(default_factory=lambda: TASK_RESULT_TTL)
    events: Optional[bool] = Field(default_factory=lambda: EVENTS)
    event_ttl: Optional[PositiveIntT] = Field(default_factory=lambda: EVENT_TTL)

    class Config:
        validate_assignment = True
        env_prefix = f"{ENV_PREFIX}TASK_"


class MessageConfig(BaseSettings):
    exchange: str = Field(default_factory=lambda: MESSAGE_EXCHANGE)
    priority: PriorityT = Field(default_factory=lambda: MESSAGE_PRIORITY)
    ttl: Optional[PositiveIntT] = Field(default_factory=lambda: MESSAGE_TTL)
    ack_late: Optional[bool] = Field(default_factory=lambda: MESSAGE_ACK_LATE)

    class Config:
        validate_assignment = True
        env_prefix = f"{ENV_PREFIX}MESSAGE_"


class Config(BaseSettings):
    backend: BackendT = Field(default_factory=lambda: BACKEND, env=f"{ENV_PREFIX}BACKEND")
    task: TaskConfig = Field(default_factory=TaskConfig)
    message: MessageConfig = Field(default_factory=MessageConfig)
    task_queues: List[str] = Field(default_factory=lambda: TASK_QUEUES)
    message_queues: List[str] = Field(default_factory=lambda: MESSAGE_QUEUES)
    pool_size: PositiveIntT = Field(default_factory=lambda: POOL_SIZE)

    class Config:
        validate_assignment = True
        env_prefix = ENV_PREFIX
