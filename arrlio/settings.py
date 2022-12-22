import os
from typing import Any, List, Optional, Set, Union
from uuid import uuid4

from pydantic import BaseModel, BaseSettings, Field, PositiveInt, constr, validator

from arrlio.tp import BackendT, ExecutorT, PluginT, PriorityT, TimeoutT

ENV_PREFIX = os.environ.get("ARRLIO_ENV_PREFIX", "ARRLIO_")

BACKEND = "arrlio.backends.local"

TASK_BIND = False
TASK_QUEUE = "arrlio.tasks"
TASK_PRIORITY = 1
TASK_TIMEOUT = 300
TASK_TTL = 300
TASK_ACK_LATE = True
TASK_RESULT_TTL = 300
TASK_RESULT_RETURN = True
TASK_EVENTS = False
TASK_EVENT_TTL = 300

MESSAGE_EXCHANGE = "arrlio.messages"
MESSAGE_PRIORITY = 1
MESSAGE_TTL = 300
MESSAGE_ACK_LATE = True

EVENT_TTL = 300

TASK_QUEUES = [TASK_QUEUE]
MESSAGE_QUEUES = [MESSAGE_EXCHANGE]


class ConfigValidatorMixIn(BaseModel):
    @validator("config", check_fields=False, allow_reuse=True)
    def validate_config(cls, v, values):  # pylint: disable=no-self-argument
        config_cls = values["module"].Config
        if isinstance(v, config_cls):
            return v
        v = v or {}
        if isinstance(v, dict):
            return config_cls(**v)
        raise TypeError("Invalid config")


class BackendConfig(BaseSettings, ConfigValidatorMixIn):
    module: BackendT = "arrlio.backends.local"
    config: Any = Field(default_factory=dict)

    class Config:
        validate_assignment = True
        env_prefix = f"{ENV_PREFIX}BACKEND_"


class TaskConfig(BaseSettings):
    bind: bool = Field(default_factory=lambda: TASK_BIND)
    queue: str = Field(default_factory=lambda: TASK_QUEUE)
    priority: PriorityT = Field(default_factory=lambda: TASK_PRIORITY)
    timeout: Optional[TimeoutT] = Field(default_factory=lambda: TASK_TIMEOUT)
    ttl: Optional[PositiveInt] = Field(default_factory=lambda: TASK_TTL)
    ack_late: Optional[bool] = Field(default_factory=lambda: TASK_ACK_LATE)
    result_return: Optional[bool] = Field(default_factory=lambda: TASK_RESULT_RETURN)
    result_ttl: Optional[PositiveInt] = Field(default_factory=lambda: TASK_RESULT_TTL)
    events: Union[Set[str], bool] = Field(default_factory=lambda: TASK_EVENTS)
    event_ttl: Optional[PositiveInt] = Field(default_factory=lambda: TASK_EVENT_TTL)

    class Config:
        validate_assignment = True
        env_prefix = f"{ENV_PREFIX}TASK_"


class MessageConfig(BaseSettings):
    exchange: str = Field(default_factory=lambda: MESSAGE_EXCHANGE)
    priority: PriorityT = Field(default_factory=lambda: MESSAGE_PRIORITY)
    ttl: Optional[PositiveInt] = Field(default_factory=lambda: MESSAGE_TTL)
    ack_late: Optional[bool] = Field(default_factory=lambda: MESSAGE_ACK_LATE)

    class Config:
        validate_assignment = True
        env_prefix = f"{ENV_PREFIX}MESSAGE_"


class EventConfig(BaseSettings):
    ttl: Optional[PositiveInt] = Field(default_factory=lambda: EVENT_TTL)


class PluginConfig(BaseSettings, ConfigValidatorMixIn):
    module: PluginT = "arrlio.plugins.events"
    config: Any = Field(default_factory=dict)

    class Config:
        validate_assignment = True
        env_prefix = f"{ENV_PREFIX}EXECUTOR_"


class ExecutorConfig(BaseSettings, ConfigValidatorMixIn):
    module: ExecutorT = "arrlio.executor"
    config: Any = Field(default_factory=dict)

    class Config:
        validate_assignment = True
        env_prefix = f"{ENV_PREFIX}EXECUTOR_"


class Config(BaseSettings):
    app_id: constr(min_length=1) = Field(default_factory=lambda: f"{uuid4()}")
    backend: BackendConfig = Field(default_factory=BackendConfig)
    task: TaskConfig = Field(default_factory=TaskConfig)
    message: MessageConfig = Field(default_factory=MessageConfig)
    event: EventConfig = Field(default_factory=EventConfig)
    task_queues: Set[str] = Field(default_factory=lambda: TASK_QUEUES)
    message_queues: Set[str] = Field(default_factory=lambda: MESSAGE_QUEUES)
    plugins: List[PluginConfig] = Field(default_factory=lambda: [PluginConfig()])
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)

    class Config:
        validate_assignment = True
        env_prefix = ENV_PREFIX
