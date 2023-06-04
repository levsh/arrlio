import os
from typing import Any, List, Optional, Set, Union
from uuid import uuid4

from pydantic import BaseModel, BaseSettings, Field, PositiveInt, constr, validator
from pydantic.utils import sequence_like

from arrlio.types import BackendModule, ExecutorModule, PluginModule, Priority, Timeout

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
TASK_EVENTS = False
TASK_EVENT_TTL = 300

EVENT_TTL = 300

TASK_QUEUES = [TASK_QUEUE]


class BaseConfig(BaseSettings):
    class Config:
        validate_assignment = True
        smart_union = True
        case_sensitive = False

        @classmethod
        def prepare_field(cls, field):
            # pylint: disable=no-member
            field_info_from_config = cls.get_field_info(field.name)
            env = field_info_from_config.get("env") or field.field_info.extra.get("env")
            if env is None and sequence_like(cls.env_prefix):
                env_names = {env_prefix + field.name for env_prefix in cls.env_prefix}
                if not cls.case_sensitive:
                    env_names = env_names.__class__(n.lower() for n in env_names)
                field.field_info.extra["env_names"] = env_names
            else:
                super().prepare_field(field)


class ConfigValidatorMixIn(BaseModel):
    @validator("config", check_fields=False, allow_reuse=True, always=True)
    def validate_config(cls, v, values):  # pylint: disable=no-self-argument
        if "module" not in values:
            return v
        config_cls = values["module"].Config
        if isinstance(v, config_cls):
            return v
        v = v or {}
        if isinstance(v, dict):
            return config_cls(**v)
        raise TypeError("Invalid config")


class BackendConfig(ConfigValidatorMixIn, BaseConfig):
    module: BackendModule = "arrlio.backends.local"
    config: Any = Field(default_factory=dict)

    class Config:
        env_prefix = f"{ENV_PREFIX}BACKEND_"


class TaskConfig(BaseConfig):
    bind: bool = Field(default_factory=lambda: TASK_BIND)
    queue: str = Field(default_factory=lambda: TASK_QUEUE)
    priority: Priority = Field(default_factory=lambda: TASK_PRIORITY)
    timeout: Optional[Timeout] = Field(default_factory=lambda: TASK_TIMEOUT)
    ttl: Optional[PositiveInt] = Field(default_factory=lambda: TASK_TTL)
    ack_late: bool = Field(default_factory=lambda: TASK_ACK_LATE)
    result_return: bool = Field(default_factory=lambda: TASK_RESULT_RETURN)
    result_ttl: Optional[PositiveInt] = Field(default_factory=lambda: TASK_RESULT_TTL)
    events: Union[Set[str], bool] = Field(default_factory=lambda: TASK_EVENTS)
    event_ttl: Optional[PositiveInt] = Field(default_factory=lambda: TASK_EVENT_TTL)

    class Config:
        env_prefix = f"{ENV_PREFIX}TASK_"


class EventConfig(BaseConfig):
    ttl: Optional[PositiveInt] = Field(default_factory=lambda: EVENT_TTL)

    class Config:
        env_prefix = f"{ENV_PREFIX}EVENT_"


class PluginConfig(ConfigValidatorMixIn, BaseConfig):
    module: PluginModule
    config: Any = Field(default_factory=dict)

    class Config:
        env_prefix = f"{ENV_PREFIX}EXECUTOR_"


class ExecutorConfig(ConfigValidatorMixIn, BaseConfig):
    module: ExecutorModule = "arrlio.executor"
    config: Any = Field(default_factory=dict)

    class Config:
        env_prefix = f"{ENV_PREFIX}EXECUTOR_"


class Config(BaseConfig):
    app_id: constr(min_length=1) = Field(default_factory=lambda: f"{uuid4()}")
    backend: BackendConfig = Field(default_factory=BackendConfig)
    task: TaskConfig = Field(default_factory=TaskConfig)
    event: EventConfig = Field(default_factory=EventConfig)
    task_queues: Set[str] = Field(default_factory=lambda: TASK_QUEUES)
    plugins: List[PluginConfig] = Field(default_factory=list)
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)

    class Config:
        env_prefix = ENV_PREFIX
