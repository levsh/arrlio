from typing import Annotated, Optional, cast
from uuid import uuid4

from annotated_types import MinLen
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio.settings import (
    BROKER,
    ENV_PREFIX,
    EVENT_BACKEND,
    EVENT_TTL,
    EXECUTOR,
    RESULT_BACKEND,
    SERIALIZER,
    TASK_ACK_LATE,
    TASK_EVENTS,
    TASK_PRIORITY,
    TASK_QUEUE,
    TASK_QUEUES,
    TASK_RESULT_RETURN,
    TASK_RESULT_TTL,
    TASK_TIMEOUT,
    TASK_TTL,
)
from arrlio.types import (
    BrokerModule,
    EventBackendModule,
    ExecutorModule,
    ModuleConfig,
    PluginModule,
    ResultBackendModule,
    SerializerModule,
    TaskPriority,
    Timeout,
    Ttl,
)


class ModuleConfigValidatorMixIn:
    @field_validator("config", mode="after", check_fields=False)
    @classmethod
    def validate_config(cls, v, info):
        if "module" not in info.data:
            return v
        config_cls = info.data["module"].Config
        if isinstance(v, config_cls):
            return v
        if isinstance(v, dict):
            return config_cls(**v)
        return config_cls(**v.model_dump())


class SerializerConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """Config for serializer module."""

    module: SerializerModule = cast(SerializerModule, SERIALIZER)
    config: ModuleConfig = Field(default_factory=BaseSettings)


class BrokerModuleConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """Config for broker module."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}BROKER_")

    module: BrokerModule = cast(BrokerModule, BROKER)
    config: ModuleConfig = Field(default_factory=BaseSettings)


class ResultBackendModuleConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """Config for result backend module."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RESULT_BACKEND_")

    module: ResultBackendModule = cast(ResultBackendModule, RESULT_BACKEND)
    config: ModuleConfig = Field(default_factory=BaseSettings)


class EventBackendModuleConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """Config for event backend module."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EVENT_BACKEND_")

    module: EventBackendModule = cast(EventBackendModule, EVENT_BACKEND)
    config: ModuleConfig = Field(default_factory=BaseSettings)


class TaskConfig(BaseSettings):
    """Config for task."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}TASK_")

    queue: str = Field(default_factory=lambda: TASK_QUEUE)
    priority: TaskPriority = Field(default_factory=lambda: TASK_PRIORITY)
    timeout: Optional[Timeout] = Field(default_factory=lambda: TASK_TIMEOUT)
    ttl: Optional[Ttl] = Field(default_factory=lambda: TASK_TTL)
    ack_late: bool = Field(default_factory=lambda: TASK_ACK_LATE)
    result_return: bool = Field(default_factory=lambda: TASK_RESULT_RETURN)
    result_ttl: Optional[Ttl] = Field(default_factory=lambda: TASK_RESULT_TTL)
    events: set[str] | bool = Field(default_factory=lambda: TASK_EVENTS)


class EventConfig(BaseSettings):
    """Config for event."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EVENT_")

    ttl: Optional[Ttl] = Field(default_factory=lambda: EVENT_TTL)


class PluginModuleConfig(ModuleConfigValidatorMixIn, BaseSettings):
    """Config for plugin module."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}PLUGIN_")

    module: PluginModule
    config: ModuleConfig = Field(default_factory=BaseSettings)


class ExecutorModuleConfig(ModuleConfigValidatorMixIn, BaseSettings):
    """Config for executor module."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EXECUTOR_")

    module: ExecutorModule = cast(ExecutorModule, EXECUTOR)
    config: ModuleConfig = Field(default_factory=BaseSettings)


class Config(BaseSettings):
    """Arrlio application config."""

    model_config = SettingsConfigDict(env_prefix=ENV_PREFIX)

    app_id: Annotated[str, MinLen(4)] = Field(default_factory=lambda: f"{uuid4().hex[-6:]}")
    broker: BrokerModuleConfig = Field(default_factory=BrokerModuleConfig)
    result_backend: ResultBackendModuleConfig = Field(default_factory=ResultBackendModuleConfig)
    event_backend: EventBackendModuleConfig = Field(default_factory=EventBackendModuleConfig)
    task: TaskConfig = Field(default_factory=TaskConfig)
    event: EventConfig = Field(default_factory=EventConfig)
    task_queues: set[str] = Field(default_factory=lambda: TASK_QUEUES)
    plugins: list[PluginModuleConfig] = Field(default_factory=list)
    executor: ExecutorModuleConfig = Field(default_factory=ExecutorModuleConfig)
