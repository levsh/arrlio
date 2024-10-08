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
    TTL,
    BrokerModule,
    EventBackendModule,
    ExecutorModule,
    ModuleConfig,
    PluginModule,
    ResultBackendModule,
    SerializerModule,
    TaskPriority,
    Timeout,
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
    """Serializer module as module path or `SerializerModule` instance or `dict` instance."""
    config: ModuleConfig = Field(default_factory=BaseSettings)
    """Module config as `dict` or `module.Config` instances."""


class BrokerModuleConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """Config for broker module."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}BROKER_")

    module: BrokerModule = cast(BrokerModule, BROKER)
    """Broker module as module path or `BrokerModule` instance or `dict` instance."""
    config: ModuleConfig = Field(default_factory=BaseSettings)
    """Module config as `dict` or `module.Config` instances."""


class ResultBackendModuleConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """Config for result backend module."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}RESULT_BACKEND_")

    module: ResultBackendModule = cast(ResultBackendModule, RESULT_BACKEND)
    """Result backend module as module path or `ResultBackendModule` instance or `dict` instance."""
    config: ModuleConfig = Field(default_factory=BaseSettings)
    """Module config as `dict` or `module.Config` instances."""


class EventBackendModuleConfig(BaseSettings, ModuleConfigValidatorMixIn):
    """Config for event backend module."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EVENT_BACKEND_")

    module: EventBackendModule = cast(EventBackendModule, EVENT_BACKEND)
    """Event backend module as module path or `EventBackendModule` instance or `dict` instance."""
    config: ModuleConfig = Field(default_factory=BaseSettings)
    """Module config as `dict` or `module.Config` instances."""


class TaskConfig(BaseSettings):
    """Config for task."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}TASK_")

    queue: str = Field(default_factory=lambda: TASK_QUEUE)
    """Default queue name for task."""
    priority: TaskPriority = Field(default_factory=lambda: TASK_PRIORITY)
    """Default task priority."""
    timeout: Optional[Timeout] = Field(default_factory=lambda: TASK_TIMEOUT)
    """Default task excute timeout in seconds."""
    ttl: Optional[TTL] = Field(default_factory=lambda: TASK_TTL)
    """Default task TTL in seconds."""
    ack_late: bool = Field(default_factory=lambda: TASK_ACK_LATE)
    """Task ack late options."""
    result_return: bool = Field(default_factory=lambda: TASK_RESULT_RETURN)
    """Is it necessary to return the task result by default."""
    result_ttl: Optional[TTL] = Field(default_factory=lambda: TASK_RESULT_TTL)
    """Result TTL in seconds."""
    events: set[str] | bool = Field(default_factory=lambda: TASK_EVENTS)
    """Will events be generated for task. This option requires `arrlio.plugins.events` plugin."""


class EventConfig(BaseSettings):
    """Config for event."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EVENT_")

    ttl: Optional[TTL] = Field(default_factory=lambda: EVENT_TTL)
    """Event TTL in seconds."""


class PluginModuleConfig(ModuleConfigValidatorMixIn, BaseSettings):
    """Config for plugin module."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}PLUGIN_")

    module: PluginModule
    """Plugin module as module path or `PluginModule` instance or `dict` instance."""
    config: ModuleConfig = Field(default_factory=BaseSettings)
    """Module config as `dict` or `module.Config` instances."""


class ExecutorModuleConfig(ModuleConfigValidatorMixIn, BaseSettings):
    """Config for executor module."""

    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EXECUTOR_")

    module: ExecutorModule = cast(ExecutorModule, EXECUTOR)
    """Executor module as module path or `ExecutorModule` instance or `dict` instance."""
    config: ModuleConfig = Field(default_factory=BaseSettings)
    """Module config as `dict` or `module.Config` instances."""


class Config(BaseSettings):
    """Arrlio application main config."""

    model_config = SettingsConfigDict(env_prefix=ENV_PREFIX)

    app_id: Annotated[str, MinLen(4)] = Field(default_factory=lambda: f"{uuid4().hex[-6:]}")
    """Arrlio application Id."""
    broker: BrokerModuleConfig = Field(default_factory=BrokerModuleConfig)
    """Broker module config."""
    result_backend: ResultBackendModuleConfig = Field(default_factory=ResultBackendModuleConfig)
    """Result backend module config."""
    event_backend: EventBackendModuleConfig = Field(default_factory=EventBackendModuleConfig)
    """Event backend module config."""
    task: TaskConfig = Field(default_factory=TaskConfig)
    """Task config."""
    event: EventConfig = Field(default_factory=EventConfig)
    """Event config."""
    task_queues: set[str] = Field(default_factory=lambda: TASK_QUEUES)
    """Task queues to consume."""
    plugins: list[PluginModuleConfig] = Field(default_factory=list)
    """List of plugins."""
    executor: ExecutorModuleConfig = Field(default_factory=ExecutorModuleConfig)
    """Executor module config."""
