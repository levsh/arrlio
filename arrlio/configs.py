from typing import Annotated, Optional
from uuid import uuid4

from annotated_types import MinLen
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from arrlio.settings import (
    BACKEND,
    ENV_PREFIX,
    EVENT_TTL,
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
from arrlio.types import BackendModule, ExecutorModule, ModuleConfig, PluginModule, TaskPriority, Timeout, Ttl


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


class BackendConfig(BaseSettings, ModuleConfigValidatorMixIn):
    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}BACKEND_")

    module: BackendModule = BACKEND
    config: ModuleConfig = Field(default_factory=BaseSettings)


class TaskConfig(BaseSettings):
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
    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EVENT_")

    ttl: Optional[Ttl] = Field(default_factory=lambda: EVENT_TTL)


class PluginConfig(ModuleConfigValidatorMixIn, BaseSettings):
    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}PLUGIN_")

    module: PluginModule
    config: ModuleConfig = Field(default_factory=BaseSettings)


class ExecutorConfig(ModuleConfigValidatorMixIn, BaseSettings):
    model_config = SettingsConfigDict(env_prefix=f"{ENV_PREFIX}EXECUTOR_")

    module: ExecutorModule = "arrlio.executor"
    config: ModuleConfig = Field(default_factory=BaseSettings)


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_prefix=ENV_PREFIX)

    app_id: Annotated[str, MinLen(1)] = Field(default_factory=lambda: f"{uuid4()}")
    backend: BackendConfig = Field(default_factory=BackendConfig)
    task: TaskConfig = Field(default_factory=TaskConfig)
    event: EventConfig = Field(default_factory=EventConfig)
    task_queues: set[str] = Field(default_factory=lambda: TASK_QUEUES)
    plugins: list[PluginConfig] = Field(default_factory=list)
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
